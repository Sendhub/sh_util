"""Unit tests for ``sh_util.mail.smtp``.

Django is not installed in this environment. ``send_html_email``'s Django
imports are local to the function body, so each test pre-seeds
``sys.modules`` with fake stand-ins (via ``monkeypatch.setitem``, auto-
restored), mirroring the pattern used for the db drivers in
``test_db_drivers_dj.py``.

Importing ``sh_util.mail.smtp`` first imports the parent package
(``sh_util/mail/__init__.py``), which does its own *module-level* Django
imports -- so a minimal, permanent set of fakes is installed below (guarded,
so it's a no-op if ``test_mail_init.py`` already installed them) before that
import is attempted.
"""

import sys
import types
from unittest import mock


def _ensure_fake_django_installed():
    if "django" in sys.modules:
        return
    django_module = types.ModuleType("django")
    django_conf_module = types.ModuleType("django.conf")
    django_conf_module.settings = types.SimpleNamespace()
    django_core_module = types.ModuleType("django.core")
    django_core_mail_module = types.ModuleType("django.core.mail")
    django_core_mail_module.send_mail = mock.Mock(name="send_mail")
    django_core_mail_module.mail_admins = mock.Mock(name="mail_admins")
    django_views_module = types.ModuleType("django.views")
    django_views_debug_module = types.ModuleType("django.views.debug")
    django_views_debug_module.get_exception_reporter_filter = mock.Mock(name="get_exception_reporter_filter")

    for name, mod in {
        "django": django_module,
        "django.conf": django_conf_module,
        "django.core": django_core_module,
        "django.core.mail": django_core_mail_module,
        "django.views": django_views_module,
        "django.views.debug": django_views_debug_module,
    }.items():
        sys.modules[name] = mod


_ensure_fake_django_installed()

from sh_util.mail import smtp as smtp_module  # noqa: E402


def _install_fake_html_deps(monkeypatch, *, render_to_string, strip_tags):
    """Seed the Django symbols ``send_html_email`` imports locally at call
    time: ``EmailMultiAlternatives``, ``Context``, ``render_to_string`` and
    ``strip_tags``. Returns the fakes so tests can assert on them."""
    email_cls = mock.MagicMock(name="EmailMultiAlternatives")
    context_cls = mock.MagicMock(name="Context")

    django_core_mail_module = types.ModuleType("django.core.mail")
    django_core_mail_module.EmailMultiAlternatives = email_cls

    django_template_module = types.ModuleType("django.template")
    django_template_module.Context = context_cls
    django_template_loader_module = types.ModuleType("django.template.loader")
    django_template_loader_module.render_to_string = render_to_string

    django_utils_module = types.ModuleType("django.utils")
    django_utils_html_module = types.ModuleType("django.utils.html")
    django_utils_html_module.strip_tags = strip_tags

    for name, mod in {
        "django.core.mail": django_core_mail_module,
        "django.template": django_template_module,
        "django.template.loader": django_template_loader_module,
        "django.utils": django_utils_module,
        "django.utils.html": django_utils_html_module,
    }.items():
        monkeypatch.setitem(sys.modules, name, mod)

    return email_cls, context_cls


class TestSendHtmlEmail:
    def test_recipient_list_is_passed_through_unchanged(self, monkeypatch):
        render_to_string = mock.Mock(return_value="<p>hi</p>")
        strip_tags = mock.Mock(return_value="hi")
        email_cls, _ = _install_fake_html_deps(monkeypatch, render_to_string=render_to_string, strip_tags=strip_tags)

        smtp_module.send_html_email("from@x.com", ["a@x.com", "b@x.com"], "Subj", "tmpl.html", {"k": "v"})

        email_cls.assert_called_once_with("Subj", "hi", "from@x.com", to=["a@x.com", "b@x.com"])

    def test_single_recipient_string_is_wrapped_in_a_list(self, monkeypatch):
        render_to_string = mock.Mock(return_value="<p>hi</p>")
        strip_tags = mock.Mock(return_value="hi")
        email_cls, _ = _install_fake_html_deps(monkeypatch, render_to_string=render_to_string, strip_tags=strip_tags)

        smtp_module.send_html_email("from@x.com", "a@x.com", "Subj", "tmpl.html", {"k": "v"})

        email_cls.assert_called_once_with("Subj", "hi", "from@x.com", to=["a@x.com"])

    def test_tuple_recipient_is_still_wrapped_in_a_single_item_list(self, monkeypatch):
        # isinstance() checks specifically for ``list`` -- a tuple (or any
        # other non-list iterable) falls through to the "single recipient"
        # branch and gets wrapped whole, i.e. one "recipient" that is itself
        # the tuple. Pinned quirk, not something to fix here.
        render_to_string = mock.Mock(return_value="<p>hi</p>")
        strip_tags = mock.Mock(return_value="hi")
        email_cls, _ = _install_fake_html_deps(monkeypatch, render_to_string=render_to_string, strip_tags=strip_tags)

        smtp_module.send_html_email("from@x.com", ("a@x.com", "b@x.com"), "Subj", "tmpl.html", {})

        email_cls.assert_called_once_with("Subj", "hi", "from@x.com", to=[("a@x.com", "b@x.com")])

    def test_render_to_string_called_with_template_and_vars_and_a_blank_context(self, monkeypatch):
        render_to_string = mock.Mock(return_value="<p>hi</p>")
        strip_tags = mock.Mock(return_value="hi")
        _, context_cls = _install_fake_html_deps(monkeypatch, render_to_string=render_to_string, strip_tags=strip_tags)

        smtp_module.send_html_email("from@x.com", "a@x.com", "Subj", "tmpl.html", {"k": "v"})

        context_cls.assert_called_once_with({})
        args, _ = render_to_string.call_args
        assert args[0] == "tmpl.html"
        assert args[1] == {"k": "v"}
        assert args[2] is context_cls.return_value

    def test_strip_tags_called_with_the_rendered_html(self, monkeypatch):
        render_to_string = mock.Mock(return_value="<p>hi there</p>")
        strip_tags = mock.Mock(return_value="hi there")
        _install_fake_html_deps(monkeypatch, render_to_string=render_to_string, strip_tags=strip_tags)

        smtp_module.send_html_email("from@x.com", "a@x.com", "Subj", "tmpl.html", {})

        strip_tags.assert_called_once_with("<p>hi there</p>")

    def test_html_alternative_is_attached_and_message_is_sent(self, monkeypatch):
        render_to_string = mock.Mock(return_value="<p>hi</p>")
        strip_tags = mock.Mock(return_value="hi")
        email_cls, _ = _install_fake_html_deps(monkeypatch, render_to_string=render_to_string, strip_tags=strip_tags)

        smtp_module.send_html_email("from@x.com", "a@x.com", "Subj", "tmpl.html", {})

        msg = email_cls.return_value
        msg.attach_alternative.assert_called_once_with("<p>hi</p>", "text/html")
        msg.send.assert_called_once_with()
