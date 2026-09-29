"""Unit tests for ``sh_util.mail`` (package ``__init__``): ``send_email`` and
``send_error_email``.

Django is not installed in this environment. ``sh_util/mail/__init__.py``
imports ``django.conf.settings``, ``django.core.mail.send_mail`` and
``django.views.debug.get_exception_reporter_filter`` at *module scope*, so
the fakes below are seeded into ``sys.modules`` before the package is first
imported -- module-scoped fixtures run too late for that (pytest fixtures
only fire once a test starts, well after collection-time imports). The
install is guarded so either this file or its sibling ``test_mail_smtp.py``
(which imports ``sh_util.mail.smtp`` and therefore also triggers the parent
package's import) can run first.

``send_error_email`` sets ``settings.DEBUG = False`` immediately before
checking ``not hasattr(settings, "DEBUG") or (hasattr(...) and not
settings.DEBUG)`` -- that assignment guarantees ``hasattr`` is true and
``settings.DEBUG`` is falsy, so the branch is always taken and
``mail_admins`` always fires, regardless of the caller's original DEBUG
value. That's a pinned quirk, not behavior under test-writer control.
"""

import sys
import types
from unittest import mock

import pytest


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

import sh_util.mail as mail_module  # noqa: E402


@pytest.fixture(autouse=True)
def _reset_fakes(monkeypatch):
    """Give every test a clean settings object and fresh call-tracking mocks."""
    monkeypatch.setattr(mail_module, "settings", types.SimpleNamespace(), raising=False)
    monkeypatch.setattr(mail_module, "send_mail", mock.Mock(name="send_mail"))
    monkeypatch.setattr(mail_module, "get_exception_reporter_filter", mock.Mock(name="get_exception_reporter_filter"))
    monkeypatch.setattr(sys.modules["django.core.mail"], "mail_admins", mock.Mock(name="mail_admins"))


class TestSendEmail:
    def test_string_to_address_is_wrapped_in_a_tuple(self):
        mail_module.settings.REALLY_SEND_EMAIL = True

        mail_module.send_email("Subj", "Body", "from@x.com", "to@x.com")

        mail_module.send_mail.assert_called_once_with("Subj", "Body", "from@x.com", ("to@x.com",), fail_silently=False)

    def test_iterable_to_address_is_passed_through_unchanged(self):
        mail_module.settings.REALLY_SEND_EMAIL = True

        mail_module.send_email("Subj", "Body", "from@x.com", ["a@x.com", "b@x.com"])

        mail_module.send_mail.assert_called_once_with("Subj", "Body", "from@x.com", ["a@x.com", "b@x.com"], fail_silently=False)

    def test_really_send_email_false_skips_send_mail(self):
        mail_module.settings.REALLY_SEND_EMAIL = False

        mail_module.send_email("Subj", "Body", "from@x.com", "to@x.com")

        mail_module.send_mail.assert_not_called()

    def test_really_send_email_not_exactly_true_skips_send_mail(self):
        # Only an exact `is True` triggers sending -- a truthy-but-not-True
        # value takes the "didn't really send" path instead.
        mail_module.settings.REALLY_SEND_EMAIL = "yes"

        mail_module.send_email("Subj", "Body", "from@x.com", "to@x.com")

        mail_module.send_mail.assert_not_called()


class TestSendErrorEmail:
    def test_always_calls_mail_admins_when_debug_true(self):
        mail_module.settings.DEBUG = True

        mail_module.send_error_email(ValueError("boom"))

        sys.modules["django.core.mail"].mail_admins.assert_called_once()

    def test_no_debug_attribute_also_calls_mail_admins(self):
        mail_module.send_error_email(ValueError("boom"))

        sys.modules["django.core.mail"].mail_admins.assert_called_once()

    def test_debug_true_exercises_the_stacktrace_branch_without_raising(self):
        mail_module.settings.DEBUG = True

        try:
            raise ValueError("boom")
        except ValueError as exc:
            mail_module.send_error_email(exc)

        sys.modules["django.core.mail"].mail_admins.assert_called_once()

    def test_request_repr_used_and_internal_ip_labeled_in_subject(self):
        request = mock.Mock()
        request.META = {"REMOTE_ADDR": "1.2.3.4"}
        request.path = "/some/path"
        mail_module.settings.INTERNAL_IPS = ["1.2.3.4"]
        filtr = mock.Mock()
        filtr.get_request_repr.return_value = "<request repr>"
        mail_module.get_exception_reporter_filter.return_value = filtr

        mail_module.send_error_email(ValueError("boom"), request=request)

        mail_module.get_exception_reporter_filter.assert_called_once_with(request)
        filtr.get_request_repr.assert_called_once_with(request)
        subject, message = sys.modules["django.core.mail"].mail_admins.call_args[0]
        assert "internal" in subject
        assert "<request repr>" in message

    def test_non_internal_ip_labeled_external_in_subject(self):
        request = mock.Mock()
        request.META = {"REMOTE_ADDR": "9.9.9.9"}
        request.path = "/some/path"
        mail_module.settings.INTERNAL_IPS = ["1.2.3.4"]
        filtr = mock.Mock()
        filtr.get_request_repr.return_value = "<request repr>"
        mail_module.get_exception_reporter_filter.return_value = filtr

        mail_module.send_error_email(ValueError("boom"), request=request)

        subject = sys.modules["django.core.mail"].mail_admins.call_args[0][0]
        assert "EXTERNAL" in subject

    def test_exception_reporter_filter_failure_falls_back_gracefully(self):
        request = mock.Mock()
        request.META = {"REMOTE_ADDR": "1.2.3.4"}
        request.path = "/some/path"
        mail_module.settings.INTERNAL_IPS = []
        mail_module.get_exception_reporter_filter.side_effect = Exception("filter broke")

        mail_module.send_error_email(ValueError("boom"), request=request)

        sys.modules["django.core.mail"].mail_admins.assert_called_once()

    def test_no_request_uses_default_subject_and_repr(self):
        mail_module.send_error_email(ValueError("boom"))

        subject, message = sys.modules["django.core.mail"].mail_admins.call_args[0]
        assert subject == "SendHub Exception Report"
        assert "Request repr() unavailable." in message
