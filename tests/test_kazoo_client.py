"""Unit tests for ``sh_util.voice.kazoo_client``.

Settings/import quirk: ``kazoo_client.py`` reads ``settings.KAZOO_CLI``,
``settings.REDIS`` and ``settings.KAZOO_AUTH_TOKEN_CACHE_EXPIRY_SECONDS`` as
**class attributes** of ``KazooClient`` -- evaluated once, at import time
(``kazoo_cli = settings.KAZOO_CLI`` etc. sit directly in the class body). In
this admin repo, the top-level ``settings`` compatibility shim does
``from src.config import *``, and ``src.config`` unconditionally builds
``KAZOO_CLI = kazoo.Client(api_key=KAZOO_BASE_API_KEY, base_url=...)`` at
import time; with no ``KAZOO_BASE_API_KEY`` env var set, that constructor
raises, the wildcard import fails, and ``settings`` ends up with none of
``KAZOO_CLI``/``REDIS``/``KAZOO_AUTH_TOKEN_CACHE_EXPIRY_SECONDS`` defined --
hence ``AttributeError: module 'settings' has no attribute 'KAZOO_CLI'`` when
anything imports ``kazoo_client``. We pre-seed fakes on ``settings`` before
importing the module, below, so the class body evaluates cleanly. Because the
real values are bound once at class-definition time, individual tests can't
change them by patching ``settings`` afterwards -- every test instead
monkeypatches the ``KazooClient.kazoo_cli`` / ``KazooClient.redis_cli`` class
attributes directly (see ``mock_kazoo_cli`` / ``mock_redis_cli`` fixtures).
Because ``settings`` is a shared singleton module imported by every other
sh_util test file in the same process, the three seeded attributes are
``delattr``'d again immediately after the ``kazoo_client`` import completes
below -- they're only ever read once, during that one class-body evaluation,
so nothing after it needs them to remain on ``settings``. This leaves no
residue for tests collected afterward.

Driver boundary: ``kazoo_client.py`` imports ``wget`` (``sh_util.sh_http.wget``)
at module scope, so it is patched as an attribute of ``kazoo_client`` itself.
Everything else the module talks to -- ``kazoo.exceptions``,
``kazoo.client.KazooClient``, ``sh_util.tel.validate_phone_number``,
``pycurl`` -- is imported locally inside the method bodies that use it, at
*call* time; ``kazoo.exceptions`` is real (harmless, just exception classes)
and used as-is, while ``sh_util.tel.validate_phone_number`` is patched at its
owning module. No test hits the network, Redis, or a real Kazoo/curl
endpoint: ``KazooClient.kazoo_cli`` is always a ``MagicMock``.

Three pre-existing bugs in the vendored source are pinned below (not fixed,
per task constraints):

1. ``__init__`` and ``create_user`` both call ``traceback.print_exc(e)``
   inside an ``except Exception as e:`` handler. ``traceback.print_exc``'s
   first positional parameter is ``limit``, not an exception object, so
   passing ``e`` makes CPython try to use the exception instance as a frame
   *count* and raise a **secondary** ``TypeError`` ("'>=' not supported
   between instances of '<Exc>' and 'int'") from inside the except block --
   confirmed directly against this venv's Python (3.14). In ``__init__``
   this means *any* auth failure surfaces to the caller as an unrelated
   ``TypeError`` instead of being swallowed (the method's evident intent,
   given it then sets ``self.auth_token = None`` and continues). In
   ``create_user`` it's worse: the ``print_exc(e)`` call sits *before* the
   partially-created-resource cleanup (``self.delete_user(...)``) and the
   final ``raise``, so both are dead code -- any failure during user
   creation surfaces as ``TypeError`` (never the original exception), and
   Kazoo resources created before the failure point (user, phone number,
   devices...) are never cleaned up.
2. ``list_devices`` does ``from kazoo.client import KazooClient``. The
   ``kazoo-api`` package actually pinned in this repo's ``requirements.txt``
   (the ``Sendhub/kazoo-python-sdk`` fork) exposes ``kazoo.client.Client``,
   not ``KazooClient`` -- so this import always raises ``ImportError``, and
   ``list_devices`` can never succeed in this environment.
3. ``de_provision_phone_number_and_remove_from_call_flow`` filters the call
   flow's stored numbers with ``[nbr for nbr in ... if number != nbr]``,
   comparing against the raw ``number`` argument -- the "+1" stripping into
   ``short_number`` only happens afterwards, for the ``delete_phone_number``
   call. So if the call flow's stored numbers are in a different "+1..."
   form than what the caller passes here, the number is silently never
   removed from the call flow (while ``delete_phone_number`` still fires
   regardless). Both the matching-form (works) and mismatched-form
   (silently no-ops) cases are pinned in ``TestDeProvisionPhoneNumber``.

Separately (not one of the pinned bugs above, and now fixed): ``copy_media``
does ``import pycurl`` as its very first statement, before the surrounding
``try`` block even starts. In this venv, pycurl's installed wheel raises
``ImportError: pycurl: libcurl link-time ssl backends (secure-transport,
openssl) do not include compile-time ssl backend (none/other)`` -- a local
build/environment mismatch, unrelated to ``kazoo_client.py``'s own logic.
This used to mask a second, genuine bug one line into the ``try`` block --
``tempfile.NamedTemporaryFile(mode="wr+b")`` was an invalid mode string, so
``io.open`` rejected it with ``ValueError: must have exactly one of
create/read/write/append mode`` before any of the pycurl upload logic below
it ever ran -- which has since been corrected to ``mode="w+b"``. The
``fake_pycurl`` fixture below seeds a minimal stand-in module into
``sys.modules['pycurl']`` (only for the tests that request it,
monkeypatch-cleaned-up afterwards) purely to get past the environment-only
import failure, so ``copy_media``'s real upload logic can be exercised
end-to-end regardless of whether this machine's pycurl wheel works -- the
same category of test-only compatibility shim as ``test_db_distributed.py``'s
sqlparse adapter or ``test_db_data.py``'s ``sys.modules['sh_util.mail']``
seeding. ``add_media`` (which calls ``copy_media``) is tested separately by
mocking ``copy_media`` itself as a collaborator, per the mocking-strategy
note above, so its own logic doesn't depend on any of this.
"""

import sys
import types
from unittest import mock

import kazoo.exceptions as kazoo_exceptions
import pytest
import settings
import sh_util.retry as retry_module

# kazoo_client reads these as KazooClient class attributes at import time;
# provide fakes before importing it. Real values/shapes (see src/config.py):
# KAZOO_CLI is a `kazoo.Client` instance (auth_token/_authenticated attrs,
# .authenticate()/.create_account()/... methods, base_url attr); REDIS is a
# redis.Redis instance (.get()/.setex()); the TTL setting is int-castable.
#
# `settings` is a shared singleton module imported by every other sh_util
# test file in the same process, so this seed must not outlive the import
# below: only attributes that don't already exist are set (never clobbering
# a real value), and every attribute *this file* set is torn back down
# (`delattr`) immediately afterwards -- see the loop below the import.
_SEEDED_SETTINGS_ATTRS = []
for _attr, _val in {
    "KAZOO_CLI": mock.MagicMock(name="settings.KAZOO_CLI"),
    "REDIS": mock.MagicMock(name="settings.REDIS"),
    "KAZOO_AUTH_TOKEN_CACHE_EXPIRY_SECONDS": 3300,
}.items():
    if not hasattr(settings, _attr):
        setattr(settings, _attr, _val)
        _SEEDED_SETTINGS_ATTRS.append(_attr)

from sh_util.voice import kazoo_client  # noqa: E402

# Torn down right away: KazooClient's class body (kazoo_client.py:47-51) is
# the only place these are ever read, and it already ran during the import
# above -- nothing later needs them on `settings`. Per-test business-method
# coverage instead monkeypatches `KazooClient.kazoo_cli`/`.redis_cli`
# directly (see `mock_kazoo_cli`/`mock_redis_cli` fixtures below).
for _attr in _SEEDED_SETTINGS_ATTRS:
    delattr(settings, _attr)
del _attr, _val, _SEEDED_SETTINGS_ATTRS


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def no_sleeping(monkeypatch):
    """``create_enterprise_account``/``create_phone_number`` wrap Kazoo calls
    in ``@retry(3)`` (default delay=3, backoff=2); stub the retry module's
    ``time.sleep`` so exhausting retries in a test doesn't actually wait."""
    monkeypatch.setattr(retry_module._time, "sleep", lambda *_a, **_kw: None)


@pytest.fixture
def mock_kazoo_cli(monkeypatch):
    """Replace the ``KazooClient.kazoo_cli`` class attribute (bound once at
    import time from ``settings.KAZOO_CLI``) with a fresh MagicMock per test."""
    cli = mock.MagicMock(name="kazoo_cli")
    monkeypatch.setattr(kazoo_client.KazooClient, "kazoo_cli", cli)
    return cli


@pytest.fixture
def mock_redis_cli(monkeypatch):
    """Replace the ``KazooClient.redis_cli`` class attribute likewise."""
    redis_cli = mock.MagicMock(name="redis_cli")
    monkeypatch.setattr(kazoo_client.KazooClient, "redis_cli", redis_cli)
    return redis_cli


@pytest.fixture
def fake_pycurl(monkeypatch):
    """Seed a minimal stand-in ``pycurl`` module into ``sys.modules`` so
    ``copy_media``'s ``import pycurl`` (broken in this venv for unrelated
    environment/build reasons -- see module docstring) succeeds and execution
    reaches its real upload logic. Besides ``Curl``, the method references a
    handful of ``pycurl.*`` opcode constants directly (``URL``,
    ``READFUNCTION``, ``POST``, ``HTTPHEADER``, ``POSTFIELDSIZE``,
    ``HTTP_CODE``); a plain ``ModuleType`` raises ``AttributeError`` on
    unset attributes (unlike a ``MagicMock``), so each is seeded with a
    placeholder value -- their actual values don't matter since ``Curl`` and
    its ``setopt``/``getinfo`` are themselves mocked. ``monkeypatch.setitem``
    restores ``sys.modules`` exactly as it was (pycurl absent) after the
    test, so this can't leak into other test files/modules sharing the
    process."""
    fake_module = types.ModuleType("pycurl")
    fake_module.Curl = mock.MagicMock(name="pycurl.Curl")
    for _opt_name in ("URL", "READFUNCTION", "POST", "HTTPHEADER", "POSTFIELDSIZE", "HTTP_CODE"):
        setattr(fake_module, _opt_name, _opt_name)
    monkeypatch.setitem(sys.modules, "pycurl", fake_module)
    return fake_module


@pytest.fixture
def client(mock_kazoo_cli, mock_redis_cli):
    """A ``KazooClient`` built via the cached-token ``__init__`` path (no
    real "authenticate" call), ready for business-method tests."""
    mock_redis_cli.get.return_value = "cached-token"
    return kazoo_client.KazooClient()


# ---------------------------------------------------------------------------
# __init__
# ---------------------------------------------------------------------------


class TestInit:
    def test_uses_cached_token_when_present(self, mock_kazoo_cli, mock_redis_cli):
        mock_redis_cli.get.return_value = "cached-token"

        instance = kazoo_client.KazooClient()

        assert instance.auth_token == "cached-token"
        assert mock_kazoo_cli.auth_token == "cached-token"
        assert mock_kazoo_cli._authenticated is True
        mock_kazoo_cli.authenticate.assert_not_called()
        mock_redis_cli.setex.assert_not_called()

    def test_authenticates_and_caches_when_no_cached_token(self, mock_kazoo_cli, mock_redis_cli):
        mock_redis_cli.get.return_value = None
        mock_kazoo_cli.authenticate.return_value = "fresh-token"

        instance = kazoo_client.KazooClient()

        assert instance.auth_token == "fresh-token"
        mock_kazoo_cli.authenticate.assert_called_once_with()
        mock_redis_cli.setex.assert_called_once_with(name="kazooAuthToken", value="fresh-token", time=3300)

    def test_pinned_bug_auth_failure_raises_unrelated_type_error(self, mock_kazoo_cli, mock_redis_cli):
        """PRE-EXISTING BUG (not fixed, per task constraints) -- see module
        docstring point 1. A Redis/Kazoo failure during __init__ should, per
        the surrounding code's evident intent (catch, log, set auth_token to
        None), be swallowed. Instead ``traceback.print_exc(e)`` raises a
        secondary ``TypeError`` that escapes __init__ uncaught."""
        mock_redis_cli.get.side_effect = ConnectionError("redis down")

        with pytest.raises(TypeError):
            kazoo_client.KazooClient()


# ---------------------------------------------------------------------------
# create_enterprise_account
# ---------------------------------------------------------------------------


class TestCreateEnterpriseAccount:
    def test_none_arguments_raise(self, client):
        with pytest.raises(kazoo_exceptions.KazooApiError):
            client.create_enterprise_account(None, "Acme")
        with pytest.raises(kazoo_exceptions.KazooApiError):
            client.create_enterprise_account("ent1", None)

    def test_success_creates_account_and_no_match_callflow(self, client, mock_kazoo_cli):
        mock_kazoo_cli.create_account.return_value = {"data": {"id": "kzacct1"}}

        result = client.create_enterprise_account("ent1", "Acme Corp")

        assert result == {"data": {"id": "kzacct1"}}
        mock_kazoo_cli.create_account.assert_called_once_with({"name": "ent1", "enterprise_id": "ent1", "enterprise_name": "Acme Corp", "realm": "ent1.sip.sendhub.com"})
        mock_kazoo_cli.create_callflow.assert_called_once_with("kzacct1", kazoo_client.NO_MATCH_CALL_FLOW)

    def test_exhausted_retries_raise_generic_exception(self, client, mock_kazoo_cli):
        mock_kazoo_cli.create_account.return_value = {"status": "error"}

        with pytest.raises(Exception, match="Kazoo account creation error"):
            client.create_enterprise_account("ent1", "Acme Corp")

        # Initial attempt + 3 retries.
        assert mock_kazoo_cli.create_account.call_count == 4
        mock_kazoo_cli.create_callflow.assert_not_called()


# ---------------------------------------------------------------------------
# get_user
# ---------------------------------------------------------------------------


class TestGetUser:
    def test_none_arguments_raise(self, client):
        with pytest.raises(kazoo_exceptions.KazooApiError):
            client.get_user(None, "u1")
        with pytest.raises(kazoo_exceptions.KazooApiError):
            client.get_user("acct1", None)

    def test_success_returns_kazoo_result(self, client, mock_kazoo_cli):
        mock_kazoo_cli.get_user.return_value = {"status": "success", "data": {"id": "u1"}}

        result = client.get_user("acct1", "u1")

        assert result == {"status": "success", "data": {"id": "u1"}}
        mock_kazoo_cli.get_user.assert_called_once_with("acct1", "u1")


# ---------------------------------------------------------------------------
# list_devices
# ---------------------------------------------------------------------------


class TestListDevices:
    def test_pinned_bug_import_error(self, client):
        """PRE-EXISTING BUG (not fixed, per task constraints) -- see module
        docstring point 2. The pinned kazoo-api fork's ``kazoo.client``
        module exports ``Client``, not ``KazooClient``, so this method can
        never succeed in this environment."""
        with pytest.raises(ImportError):
            client.list_devices("acct1", "owner1")


# ---------------------------------------------------------------------------
# Phone/device template helpers
# ---------------------------------------------------------------------------


class TestPhoneTemplates:
    def test_soft_phone_template(self, client):
        template = client._soft_phone_template("owner1", "user1", "pw1")
        assert template == {
            "name": "user1",
            "sip": {"method": "password", "username": "user1", "password": "pw1"},
            "device_type": "softphone",
            "owner_id": "owner1",
        }

    def test_physical_phone_template_defaults_to_cellphone(self, client):
        template = client._physical_phone_template("owner1", "5551234567")
        assert template["device_type"] == "cellphone"
        assert template["name"] == "5551234567"
        assert template["owner_id"] == "owner1"
        assert template["call_forward"]["number"] == "5551234567"

    def test_physical_phone_template_accepts_type_override(self, client):
        template = client._physical_phone_template("owner1", "5551234567", type="landline")
        assert template["device_type"] == "landline"


# ---------------------------------------------------------------------------
# create_device
# ---------------------------------------------------------------------------


class TestCreateDevice:
    def test_invalid_type_asserts(self, client):
        with pytest.raises(AssertionError):
            client.create_device("desk_phone", "acct1", "u1", "owner1", "5551234567")

    def test_invalid_phone_number_returns_none_without_calling_kazoo(self, client, mock_kazoo_cli, monkeypatch):
        monkeypatch.setattr("sh_util.tel.validate_phone_number", lambda number: False)

        result = client.create_device("cellphone", "acct1", "u1", "owner1", "bad-number")

        assert result is None
        mock_kazoo_cli.create_device.assert_not_called()

    def test_softphone_success(self, client, mock_kazoo_cli, monkeypatch):
        monkeypatch.setattr("sh_util.tel.validate_phone_number", lambda number: True)
        mock_kazoo_cli.create_device.return_value = {"data": {"id": "dev1"}}

        result = client.create_device("softphone", "acct1", "u1", "owner1", "5551234567", username="sip1", password="pw1")

        assert result == {"data": {"id": "dev1"}}
        mock_kazoo_cli.create_device.assert_called_once_with("acct1", client._soft_phone_template("owner1", "sip1", "pw1"))

    def test_cellphone_success(self, client, mock_kazoo_cli, monkeypatch):
        monkeypatch.setattr("sh_util.tel.validate_phone_number", lambda number: True)
        mock_kazoo_cli.create_device.return_value = {"data": {"id": "dev2"}}

        result = client.create_device("cellphone", "acct1", "u1", "owner1", "5551234567")

        assert result == {"data": {"id": "dev2"}}
        mock_kazoo_cli.create_device.assert_called_once_with("acct1", client._physical_phone_template("owner1", "5551234567"))

    def test_duplicate_sip_username_is_swallowed(self, client, mock_kazoo_cli, monkeypatch):
        monkeypatch.setattr("sh_util.tel.validate_phone_number", lambda number: True)
        err = kazoo_exceptions.KazooApiBadDataError({"sip.username": ["unique"]})
        mock_kazoo_cli.create_device.side_effect = err

        result = client.create_device("softphone", "acct1", "u1", "owner1", "5551234567", username="sip1", password="pw1")

        assert result is None

    def test_unrelated_bad_data_error_is_reraised(self, client, mock_kazoo_cli, monkeypatch):
        monkeypatch.setattr("sh_util.tel.validate_phone_number", lambda number: True)
        err = kazoo_exceptions.KazooApiBadDataError({"name": ["required"]})
        mock_kazoo_cli.create_device.side_effect = err

        with pytest.raises(kazoo_exceptions.KazooApiBadDataError):
            client.create_device("softphone", "acct1", "u1", "owner1", "5551234567", username="sip1", password="pw1")


# ---------------------------------------------------------------------------
# create_phone_number
# ---------------------------------------------------------------------------


class TestCreatePhoneNumber:
    def test_success_strips_plus_one_prefix(self, client, mock_kazoo_cli):
        mock_kazoo_cli.create_phone_number.return_value = {"data": {"id": "num1"}}

        result = client.create_phone_number("acct1", "+15551234567")

        assert result == {"data": {"id": "num1"}}
        mock_kazoo_cli.create_phone_number.assert_called_once_with("acct1", "5551234567")

    def test_number_without_plus_one_is_used_verbatim(self, client, mock_kazoo_cli):
        mock_kazoo_cli.create_phone_number.return_value = {"data": {"id": "num2"}}

        client.create_phone_number("acct1", "5551234567")

        mock_kazoo_cli.create_phone_number.assert_called_once_with("acct1", "5551234567")

    def test_exception_is_logged_and_result_stays_empty_after_retries(self, client, mock_kazoo_cli):
        mock_kazoo_cli.create_phone_number.side_effect = Exception("kazoo down")

        result = client.create_phone_number("acct1", "5551234567")

        assert result == {}
        assert mock_kazoo_cli.create_phone_number.call_count == 4


# ---------------------------------------------------------------------------
# provision_phone_number_and_add_to_call_flow / de_provision...
# ---------------------------------------------------------------------------


class TestProvisionPhoneNumber:
    def test_invalid_call_flow_asserts(self, client, mock_kazoo_cli):
        mock_kazoo_cli.get_callflow.return_value = {"data": {}}

        with pytest.raises(AssertionError):
            client.provision_phone_number_and_add_to_call_flow("acct1", "cf1", "5551234567")

    def test_success_appends_number_and_updates_callflow(self, client, mock_kazoo_cli, monkeypatch):
        mock_kazoo_cli.get_callflow.return_value = {"data": {"numbers": ["existing"]}}
        monkeypatch.setattr(kazoo_client.KazooClient, "create_phone_number", lambda self, account_id, number: {"data": {"id": "num1"}})

        client.provision_phone_number_and_add_to_call_flow("acct1", "cf1", "5551234567")

        mock_kazoo_cli.update_callflow.assert_called_once_with("acct1", "cf1", {"numbers": ["existing", "5551234567"]})

    def test_failed_number_creation_does_not_update_callflow(self, client, mock_kazoo_cli, monkeypatch):
        mock_kazoo_cli.get_callflow.return_value = {"data": {"numbers": ["existing"]}}
        monkeypatch.setattr(kazoo_client.KazooClient, "create_phone_number", lambda self, account_id, number: {})

        client.provision_phone_number_and_add_to_call_flow("acct1", "cf1", "5551234567")

        mock_kazoo_cli.update_callflow.assert_not_called()


class TestDeProvisionPhoneNumber:
    def test_invalid_call_flow_asserts(self, client, mock_kazoo_cli):
        mock_kazoo_cli.get_callflow.return_value = {"data": {}}

        with pytest.raises(AssertionError):
            client.de_provision_phone_number_and_remove_from_call_flow("acct1", "cf1", "5551234567")

    def test_success_removes_number_and_deletes_it(self, client, mock_kazoo_cli):
        # The call-flow filter compares against the *unstripped* `number`
        # argument (the "+1" strip only happens afterwards, for the
        # delete_phone_number call) -- so the call-flow's stored numbers
        # must be in the same "+1..." form `provision_...` would have
        # stored them in for the removal to actually match.
        mock_kazoo_cli.get_callflow.return_value = {"data": {"numbers": ["+15551234567", "other"]}}

        client.de_provision_phone_number_and_remove_from_call_flow("acct1", "cf1", "+15551234567")

        mock_kazoo_cli.update_callflow.assert_called_once_with("acct1", "cf1", {"numbers": ["other"]})
        mock_kazoo_cli.delete_phone_number.assert_called_once_with("acct1", "5551234567")

    def test_number_not_matching_stored_form_is_not_removed(self, client, mock_kazoo_cli):
        """The call-flow filter uses the raw `number` argument, not the
        "+1"-stripped `short_number` -- so a stored short-form number is
        never actually removed when the caller passes the "+1..." form.
        Not fixed (pre-existing behavior, per task constraints); pinned here
        alongside the matching-form case above so both branches are on
        record."""
        mock_kazoo_cli.get_callflow.return_value = {"data": {"numbers": ["5551234567", "other"]}}

        client.de_provision_phone_number_and_remove_from_call_flow("acct1", "cf1", "+15551234567")

        mock_kazoo_cli.update_callflow.assert_called_once_with("acct1", "cf1", {"numbers": ["5551234567", "other"]})


# ---------------------------------------------------------------------------
# update_vm_box
# ---------------------------------------------------------------------------


class TestUpdateVmBox:
    def test_none_arguments_raise(self, client):
        with pytest.raises(kazoo_exceptions.KazooApiError):
            client.update_vm_box(None, "vm1", {})
        with pytest.raises(kazoo_exceptions.KazooApiError):
            client.update_vm_box("acct1", None, {})
        with pytest.raises(kazoo_exceptions.KazooApiError):
            client.update_vm_box("acct1", "vm1", None)

    def test_non_success_get_raises(self, client, mock_kazoo_cli):
        mock_kazoo_cli.get_voicemail_box.return_value = {"status": "error"}

        with pytest.raises(kazoo_exceptions.KazooApiError):
            client.update_vm_box("acct1", "vm1", {"name": "new"})

    def test_success_merges_and_updates(self, client, mock_kazoo_cli):
        mock_kazoo_cli.get_voicemail_box.return_value = {"status": "success", "data": {"name": "old", "keep": 1}}
        mock_kazoo_cli.update_voicemail_box.return_value = {"status": "success"}

        result = client.update_vm_box("acct1", "vm1", {"name": "new"})

        mock_kazoo_cli.update_voicemail_box.assert_called_once_with("acct1", "vm1", {"name": "new", "keep": 1})
        assert result == {"status": "success"}


# ---------------------------------------------------------------------------
# update_menu / update_temporal_rules / update_call_flow
# ---------------------------------------------------------------------------


class TestUpdateMenu:
    def test_calls_kazoo_with_expected_payload(self, client, mock_kazoo_cli):
        client.update_menu("acct1", "menu1", "u1", "media1")

        mock_kazoo_cli.update_menu.assert_called_once_with(
            "acct1",
            "menu1",
            {"name": "u1", "retries": 3, "timeout": "10000", "max_extension_length": "1", "media": {"exit_media": True, "greeting": "media1", "invalid_media": True, "transfer_media": True}},
        )


class TestUpdateTemporalRules:
    def test_calls_kazoo_with_expected_payload(self, client, mock_kazoo_cli):
        client.update_temporal_rules("acct1", "rule1", "u1", 0, 86400, ["monday"])

        args, _ = mock_kazoo_cli.update_temporal_rule.call_args
        assert args[0] == "acct1"
        assert args[1] == "rule1"
        assert args[2]["time_window_start"] == 0
        assert args[2]["time_window_stop"] == 86400
        assert args[2]["wdays"] == ["monday"]
        assert args[2]["name"] == "u1"


class TestUpdateCallFlow:
    def test_calls_kazoo_with_given_data(self, client, mock_kazoo_cli):
        client.update_call_flow("acct1", "cf1", {"numbers": ["x"]})

        mock_kazoo_cli.update_callflow.assert_called_once_with("acct1", "cf1", {"numbers": ["x"]})


# ---------------------------------------------------------------------------
# copy_media
# ---------------------------------------------------------------------------


class TestCopyMedia:
    def test_success_uploads_media_via_shimmed_pycurl(self, client, mock_kazoo_cli, monkeypatch, fake_pycurl):
        """``import pycurl`` fails in this venv for unrelated environment/build
        reasons (see module docstring); ``fake_pycurl`` stands in for it so
        the method's real upload logic -- writing ``wget``'s bytes to a temp
        file and posting them via curl -- can be exercised end-to-end."""
        wget_mock = mock.Mock(return_value=b"media-bytes")
        monkeypatch.setattr(kazoo_client, "wget", wget_mock)
        mock_kazoo_cli.base_url = "http://kazoo.test"
        mock_kazoo_cli.auth_token = "tok"
        curl_instance = fake_pycurl.Curl.return_value
        curl_instance.getinfo.return_value = 200

        client.copy_media("acct1", "media1", "http://source.test/file.mp3")

        wget_mock.assert_called_once_with("http://source.test/file.mp3", num_tries=3)
        curl_instance.perform.assert_called_once_with()
        curl_instance.close.assert_called_once_with()

    def test_raises_when_upload_returns_non_200(self, client, mock_kazoo_cli, monkeypatch, fake_pycurl):
        """Covers the ``if return_code != 200: raise`` branch, previously
        unreachable because of the environment-only import failure and the
        (now-fixed) invalid ``tempfile`` mode string that both used to raise
        before this point in the method."""
        wget_mock = mock.Mock(return_value=b"media-bytes")
        monkeypatch.setattr(kazoo_client, "wget", wget_mock)
        mock_kazoo_cli.base_url = "http://kazoo.test"
        mock_kazoo_cli.auth_token = "tok"
        fake_pycurl.Curl.return_value.getinfo.return_value = 500

        with pytest.raises(kazoo_exceptions.KazooApiError, match="500"):
            client.copy_media("acct1", "media1", "http://source.test/file.mp3")


# ---------------------------------------------------------------------------
# add_media / delete_media / add_tts_media
# ---------------------------------------------------------------------------


class TestAddMedia:
    def test_success(self, client, mock_kazoo_cli, monkeypatch):
        mock_kazoo_cli.create_media.return_value = {"data": {"id": "media1"}}
        copy_media_mock = mock.Mock(return_value=None)
        monkeypatch.setattr(kazoo_client.KazooClient, "copy_media", copy_media_mock)

        result = client.add_media("acct1", "http://source.test/dir/file.mp3", "Greeting")

        assert result == {"data": {"id": "media1"}}
        mock_kazoo_cli.create_media.assert_called_once_with("acct1", {"streamable": True, "name": "Greeting", "description": "C:\\fakepath\\file.mp3"})
        # copy_media_mock is a plain Mock (not a function), so it isn't
        # bound as a method when accessed via the instance -- `self` is not
        # passed as an implicit first argument.
        copy_media_mock.assert_called_once_with("acct1", "media1", "http://source.test/dir/file.mp3")

    def test_exception_is_logged_and_reraised(self, client, mock_kazoo_cli, monkeypatch):
        mock_kazoo_cli.create_media.return_value = {"data": {"id": "media1"}}
        monkeypatch.setattr(kazoo_client.KazooClient, "copy_media", mock.Mock(side_effect=RuntimeError("upload failed")))

        with pytest.raises(RuntimeError, match="upload failed"):
            client.add_media("acct1", "http://source.test/file.mp3", "Greeting")


class TestDeleteMedia:
    def test_success(self, client, mock_kazoo_cli):
        mock_kazoo_cli.delete_media.return_value = {"status": "success"}

        result = client.delete_media("acct1", "media1")

        assert result == {"status": "success"}
        mock_kazoo_cli.delete_media.assert_called_once_with("acct1", "media1")

    def test_exception_is_logged_and_reraised(self, client, mock_kazoo_cli):
        mock_kazoo_cli.delete_media.side_effect = RuntimeError("boom")

        with pytest.raises(RuntimeError, match="boom"):
            client.delete_media("acct1", "media1")


class TestAddTtsMedia:
    def test_success(self, client, mock_kazoo_cli):
        mock_kazoo_cli.create_media.return_value = {"data": {"id": "tts1"}}

        result = client.add_tts_media("acct1", "Hello there", "Greeting")

        assert result == {"data": {"id": "tts1"}}
        mock_kazoo_cli.create_media.assert_called_once_with("acct1", {"streamable": True, "name": "Greeting", "media_source": "tts", "tts": {"text": "Hello there", "voice": "female/en-US"}})

    def test_exception_is_logged_and_reraised(self, client, mock_kazoo_cli):
        mock_kazoo_cli.create_media.side_effect = RuntimeError("boom")

        with pytest.raises(RuntimeError, match="boom"):
            client.add_tts_media("acct1", "Hello there", "Greeting")


# ---------------------------------------------------------------------------
# add_device_to_group
# ---------------------------------------------------------------------------


class TestAddDeviceToGroup:
    def test_missing_data_or_endpoints_is_a_noop(self, client, mock_kazoo_cli):
        mock_kazoo_cli.get_group.return_value = {}

        client.add_device_to_group("acct1", "group1", "dev1", "u1")

        mock_kazoo_cli.update_group.assert_not_called()

    def test_existing_device_is_a_noop(self, client, mock_kazoo_cli):
        mock_kazoo_cli.get_group.return_value = {"data": {"endpoints": {"dev1": {"type": "device"}}}}

        client.add_device_to_group("acct1", "group1", "dev1", "u1")

        mock_kazoo_cli.update_group.assert_not_called()

    def test_new_device_is_added(self, client, mock_kazoo_cli):
        endpoints = {}
        mock_kazoo_cli.get_group.return_value = {"data": {"endpoints": endpoints}}

        client.add_device_to_group("acct1", "group1", "dev1", "u1")

        assert endpoints == {"dev1": {"type": "device"}}
        mock_kazoo_cli.update_group.assert_called_once_with(
            "acct1", "group1", {"music_on_hold": {}, "name": "u1", "check_if_owner": True, "require_pin": False, "delete_after_notify": True}
        )


# ---------------------------------------------------------------------------
# create_user
# ---------------------------------------------------------------------------


class TestCreateUser:
    def _base_kwargs(self):
        return dict(account_id="acct1", name="Jane", user_id="u1", password="pw1", enterprise_id="ent1", sip_username="sip1", sip_password="sippw1")

    def test_none_arguments_raise(self, client):
        with pytest.raises(kazoo_exceptions.KazooApiError):
            client.create_user(**{**self._base_kwargs(), "name": None})
        with pytest.raises(kazoo_exceptions.KazooApiError):
            client.create_user(**{**self._base_kwargs(), "user_id": None})
        with pytest.raises(kazoo_exceptions.KazooApiError):
            client.create_user(**{**self._base_kwargs(), "password": None})

    def test_success_with_soft_phone_and_cell_numbers(self, client, mock_kazoo_cli, monkeypatch):
        mock_kazoo_cli.create_user.return_value = {"status": "success", "data": {"id": "kzu1", "first_name": "Jane", "username": "u1", "enterprise_id": "ent1"}}
        mock_kazoo_cli.create_voicemail_box.return_value = {"data": {"id": "vm1"}}
        mock_kazoo_cli.create_callflow.return_value = {"data": {"id": "cf1"}}
        mock_kazoo_cli.create_menu.return_value = {"data": {"id": "menu1"}}
        mock_kazoo_cli.create_temporal_rule.return_value = {"data": {"id": "tr1"}}

        monkeypatch.setattr(kazoo_client.KazooClient, "create_phone_number", lambda self, account_id, number: {"data": {"id": "num1"}})

        def fake_create_device(self, type, account_id, user_id, owner_id, number, username="", password=""):
            return {"data": {"id": f"dev-{type}-{number}", "call_forward": {"number": number}}}

        monkeypatch.setattr(kazoo_client.KazooClient, "create_device", fake_create_device)

        result = client.create_user(
            **self._base_kwargs(),
            soft_phone_number="+15551110000",
            cell_phone_numbers=["+15552220000", None],
        )

        assert result["id"] == "kzu1"
        assert result["username"] == "u1"
        assert result["enterpriseId"] == "ent1"
        assert result["softphoneId"] == "dev-softphone-5551110000"
        assert result["cellphoneIds"] == [{"id": "dev-cellphone-5552220000", "number": "+15552220000"}]
        assert result["voicemailId"] == "vm1"
        assert result["callFlowId"] == "cf1"
        assert result["autoAttendantMenuId"] == "menu1"
        assert result["temporalRuleId"] == "tr1"

        create_user_call = mock_kazoo_cli.create_user.call_args
        assert create_user_call.args[0] == "acct1"
        assert create_user_call.args[1]["email"] == "None@no-reply.sendhub.com"
        assert create_user_call.args[1]["caller_id"]["internal"]["number"] == "5551110000"

    def test_success_without_soft_phone_or_cell_numbers_uses_given_email(self, client, mock_kazoo_cli, monkeypatch):
        mock_kazoo_cli.create_user.return_value = {"status": "success", "data": {"id": "kzu2", "first_name": "Jane", "username": "u1", "enterprise_id": "ent1"}}
        mock_kazoo_cli.create_voicemail_box.return_value = {"data": {"id": "vm2"}}
        mock_kazoo_cli.create_callflow.return_value = {"data": {"id": "cf2"}}
        mock_kazoo_cli.create_menu.return_value = {"data": {"id": "menu2"}}
        mock_kazoo_cli.create_temporal_rule.return_value = {"data": {"id": "tr2"}}

        result = client.create_user(**self._base_kwargs(), email="jane@example.com")

        assert result["softphoneId"] is None
        assert result["cellphoneIds"] == []
        create_user_call = mock_kazoo_cli.create_user.call_args
        assert create_user_call.args[1]["email"] == "jane@example.com"
        assert "caller_id" not in create_user_call.args[1]

    def test_pinned_bug_failure_raises_type_error_and_skips_cleanup(self, client, mock_kazoo_cli, monkeypatch):
        """PRE-EXISTING BUG (not fixed, per task constraints) -- see module
        docstring point 1. Any failure during user creation should trigger
        the cleanup branch (``self.delete_user(...)``) and re-raise the
        original exception. Instead ``traceback.print_exc(e)`` raises a
        secondary ``TypeError`` first, so cleanup never runs and the
        original exception is lost."""
        mock_kazoo_cli.create_user.side_effect = RuntimeError("kazoo create_user failed")
        delete_user_mock = mock.Mock()
        monkeypatch.setattr(kazoo_client.KazooClient, "delete_user", delete_user_mock)

        with pytest.raises(TypeError):
            client.create_user(**self._base_kwargs())

        delete_user_mock.assert_not_called()

    def test_pinned_bug_soft_phone_number_failure_also_raises_type_error(self, client, mock_kazoo_cli, monkeypatch):
        """Same pinned bug as above, triggered via a different, later
        failure point inside the try block: the soft-phone-number branch's
        own domain-specific ``KazooApiError`` (raised when
        ``create_phone_number`` doesn't come back with an id) is *also*
        swallowed and replaced by the ``traceback.print_exc(e)`` TypeError,
        never surfacing to the caller."""
        mock_kazoo_cli.create_user.return_value = {"status": "success", "data": {"id": "kzu1", "first_name": "Jane", "username": "u1", "enterprise_id": "ent1"}}
        monkeypatch.setattr(kazoo_client.KazooClient, "create_phone_number", lambda self, account_id, number: {})
        delete_user_mock = mock.Mock()
        monkeypatch.setattr(kazoo_client.KazooClient, "delete_user", delete_user_mock)

        with pytest.raises(TypeError):
            client.create_user(**self._base_kwargs(), soft_phone_number="+15551110000")

        delete_user_mock.assert_not_called()


# ---------------------------------------------------------------------------
# update_user
# ---------------------------------------------------------------------------


class TestUpdateUser:
    def test_none_arguments_raise(self, client):
        with pytest.raises(kazoo_exceptions.KazooApiError):
            client.update_user(None, "u1", {})
        with pytest.raises(kazoo_exceptions.KazooApiError):
            client.update_user("acct1", None, {})
        with pytest.raises(kazoo_exceptions.KazooApiError):
            client.update_user("acct1", "u1", None)

    def test_non_success_get_raises(self, client, mock_kazoo_cli):
        mock_kazoo_cli.get_user.return_value = {"status": "error"}

        with pytest.raises(kazoo_exceptions.KazooApiError):
            client.update_user("acct1", "u1", {"name": "new"})

    def test_success_merges_and_updates(self, client, mock_kazoo_cli):
        mock_kazoo_cli.get_user.return_value = {"status": "success", "data": {"name": "old", "keep": 1}}
        mock_kazoo_cli.update_user.return_value = {"status": "success"}

        result = client.update_user("acct1", "u1", {"name": "new"})

        mock_kazoo_cli.update_user.assert_called_once_with("acct1", "u1", {"name": "new", "keep": 1})
        assert result == {"status": "success"}


# ---------------------------------------------------------------------------
# delete_account
# ---------------------------------------------------------------------------


class TestDeleteAccount:
    def test_success(self, client, mock_kazoo_cli):
        client.delete_account("acct1")

        mock_kazoo_cli.delete_account.assert_called_once_with("acct1")

    def test_exception_is_swallowed(self, client, mock_kazoo_cli):
        mock_kazoo_cli.delete_account.side_effect = RuntimeError("boom")

        client.delete_account("acct1")  # must not raise


# ---------------------------------------------------------------------------
# delete_user
# ---------------------------------------------------------------------------


class TestDeleteUser:
    def test_minimal_call_only_deletes_user(self, client, mock_kazoo_cli):
        client.delete_user("acct1", "u1")

        mock_kazoo_cli.delete_user.assert_called_once_with("acct1", "u1")
        mock_kazoo_cli.delete_menu.assert_not_called()
        mock_kazoo_cli.delete_temporal_rule.assert_not_called()
        mock_kazoo_cli.delete_callflow.assert_not_called()
        mock_kazoo_cli.delete_voicemail_box.assert_not_called()
        mock_kazoo_cli.delete_device.assert_not_called()
        mock_kazoo_cli.delete_phone_number.assert_not_called()

    def test_full_call_deletes_every_resource_and_strips_plus_one(self, client, mock_kazoo_cli):
        client.delete_user(
            "acct1",
            "u1",
            phone_number="+15551234567",
            device_ids=["dev1", "dev2"],
            voicemail_id="vm1",
            call_flow_id="cf1",
            menu_id="menu1",
            temporal_rule_id="tr1",
        )

        mock_kazoo_cli.delete_menu.assert_called_once_with("acct1", "menu1")
        mock_kazoo_cli.delete_temporal_rule.assert_called_once_with("acct1", "tr1")
        mock_kazoo_cli.delete_callflow.assert_called_once_with("acct1", "cf1")
        mock_kazoo_cli.delete_voicemail_box.assert_called_once_with("acct1", "vm1")
        assert mock_kazoo_cli.delete_device.call_args_list == [mock.call("acct1", "dev1"), mock.call("acct1", "dev2")]
        mock_kazoo_cli.delete_phone_number.assert_called_once_with("acct1", "5551234567")
        mock_kazoo_cli.delete_user.assert_called_once_with("acct1", "u1")

    def test_every_deletion_failure_is_individually_swallowed(self, client, mock_kazoo_cli):
        mock_kazoo_cli.delete_menu.side_effect = RuntimeError("boom")
        mock_kazoo_cli.delete_temporal_rule.side_effect = RuntimeError("boom")
        mock_kazoo_cli.delete_callflow.side_effect = RuntimeError("boom")
        mock_kazoo_cli.delete_voicemail_box.side_effect = RuntimeError("boom")
        mock_kazoo_cli.delete_device.side_effect = RuntimeError("boom")
        mock_kazoo_cli.delete_phone_number.side_effect = RuntimeError("boom")
        mock_kazoo_cli.delete_user.side_effect = RuntimeError("boom")

        # Must not raise despite every collaborator call failing.
        client.delete_user(
            "acct1",
            "u1",
            phone_number="5551234567",
            device_ids=["dev1"],
            voicemail_id="vm1",
            call_flow_id="cf1",
            menu_id="menu1",
            temporal_rule_id="tr1",
        )

        assert mock_kazoo_cli.delete_user.call_count == 1
