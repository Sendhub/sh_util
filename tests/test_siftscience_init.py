"""Unit tests for ``sh_util.siftscience`` (package ``__init__``).

``label_user`` posts to the Sift Science API through ``wget`` (imported into
this module's namespace, patched directly here rather than hitting the
network) and retries failures via ``sh_util.retry.retry``; ``time.sleep`` is
stubbed the same way ``test_retry.py`` does so a run that exhausts its
retries doesn't actually wait through the backoff.

Every "reason" in this module is a full ``(label, reason_string)`` tuple
from ``SIFTSCIENCE_CHOICES`` -- not just the reason string -- since
``is_bad_reason``/``map_reason_to_sift_science_label`` both do
``SIFTSCIENCE_CHOICES.index(reason)``, which needs an exact tuple match.

One quirk is pinned because it's surprising:

- ``retry(3, ...)`` means the initial call *plus* 3 retries -- 4 total
  attempts on persistent failure, not 3 (matches the "success check happens
  inside the loop" quirk pinned in ``test_retry.py``).
"""

from unittest import mock

import pytest

import sh_util.siftscience as siftscience_module
from sh_util import retry as retry_module
from sh_util.siftscience import is_bad_reason, label_user, map_reason_to_sift_science_label


@pytest.fixture(autouse=True)
def no_sleeping(monkeypatch):
    monkeypatch.setattr(retry_module._time, "sleep", lambda *_args, **_kwargs: None)


@pytest.fixture(autouse=True)
def fake_settings(monkeypatch):
    monkeypatch.setattr(siftscience_module.settings, "SIFTSCIENCE_ENABLED", "1", raising=False)
    monkeypatch.setattr(siftscience_module.settings, "SIFTSCIENCE_API_KEY", "test-api-key", raising=False)


class TestIsBadReason:
    def test_known_reason_is_bad(self):
        assert is_bad_reason(("$spam", "nigeria")) is True

    def test_unknown_reason_is_not_bad(self):
        assert is_bad_reason(("$unknown", "whatever")) is False


class TestMapReasonToSiftScienceLabel:
    def test_known_reason_maps_to_its_label(self):
        assert map_reason_to_sift_science_label(("$chargeback", "chargeback")) == "$chargeback"

    def test_unknown_reason_raises_value_error(self):
        with pytest.raises(ValueError):
            map_reason_to_sift_science_label(("$unknown", "whatever"))


class TestLabelUser:
    def test_disabled_via_settings_skips_the_api_call(self, monkeypatch):
        monkeypatch.setattr(siftscience_module.settings, "SIFTSCIENCE_ENABLED", "0", raising=False)
        wget = mock.Mock()
        monkeypatch.setattr(siftscience_module, "wget", wget)

        label_user(42, True, ("$spam", "nigeria"))

        wget.assert_not_called()

    def test_is_bad_true_with_a_valid_reason_posts_the_label(self, monkeypatch):
        wget = mock.Mock(return_value=b"ok")
        monkeypatch.setattr(siftscience_module, "wget", wget)

        label_user(42, True, ("$spam", "nigeria"))

        wget.assert_called_once()
        args, kwargs = wget.call_args
        assert "users/42/labels" in args[0]
        assert kwargs["request_type"] == "POST"
        assert '"$is_bad": true' in kwargs["body"]
        assert '"$reasons": ["$spam"]' in kwargs["body"]

    def test_is_bad_false_omits_reasons_from_the_payload(self, monkeypatch):
        wget = mock.Mock(return_value=b"ok")
        monkeypatch.setattr(siftscience_module, "wget", wget)

        label_user(42, False, None)

        _args, kwargs = wget.call_args
        assert '"$reasons"' not in kwargs["body"]
        assert '"$is_bad": false' in kwargs["body"]

    def test_is_bad_true_with_an_invalid_reason_raises_assertion_error(self, monkeypatch):
        wget = mock.Mock()
        monkeypatch.setattr(siftscience_module, "wget", wget)

        with pytest.raises(AssertionError, match="is not a valid reason to label as bad"):
            label_user(42, True, ("$unknown", "whatever"))

        wget.assert_not_called()

    def test_wget_failure_is_retried_and_eventually_gives_up(self, monkeypatch):
        wget = mock.Mock(side_effect=Exception("network error"))
        monkeypatch.setattr(siftscience_module, "wget", wget)

        label_user(42, False, None)

        # 1 initial attempt + 3 retries -- see the module docstring's quirk note.
        assert wget.call_count == 4

    def test_wget_success_on_first_attempt_does_not_retry(self, monkeypatch):
        wget = mock.Mock(return_value=b"ok")
        monkeypatch.setattr(siftscience_module, "wget", wget)

        label_user(42, False, None)

        assert wget.call_count == 1
