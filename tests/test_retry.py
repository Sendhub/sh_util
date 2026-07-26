"""Unit tests for ``sh_util.retry``.

``retry`` is a decorator factory with exponential backoff. ``time.sleep`` is
stubbed throughout so the backoff schedule can be asserted without waiting.

Two quirks are pinned because they are surprising: the success check happens
*inside* the retry loop, so ``tries=0`` never reports success even when the very
first call succeeds; and the documented ``fail_value`` argument is only logged
(SonarQube S1172 fix) but still never returned — failure always yields
``False``.
"""

import pytest
from sh_util import retry as retry_module
from sh_util.retry import retry


@pytest.fixture(autouse=True)
def no_sleeping(monkeypatch):
    """Record backoff delays instead of sleeping through them."""
    slept = []
    monkeypatch.setattr(retry_module._time, "sleep", slept.append)
    return slept


class TestArgumentValidation:
    @pytest.mark.parametrize("backoff", [1, 0, -1, 0.5])
    def test_backoff_must_exceed_one(self, backoff):
        with pytest.raises(ValueError, match="Backoff must be greater than 1"):
            retry(3, backoff=backoff)

    def test_tries_must_not_be_negative(self):
        with pytest.raises(ValueError, match="Tries must be 0 or greater"):
            retry(-1)

    @pytest.mark.parametrize("delay", [0, -5])
    def test_delay_must_be_positive(self, delay):
        with pytest.raises(ValueError, match="Delay must be greater than 0"):
            retry(3, delay=delay)

    def test_fractional_tries_are_floored(self, no_sleeping):
        calls = []

        @retry(2.9, delay=1)
        def always_fails():
            calls.append(1)
            return False

        assert always_fails() is False
        # floor(2.9) == 2 retries after the initial attempt.
        assert len(calls) == 3


class TestRetryBehaviour:
    def test_returns_immediately_when_the_first_attempt_succeeds(self, no_sleeping):
        calls = []

        @retry(3, delay=1)
        def succeeds():
            calls.append(1)
            return True

        assert succeeds() is True
        assert len(calls) == 1
        assert no_sleeping == []

    def test_retries_until_success(self, no_sleeping):
        outcomes = [False, False, True]

        @retry(5, delay=1)
        def eventually():
            return outcomes.pop(0)

        assert eventually() is True
        assert outcomes == []

    def test_returns_false_once_retries_are_exhausted(self, no_sleeping):
        calls = []

        @retry(2, delay=1)
        def never():
            calls.append(1)
            return False

        assert never() is False
        # One initial attempt plus two retries.
        assert len(calls) == 3

    def test_delay_grows_by_the_backoff_factor(self, no_sleeping):
        @retry(3, delay=2, backoff=3)
        def never():
            return False

        never()
        assert no_sleeping == [2, 6, 18]

    def test_arguments_are_forwarded_on_every_attempt(self, no_sleeping):
        seen = []

        @retry(2, delay=1)
        def echo(*args, **kwargs):
            seen.append((args, kwargs))
            return False

        echo(1, key="v")
        assert seen == [((1,), {"key": "v"})] * 3

    def test_zero_tries_never_reports_success(self, no_sleeping):
        # The success comparison lives inside ``while mtries > 0``, so with no
        # retries budgeted the result is discarded and False is returned.
        calls = []

        @retry(0, delay=1)
        def succeeds():
            calls.append(1)
            return True

        assert succeeds() is False
        assert len(calls) == 1


class TestDesiredOutcome:
    def test_matches_a_literal_desired_value(self, no_sleeping):
        outcomes = ["pending", "ready"]

        @retry(3, delay=1, desired_outcome="ready")
        def poll():
            return outcomes.pop(0)

        assert poll() == "ready"

    def test_callable_predicate_decides_success(self, no_sleeping):
        outcomes = [1, 2, 5]

        @retry(5, delay=1, desired_outcome=lambda value: value > 3)
        def climb():
            return outcomes.pop(0)

        assert climb() == 5

    def test_callable_predicate_must_return_exactly_true(self, no_sleeping):
        # A truthy-but-not-True result does not count as success.
        @retry(1, delay=1, desired_outcome=lambda value: "yes")
        def poll():
            return "anything"

        assert poll() is False

    def test_fail_value_is_ignored(self, no_sleeping):
        # ``fail_value`` is part of the signature but the implementation always
        # returns False on exhaustion.
        @retry(1, delay=1, fail_value="custom-failure")
        def never():
            return False

        assert never() is False
