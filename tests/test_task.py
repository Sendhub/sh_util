"""Unit tests for ``sh_util.task``.

``_on_failure`` sends a devops alert via ``sh_util.mail.send_email`` when a
task fails. These tests mock ``send_email`` to verify it is invoked with the
expected message, without sending real mail.

Celery is installed in this environment, so ``ShTask`` / ``ShPeriodicTask``
and the ``current_app``-based decorators are exercised against the real
library instead of fakes.
"""

from unittest import mock

import pytest
from celery.exceptions import MaxRetriesExceededError

from sh_util.task import ShPeriodicTask, ShTask, _generate_subject, _on_failure, sh_periodic_task, sh_task

DEFAULT_SUBJECT = "[Django] [ERROR] (Async worker exception)"


class TestGenerateSubject:
    def test_no_file_lines_returns_the_default(self):
        assert _generate_subject("ZeroDivisionError: division by zero") == DEFAULT_SUBJECT

    def test_empty_string_returns_the_default(self):
        assert _generate_subject("") == DEFAULT_SUBJECT

    def test_custom_default_is_used_when_no_file_lines_present(self):
        assert _generate_subject("no file lines here", default="Custom") == "Custom"

    def test_app_prefix_is_stripped_from_the_file_path(self):
        trace = 'Traceback (most recent call last):\n  File "/app/foo.py", line 10, in bar\nZeroDivisionError: division by zero'
        assert _generate_subject(trace) == f"{DEFAULT_SUBJECT}: foo.py.bar @ line 10"

    def test_file_path_without_app_prefix_is_kept_in_full(self):
        trace = '  File "/other/place/baz.py", line 5, in qux'
        assert _generate_subject(trace) == f"{DEFAULT_SUBJECT}: /other/place/baz.py.qux @ line 5"

    def test_last_matching_file_line_wins_when_there_are_several(self):
        trace = (
            'Traceback (most recent call last):\n'
            '  File "/app/foo.py", line 10, in bar\n'
            '  File "/app/baz.py", line 42, in qux\n'
            'ZeroDivisionError: division by zero'
        )
        assert _generate_subject(trace) == f"{DEFAULT_SUBJECT}: baz.py.qux @ line 42"

    def test_file_line_that_does_not_match_the_expected_shape_is_ignored(self):
        assert _generate_subject('File "unterminated') == DEFAULT_SUBJECT


class TestOnFailure:
    def test_logs_the_failure_body_then_sends_the_devops_email(self, caplog):
        with mock.patch("sh_util.mail.send_email") as mock_send_email:
            with caplog.at_level("ERROR"):
                _on_failure(None, ValueError("boom"), "task-1", (1, 2), {"k": "v"}, "einfo-details")

        assert "Async task on failure triggered" in caplog.text
        assert "boom" in caplog.text
        assert "task-1" in caplog.text
        mock_send_email.assert_called_once_with(
            subject=mock.ANY,
            body=mock.ANY,
            from_address="devops@sendhub.com",
            to_address="devops@sendhub.com",
        )


class TestShTask:
    def test_max_retries_exceeded_is_suppressed(self, caplog):
        instance = ShTask()

        with caplog.at_level("ERROR"):
            instance.on_failure(MaxRetriesExceededError("boom"), "task-1", (), {}, "einfo")

        assert "Suppressing MaxRetriesExceededError" in caplog.text

    def test_other_exceptions_delegate_to_on_failure_and_send_email(self):
        instance = ShTask()

        with mock.patch("sh_util.mail.send_email") as mock_send_email:
            instance.on_failure(ValueError("boom"), "task-1", (), {}, "einfo")

        mock_send_email.assert_called_once()


class TestShPeriodicTask:
    def test_missing_run_every_raises_not_implemented(self):
        class NoScheduleTask(ShPeriodicTask):
            pass

        with pytest.raises(NotImplementedError, match="run_every attribute"):
            NoScheduleTask()

    def test_run_every_is_normalized_via_maybe_schedule(self):
        class EveryThirtySeconds(ShPeriodicTask):
            run_every = 30

        instance = EveryThirtySeconds()

        assert instance.run_every.seconds == 30

    def test_on_bound_registers_the_task_in_celerybeat_schedule(self):
        class Nightly(ShPeriodicTask):
            name = "test.task.nightly"
            run_every = 60
            options = {"queue": "low"}

        app = mock.Mock()
        app.conf.CELERYBEAT_SCHEDULE = {}

        Nightly.on_bound(app)

        entry = app.conf.CELERYBEAT_SCHEDULE["test.task.nightly"]
        assert entry["task"] == "test.task.nightly"
        assert entry["options"] == {"queue": "low"}
        assert entry["relative"] is False

    def test_on_failure_delegates_to_on_failure_and_sends_email(self):
        class Nightly(ShPeriodicTask):
            run_every = 60

        instance = Nightly()

        with mock.patch("sh_util.mail.send_email") as mock_send_email:
            instance.on_failure(ValueError("boom"), "task-1", (), {}, "einfo")

        mock_send_email.assert_called_once()


class TestShTaskDecorator:
    def test_wraps_the_function_with_shtask_base(self):
        @sh_task(name="test.task.sh_task_decorator_wraps")
        def my_func():
            return "ok"

        assert isinstance(my_func, ShTask)


class TestShPeriodicTaskDecorator:
    def test_wraps_the_function_with_shperiodictask_base(self):
        @sh_periodic_task(run_every=30, name="test.task.sh_periodic_task_decorator_wraps")
        def my_periodic():
            return "ok"

        assert isinstance(my_periodic, ShPeriodicTask)
