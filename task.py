"""
This module extends Celery task decorators with additional functionality, including the ability to send error emails.
"""

__author__ = "Jay Taylor [@jtaylor]"


import logging
import re

# Task import kept at module level as it's needed for class inheritance
# Other celery imports moved to function level to avoid circular import issues
from celery import Task

_fileLineFunctionExtractor = re.compile(r'^File "(?:\/app\/?)?(?P<file>[^"]+)".*? (?P<line>\d+), in (?P<fn>.*)$')


def _generate_subject(stack_trace_str, default="[Django] [ERROR] (Async worker exception)"):
    """
    Generating a subject line for error emails by incorporating file and function information from the stack trace.

    Args:
        stackTraceStr (str): The stack trace string.
        default (str): The default subject line.

    Returns:
        str: The generated subject line.
    """

    out = default
    pruned = [line for line in [line.strip() for line in stack_trace_str.split("\n")] if line.startswith("File ")]  # noqa
    if len(pruned) > 0:
        m = _fileLineFunctionExtractor.match(pruned[-1])
        if m is not None:
            file_name = m.group("file")
            line_no = m.group("line")
            fn = m.group("fn")
            out = "{0}: {file_name}.{fn} @ line {line_no}".format(out, file_name=file_name, line_no=line_no, fn=fn)

    return out


def _on_failure(self, exc, task_id, args, kwargs, einfo):
    """
    Handling task failure by logging the error and sending an email notification.

    Args:
        exc (Exception): The exception raised.
        task_id (str): The ID of the failed task.
        args (tuple): The positional arguments passed to the task.
        kwargs (dict): The keyword arguments passed to the task.
        einfo (ExceptionInfo): The exception information.
    """

    body = """Async task on failure triggered:
--------------------------------------------------------------------------------
exc: {exc}
task_id: {task_id}
args: {args}
kwargs: {kwargs}
einfo: {einfo}
--------------------------------------------------------------------------------
""".format(exc=exc, task_id=task_id, args=args, kwargs=kwargs, einfo=einfo)
    logging.error(body)

    # Import moved here to avoid circular import issues
    from sh_util.mail import send_email

    send_email(subject=_generate_subject(str(einfo)), body=body, from_address="devops@sendhub.com", to_address="devops@sendhub.com")


class ShTask(Task):
    """
    A custom Celery Task class that implements an on_failure callback handler.
    """

    name = "sh_util.task.ShTask"

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        """
        Delegating the failure handling to the _on_failure function.

        Args:
            exc (Exception): The exception raised.
            task_id (str): The ID of the failed task.
            args (tuple): The positional arguments passed to the task.
            kwargs (dict): The keyword arguments passed to the task.
            einfo (ExceptionInfo): The exception information.
        """
        # Import moved here to avoid circular import issues
        from celery.exceptions import MaxRetriesExceededError

        if isinstance(exc, MaxRetriesExceededError):
            logging.error("Suppressing MaxRetriesExceededError exception")
            return

        _on_failure(self, exc, task_id, args, kwargs, einfo)


class ShPeriodicTask(Task):
    """
    A custom Celery Task class for periodic tasks, with an on_failure callback handler.

    Periodic tasks add themselves to the :setting:`CELERYBEAT_SCHEDULE` setting.
    """

    name = "sh_util.task.ShPeriodicTask"
    abstract = True
    ignore_result = True
    relative = False
    options = None
    compat = True

    def __init__(self):
        """
        Initializing the periodic task and validating the presence of the run_every attribute.
        """

        if not hasattr(self, "run_every"):
            raise NotImplementedError("Periodic tasks must have a run_every attribute")

        # Import moved here to avoid circular import issues
        from celery.schedules import maybe_schedule

        self.run_every = maybe_schedule(self.run_every, self.relative)
        super().__init__()

    @classmethod
    def on_bound(cls, app):
        """
        Adding the periodic task to the Celery beat schedule.

        Args:
            app (Celery): The Celery application instance.
        """

        app.conf.CELERYBEAT_SCHEDULE[cls.name] = {
            "task": cls.name,
            "schedule": cls.run_every,
            "args": (),
            "kwargs": {},
            "options": cls.options or {},
            "relative": cls.relative,
        }

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        """
        Delegating the failure handling to the _on_failure function.

        Args:
            exc (Exception): The exception raised.
            task_id (str): The ID of the failed task.
            args (tuple): The positional arguments passed to the task.
            kwargs (dict): The keyword arguments passed to the task.
            einfo (ExceptionInfo): The exception information.
        """

        _on_failure(self, exc, task_id, args, kwargs, einfo)


def sh_task(*args, **kwargs):
    """
    A decorator for creating Celery tasks with the ShTask base class.

    Args:
        *args: Positional arguments for the task.
        **kwargs: Keyword arguments for the task.

    Returns:
        Task: The decorated Celery task.
    """
    # Import moved here to avoid circular import issues
    from celery import current_app

    return current_app.task(*args, **dict({"base": ShTask}, **kwargs))


def sh_periodic_task(*args, **options):
    """
    A decorator for creating periodic Celery tasks with the ShPeriodicTask base class.

    Args:
        *args: Positional arguments for the task.
        **options: Options for the periodic task.

    Returns:
        Task: The decorated periodic Celery task.
    """
    # Import moved here to avoid circular import issues
    from celery import current_app

    return current_app.task(**dict({"base": ShPeriodicTask}, **options))
