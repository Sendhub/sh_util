# encoding: utf-8

"""Extended Celery task decorators with the ability to send error emails."""

__author__ = 'Jay Taylor [@jtaylor]'

from celery import current_app
from celery.task.base import Task
import logging

import re

_fileLineFunctionExtractor = re.compile(
    r'^File "(?:\/app\/?)?(?P<file>[^"]+)".*? (?P<line>[0-9]+), in (?P<fn>.*)$'
)

def _generateSubject(
    stackTraceStr,
    default='[Django] [ERROR] (Async worker exception)'
):
    """
    Takes in a stack trace string and incorporates file/function information in
    with the default subject line to (hopefully) provide a more helpful subject
    line.
    """
    out = default
    pruned = [line for line in [line.strip() for line in stackTraceStr.split('\n')] if line.startswith('File ')]
    if len(pruned) > 0:
        m = _fileLineFunctionExtractor.match(pruned[-1])
        if m is not None:
            fileName = m.group('file')
            lineNo = m.group('line')
            fn = m.group('fn')
            out = '{0}: {fileName}.{fn} @ line {lineNo}' \
                .format(out, fileName=fileName, lineNo=lineNo, fn=fn)

    return out


def _on_failure(self, exc, task_id, args, kwargs, einfo):
    """Failure callback handler with email support."""
    body = '''Async task on failure triggered:
--------------------------------------------------------------------------------
exc: {exc}
task_id: {task_id}
args: {args}
kwargs: {kwargs}
einfo: {einfo}
--------------------------------------------------------------------------------
'''.format(exc=exc, task_id=task_id, args=args, kwargs=kwargs, einfo=einfo)
    logging.error(body)

    from sh_util.mail import sendEmail
    sendEmail(
        subject=_generateSubject(str(einfo)),
        body=body,
        fromAddress='devops@sendhub.com',
        toAddress='devops@sendhub.com'
    )

class ShTask(Task):
    """Decorator class which implements on_failure callback handler."""

    name = 'sh_util.task.ShTask'

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        """Pass-through to failure handler."""
        from celery.exceptions import MaxRetriesExceededError

        if isinstance(exc, MaxRetriesExceededError):
            logging.error('Suppressing MaxRetriesExceededError exception')
            return

        _on_failure(self, exc, task_id, args, kwargs, einfo)


class ShPeriodicTask(Task):
    """
    Decorator class which implements on_failure callback handler.

    A periodic task is a task that adds itself to the
    :setting:`CELERYBEAT_SCHEDULE` setting.
    """
    name = 'sh_util.task.ShPeriodicTask'
    abstract = True
    ignore_result = True
    relative = False
    options = None
    compat = True

    def __init__(self):
        if not hasattr(self, 'run_every'):
            raise NotImplementedError(
                'Periodic tasks must have a run_every attribute'
            )
        from celery.schedules import maybe_schedule
        self.run_every = maybe_schedule(self.run_every, self.relative)
        super(ShPeriodicTask, self).__init__()

    @classmethod
    def on_bound(cls, app):
        """Copied from celery."""
        app.conf.CELERYBEAT_SCHEDULE[cls.name] = {
            'task': cls.name,
            'schedule': cls.run_every,
            'args': (),
            'kwargs': {},
            'options': cls.options or {},
            'relative': cls.relative,
        }

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        """Pass-through to failure handler."""
        _on_failure(self, exc, task_id, args, kwargs, einfo)


def shTask(*args, **kwargs):
    """Task decorator."""
    return current_app.task(
        *args,
        **dict(
            {'accept_magic_kwargs': False, 'base': ShTask},
            **kwargs
        )
    )

def shPeriodicTask(*args, **options):
    """Periodic task decorator."""
    return current_app.task(**dict({'base': ShPeriodicTask}, **options))

