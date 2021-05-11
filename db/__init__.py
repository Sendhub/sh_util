# -*- coding: utf-8 -*-

"""Django database tools."""

__author__ = 'Jay Taylor [@jtaylor]'

import settings

DEBUG = False


def _use_django_driver():
    return settings.SH_UTIL_DB_DRIVER.lower() == 'django'


def _use_sqlalchemy_driver():
    return settings.SH_UTIL_DB_DRIVER.lower() in ('sqlalchemy', 'sa')


if _use_django_driver():
    from .drivers.dj import *  # noqa
elif _use_sqlalchemy_driver():
    from .drivers.sa import *  # noqa
else:
    raise Exception('Unrecognized sh_util db driver: {0}'.format(settings.SH_UTIL_DB_DRIVER))  # noqa


def begin(using):
    """Begin a transaction."""
    return db_exec('BEGIN', using=using)  # noqa


def commit(using):
    """Commit a transaction."""
    return db_exec('COMMIT', using=using)  # noqa


def rollback(using):
    """Commit a transaction."""
    return db_exec('ROLLBACK', using=using)  # noqa
