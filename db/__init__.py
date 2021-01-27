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
    from .drivers.dj import *
elif _use_sqlalchemy_driver():
    from .drivers.sa import *
else:
    raise Exception('Unrecognized sh_util db driver: {0}'.format(settings.SH_UTIL_DB_DRIVER))


def begin(using):
    """Begin a transaction."""
    return db_exec('BEGIN', using=using)


def commit(using):
    """Commit a transaction."""
    return db_exec('COMMIT', using=using)


def rollback(using):
    """Commit a transaction."""
    return db_exec('ROLLBACK', using=using)
