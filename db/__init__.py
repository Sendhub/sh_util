# -*- coding: utf-8 -*-

"""Django database tools."""

__author__ = 'Jay Taylor [@jtaylor]'

import logging, settings


DEBUG = False

_useDjangoDriver =  lambda: settings.SH_UTIL_DB_DRIVER.lower() == 'django'
_useSqlAlchemyDriver = lambda: settings.SH_UTIL_DB_DRIVER.lower() in ('sqlalchemy', 'sa')

if _useDjangoDriver():
    from .drivers.dj import *

elif _useSqlAlchemyDriver():
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

