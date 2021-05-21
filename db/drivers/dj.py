# -*- coding: utf-8 -*-

"""Django sh_util database driver."""

__author__ = 'Jay Taylor [@jtaylor]'
# pylint: disable=C0415,C0103
import logging
import settings


def connections():
    """Infer and return appropriate set of connections."""
    from django.db import connections
    return connections


def switchDefaultDatabase(name):
    """Swap in a different default database."""
    from django.db import connections
    connections['default'] = connections[name]
    settings.DATABASES['default'] = settings.DATABASES[name]


def _dictfetchall(cursor):
    """Returns all rows from a cursor as a dict."""
    desc = cursor.description
    return [dict(list(zip([col[0] for col in desc], row))) for row in cursor.fetchall()]  # noqa


def getRealShardConnectionName(using):
    """Lookup and return the ACTUAL connection name, never use 'default'."""
    if using == 'default':
        # Avoid circular imports.
        from ...sharding import ShardedResource

        # Lookup the ACTUAL connection name, never use 'default'.
        using = ShardedResource.getCurrentShard()

    return using


def db_query(sql, args=None, as_dict=False, using='default',
             force=False, debug=False):
    """
    Execute raw select queries.  Not tested or guaranteed to work with any
    other type of query.

    @param force boolean Defaults to False. Whether or not to force the
    named connection to be used.
    """
    from ..import DEBUG

    if args is None:
        args = tuple()

    # Execute the raw query.
    cursor = connections()[using].cursor()

    if DEBUG is True or debug is True:
        logging.info('-- [DEBUG] DB_QUERY, using=%s ::\n%s',
                     str(using), str(sql))

    cursor.execute(sql, args)

    res = _dictfetchall(cursor) if as_dict is True else cursor.fetchall()
    cursor.close()
    return res


def db_exec(sql, args=None, using='default', force=False, debug=False):
    """
    Execute a raw query on the requested database connection.

    @param force boolean Defaults to False. Whether or not to force the
    named connection to be used.
    """
    from ..import DEBUG

    if args is None:
        args = tuple()

    if DEBUG is True or debug is True:
        logging.info('-- [DEBUG] DB_EXEC, using=%s ::\n%s',
                     str(using), str(sql))

    cursor = connections()[using].cursor()
    result = cursor.execute(sql, args)

    cursor.close()
    return result


_djangoConfigToPsql = (
    ('NAME', 'dbname'),
    ('USER', 'user'),
    ('PASSWORD', 'password'),
    ('HOST', 'host'),
    ('PORT', 'port'),
)


def getPsqlConnectionString(connectionName, secure=True):
    """Generate a PSQL-format connection string for a given connection."""
    assert connectionName in settings.DATABASES, \
        'Requested connection missing: {0}'.format(connectionName)

    dbConfig = settings.DATABASES[connectionName]

    out = 'sslmode=require' if secure is True else ''

    filtered = [key__ for key__ in _djangoConfigToPsql if key__[0] in dbConfig and dbConfig[key__[0]] is not None and dbConfig[key__[0]] != '']  # noqa

    psqlTuples = ['{0}={1}'.format(key_param[1], dbConfig[key_param[0]]) for key_param in filtered]  # noqa

    out = ' '.join(psqlTuples) + (' sslmode=require' if secure is True else '')
    return out
