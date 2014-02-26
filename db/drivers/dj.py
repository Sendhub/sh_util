# -*- coding: utf-8 -*-

"""Django sh_util database driver."""

__author__ = 'Jay Taylor [@jtaylor]'

import logging, settings


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
    return [dict(zip([col[0] for col in desc], row)) for row in cursor.fetchall()]


def getRealShardConnectionName(using):
    """Lookup and return the ACTUAL connection name, never use 'default'."""
    if using == 'default':
        # Avoid circular imports.
        from ...sharding import ShardedResource

        # Lookup the ACTUAL connection name, never use 'default'.
        using = ShardedResource.getCurrentShard()

    return using


def db_query(sql, args=None, as_dict=False, using='default', force=False, debug=False):
    """
    Execute raw select queries.  Not tested or guaranteed to work with any
    other type of query.

    @param force boolean Defaults to False. Whether or not to force the named connection to be used.
    """
    from ..import DEBUG

    if args is None:
        args = tuple()

    if force is False:
        using = getRealShardConnectionName(using)

    # Execute the raw query.
    cursor = connections()[using].cursor()

    if DEBUG is True or debug is True:
        logging.info(u'-- [DEBUG] DB_QUERY, using={0} ::\n{1}'.format(using, sql))

    cursor.execute(sql, args)

    res = _dictfetchall(cursor) if as_dict is True else cursor.fetchall()
    cursor.close()
    return res


def db_exec(sql, args=None, using='default', force=False, debug=False):
    """
    Execute a raw query on the requested database connection.

    @param force boolean Defaults to False. Whether or not to force the named connection to be used.
    """
    from ..import DEBUG

    if args is None:
        args = tuple()

    if force is False:
        using = getRealShardConnectionName(using)

    if DEBUG is True or debug is True:
        logging.info(u'-- [DEBUG] DB_EXEC, using={0} ::\n{1}'.format(using, sql))

    cursor = connections()[using].cursor()
    result = cursor.execute(sql, args)

    cursor.close()
    return result


_djangoConfigToPsql = (
    ('NAME', 'dbname'),
    ('USER', 'user'),
    ('PASSWORD', 'password'),
    ('PORT', 'port'),
)


def getPsqlConnectionString(connectionName, secure=True):
    """Generate a PSQL-format connection string for a given connection."""
    assert connectionName in settings.DATABASES, 'Requested connection missing: {0}'.format(connectionName)

    dbConfig = settings.DATABASES[connectionName]

    out = 'sslmode=require' if secure is True else ''

    filtered = filter(
        lambda (key, _): key in dbConfig and dbConfig[key] is not None and dbConfig[key] != '',
        _djangoConfigToPsql
    )

    if 'DIRECT_HOST' in dbConfig:
        filtered.append(('DIRECT_HOST', 'host'))
    else:
        filtered.append(('HOST', 'host'))

    psqlTuples = map(lambda (key, param): '{0}={1}'.format(param, dbConfig[key]), filtered)

    out = ' '.join(psqlTuples) + (' sslmode=require' if secure is True else '')
    return out


