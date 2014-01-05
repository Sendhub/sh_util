# -*- coding: utf-8 -*-

"""SqlAlchemy sh_util db driver."""

__author__ = 'Jay Taylor [@jtaylor]'

import logging, re, settings
from sqlalchemy.sql.expression import bindparam, text


_argRe = re.compile(r'([^%])%s')

def sqlAndArgsToText(sql, args=None):
    """
    Convert plain old combination of sql/args to SqlAlchemy `text` instance.

    It seems ridiculous to have to do this, but I really want to use the `text` instances to turn off auto-commit.
    """
    if not args:
        return text(sql)

    bindparams = []
    i = [-1] # Using a list since we need to mutate the variable which isn't allowed with a direct variable reference.

    def nextBindSub(match):
        i[0] += 1
        binding = '${0}'.format(i[0])
        bindparams.append(bindparam(binding, args[i[0]]))
        return '{0}:{1}'.format(match.group(1), binding)

    transformedSql = _argRe.sub(nextBindSub, sql)
    return text(transformedSql, bindparams=bindparams)


def connections():
    """Infer and return appropriate set of connections."""
    from app import app
    #from flask.globals import current_app
    return app.engines


def switchDefaultDatabase(name):
    """Swap in a different default database."""
    pass


def getRealShardConnectionName(using):
    """Lookup and return the ACTUAL connection name, never use 'default'."""
    if using == 'default':
        if hasattr(settings, 'settings.DATABASE_DEFAULT_SHARD'):
            using = settings.DATABASE_DEFAULT_SHARD
        else:
            using = connections().keys()[0]

    return using


def _dictfetchall(resultProxy):
    """Returns all rows from a cursor as a dict."""
    desc = resultProxy.keys()
    return [dict(zip([col for col in desc], row)) for row in resultProxy.fetchall()]


def db_query(sql, args=None, as_dict=False, using='default', force=False, debug=False):
    """
    Execute raw select queries.  Not tested or guaranteed to work with any
    other type of query.

    @param force boolean Defaults to False. Whether or not to force the named connection to be used.
    """
    from ..import DEBUG
    from app import ScopedSessions

    if args is None:
        args = tuple()

    if force is False:
        using = getRealShardConnectionName(using)

    if 1 or DEBUG is True or debug is True:
        logging.info(u'-- [DEBUG] DB_QUERY, using={0} ::\n{1} {2}'.format(using, sql, args))

    #resultProxy = ScopedSessions[using]().execute(sqlAndArgsToText(sql, args).execution_options(autocommit=False))
    resultProxy = ScopedSessions[using]().execute(sqlAndArgsToText(sql, args))
    #resultProxy = ScopedSessions[using]().execute(sql, args)

    res = _dictfetchall(resultProxy) if as_dict is True else resultProxy.fetchall()
    resultProxy.close()
    return res


def db_exec(sql, args=None, using='default', force=False, debug=False):
    """
    Execute a raw query on the requested database connection.
    
    @param force boolean Defaults to False. Whether or not to force the named connection to be used.
    """
    from sqlalchemy.exc import InvalidRequestError
    from ..import DEBUG
    from app import ScopedSessions

    if args is None:
        args = tuple()

    if force is False:
        using = getRealShardConnectionName(using)

    if DEBUG is True or debug is True:
        logging.info(u'-- [DEBUG] DB_EXEC, using={0} ::\n{1}'.format(using, sql))

    txCandidate = sql.strip().rstrip(';').strip().lower()
    if txCandidate == 'begin':
        try:
            ScopedSessions[using]().begin()
        except InvalidRequestError:
            pass
    elif txCandidate == 'rollback':
        ScopedSessions[using]().rollback()
    elif txCandidate == 'commit':
        ScopedSessions[using]().commit()
    else:
        #statement = sqlAndArgsToText(sql, args).execution_options(autocommit=False)
        #ScopedSessions[using]().execute(statement)
        ScopedSessions[using]().execute(sqlAndArgsToText(sql, args))
        #ScopedSessions[using]().execute(sql, args)


_saAttrsToPsql = (
    ('database', 'dbname', 'sendhub'),
    ('username', 'user', None),
    ('password', 'password', None),
    ('host', 'host', None),
    ('port', 'port', '5432'),
)


def getPsqlConnectionString(connectionName, secure=True):
    """Generate a PSQL-format connection string for a given connection."""
    assert connectionName in settings.DATABASE_URLS

    engine = connections()[connectionName]

    out = 'sslmode=require' if secure is True else ''

    psqlTuples = map(lambda (key, param, default): '{0}={1}'.format(param, getattr(engine.url, key) or default), _saAttrsToPsql)

    out = ' '.join(psqlTuples) + (' sslmode=require' if secure is True else '')
    return out

