# -*- coding: utf-8 -*-

"""SqlAlchemy sh_util db driver."""

__author__ = 'Jay Taylor [@jtaylor]'

import logging, re, settings
from sqlalchemy.sql.expression import bindparam, text


_argRe = re.compile(r'([^%])%s')

def sqlAndArgsToText(sql, args=None):

    if isinstance(sql, tuple):
        sql = sql[0]

    if not args:
        return text(sql)

    bindparams = []
    i = -1

    def nextBindSub(match):
        nonlocal i
        i += 1
        name = f"arg{i}"
        bindparams.append(bindparam(name, args[i]))
        return f"{match.group(1)}:{name}"

    transformedSql = _argRe.sub(nextBindSub, sql)

    clause = text(transformedSql)
    for bp in bindparams:
        clause = clause.bindparams(bp)
    return clause

def connections():
    """Infer and return appropriate set of connections."""
    try:
        from app import app

    except ImportError:
        from src.app import app

    return app.engines


def switchDefaultDatabase(name):
    """Swap in a different default database."""
    pass


def getRealShardConnectionName(using):
    """Lookup and return the ACTUAL connection name, never use 'default'."""
    if using == 'default':
        if hasattr(settings, 'DATABASE_DEFAULT_SHARD'):
            using = settings.DATABASE_DEFAULT_SHARD
        else:
            using = next(iter(connections()), None)

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
    try:
        from app import ScopedSessions
    except ImportError:
        from src.app import ScopedSessions

    if args is None:
        args = tuple()

    if force is False:
        using = getRealShardConnectionName(using)

    if DEBUG is True or debug is True:
        logging.info(u'-- [DEBUG] DB_QUERY, using={0} ::\n{1} {2}'.format(using, sql, args))

    ret = sqlAndArgsToText(sql, args)

    if isinstance(ret, tuple):
        clause, params = ret
    else:
        clause, params = ret, None

    if params:
        resultProxy = ScopedSessions[using]().execute(clause, params)
    else:
        resultProxy = ScopedSessions[using]().execute(clause)

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

    try:
        from app import ScopedSessions
    except ImportError:
        from src.app import ScopedSessions

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
        ret = sqlAndArgsToText(sql, args)

        if isinstance(ret, tuple):
            clause, params = ret
        else:
            clause, params = ret, None

        if params:
            resultProxy = ScopedSessions[using]().execute(clause, params)
        else:
            resultProxy = ScopedSessions[using]().execute(clause)


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

    psqlTuples = map(lambda t: '{0}={1}'.format(t[1], getattr(engine.url, t[0]) or t[2]), _saAttrsToPsql)

    out = ' '.join(psqlTuples) + (' sslmode=require' if secure is True else '')
    return out

