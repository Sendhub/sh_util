# -*- coding: utf-8 -*-

"""SqlAlchemy sh_util db driver."""

__author__ = 'Jay Taylor [@jtaylor]'

import logging, re, settings
from sqlalchemy.sql.expression import bindparam, text


_argRe = re.compile(r'([^%])%s')

def sqlAndArgsToText(sql, args=None):
    """
    Convert plain old combination of sql/args to SqlAlchemy `text` instance.

    It seems ridiculous to have to do this, but I really want to use the `text`
    instances to turn off auto-commit.
    """
    if not args:
        return text(sql)

    bindparams = []
    # Using a list since we need to mutate the variable which isn't allowed
    # with a direct variable reference.
    i = [-1]

    def nextBindSub(match):
        i[0] += 1
        binding = 'arg{0}'.format(i[0])
        bindparams.append(bindparam(binding, args[i[0]]))
        return '{0}:{1}'.format(match.group(1), binding)

    transformedSql = _argRe.sub(nextBindSub, sql)
    return text(transformedSql, bindparams=bindparams)


def connections():
    """Infer and return appropriate set of connections."""
    try:
        from app import app
    except ImportError:
        from src.app import app

    #from flask.globals import current_app
    return app.engines


def _dictfetchall(resultProxy):
    """Returns all rows from a cursor as a dict."""
    desc = resultProxy.keys()
    return [dict(zip([col for col in desc], row)) for row in
            resultProxy.fetchall()]


def db_query(sql, args=None, as_dict=False, using='default', debug=False):
    """
    Execute raw select queries.  Not tested or guaranteed to work with any
    other type of query.
    """
    from ..import DEBUG
    try:
        from app import ScopedSessions
    except ImportError:
        from src.app import ScopedSessions

    if args is None:
        args = tuple()

    if DEBUG is True or debug is True:
        logging.info(u'-- [DEBUG] DB_QUERY, using={0} ::\n{1} {2}'.format(
            using, sql, args))

    resultProxy = ScopedSessions[using]().execute(sqlAndArgsToText(sql, args))

    res = _dictfetchall(resultProxy) if as_dict is True \
        else resultProxy.fetchall()
    resultProxy.close()
    return res


def db_exec(sql, args=None, using='default', debug=False):
    """
    Execute a raw query on the requested database connection.
    """
    from sqlalchemy.exc import InvalidRequestError
    from ..import DEBUG

    try:
        from app import ScopedSessions
    except ImportError:
        from src.app import ScopedSessions

    if args is None:
        args = tuple()

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
        ScopedSessions[using]().execute(sqlAndArgsToText(sql, args))
