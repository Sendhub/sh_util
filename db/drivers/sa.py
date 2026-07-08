"""SqlAlchemy sh_util db driver."""

__author__ = "Jay Taylor [@jtaylor]"

import logging
import re

import settings
from sqlalchemy.sql.expression import bindparam, text

# _argRe = re.compile(r'([^%])%s')
_argRe = re.compile(r"(?<!%)%s|(\?)")


def _normalize_sql_and_args(sql, args=None):
    """Support callers that pass `(sql, args)` as the first argument."""
    if isinstance(sql, tuple):
        if len(sql) != 2:
            raise ValueError("SQL tuple must be in the form (sql, args)")
        inline_sql, inline_args = sql
        if args not in (None, (), []):
            raise ValueError("SQL args provided twice")
        return inline_sql, inline_args
    return sql, args


def sqlAndArgsToText(sql, args=None):

    sql, args = _normalize_sql_and_args(sql, args)

    if not args:
        return text(sql)

    bindparams = []
    i = -1

    def nextBindSub(match):
        nonlocal i
        i += 1
        name = f"arg{i}"
        bindparams.append(bindparam(name, args[i]))
        prefix = match.group(1) or ""
        return f"{prefix}:{name}"

    transformedSql = _argRe.sub(nextBindSub, sql)

    clause = text(transformedSql)
    for bp in bindparams:
        clause = clause.bindparams(bp)
    return clause


def connections():
    """Infer and return appropriate set of connections."""
    try:
        # Prefer the canonical package import to avoid importing src/app.py as
        # a top-level module named "app" (which can result in a separate module
        # namespace and an empty ScopedSessions registry).
        from src.app import app as flask_app
    except Exception:
        from app import app as flask_app

    logging.info(f"Engines: {flask_app.engines}")
    return flask_app.engines


def switchDefaultDatabase(name):
    """Swap in a different default database."""
    pass


def getRealShardConnectionName(using):
    """Lookup and return the ACTUAL connection name, never use 'default'."""
    if using == "default":
        if hasattr(settings, "DATABASE_DEFAULT_SHARD"):
            using = settings.DATABASE_DEFAULT_SHARD
        else:
            using = next(iter(connections()), None)

    return using


def _dictfetchall(resultProxy):
    """Returns all rows from a cursor as a dict."""
    desc = list(resultProxy.keys())
    return [dict(list(zip([col for col in desc], row))) for row in resultProxy.fetchall()]  # noqa


def db_query(sql, args=None, as_dict=False, using="default", force=False, debug=False):
    """
    Execute raw select queries.  Not tested or guaranteed to work with any
    other type of query.

    @param force boolean Defaults to False. Whether or not to force the
    named connection to be used.
    """
    from .. import DEBUG

    try:
        from src.database import ScopedSessions
    except Exception:
        try:
            from src.app import ScopedSessions
        except Exception:
            from app import ScopedSessions

    sql, args = _normalize_sql_and_args(sql, args)

    if args is None:
        args = tuple()

    if force is False:
        using = getRealShardConnectionName(using)

    # Safety net: avoid KeyError when the requested connection name isn't configured.
    # Common in deployments where only one bind is initialized or names differ.
    if using not in ScopedSessions:
        fallback = next(iter(ScopedSessions), None)
        if fallback is None:
            raise RuntimeError("No database sessions configured (ScopedSessions is empty). Check DATABASE_URL / DATABASE_URLS and DB initialization logs.")
        logging.warning(
            "Requested DB session '%s' not configured; falling back to '%s'",
            using,
            fallback,
        )
        using = fallback

    if DEBUG is True or debug is True:
        logging.debug(f"-- [DEBUG] DB_QUERY, using={using} ::\n{sql} {args}")

    ret = sqlAndArgsToText(sql, args)

    if isinstance(ret, tuple):
        clause, params = ret
    else:
        clause, params = ret, None

    if params:
        resultProxy = ScopedSessions[using]().execute(clause, params)
    else:
        resultProxy = ScopedSessions[using]().execute(clause)

    try:
        res = _dictfetchall(resultProxy) if as_dict is True else resultProxy.fetchall()  # noqa
        return res
    finally:
        resultProxy.close()


def db_exec(sql, args=None, using="default", force=False, debug=False):
    """
    Execute a raw query on the requested database connection.

    @param force boolean Defaults to False. Whether or not to force the
    named connection to be used.
    """
    from sqlalchemy.exc import InvalidRequestError

    from .. import DEBUG

    try:
        from src.database import ScopedSessions
    except Exception:
        try:
            from src.app import ScopedSessions
        except Exception:
            from app import ScopedSessions

    sql, args = _normalize_sql_and_args(sql, args)

    if args is None:
        args = tuple()

    if force is False:
        using = getRealShardConnectionName(using)

    if using not in ScopedSessions:
        fallback = next(iter(ScopedSessions), None)
        if fallback is None:
            raise RuntimeError("No database sessions configured (ScopedSessions is empty). Check DATABASE_URL / DATABASE_URLS and DB initialization logs.")
        logging.warning(
            "Requested DB session '%s' not configured; falling back to '%s'",
            using,
            fallback,
        )
        using = fallback

    if DEBUG is True or debug is True:
        logging.debug("-- [DEBUG] DB_EXEC, using={using} ::\n{sql}")

    txCandidate = sql.strip().rstrip(";").strip().lower()
    if txCandidate == "begin":
        try:
            ScopedSessions[using]().begin()
        except InvalidRequestError:
            pass
    elif txCandidate == "rollback":
        ScopedSessions[using]().rollback()
    elif txCandidate == "commit":
        ScopedSessions[using]().commit()
    else:
        ret = sqlAndArgsToText(sql, args)

        if isinstance(ret, tuple):
            clause, params = ret
        else:
            clause, params = ret, None

        if params:
            ScopedSessions[using]().execute(clause, params)
        else:
            ScopedSessions[using]().execute(clause)


_saAttrsToPsql = (
    ("database", "dbname", "sendhub"),
    ("username", "user", None),
    ("password", "password", None),
    ("host", "host", None),
    ("port", "port", "5432"),
)


def getPsqlConnectionString(connectionName, secure=True):
    """Generate a PSQL-format connection string for a given connection."""
    assert connectionName in settings.DATABASE_URLS

    engine = connections()[connectionName]

    out = "sslmode=require" if secure is True else ""

    psqlTuples = map(lambda t: "{0}={1}".format(t[1], getattr(engine.url, t[0]) or t[2]), _saAttrsToPsql)

    out = " ".join(psqlTuples) + (" sslmode=require" if secure is True else "")
    return out
