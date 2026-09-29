"""SqlAlchemy sh_util db driver."""

__author__ = "Jay Taylor [@jtaylor]"

import logging
import re

import settings
from sqlalchemy.sql.expression import bindparam, text

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


def sql_and_args_to_text(sql, args=None):

    sql, args = _normalize_sql_and_args(sql, args)

    if not args:
        return text(sql)

    bindparams = []
    i = -1

    def next_bind_sub(match):
        nonlocal i
        i += 1
        name = f"arg{i}"
        bindparams.append(bindparam(name, args[i]))
        prefix = match.group(1) or ""
        return f"{prefix}:{name}"

    transformed_sql = _argRe.sub(next_bind_sub, sql)

    clause = text(transformed_sql)
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


def switch_default_database(name):
    """Swap in a different default database."""
    pass


def get_real_shard_connection_name(using):
    """Lookup and return the ACTUAL connection name, never use 'default'."""
    if using == "default":
        if hasattr(settings, "DATABASE_DEFAULT_SHARD"):
            using = settings.DATABASE_DEFAULT_SHARD
        else:
            using = next(iter(connections()), None)

    return using


def _dictfetchall(result_proxy):
    """Returns all rows from a cursor as a dict."""
    desc = list(result_proxy.keys())
    return [dict(list(zip([col for col in desc], row))) for row in result_proxy.fetchall()]  # noqa


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
        args = ()

    if force is False:
        using = get_real_shard_connection_name(using)

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

    ret = sql_and_args_to_text(sql, args)

    if isinstance(ret, tuple):
        clause, params = ret
    else:
        clause, params = ret, None

    if params:
        result_proxy = ScopedSessions[using]().execute(clause, params)
    else:
        result_proxy = ScopedSessions[using]().execute(clause)

    try:
        res = _dictfetchall(result_proxy) if as_dict is True else result_proxy.fetchall()  # noqa
        return res
    finally:
        result_proxy.close()


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
        args = ()

    if force is False:
        using = get_real_shard_connection_name(using)

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

    tx_candidate = sql.strip().rstrip(";").strip().lower()
    if tx_candidate == "begin":
        try:
            ScopedSessions[using]().begin()
        except InvalidRequestError:
            pass
    elif tx_candidate == "rollback":
        ScopedSessions[using]().rollback()
    elif tx_candidate == "commit":
        ScopedSessions[using]().commit()
    else:
        ret = sql_and_args_to_text(sql, args)

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


def get_psql_connection_string(connection_name, secure=True):
    """Generate a PSQL-format connection string for a given connection."""
    assert connection_name in settings.DATABASE_URLS

    engine = connections()[connection_name]

    psql_tuples = ["{0}={1}".format(t[1], getattr(engine.url, t[0]) or t[2]) for t in _saAttrsToPsql]

    out = " ".join(psql_tuples) + (" sslmode=require" if secure is True else "")
    return out
