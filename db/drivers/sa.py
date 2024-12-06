# -*- coding: utf-8 -*-

"""SqlAlchemy sh_util db driver."""

__author__ = 'Jay Taylor [@jtaylor]'
# pylint: disable=C0415,C0103
import logging
import re
from sqlalchemy.sql.expression import bindparam, text

import settings

# Updated for Python 3.11 compatibility and modern Python best practices

_arg_re = re.compile(r'([^%])%s')


def sql_and_args_to_text(sql, args=None):
    """
    Convert plain SQL and arguments to a SqlAlchemy `text` instance.
    """
    if not args:
        return text(sql)

    bindparams = []
    i = [-1]  # Use a list to allow mutation within `next_bind_sub`.

    def next_bind_sub(match):
        i[0] += 1
        binding = f'arg{i[0]}'
        bindparams.append(bindparam(binding, args[i[0]]))
        return f'{match.group(1)}:{binding}'

    transformed_sql = _arg_re.sub(next_bind_sub, sql)
    return text(transformed_sql).bindparams(*bindparams)


def connections():
    """Infer and return appropriate set of connections."""
    try:
        from app import app
    except ImportError:
        from src.app import app

    return app.engines


def switch_default_database(name):
    """Swap in a different default database (Placeholder function)."""
    raise NotImplementedError("switch_default_database is not implemented yet.")




def get_real_shard_connection_name(using):
    """Lookup and return the actual connection name, never use 'default'."""
    from settings import DATABASE_URLS

    if using == 'default':
        # Use 'default' from DATABASE_URLS if DATABASE_DEFAULT_SHARD is not set
        using = getattr(settings, 'DATABASE_DEFAULT_SHARD', 'default')
        if using not in DATABASE_URLS:
            raise KeyError(f"Database shard '{using}' not found in DATABASE_URLS.")

    return using

def dict_fetch_all(result_proxy):
    """Returns all rows from a cursor as a list of dictionaries."""
    keys = result_proxy.keys()
    return [dict(zip(keys, row)) for row in result_proxy.fetchall()]


def db_query(sql, args=None, as_dict=False, using='default', force=False, debug=False):
    logging.info(f"Executing DB query: {sql} with args: {args}")
    try:
        from app import ScopedSessions
    except ImportError:
        from src.app import ScopedSessions

    args = args or ()
    if not force:
        using = get_real_shard_connection_name(using)

    if debug:
        logging.info(f"Using DB connection: {using}")

    with ScopedSessions[using]() as session:
        result_proxy = session.execute(sql_and_args_to_text(sql, args))
        result = dict_fetch_all(result_proxy) if as_dict else result_proxy.fetchall()
        result_proxy.close()
        logging.info(f"Query result: {result}")
        return result



def db_exec(sql, args=None, using='default', force=False, debug=False):
    """
    Execute raw database queries.
    """
    from sqlalchemy.exc import InvalidRequestError

    try:
        from app import ScopedSessions
    except ImportError:
        from src.app import ScopedSessions

    args = args or ()

    if not force:
        using = get_real_shard_connection_name(using)

    if debug:
        logging.info('-- [DEBUG] DB_EXEC, using=%s ::\n%s', using, sql)

    sql_stripped = sql.strip().lower()
    try:
        with ScopedSessions[using]() as session:
            if sql_stripped == 'begin':
                session.begin()
            elif sql_stripped == 'rollback':
                session.rollback()
            elif sql_stripped == 'commit':
                session.commit()
            else:
                session.execute(sql_and_args_to_text(sql, args))
    except InvalidRequestError as e:
        logging.error("InvalidRequestError during DB execution: %s", e)
        raise


_sa_attrs_to_psql = (
    ('database', 'dbname', 'sendhub'),
    ('username', 'user', None),
    ('password', 'password', None),
    ('host', 'host', None),
    ('port', 'port', '5432'),
)


def get_psql_connection_string(connection_name, secure=True):
    """Generate a PSQL-format connection string for a given connection."""
    from settings import DATABASE_URLS

    assert connection_name in DATABASE_URLS, f"Connection {connection_name} not found in DATABASE_URLS."

    engine = connections()[connection_name]
    ssl_mode = 'sslmode=require' if secure else ''
    psql_tuples = [
        f"{param}={getattr(engine.url, attr, default)}"
        for attr, param, default in _sa_attrs_to_psql
    ]
    return ' '.join(psql_tuples) + f" {ssl_mode}"
