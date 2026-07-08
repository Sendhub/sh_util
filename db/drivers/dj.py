"""
Django sh_util database driver.
"""

__author__ = "Jay Taylor [@jtaylor]"

import logging

import settings


def connections():
    """
    Infer and return appropriate set of connections.

    Returns:
        object: Django connections registry.
    """

    from django.db import connections

    return connections


def switchDefaultDatabase(name):
    """
    Swap in a different default database.

    Args:
        name (str): Connection name to use as default.
    """

    from django.db import connections

    connections["default"] = connections[name]
    settings.DATABASES["default"] = settings.DATABASES[name]


def _dictfetchall(cursor):
    """
    Returns all rows from a cursor as a dict.

    Args:
        cursor (object): Database cursor with a result set.

    Returns:
        list: List of dictionaries keyed by column name.
    """

    desc = cursor.description
    return [dict(list(zip([col[0] for col in desc], row))) for row in cursor.fetchall()]  # noqa


def getRealShardConnectionName(using):
    """
    Lookup and return the ACTUAL connection name, never use 'default'.

    Args:
        using (str): Requested connection name.

    Returns:
        str: Resolved connection name.
    """

    if using == "default":
        # Avoid circular imports.
        from ...sharding import ShardedResource

        # Lookup the ACTUAL connection name, never use 'default'.
        using = ShardedResource.getCurrentShard()

    return using


def db_query(sql, args=None, as_dict=False, using="default", force=False, debug=False):
    """
    Execute raw select queries. Not tested or guaranteed to work with any
    other type of query.

    Args:
        sql (str): SQL statement to execute.
        args (tuple, optional): Query parameters.
        as_dict (bool, optional): Whether to return rows as dictionaries.
        using (str, optional): Django connection name.
        force (bool, optional): Whether to force the named connection to be used.
        debug (bool, optional): Whether to log debug SQL output.

    Returns:
        list: Query results, or None on error.
    """

    from .. import DEBUG

    if args is None:
        args = tuple()

    # Execute the raw query.
    cursor = connections()[using].cursor()
    try:
        if DEBUG is True or debug is True:
            logging.info("-- [DEBUG] DB_QUERY, using=%s ::\n%s", str(using), str(sql))

        cursor.execute(sql, args)

        res = _dictfetchall(cursor) if as_dict is True else cursor.fetchall()
        return res
    except Exception as conn_err:
        logging.error(f"Error on executing the query: {conn_err}")
        return
    finally:
        cursor.close()


def db_exec(sql, args=None, using="default", force=False, debug=False):
    """
    Execute a raw query on the requested database connection.

    Args:
        sql (str): SQL statement to execute.
        args (tuple, optional): Query parameters.
        using (str, optional): Django connection name.
        force (bool, optional): Whether or not to force the named connection to be used.
        debug (bool, optional): Whether to log debug SQL output.

    Returns:
        object: Result of cursor execution.
    """

    from .. import DEBUG

    if args is None:
        args = tuple()

    if DEBUG is True or debug is True:
        logging.info(f"-- [DEBUG] DB_EXEC, using={using} ::\n{sql}")

    cursor = connections()[using].cursor()
    try:
        logging.info(f"executing the sql {sql} using {using}")
        result = cursor.execute(sql, args)
        return result
    finally:
        cursor.close()


_djangoConfigToPsql = (
    ("NAME", "dbname"),
    ("USER", "user"),
    ("PASSWORD", "password"),
    ("HOST", "host"),
    ("PORT", "port"),
)


def getPsqlConnectionString(connectionName, secure=True):
    """
    Generate a PSQL-format connection string for a given connection.

    Args:
        connectionName (str): Connection name in Django settings.
        secure (bool, optional): Whether to require SSL.

    Returns:
        str: PSQL-format connection string.
    """

    assert connectionName in settings.DATABASES, f"Requested connection missing: {connectionName}"

    dbConfig = settings.DATABASES[connectionName]

    out = "sslmode=require" if secure is True else ""

    filtered = [key__ for key__ in _djangoConfigToPsql if key__[0] in dbConfig and dbConfig[key__[0]] is not None and dbConfig[key__[0]] != ""]  # noqa

    psqlTuples = [f"{key_param[1]}={dbConfig[key_param[0]]}" for key_param in filtered]  # noqa

    out = " ".join(psqlTuples) + (" sslmode=require" if secure is True else "")
    return out
