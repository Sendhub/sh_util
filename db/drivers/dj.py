# -*- coding: utf-8 -*-

"""Django sh_util database driver."""

__author__ = 'Jay Taylor [@jtaylor]'

import logging


def connections():
    """Infer and return appropriate set of connections."""
    from django.db import connections
    return connections


def _dictfetchall(cursor):
    """Returns all rows from a cursor as a dict."""
    desc = cursor.description
    return [dict(zip([col[0] for col in desc], row))
            for row in cursor.fetchall()]


def db_query(sql, args=None, as_dict=False, using='default', debug=False):
    """
    Execute raw select queries.  Not tested or guaranteed to work with any
    other type of query.
    """
    from ..import DEBUG

    if args is None:
        args = tuple()

    # Execute the raw query.
    cursor = connections()[using].cursor()

    if DEBUG is True or debug is True:
        logging.info(u'-- [DEBUG] DB_QUERY, using={0} ::\n{1}'.format(
            using, sql))

    cursor.execute(sql, args)

    res = _dictfetchall(cursor) if as_dict is True else cursor.fetchall()
    cursor.close()
    return res


def db_exec(sql, args=None, using='default', debug=False):
    """
    Execute a raw query on the requested database connection.
    """
    from ..import DEBUG

    if args is None:
        args = tuple()

    if DEBUG is True or debug is True:
        logging.info(u'-- [DEBUG] DB_EXEC, using={0} ::\n{1}'.format(
            using, sql))

    cursor = connections()[using].cursor()
    result = cursor.execute(sql, args)

    if DEBUG is True or debug is True:
        logging.info("%d rows updated" % cursor.rowcount)

    cursor.close()

    return result
