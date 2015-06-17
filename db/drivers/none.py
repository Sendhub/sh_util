# -*- coding: utf-8 -*-

"""Dummy driver"""

class NoDatabaseDriver(Exception):
    pass


def connections():
    """Infer and return appropriate set of connections."""
    raise NoDatabaseDriver('No DB driver defined')


def _dictfetchall(cursor):
    """Returns all rows from a cursor as a dict."""
    raise NoDatabaseDriver('No DB driver defined')


def db_query(sql, args=None, as_dict=False, using='default', debug=False):
    """
    Execute raw select queries.  Not tested or guaranteed to work with any
    other type of query.
    """
    raise NoDatabaseDriver('No DB driver defined')


def db_exec(sql, args=None, using='default', debug=False):
    """
    Execute a raw query on the requested database connection.
    """
    raise NoDatabaseDriver('No DB driver defined')
