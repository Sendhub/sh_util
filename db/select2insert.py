#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Utility to enable generation of SELECT SQL which executes to INSERT SQL.

NB: Only tested for compatibility with Postgres.
"""

__author__ = 'Jay Taylor [@jtaylor]'


def select2insert(table, description, whereClause=None):
    """
    Generate a SELECT statement that can be executed to produce an INSERT statement for each matching column.

    @param table str Table name.
    @param description sequence of column meta-data, where the first element of each tuple contains the a column name.
    @param whereClause optional str Defaults to None.

    @return str SELECT statement.

    >>> select2insert(
    ...     'auth_user',
    ...     [
    ...         (u'id', u'bigint'),
    ...         (u'username', u'character varying(85)'),
    ...         (u'first_name', u'character varying(120)'),
    ...         (u'last_name', u'character varying(30)'),
    ...         (u'email', u'character varying(75)'),
    ...         (u'password', u'character varying(128)'),
    ...         (u'is_staff', u'boolean'),
    ...         (u'is_active', u'boolean'),
    ...         (u'is_superuser', u'boolean'),
    ...         (u'last_login', u'timestamp without time zone'),
    ...         (u'date_joined', u'timestamp without time zone')
    ...     ]
    ... )
    'SELECT \\'INSERT INTO "auth_user" ("id","username","first_name","last_name","email","password","is_staff","is_active","is_superuser","last_login","date_joined") VALUES (\\' || quote_nullable("id") || \\',\\' || quote_nullable("username") || \\',\\' || quote_nullable("first_name") || \\',\\' || quote_nullable("last_name") || \\',\\' || quote_nullable("email") || \\',\\' || quote_nullable("password") || \\',\\' || quote_nullable("is_staff") || \\',\\' || quote_nullable("is_active") || \\',\\' || quote_nullable("is_superuser") || \\',\\' || quote_nullable("last_login") || \\',\\' || quote_nullable("date_joined") || \\');\\' FROM "auth_user";'

    """
    columns = ','.join(['"{0}"'.format(tup[0]) for tup in description])

    values = " || ',' || ".join(['quote_nullable("{0}")'.format(tup[0]) for tup in description])

    if whereClause is not None and not whereClause.lower().strip().startswith('where '):
        whereClause = 'WHERE {0}'.format(whereClause)

    where = '{0}'.format(whereClause) if whereClause is not None else ''

    intermediateSql = \
        '''SELECT 'INSERT INTO "{table}" ({columns}) VALUES (' || {values} || ');' FROM "{table}"{where};''' \
        .format(table=table, columns=columns, values=values, where=where)

    return intermediateSql


def select2multiInsert(using, table, description, whereClause=None):
    """Evaluates intermediate SQL and returns combined multi-insert statement."""
    from . import db_query

    values = " || ',' || ".join(['quote_nullable("{0}")'.format(tup[0]) for tup in description])

    if whereClause is not None and not whereClause.lower().strip().startswith('where '):
        whereClause = 'WHERE {0}'.format(whereClause)

    where = '{0}'.format(whereClause) if whereClause is not None else ''

    intermediateSql = \
        '''SELECT '(' || {values} || ')' FROM "{table}"{where};'''.format(values=values, table=table, where=where)

    actualValues = ','.join([tup[0] for tup in db_query(intermediateSql, using=using)])
    if len(actualValues) == 0:
        return None

    columns = ','.join(['"{0}"'.format(tup[0]) for tup in description])

    finalSql = 'INSERT INTO "{table}" ({columns}) VALUES {actualValues};' \
        .format(table=table, columns=columns, actualValues=actualValues)

    return finalSql


if __name__ == '__main__':
    import doctest
    doctest.testmod()

