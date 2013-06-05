# -*- coding: utf-8 -*-

"""Postgres-specific meta-data reflection querying tools."""

__author__ = 'Jay Taylor [@jtaylor]'

import re
import logging
from ..functional import memoize


@memoize
def allTableNamesAndPrimaryKeys(using='default'):
    """@return dict of table names and lists of pks."""
    from . import db_query

    sql = '''
        SELECT "tc"."table_name" "tableName", "c"."column_name" "columnName"
        FROM "information_schema"."table_constraints" "tc"
        JOIN "information_schema"."constraint_column_usage" "ccu"
            USING ("constraint_schema", "constraint_name")
        JOIN "information_schema"."columns" "c"
            ON
                "c"."table_schema" = "tc"."constraint_schema" AND
                "tc"."table_name" = "c"."table_name" AND
                "ccu"."column_name" = "c"."column_name"
        WHERE
            "constraint_type" = 'PRIMARY KEY'
        ORDER BY "tc"."table_name" ASC
    '''

    rows = db_query(sql, using=using)

    tableToPrimaryKeys = {}

    for tableName, columnName in rows:
        if tableName not in tableToPrimaryKeys:
            tableToPrimaryKeys[tableName] = []

        tableToPrimaryKeys[tableName].append(columnName)

    return tableToPrimaryKeys


@memoize
def getPrimaryKeyColumns(table, using='default'):
    """
    @return list of strings containing the names of the columns composing the
    primary key for the table.
    """
    return allTableNamesAndPrimaryKeys(using=using).get(table, [])


@memoize
def plFunctionReturnType(function, as_dict=False, using='default'):
    """Get the return type for a user defined PL/SQL function."""
    from . import db_query

    sql = '''
        SELECT pg_catalog.format_type(pg_proc.prorettype, NULL)
        FROM pg_catalog.pg_proc
        WHERE pg_proc.proname = '{function}';
    '''.format(function=function)

    return db_query(sql, as_dict=as_dict, using=using)


@memoize
def isNullable(table, column, using='default'):
    """@return True if a column accepts null values, otherwise False."""
    from . import db_query

    sql = '''
        SELECT "is_nullable"
        FROM "information_schema"."columns"
        WHERE "table_name" = '{0}' AND "column_name" = '{1}'
    '''.format(table.replace('"', ''), column.replace("'", ''))

    result = db_query(sql, using=using)

    logging.info(u'ISNULLABLE: {0} {1} => {2}'.format(table, column, len(result) > 0 and result[0][0] == 'YES'))
    return len(result) > 0 and result[0][0] == 'YES'


@memoize
def describePublic(using='default'):
    """
    Describe all tables in the "public" namespace in the correct order
    per-table by column position.
    """
    from . import db_query

    sql = '''
        SELECT
            "p"."relname" AS "table",
            "a"."attname" AS "column",
            "pg_catalog".format_type("a"."atttypid", "a"."atttypmod") AS "type"
        FROM "pg_catalog"."pg_attribute" "a"
            LEFT JOIN "pg_catalog"."pg_class" "p" ON "p"."oid" = "a"."attrelid"
        WHERE
            NOT "a"."attisdropped" AND
            "a"."attnum" > 0 AND
            "a"."attrelid" IN (
                SELECT "c"."oid"
                FROM "pg_catalog"."pg_class" "c"
                LEFT JOIN "pg_catalog"."pg_namespace" "n" ON "n"."oid" = "c"."relnamespace"
                WHERE
                    "n"."nspname" = 'public' AND
                    "pg_catalog".pg_table_is_visible("c"."oid")
            )
        ORDER BY "p"."relname", "a"."attnum" ASC
    '''

    out = {}

    for table, column, dataType in db_query(sql, using=using):
        if table not in out:
            out[table] = []
        out[table].append((column, dataType))

    return out


@memoize
def describe(table, using='default'):
    """Describe a table's columns/types."""
    return describePublic().get(table, [])
    #from . import db_query
    #sql = '''
    #    SELECT
    #        "a"."attname" AS "column",
    #        "pg_catalog".format_type("a"."atttypid", "a"."atttypmod") AS "type"
    #    FROM "pg_catalog"."pg_attribute" "a"
    #    WHERE
    #        NOT "a"."attisdropped" AND
    #        "a"."attnum" > 0 AND
    #        "a"."attrelid" = (
    #            SELECT "c"."oid"
    #            FROM "pg_catalog"."pg_class" "c"
    #            LEFT JOIN "pg_catalog"."pg_namespace" "n"
    #                ON "n"."oid" = "c"."relnamespace"
    #            WHERE
    #                "c"."relname" = '{table}' AND
    #                "pg_catalog".pg_table_is_visible("c"."oid")
    #      )'''.format(table=table)
    #return db_query(sql, using=using)


@memoize
def listTables(using='default'):
    """Get a list of all the table names for a database."""
    from . import db_query

    rows = db_query('''
        SELECT "table_name"
        FROM "information_schema"."tables"
        WHERE "table_schema"='public'
    ''', using=using)

    return map(lambda row: row[0], rows)


_userIdRe = re.compile(r'''.*user_?id.*''', re.I)

def findUserIdColumnFromDescription(description):
    """
    NB: columns which contain a 'userId' or 'user_id' but also contain the
        string 'parent' will not count towards user id columns.

    @param description list of Tuple2(column, type).

    @return str containing the user-id related column name or None if no
        user-id column found.

    >>> print findUserIdColumnFromDescription((('id',), ('user_id',)))
    user_id

    >>> print findUserIdColumnFromDescription((('userId',), ('userId',)))
    userId

    >>> print findUserIdColumnFromDescription((('userId',), ('someOtherId',)))
    userId

    >>> print findUserIdColumnFromDescription((
    ...     ('userId',),
    ...     ('someOtherId',),
    ...     ('parentUserId',),
    ... ))
    userId

    >>> print findUserIdColumnFromDescription((
    ...     ('userId',),
    ...     ('parentUserId',),
    ...     ('someOtherId',),
    ... ))
    userId

    >>> print findUserIdColumnFromDescription((
    ...     ('parentUserId',),
    ...     ('someOtherId',),
    ...     ('user_id',),
    ... ))
    user_id

    >>> print findUserIdColumnFromDescription((('id',), ('theUserId',),))
    theUserId

    >>> print findUserIdColumnFromDescription((('parentUserId',), ('id',),))
    None

    >>> print findUserIdColumnFromDescription((('id',), ('parent_user_id',),))
    None
    """
    for column in map(lambda row: row[0], description):
        if 'parent' not in column.lower() and \
            _userIdRe.match(column) is not None:
            return column
    return None


@memoize
def findTablesWithUserIdColumn(using='default'):
    """
    Dynamically find all tables with a userId or user_id column.

    @return list of tuples of (table, userIdColumn).
    """
    out = [('auth_user', 'id')]

    for table in listTables(using=using):
        description = describe(table, using=using)
        userIdColumn = findUserIdColumnFromDescription(description)
        if userIdColumn is not None:
            out.append((table, userIdColumn))

    return out


@memoize
def discoverDependencies(tables, using='default', discovered=None):
    """
    Build an inverse dependency mapping of new pairs of (table, column) for the
    requested tables.

    Recursively find all previously unknown referencing tables.

    Pass in a list of tables of interest and get a dict of table keys
    pointing to a list of tuples of (column, fkTable, fkColumn).

    Stopping case: No new dependencies are found.

    @return dict of tables with lists of downstream relational dependencies
        (column, fkTable, fkColumn).

    e.g.:
    main_usermessage referenced by ._____ main_usermessage_contacts
                                    \____ main_usermessage_groups
                                     \___ main_receipt
                                      \__ main_block .___ etc..
                                                      \__ etc..
    NB: That textual image is inaccurate -JT
    """
    foundAny = False
    if discovered is None:
        discovered = {}

    for table in tables:
        related = filter(lambda ref: ref[0] not in tables, referencedByTables(table))

        if len(related) > 0:
            discovered[table] = list(discovered.get(table, []))

            startLength = len(discovered[table])

            map(discovered[table].append, related)

            discovered[table] = set(discovered[table])

            foundAny = foundAny or len(discovered[table]) > startLength

    from pprint import pformat
    logging.debug(pformat(discovered))

    return discovered if foundAny is False else discoverDependencies(tables, using, discovered)


@memoize
def allTableRelations(using='default'):
    """
    Get all table references organized by foreign table.

    @return (references, referencedBy)
    """
    from . import db_query

    sql = '''
        SELECT
            "tc"."table_name" "foreignTableName",
            "kcu"."column_name" "foreignColumnName",
            "ccu"."table_name" "tableName",
            "ccu"."column_name" "columnName"
        FROM "information_schema"."table_constraints" "tc"
            JOIN "information_schema"."constraint_column_usage" "ccu"
                ON "ccu"."constraint_name" = "tc"."constraint_name"
            JOIN "information_schema"."key_column_usage" "kcu"
                ON "tc"."constraint_name" = "kcu"."constraint_name"
        WHERE "tc"."constraint_type" = 'FOREIGN KEY'
        ORDER BY "tc"."table_name" ASC
    '''

    rows = db_query(sql)

    references = {}
    referencedBy = {}

    for foreignTableName, foreignColumnName, tableName, columnName in rows:
        if foreignTableName not in references:
            references[foreignTableName] = []

        references[foreignTableName].append(
            (foreignColumnName, tableName, columnName)
        )

        if tableName not in referencedBy:
            referencedBy[tableName] = []

        referencedBy[tableName].append(
            (columnName, foreignTableName, foreignColumnName)
        )

    return (references, referencedBy)


@memoize
def referencesTables(table, using='default'):
    """
    Get a list of the tables referenced by a particular table.

    @return list of (fkColumn, table, column)
    """
    return allTableRelations(using=using)[0].get(table, [])


@memoize
def referencedByTables(table, using='default', recurse=False):
    """
    Get all tables which use this table as a foreign-key.

    @return list of (column, foreignTable, fkColumn)
    """
    return allTableRelations(using=using)[1].get(table, [])


if __name__ == '__main__':
    import doctest
    doctest.testmod()

