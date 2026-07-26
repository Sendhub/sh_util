"""Postgres-specific meta-data reflection querying tools."""

__author__ = "Jay Taylor [@jtaylor]"

import logging
import re

from ..functional import memoize


@memoize
def all_table_names_and_primary_keys(using="default"):
    """@return dict of table names and lists of pks."""
    from . import db_query

    sql = """
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
    """

    rows = db_query(sql, using=using)

    table_to_primary_keys = {}

    for table_name, column_name in rows:
        if table_name not in table_to_primary_keys:
            table_to_primary_keys[table_name] = []

        table_to_primary_keys[table_name].append(column_name)

    return table_to_primary_keys


@memoize
def get_primary_key_columns(table, using="default"):
    """
    @return list of strings containing the names of the columns composing the
    primary key for the table.
    """
    return all_table_names_and_primary_keys(using=using).get(table, [])


def update_primary_key_id(table, current_id, new_id, using):
    """Update a primary-key id to a new value everywhere it is referenced."""
    from . import db_exec

    pk_columns = get_primary_key_columns(table)
    assert len(pk_columns) == 1, 'updatePrimaryKeyId can only operate on tables with 1 primary key, but table "{}" had {}'.format(table, len(pk_columns))
    discovered_relations = discover_dependencies([table])
    # NB: If the table is not in the returned dict,
    # then there are no dependencies.
    if table in discovered_relations:
        relations = discovered_relations[table]
        db_exec("SET CONSTRAINTS ALL DEFERRED", using=using)
        for _, rel_table, rel_column in relations:
            db_exec('UPDATE "{rel_table}" SET "{rel_column}" = {new_id} WHERE "{rel_column}" = {current_id}'.format(rel_table=rel_table, rel_column=rel_column, new_id=new_id, current_id=current_id), using=using)
    db_exec('UPDATE "{table}" SET "{pkColumn}" = {new_id} WHERE "{pkColumn}" = {current_id}'.format(table=table, pkColumn=pk_columns[0], new_id=new_id, current_id=current_id), using=using)


@memoize
def pl_function_return_type(function, as_dict=False, using="default"):
    """Get the return type for a user defined PL/SQL function."""
    from . import db_query

    sql = """
        SELECT pg_catalog.format_type(pg_proc.prorettype, NULL)
        FROM pg_catalog.pg_proc
        WHERE pg_proc.proname = '{function}';
    """.format(function=function)

    return db_query(sql, as_dict=as_dict, using=using)


@memoize
def is_nullable(table, column, using="default"):
    """@return True if a column accepts null values, otherwise False."""
    from . import db_query

    sql = """
        SELECT "is_nullable"
        FROM "information_schema"."columns"
        WHERE "table_name" = '{}' AND "column_name" = '{}'
    """.format(table.replace('"', ""), column.replace("'", ""))

    result = db_query(sql, using=using)

    logging.info("ISNULLABLE: %s %s => %s", str(table), str(column), str(len(result) > 0 and result[0][0] == "YES"))
    return len(result) > 0 and result[0][0] == "YES"


@memoize
def describe_public(using="default"):
    """
    Describe all tables in the "public" namespace in the correct order
    per-table by column position.
    """
    from . import db_query

    sql = """
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
                LEFT JOIN "pg_catalog"."pg_namespace" "n" ON
                "n"."oid" = "c"."relnamespace"
                WHERE
                    "n"."nspname" = 'public' AND
                    "pg_catalog".pg_table_is_visible("c"."oid")
            )
        ORDER BY "p"."relname", "a"."attnum" ASC
    """

    out = {}

    for table, column, data_type in db_query(sql, using=using):
        if table not in out:
            out[table] = []
        out[table].append((column, data_type))

    return out


@memoize
def describe(table, using="default"):
    """Describe a table's columns/types."""
    return describe_public().get(table, [])


@memoize
def list_tables(using="default"):
    """Get a list of all the table names for a database."""
    from . import db_query

    rows = db_query(
        """
        SELECT "table_name"
        FROM "information_schema"."tables"
        WHERE "table_schema"='public'
    """,
        using=using,
    )

    return [row[0] for row in rows]


_userIdRe = re.compile(r""".*user_?id.*""", re.I)


def find_user_id_column_from_description(description):
    """
    NB: columns which contain a 'user_id' or 'user_id' but also contain the
        string 'parent' will not count towards user id columns.

    @param description list of Tuple2(column, type).

    @return str containing the user-id related column name or None if no
        user-id column found.

    >>> print findUserIdColumnFromDescription((('id',), ('user_id',)))
    user_id

    >>> print findUserIdColumnFromDescription((('user_id',), ('user_id',)))
    user_id

    >>> print findUserIdColumnFromDescription((('user_id',), ('someOtherId',)))
    user_id

    >>> print findUserIdColumnFromDescription((
    ...     ('user_id',),
    ...     ('someOtherId',),
    ...     ('parentUserId',),
    ... ))
    user_id

    >>> print findUserIdColumnFromDescription((
    ...     ('user_id',),
    ...     ('parentUserId',),
    ...     ('someOtherId',),
    ... ))
    user_id

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
    for column in [row[0] for row in description]:
        if "parent" not in column.lower() and _userIdRe.match(column) is not None:
            return column
    return None


@memoize
def find_tables_with_user_id_column(using="default"):
    """
    Dynamically find all tables with a user_id or user_id column.

    @return list of tuples of (table, userIdColumn).
    """
    out = [("auth_user", "id")]

    for table in list_tables(using=using):
        description = describe(table, using=using)
        user_id_column = find_user_id_column_from_description(description)
        if user_id_column is not None:
            out.append((table, user_id_column))

    return out


@memoize
def discover_dependencies(tables, using="default", discovered=None):
    r"""
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
                                    |____ main_usermessage_groups
                                    |____ main_receipt
                                    |____ main_block    .___ etc..
                                                        |__ etc..
    NB: That textual image is inaccurate -JT
    """
    found_any = False
    if discovered is None:
        discovered = {}

    for table in tables:
        related = [ref for ref in referenced_by_tables(table) if ref[0] not in tables]  # noqa

        if len(related) > 0:
            discovered[table] = list(discovered.get(table, []))

            start_length = len(discovered[table])

            list(map(discovered[table].append, related))

            discovered[table] = set(discovered[table])

            found_any = found_any or len(discovered[table]) > start_length

    from pprint import pformat

    logging.debug(pformat(discovered))

    return discovered if found_any is False else discover_dependencies(tables, using, discovered)


@memoize
def all_table_relations(using="default"):
    """
    Get all table references organized by foreign table.

    @return (references, referencedBy)
    """
    from . import db_query

    sql = """
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
    """

    rows = db_query(sql)

    references = {}
    referenced_by = {}

    for foreign_table_name, foreign_column_name, table_name, column_name in rows:
        if foreign_table_name not in references:
            references[foreign_table_name] = []

        references[foreign_table_name].append((foreign_column_name, table_name, column_name))

        if table_name not in referenced_by:
            referenced_by[table_name] = []

        referenced_by[table_name].append((column_name, foreign_table_name, foreign_column_name))

    return (references, referenced_by)


@memoize
def references_tables(table, using="default"):
    """
    Get a list of the tables referenced by a particular table.

    @return list of (fkColumn, table, column)
    """
    return all_table_relations(using=using)[0].get(table, [])


@memoize
def referenced_by_tables(table, using="default", recurse=False):
    """
    Get all tables which use this table as a foreign-key.

    @return list of (column, foreignTable, fkColumn)
    """
    return all_table_relations(using=using)[1].get(table, [])


if __name__ == "__main__":
    import doctest

    doctest.testmod()
