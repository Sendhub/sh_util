# -*- coding: utf-8 -*-

"""Postgres-specific distributed operations tools."""

__author__ = "Jay Taylor [@jtaylor]"
# pylint: disable=C0103,C0103,C0301,C0415,R0913
import logging
import re

# Note: settings import kept at module level for critical configuration
import settings  # , time

from ..text import toSingleLine

_QUOTED_TEMPLATE = '"{0}"'
_CHARACTER_VARYING = "character varying"


def table_description_to_db_link_t(description, columns="*"):
    """
    Transform a tables description into a dblink "t" statement.

    e.g.: t(col type, ..)

    @param description list of dicts or tuples where each entry contains a
        column & type.
        Must have one of the following forms, e.g.:
            [
                {'column': 'id', 'type': 'integer'},
                {'column': 'name', 'type': 'character varying(128)'},
            ]
            or
            [('id', 'integer'), ('name', 'character varying(128)')]

    @param columns str or list, if str, either * or a comma delimited list of
        column names, if a list, then a list of column names.

    @return str

    >>> tableDescriptionToDbLinkT([
    ...     {'column': 'id', 'type': 'integer'},
    ...     {'column': 'name', 'type': 'character varying(128)'}
    ... ])
    't("id" integer, "name" character varying(128))'

    >>> tableDescriptionToDbLinkT(
    ...     [('id', 'integer'), ('name', 'character varying(128)')]
    ... )
    't("id" integer, "name" character varying(128))'

    >>> tableDescriptionToDbLinkT(
    ...     [('id', 'integer'), ('name', 'character varying(128)')],
    ...     '*'
    ... )
    't("id" integer, "name" character varying(128))'

    >>> tableDescriptionToDbLinkT(
    ...     [('id', 'integer'), ('name', 'character varying(128)')],
    ...     '*'
    ... )
    't("id" integer, "name" character varying(128))'

    >>> tableDescriptionToDbLinkT(
    ...     [('id', 'integer'), ('name', 'character varying(128)')],
    ...     'id'
    ... )
    't("id" integer)'

    >>> tableDescriptionToDbLinkT(
    ...     [('id', 'integer'), ('name', 'character varying(128)')],
    ...     ['id']
    ... )
    't("id" integer)'

    >>> tableDescriptionToDbLinkT(
    ...     [('id', 'integer'), ('name', 'character varying(128)')],
    ...     'id,name'
    ... )
    't("id" integer, "name" character varying(128))'

    >>> tableDescriptionToDbLinkT(
    ...     [('id', 'integer'), ('name', 'character varying(128)')],
    ...     ['id', 'name']
    ... )
    't("id" integer, "name" character varying(128))'
    """
    # Assert that description is in expected format.
    assert len(description) > 0 and all(len(row) == 2 for row in description)
    assert "column" in description[0] if hasattr(description, "keys") else True

    def _resolve_column_type_pairs(columns):
        """Resolve a columns specifier to a list of tuples
        of (column, type)."""
        # NB: r stands for 'row'.
        getColumn = lambda r: r["column"] if hasattr(r, "keys") else r[0]  # noqa
        getType = lambda r: r["type"] if hasattr(r, "keys") else r[1]  # noqa

        if columns == "*":
            column_names = list(map(getColumn, description))
        elif isinstance(columns, str) or isinstance(columns, str):
            column_names = columns.split(",")
        elif hasattr(columns, "__iter__"):
            column_names = columns
        else:
            raise ValueError("Unexpecte columns value: {0}".format(columns))

        # Prepare/organize output:
        result = [(getColumn(row), getType(row)) for row in [row for row in description if getColumn(row) in column_names]]  # noqa
        return result

    pairs = _resolve_column_type_pairs(columns)

    return "t({0})".format(", ".join(['"{0}" {1}'.format(c_t[0].strip('"'), c_t[1]) for c_t in pairs]))  # noqa


def pg_strip_double_quotes(s):
    """
    Use the included character casing if the clause is surrounded by
    doublequotes, otherwise return a lowercase form of
    the string.
    """
    if not isinstance(s, str) and not isinstance(s, str):
        return s
    return s.strip('"') if s.startswith('"') and s.endswith('"') else s.lower()


def pg_get_persistent_connection_handles(using):
    """@return List of strings of connection handle names.
    Note: This is a cheap query; should only take a few ms."""
    from . import db_query

    # This query returns a postgres list.
    handles = db_query("SELECT dblink_get_connections()", using=using)[0][0]
    return handles


def pg_connect_persistent_db_link(using, handle, psql_connection_string):
    """Create a single persistent dblink connection."""
    from . import db_exec

    logging.info('Connecting persistent dblink "%s" on connection %s', str(handle), str(using))
    db_exec("""SELECT dblink_connect('{0}', '{1}')""".format(handle, psql_connection_string), using=using)


def pg_connect_persistent_db_links(using, *handles, **custom):
    """
    Verify that a persistent dblink connection exists for each of
    the named connections.  For any connection which
    doesn't have a persistent dblink already, create it.

    NB: Take care to ensure that custom handles don't conflict with
    connection names.

    @param using string Connection name to connect the dblinks to.

    @param *specifiers List of strings of connection names.

    @param **custom Dict of desired handle -> raw psql connection string.
    """
    from . import connections, db_query, get_psql_connection_string

    if len(handles) == 0 and len(custom) == 0:
        logging.warning("pgConnectPersistentDbLinks invoked with no handles, no action taken")
        return

    connection_names = connections()

    already_connected = pg_get_persistent_connection_handles(using=using) or []

    for c in handles:
        assert c in connection_names, 'Connection "{0}" was not found in connections ({1})'.format(c, connection_names)

    # Generate a single statement to connect to all dblinks.
    connect_statements = ["""dblink_connect('{0}', '{1}')""".format(c, get_psql_connection_string(c)) for c in [c for c in handles if c not in already_connected]] + list(
        map(  # noqa
            lambda c, psql_connection_string: """dblink_connect('{0}', '{1}')""".format(c, psql_connection_string),  # noqa
            list(filter(lambda c, _: c not in already_connected, list(custom.items()))),  # noqa
        )
    )
    if len(connect_statements) > 0:
        sql = "SELECT {0}".format(", ".join(connect_statements))
        db_query(sql, using=using)


def _resolve_connections_or_shards(connections=None):
    """
    When connections is None, all shards will be returned, otherwise
    connections is returned unmodified.

    @param connections mixed List of connection names or Dict of
    handle->psqlConnectionString.  Defaults to None.  If
        None, all primary shard connections will be used.
    """
    if connections is None:
        # Default to all shards.
        from sh_util.sharding import ShardedResource

        return ShardedResource.all_shard_connection_names()

    else:
        return connections


def pg_initialize_db_links(using, connections=None):
    """
    Ensure dblinks are initialized for one or more connections.

    @param connections list Optional, defaults to None in which case all
    shard connections will be used.
    """
    resolved_connections = _resolve_connections_or_shards(connections)
    # logging.info(u'Resolved connections: {0}'.format(resolvedConnections))

    # If the number of connections is 1, then the query does not need
    # to use dblink.
    if len(resolved_connections) != 1:
        pg_connect_persistent_db_links(
            using,
            *(resolved_connections if isinstance(resolved_connections, list) else []),  # noqa
            **(resolved_connections if isinstance(resolved_connections, dict) else {}),  # noqa
        )


def evaluated_distributed_select(sql, args=None, as_dict=False, using="default", include_shard_info=False, connections=None, use_persistent_db_link=None):
    """
    Generate and then evaluate a distributed query.

    @param connections mixed List of connection names or Dict of
    handle->psqlConnectionString.  Defaults to None.  If
        None, all primary shard connections will be used.

    @param usePersistentDbLink boolean Defaults to None.
    If True or enabled by settings configuration, then persistent
        dblink connections will be initialized if they don't exist already,
        and the returned query will use them instead
        of new dblink connections.  This can result in an overall speedup when
        many dblink queries will be executed, at
        the cost to initialize and always check that the persistent dblink
        connections exist.

    @return list Evaluated result of distributed select.
    """
    from . import db_query

    if args is None:
        args = ()

    # Use supplied value if not None, otherwise read from environment.
    use_persistent_db_link = use_persistent_db_link if use_persistent_db_link is not None else getattr(settings, "SH_UTIL_USE_PERSISTENT_DBLINK", False)

    sql, args = distributed_select(sql=sql, args=args, include_shard_info=include_shard_info, connections=connections, use_persistent_db_link=use_persistent_db_link)

    # logging.info(u'usePersistentDbLink={0}'.format(usePersistentDbLink))

    if use_persistent_db_link is not False:
        pg_initialize_db_links(using, connections)

    return db_query(sql, args, using=using, as_dict=as_dict)


_stringArgumentFinder = re.compile(r"%s")

_offsetLimitRe = re.compile(r"(:?OFFSET|LIMIT)\s+\d+", re.I)


def distributed_select(sql, args=None, include_shard_info=False, connections=None, use_persistent_db_link=None, alias="q0"):
    """
    Generate a distributed query and associated args.  Note: when there is
    only one connection (or shard), the same
    sql/args will be returned to avoid doing unnecessary work.

    NB: Due to the dynamic nature of this mechanism, it will not work
    with joins.  Only use standard SELECT statements,
        without subqueries.

    @param args Positional arguments.

    @param includeShardInfo bool Defaults to False.  Whether or not to
    include a "shardId" column in the results.

    @param connections mixed List of connection names or Dict of
    handle->psqlConnectionString.  Defaults to None.  If
        None, all primary shard connections will be used.

    @param usePersistentDbLink boolean Defaults to None.  If True or
    enabled by configuration the generated query will
        use persistent named dblink connections instead of new dblink
        connections.  This can result in an overall
        speedup when many dblink queries are executed, at the cost of
        initializing and always checking that the
        persistent dblink connections exist.
    """
    import sqlparse
    from sqlparse.sql import Function, Identifier, IdentifierList, Where
    from sqlparse.tokens import Keyword, Wildcard

    from . import get_psql_connection_string

    sql = toSingleLine(sql)

    if args is None:
        args = ()

    # Remove trailing semicolons from sql.
    sql = sql.rstrip(";")

    shards = _resolve_connections_or_shards(connections)
    if isinstance(shards, dict):
        # Only interested in the connection handles.
        shards = list(shards.keys())

    # ALWAYS USE DBLINK: this is because this produces different result
    # sets (ex: dblink returns table names in the result set)
    # which makes for shitty special case programming
    # if len(shards) == 1: # and includeShardInfo is False:
    #    # Is it desirable to use DB-Link when there is only 1 shard? No..
    #    return (sql, args)

    # Use supplied value if not None, otherwise read from environment.
    use_persistent_db_link = use_persistent_db_link if use_persistent_db_link is not None else getattr(settings, "SH_UTIL_USE_PERSISTENT_DBLINK", False)

    parsed = sqlparse.parse(sql)[0]

    def _tokens_with_sub_tokens_for(*classes):
        """Generate a token list with expanded tokens for matching
        class token types."""
        tokens = []
        for token in parsed.tokens:
            if isinstance(token, classes):
                tokens += token.tokens
            else:
                tokens.append(token)
        return tokens

    def _remap_token_to_alias(token):
        """Takes a token and produces the aliased name of the field
        when applicable."""
        # logging.info('CANDIDATE IS: &{}&'.format(token))
        if not isinstance(token, str):
            # Assume this is an sqlparse token.
            tokens = [token.value, token.value.replace('"."', "_")]
        else:
            tokens = [token]

        for t in tokens:
            if t in columns_to_aliases:
                # logging.info(u'FOUND A MATCH!!! {}'.format(t))
                return columns_to_aliases[t]
            # else:
            #    logging.info(u'NOMATCHFOUNDFOR: {}'.format(t))

        return token

    def _find_where_tail(parsed):
        """
        @param parsed sqlparse result

        @return str including the `where` clause and everything after it.
        """
        seen_interesting_keyword = False
        outer_tokens = []
        extra_identifiers = []
        for token in _tokens_with_sub_tokens_for(Where, IdentifierList):
            # WHERE or GROUP BY keywords..
            if seen_interesting_keyword is not True and str(token).lower() in ("group", "limit", "order"):
                seen_interesting_keyword = True

            if seen_interesting_keyword is True:
                outer_tokens.append(token.value.replace('"."', "_"))
                if isinstance(token, Identifier) and token.value not in list(columns_to_aliases.values()) + [t.value for t in extra_identifiers]:
                    extra_identifiers.append(token)

        # Strip offsets and limits from the outermost where tail
        # (should retain only order-by clauses).
        outerTail = _offsetLimitRe.sub("", "".join(map(_remap_token_to_alias, outer_tokens)).replace("\n", " ")).strip()  # noqa
        # logging.info(u'_findWhereTail ::
        # outerTail={0}\nextraIdentifiers={1}'
        # .format(outerTail, extraIdentifiers))

        return (outerTail, extra_identifiers)

    def _find_table(parsed):
        """@return str containing the name of the table being queried."""
        # Flag to track whether or not the "FROM" keyword has been seen yet.
        seen_from_keyword = False

        for token in parsed.tokens:
            if seen_from_keyword is True and isinstance(token, Identifier):
                return token.value

            elif seen_from_keyword is not True and token.ttype is Keyword and token.value.lower() == "from":
                seen_from_keyword = True

        return None

    def _find_referenced_tables(parsed):
        """@return list of join tokens."""
        results = []
        preceded_by_join_or_from_keyword = False

        for token in parsed.tokens:
            # Skip all whitespace.
            if token.is_whitespace():
                continue

            # Determine if we'll be interested in the next token.
            if preceded_by_join_or_from_keyword is not True and token.value.lower() in ("from", "join"):
                preceded_by_join_or_from_keyword = True
                continue

            if preceded_by_join_or_from_keyword is True:
                # Enforce sanity, table ref ttypes are always none.
                assert token.ttype is None

                # Add this table reference to the results.
                results.append({"table": token.value, "alias": token.get_alias()})  # noqa

                # Reset to detect next interesting token.
                preceded_by_join_or_from_keyword = False

        return results

    def _find_columns(parsed, table):
        """@return list of strings containing the identifier clauses."""
        from ..functional import flatten
        from .reflect import describe

        table = pg_strip_double_quotes(table)

        def _find_selecting():
            """Watch for the "FROM" keyword and set a flag
            once it's been seen."""
            isInteresting = lambda token: isinstance(token, IdentifierList) or isinstance(token, Identifier) or isinstance(token, Function)  # noqa

            found = []

            # Search for columns before a "FROM" clause.
            for token in parsed.tokens:
                if str(token).lower() == "from":
                    break

                if isInteresting(token):
                    found.append(token)

            if len(found) == 0:
                # Search for columns after a "RETURNING" clause.
                active = False

                # Build list of tokens, making sure to break down everything
                # in the `WHERE` clause.
                for token in _tokens_with_sub_tokens_for(Where):
                    # Attempt to find any fields listed after a `RETURNING`
                    # clause.
                    # logging.info('>>>>>>>> {}/{}'.format(str(token),
                    # type(token)))
                    if str(token).lower() == "returning":
                        active = True

                    if active and isInteresting(token):
                        found.append(token)

            # logging.info(u'SELECTING FOUND {0}'
            # .format(map(lambda x: str(x), found)))
            return found

        selecting = _find_selecting()

        if len(selecting) == 0:
            # Maybe there is a wildcard?
            wildcards = [t for t in parsed.tokens if t.ttype is Wildcard]
            if len(wildcards) == 0:
                raise ValueError("Failed to find any columns in the select statement: {0}".format(sql))

            # A wildcard results in all columns being included.
            return ([_QUOTED_TEMPLATE.format(tup[0]) for tup in describe(table)], {})

        columns = {tup[0].lower(): tup[0] for tup in describe(table)}

        # `lambda x: x` used to fill out the generator so the contents can be
        # iterated over multiple times.
        flatIdentifiers = [x for x in flatten([s.get_identifiers() if isinstance(s, IdentifierList) else s for s in selecting])]  # noqa

        def joiner(column):
            """Transform a sqlparse column into a SELECT-clause fragment."""
            p_ident = parse_identifier(str(column))
            return "{0}{1}".format(
                columns[column.value.strip('"')] if column.value.strip('"') in columns else column.value,  # noqa
                ' AS "{0}"'.format(p_ident["alias"]) if p_ident["alias"] is not None else "",  # noqa
                # c.get_alias()) if hasattr(c, 'has_alias')
                # and c.has_alias() else ''
            )

        joined_out = list(map(joiner, flatIdentifiers))

        def column_alias_mapper(column, replace_periods=False):
            """Given an identifier, resolves to a column/alias tuple."""
            p_ident = parse_identifier(str(column))
            value = column.value.strip('"')
            if replace_periods is True:
                value = value.replace('"."', "_")
            a = columns[value] if value in columns else column.value
            b = _QUOTED_TEMPLATE.format((p_ident["alias"] if p_ident["alias"] is not None else a).strip('"'))  # noqa
            # (column.get_alias() if hasattr(column, 'has_alias') and
            # column.has_alias() else a).strip('"')
            return (a, b)

        columns_to_aliases = dict(list(map(column_alias_mapper, flatIdentifiers)) + [column_alias_mapper(c, True) for c in flatIdentifiers])
        # logging.info(u'_findColumns ::
        # joinedOut={0}\ncolumnsToAliases={1}'
        # .format(joinedOut, columnsToAliases))
        return (joined_out, columns_to_aliases)

    def _to_db_link_t(identifiers, table, list_of_referenced_tables=None):
        """
        Take parsed SQL identifiers (e.g. "id" part of "select id
        from auth_user") targeted towards an existing table
        and deduce what the t(...) statement should look like, generate
        and return it.
        """
        annotatedIdents = [parse_identifier(c, table, list_of_referenced_tables) for c in identifiers]  # noqa

        description = [
            (
                identifier["alias"] if identifier["alias"] is not None else identifier["column"],  # noqa
                identifier["type"],
            )
            for identifier in annotatedIdents
        ]
        identifier_names = [x[0] for x in description]

        db_link_t = table_description_to_db_link_t(description, identifier_names)
        return db_link_t

    def _remap_function_identifiers(identifiers, table, list_of_referenced_tables, strip_functions=False):
        """
        For distributed queries to return correct results, count(*) needs to
        be remapped to sum(*) in the outermost query.
        """
        remapped = []
        for identifier in identifiers:
            p = parse_identifier(identifier, table, list_of_referenced_tables)

            # logging.info('........identifier={}'.format(p))
            identifier = p["alias"] if p["alias"] is not None else p["column"]

            # Add quoting if appropriate.
            stripped = identifier.strip('"')
            if not stripped.endswith("*"):
                identifier = _QUOTED_TEMPLATE.format(stripped)
            del stripped

            if strip_functions is False and p["function"] is not None and p["function"].lower() in list(_aggregateFunctionTransformMappings.keys()):
                # Apply any remapping.
                p["function"] = _aggregateFunctionTransformMappings[p["function"]]  # noqa
                remapped.append(
                    "{0}({1}) {2}".format(
                        p["function"].upper(),
                        identifier if identifier != "*" else _QUOTED_TEMPLATE.format(identifier),  # noqa
                        identifier if identifier != "*" else "",
                    ).strip()
                )

            else:
                remapped.append("{0}".format(identifier))

        return remapped

    def _prepare_db_link_query(sql, extra_identifiers):
        """
        Double-quotes strings inside the dblink query.

        @param extraIdentifiers list of extra tokens to append to
        select clause.
        """
        # @FIXME This breaks for queries with incidential '%s'
        # substrings, e.g.: .. LIKE '%super%'

        def positional_callback(match):
            """
            Regex callback to determine the %s position and apply additional
            quotes if appropriate depending on the arg type.
            """
            try:
                if not any(isinstance(args[positional_callback.position], t) for t in (int, bool)):
                    # Add extra set of single quotes, which will become ''arg''
                    # once the db adds additional quotes.
                    return "''{0}''".format(match.group(0))
                return match.group(0)

            finally:
                positional_callback.position += 1

        positional_callback.position = 0

        # First, change all existing single quotes to 2 single quotes.
        db_link_sql = sql.replace("'", "''")

        if len(args) > 0:
            # Then add 2 single quotes around any %s string arguments.
            db_link_sql = _stringArgumentFinder.sub(positional_callback, db_link_sql)

        return re.sub(r"([\n ])FROM([\n ])", r", {0}\1FROM\2".format(", ".join(extra_identifiers)), db_link_sql, 1) if len(extra_identifiers) > 0 else db_link_sql  # noqa

    def _prepare_grouping_tail(identifiers, table, list_of_referenced_tables, outer_where_tail):
        """Identify and extract grouping clause to generate outer query
        grouping clause."""
        # For counts or sums where that was the only thing queried,
        # chop off the
        # "where" portion of the outermost query.
        # logging.info('OOOOOOOOOOOOUTER WHERE TAIL={}'.format(outerWhereTail))
        initial = "GROUP BY"
        where_tail = outer_where_tail or initial
        next_token = " " if outer_where_tail else ", "

        if len(identifiers) == 1:
            ident = parse_identifier(identifiers[0], table, list_of_referenced_tables)
            if ident["function"] == "count" and include_shard_info is True:
                where_tail += '{0}"shard"'.format(next_token)

        else:
            # List of parsed identifiers.
            pids = [parse_identifier(i, table, list_of_referenced_tables) for i in identifiers]  # noqa
            # List of aggregate function names.
            aggregates = list(_sqlFunctionTypeMappings.keys())
            # Check for aggregate function mixed with fields, and create
            # appropriate group-by clause.
            containsAggregate = len([pi for pi in pids if pi["function"] in aggregates]) > 0  # noqa
            if containsAggregate is True:
                where_tail += "{0}{1}".format(
                    next_token,
                    ", ".join([pi["column"] for pi in [pi for pi in pids if pi["function"] not in aggregates]]),  # noqa
                )

        # logging.info('!!!!!!!!!!!! {}'.format(whereTail))
        return where_tail if where_tail != initial else ""

    table = _find_table(parsed)
    list_of_referenced_tables = _find_referenced_tables(parsed)

    # NB: @var columnsToAliases Dict of column name to alias.
    # Used to generate a proper outer tail.
    identifiers, columns_to_aliases = _find_columns(parsed, table)
    # logging.info(u'columnsToAliases={0}'.format(columnsToAliases))

    outer_where_tail, extra_identifiers = _find_where_tail(parsed)

    # Create inner identifiers set.
    innerIdentifiers = [t.value for t in [t for t in extra_identifiers if _remap_token_to_alias(t) not in identifiers]]  # noqa

    db_link_t = _to_db_link_t(identifiers + innerIdentifiers, table, list_of_referenced_tables)

    std_args = (identifiers, table, list_of_referenced_tables)

    # Sometimes count(*) needs to be remapped to sum(*) in the outermost query.
    remappedIdentifiers = _remap_function_identifiers(*std_args) + (["shard"] if include_shard_info is True else [])  # noqa

    grouping_tail = _prepare_grouping_tail(*std_args, outer_where_tail=outer_where_tail)

    # Get SQL with single quotes -> double single quotes.
    db_link_sql = _prepare_db_link_query(sql, innerIdentifiers)
    # logging.info('usePersistentDbLink={}'.format(usePersistentDbLink))

    multi_shard_sql = "\nUNION ALL\n".join(
        [
            """SELECT *{maybeSelectShardId}
                    FROM dblink('{connectionString}', '{db_link_sql}') AS
                    {tClause}""".format(
                maybeSelectShardId=''', '{0}' AS
                    "shard"'''.format(shard)
                if include_shard_info is True
                else "",  # noqa
                # Generate the dblink connection string if not using
                # persistent, otherwise just use the connection name.
                connectionString=get_psql_connection_string(shard) if not use_persistent_db_link else shard,  # noqa
                db_link_sql=db_link_sql,
                tClause=db_link_t,
            )
            for shard in shards
        ]
    )

    if len(innerIdentifiers) > 0:
        # Sometimes count(*) needs to be remapped to
        # sum(*) in the outermost query.
        outer_remapped_identifiers = _remap_function_identifiers(*std_args, strip_functions=True) + (["shard"] if include_shard_info is True else [])

        distributed_sql = "SELECT {outerRemapped}\nFROM (SELECT {remapped}, {inner} FROM (\n{multi_shard_sql}\n) {alias} {tail}) q1".format(
            outerRemapped=", ".join(outer_remapped_identifiers),
            remapped=", ".join(remappedIdentifiers),
            inner=", ".join([i.replace('"."', "_") for i in innerIdentifiers]),  # noqa
            multi_shard_sql=multi_shard_sql,
            alias=alias,
            tail=grouping_tail,
        ).strip()

    else:
        distributed_sql = "SELECT {remapped} FROM (\n{multi_shard_sql}\n) {alias} {tail}".format(
            remapped=", ".join(remappedIdentifiers),
            multi_shard_sql=multi_shard_sql,
            alias=alias,
            tail=grouping_tail,
        ).strip()

    if settings.DEBUG is True:
        logging.debug("IN: %s", str(sql))
        logging.debug("OUT: %s", str(distributed_sql))

    return (distributed_sql % (args * len(shards))).replace("%", "%%"), ()


# Some aggregate functions require remapping in the outermost
# part of the distributed query to produce the expected
# combined result.  e.g. count -> sum
_aggregateFunctionTransformMappings = {
    "count": "sum",
}

_aggregateFunctionTypeMappings = {
    # NB: <T> is used to indicate the same as the underlying type of the input.
    "avg": "numeric",
    "bit_and": "<T>",
    "bit_or": "<T>",
    "bool_and": "bool",
    "bool_or": "bool",
    "count": "bigint",
    "every": "bool",
    "max": "<T>",
    "min": "<T>",
    "string_agg": "<T>",
    "sum": "numeric",
    "to_char": _CHARACTER_VARYING,
    "xmlagg": "xml",
}

# Recognized functions:
_sqlFunctionTypeMappings = dict(
    list(
        {
            "to_char": _CHARACTER_VARYING,
            "array_agg": "bigint[]",  # NB: actually returns array[T] (Not fully supported, bigint[] is just a common case).  # noqa
        }.items()
    )
    + list(_aggregateFunctionTypeMappings.items())
)

# Regex complexity here is inherent to correctly parsing an optionally
# function-wrapped, optionally-aliased SQL identifier; restructuring risks
# changing parsing behavior for this existing, exercised regex. Resolved as
# Won't Fix in SonarQube (see the SonarQube issue-status workflow) rather
# than reworked, to avoid destabilizing a working parser.
_identifierParserRe = re.compile(
    r"""
        ^\s*
        (?P<identifier>(?:\w+\()?(?P<column>.*?)(?:\))?)
        (?:\s+(?:as\s+)?(?P<alias>(?:[a-z0-9_]+|"[^"]+"?)))?
        \s*$
    """,
    re.I | re.X,
)

_functionParserRe = re.compile(r"""^(?P<function>{0})\(\s*(?P<arg1>.*?)(?P<rest>(?:\s*,\s*.*?\s*)*)\)$""".format("|".join(list(_sqlFunctionTypeMappings.keys()))), re.I)

_tableColumnRe = re.compile(r'(?P<table>"?[a-z0-9_]+"?)\.(?P<column>"?[a-z0-9_]+"?)(?: .*)?', re.I)  # noqa


def parse_identifier(identifier_fragment, table=None, list_of_referenced_tables=None):
    """
    Parse an identifier (e.g. the `avg(score) myScore` portion of the
    statement `select avg(score) myScore from x` into
    it's constituent parts.

    NB: In instances where there is ambiguity with regard to what the return
    type will be, we default to
        'character varying'.  See the postgresql documentation for more info:
        http://www.postgresql.org/docs/9.2/static/functions-aggregate.html

    @param identifierFragment str containing SQL fragment to parse.

    @param table Optional str name of table to use to match columns with
    return type.

    @param listOfReferencedTables list of dictionaries of 'table' and
    'alias' keys, matching the format returned by
        _findReferencedTables().

    @return dict containing the parsed identifier.
        e.g. {'column': 'score', alias: 'myScore', 'type': 'bigint'}
    """
    from .reflect import describe, pl_function_return_type

    if list_of_referenced_tables is None:
        list_of_referenced_tables = []

    m = _identifierParserRe.match(identifier_fragment)
    if m is None:
        raise ValueError('No identifer found in "{0}"'.format(identifier_fragment))

    out = {"function": None}

    out["identifier"], out["column"], out["alias"] = list(map(pg_strip_double_quotes, m.groups()))  # noqa
    # logging.info(u'in={}, column={}, alias={}'
    # .format(identifierFragment, out['column'], out['alias']))

    def _find_column(name):
        """Try to find a specific column name from the table description."""
        # Test for table.column or "table"."column"-style column name:
        # logging.info('NAME={}'.format(name))
        table_column_match = _tableColumnRe.match(out["column"])
        if table_column_match is not None:
            name = table_column_match.group("column").replace('"', "")
            _table = table_column_match.group("table").replace('"', "")

            # Resolve prefix containing candidate table alias.
            for ref in list_of_referenced_tables:
                if ref["alias"] == _table:
                    _table = ref["table"].strip('"')
                    break

        else:
            _table = None

        if table is None and _table is None:
            return None

        column = [c for c in describe(pg_strip_double_quotes(_table or table)) if c[0].lower() == name.lower()]  # noqa

        if len(column) > 0:
            out["column"] = "{0}{1}".format('"{0}".'.format(_table) if _table is not None else "", _QUOTED_TEMPLATE.format(column[0][0].replace('"', "")))
            out["type"] = column[0][1]

        return column[0] if len(column) > 0 else None

    def _attempt_type_inference():
        """Infer the identifiers return type."""
        aggregate_test = _functionParserRe.match(out["identifier"])
        if aggregate_test is None:
            return

        out["function"], arg1, rest = list(map(pg_strip_double_quotes, aggregate_test.groups()))  # noqa

        out["function"] = out["function"].lower()

        out["args"] = "{0}{1}".format(arg1, rest)

        # @FIXME Assuming this will contain the column of interest is very
        # naiive; not a safe assumption.
        found = _find_column(out["column"])
        if found is None and _find_column(arg1) is not None:
            out["column"] = arg1

        # Function return type inference/lookup.
        if out["function"] in _aggregateFunctionTypeMappings:
            out["type"] = _sqlFunctionTypeMappings[out["function"]]

        else:
            # If not in _aggregateFunctionTypeMappings, try to query
            # for the return type.
            return_type = pl_function_return_type(out["function"])
            if len(return_type) > 0:
                out["type"] = return_type[0][0]

        if "type" not in out:
            logging.warning("[WARN] distributed.parseIdentifier type inference failed, out=%s", str(out))

    _attempt_type_inference()

    # Do our best to infer the type if the attempt failed or resulted in
    # a '<T>'.
    found = _find_column(out["identifier"])
    if "type" not in out or out["type"] == "<T>":
        # Try to find the column type from the description of the table.
        if found is not None:
            out["type"] = found[1]
        else:
            # Default.
            out["type"] = _CHARACTER_VARYING

    # @TODO Add support for inferring `1 as q` as bigint,
    # 'someval' as character varying, etc.

    # NB: For our purposes, the column will always be referred to by the
    # full auto-generated alias (with underscores)
    # rather than the table.column.
    out["column"] = out["column"].replace('"."', "_")
    return out


def multi_shard_exec(sql):
    """Execute a statement across all shards."""
    from sh_util.sharding import ShardedResource

    from . import db_exec

    for connection_name in ShardedResource.all_shard_connection_names():
        db_exec(sql, using=connection_name)


if __name__ == "__main__":
    import doctest

    doctest.testmod()
