# -*- coding: utf-8 -*-

"""Postgres-specific distributed operations tools."""

__author__ = 'Jay Taylor [@jtaylor]'

import logging, re, settings #, time
from ..text import toSingleLine


def tableDescriptionToDbLinkT(description, columns='*'):
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
    assert len(description) > 0 and all(map(lambda row: len(row) == 2, description))
    assert 'column' in description[0] if hasattr(description, 'keys') else True

    def _resolveColumnTypePairs(columns):
        """Resolve a columns specifier to a list of tuples of (column, type)."""
        # NB: r stands for 'row'.
        getColumn = lambda r: r['column'] if hasattr(r, 'keys') else r[0]
        getType = lambda r: r['type'] if hasattr(r, 'keys') else r[1]

        if columns == '*':
            columnNames = map(getColumn, description)
        elif isinstance(columns, str) or isinstance(columns, unicode):
            columnNames = columns.split(',')
        elif hasattr(columns, '__iter__'):
            columnNames = columns
        else:
            raise Exception('Unexpecte columns value: {0}'.format(columns))

        # Prepare/organize output:
        result = map(
            lambda row: (getColumn(row), getType(row)),
            filter(lambda row: getColumn(row) in columnNames, description)
        )
        return result

    pairs = _resolveColumnTypePairs(columns)

    return 't({0})'.format(', '.join(map(lambda (c, t): '"{0}" {1}'.format(c.strip('"'), t), pairs)))


def pgStripDoubleQuotes(s):
    """
    Use the included character casing if the clause is surrounded by doublequotes, otherwise return a lowercase form of
    the string.
    """
    if not isinstance(s, str) and not isinstance(s, unicode):
        return s
    return s.strip('"') if s.startswith('"') and s.endswith('"') else s.lower()


def pgGetPersistentConnectionHandles(using):
    """@return List of strings of connection handle names.  Note: This is a cheap query; should only take a few ms."""
    from . import db_query
    # This query returns a postgres list.
    handles = db_query('SELECT dblink_get_connections()', using=using)[0][0]
    return handles


def pgConnectPersistentDbLink(using, handle, psqlConnectionString):
    """Create a single persistent dblink connection."""
    from . import db_exec
    logging.info(u'Connecting persistent dblink "{0}" on connection {1}'.format(handle, using))
    db_exec('''SELECT dblink_connect('{0}', '{1}')'''.format(handle, psqlConnectionString), using=using)


def pgConnectPersistentDbLinks(using, *handles, **custom):
    """
    Verify that a persistent dblink connection exists for each of the named connections.  For any connection which
    doesn't have a persistent dblink already, create it.

    NB: Take care to ensure that custom handles don't conflict with connection names.

    @param using string Connection name to connect the dblinks to.

    @param *specifiers List of strings of connection names.

    @param **custom Dict of desired handle -> raw psql connection string.
    """
    from . import connections, db_query, getPsqlConnectionString

    if len(handles) == 0 and len(custom) == 0:
        logging.warn('pgConnectPersistentDbLinks invoked with no handles, no action taken')
        return

    connectionNames = connections()

    alreadyConnected = pgGetPersistentConnectionHandles(using=using) or []

    for c in handles:
        assert c in connectionNames, 'Connection "{0}" was not found in connections ({1})' \
            .format(c, connectionNames)

    # Generate a single statement to connect to all dblinks.
    connectStatements = map(
        lambda c: '''dblink_connect('{0}', '{1}')'''.format(c, getPsqlConnectionString(c)),
        filter(lambda c: c not in alreadyConnected, handles)
    ) + map(
        lambda c, psqlConnectionString: '''dblink_connect('{0}', '{1}')'''.format(c, psqlConnectionString),
        filter(lambda c, _: c not in alreadyConnected, custom.items())
    )
    if len(connectStatements) > 0:
        sql = 'SELECT {0}'.format(', '.join(connectStatements))
        db_query(sql, using=using)


def _resolveConnectionsOrShards(connections=None):
    """
    When connections is None, all shards will be returned, otherwise connections is returned unmodified.

    @param connections mixed List of connection names or Dict of handle->psqlConnectionString.  Defaults to None.  If
        None, all primary shard connections will be used.
    """
    if connections is None:
        # Default to all shards.
        from sh_util.sharding import ShardedResource
        return ShardedResource.allShardConnectionNames()

    else:
        return connections


def pgInitializeDbLinks(using, connections=None):
    """
    Ensure dblinks are initialized for one or more connections.

    @param connections list Optional, defaults to None in which case all shard connections will be used.
    """
    resolvedConnections = _resolveConnectionsOrShards(connections)
    #logging.info(u'Resolved connections: {0}'.format(resolvedConnections))

    # If the number of connections is 1, then the query does not need to use dblink.
    if len(resolvedConnections) != 1:
        pgConnectPersistentDbLinks(
            using,
            *(resolvedConnections if isinstance(resolvedConnections, list) else []),
            **(resolvedConnections if isinstance(resolvedConnections, dict) else {})
        )


def evaluatedDistributedSelect(
   sql,
    args=None,
    asDict=False,
    using='default',
    includeShardInfo=False,
    connections=None,
    usePersistentDbLink=None
):
    """
    Generate and then evaluate a distributed query.

    @param connections mixed List of connection names or Dict of handle->psqlConnectionString.  Defaults to None.  If
        None, all primary shard connections will be used.

    @param usePersistentDbLink boolean Defaults to None.  If True or enabled by settings configuration, then persistent
        dblink connections will be initialized if they don't exist already, and the returned query will use them instead
        of new dblink connections.  This can result in an overall speedup when many dblink queries will be executed, at
        the cost to initialize and always check that the persistent dblink connections exist.

    @return list Evaluated result of distributed select.
    """
    from . import db_query

    if args is None:
        args = tuple()

    # Use supplied value if not None, otherwise read from environment.
    usePersistentDbLink = usePersistentDbLink if usePersistentDbLink is not None \
        else getattr(settings, 'SH_UTIL_USE_PERSISTENT_DBLINK', False)

    sql, args = distributedSelect(
        sql=sql,
        args=args,
        includeShardInfo=includeShardInfo,
        connections=connections,
        usePersistentDbLink=usePersistentDbLink
    )

    #logging.info(u'usePersistentDbLink={0}'.format(usePersistentDbLink))

    if usePersistentDbLink is not False:
        pgInitializeDbLinks(using, connections)

    return db_query(sql, args, using=using, as_dict=asDict)


_stringArgumentFinder = re.compile(r'%s')

_offsetLimitRe = re.compile(r'(:?OFFSET|LIMIT)\s+\d+', re.I)

def distributedSelect(sql, args=None, includeShardInfo=False, connections=None, usePersistentDbLink=None):
    """
    Generate a distributed query and associated args.  Note: when there is only one connection (or shard), the same
    sql/args will be returned to avoid doing unnecessary work.

    NB: Due to the dynamic nature of this mechanism, it will not work with joins.  Only use standard SELECT statements,
        without subqueries.

    @param args Positional arguments.

    @param includeShardInfo bool Defaults to False.  Whether or not to include a "shardId" column in the results.

    @param connections mixed List of connection names or Dict of handle->psqlConnectionString.  Defaults to None.  If
        None, all primary shard connections will be used.

    @param usePersistentDbLink boolean Defaults to None.  If True or enabled by configuration the generated query will
        use persistent named dblink connections instead of new dblink connections.  This can result in an overall
        speedup when many dblink queries are executed, at the cost of initializing and always checking that the
        persistent dblink connections exist.
    """
    import sqlparse
    from sqlparse.sql import Identifier, IdentifierList, Function, Where
    from sqlparse.tokens import Keyword, Wildcard
    from . import getPsqlConnectionString
    #startedTs = time.time()

    sql = toSingleLine(sql)

    if args is None:
        args = tuple()

    # Remove trailing semicolons from sql.
    sql = sql.rstrip(';')

    shards = _resolveConnectionsOrShards(connections)
    if isinstance(shards, dict):
        # Only interested in the connection handles.
        shards = shards.keys()

    if len(shards) == 1: # and includeShardInfo is False:
        # Is it desirable to use DB-Link when there is only 1 shard? No..
        return (sql, args)

    # Use supplied value if not None, otherwise read from environment.
    usePersistentDbLink = usePersistentDbLink if usePersistentDbLink is not None \
        else getattr(settings, 'SH_UTIL_USE_PERSISTENT_DBLINK', False)

    parsed = sqlparse.parse(sql)[0]

    def _tokensWithSubTokensFor(*classes):
        """Generate a token list with expanded tokens for matching class token types."""
        tokens = []
        for token in parsed.tokens:
            if isinstance(token, classes):
                tokens += token.tokens
            else:
                tokens.append(token)
        return tokens

    def _remapTokenToAlias(token):
        """Takes a token and produces the aliased name of the field when applicable."""
        #logging.info('CANDIDATE IS: &{}&'.format(token))
        if not isinstance(token, (str, unicode)):
            # Assume this is an sqlparse token.
            tokens = [token.value, token.value.replace('"."', '_')]
        else:
            tokens = [token]

        for t in tokens:
            if t in columnsToAliases:
                #logging.info(u'FOUND A MATCH!!! {}'.format(t))
                return columnsToAliases[t]
            #else:
            #    logging.info(u'NOMATCHFOUNDFOR: {}'.format(t))

        return token

    def _findWhereTail(parsed):
        """
        @param parsed sqlparse result

        @return str including the `where` clause and everything after it.
        """
        seenInterestingKeyword = False
        outerTokens = []
        extraIdentifiers = []
        for token in _tokensWithSubTokensFor(Where, IdentifierList):
            # WHERE or GROUP BY keywords..
            if seenInterestingKeyword is not True and str(token).lower() in ('group', 'limit', 'order'):
                seenInterestingKeyword = True

            if seenInterestingKeyword is True:
                outerTokens.append(token.value.replace('"."', '_'))
                if isinstance(token, Identifier) and token.value not in \
                    columnsToAliases.values() + map(lambda t: t.value, extraIdentifiers):
                    extraIdentifiers.append(token)

        # Strip offsets and limits from the outermost where tail (should retain only order-by clauses).
        outerTail = _offsetLimitRe.sub('', ''.join(map(_remapTokenToAlias, outerTokens)).replace('\n', ' ')).strip()
        #logging.info(u'_findWhereTail :: outerTail={0}\nextraIdentifiers={1}'.format(outerTail, extraIdentifiers))

        return (outerTail, extraIdentifiers)

    def _findTable(parsed):
        """@return str containing the name of the table being queried."""
        # Flag to track whether or not the "FROM" keyword has been seen yet.
        seenFromKeyword = False

        for token in parsed.tokens:
            if seenFromKeyword is True and isinstance(token, Identifier):
                return token.value

            elif seenFromKeyword is not True and token.ttype is Keyword and token.value.lower() == 'from':
                seenFromKeyword = True

        return None

    def _findReferencedTables(parsed):
        """@return list of join tokens."""
        results = []
        precededByJoinOrFromKeyword = False

        for token in parsed.tokens:
            # Skip all whitespace.
            if token.is_whitespace():
                continue

            # Determine if we'll be interested in the next token.
            if precededByJoinOrFromKeyword is not True and token.value.lower() in ('from', 'join'):
                precededByJoinOrFromKeyword = True
                continue

            if precededByJoinOrFromKeyword is True:
                # Enforce sanity, table ref ttypes are always none.
                assert token.ttype is None

                # Add this table reference to the results.
                results.append({'table': token.value, 'alias': token.get_alias()})

                # Reset to detect next interesting token.
                precededByJoinOrFromKeyword = False

        return results

    def _findColumns(parsed, table):
        """@return list of strings containing the identifier clauses."""
        from ..functional import flatten
        from .reflect import describe

        table = pgStripDoubleQuotes(table)

        def _findSelecting():
            """Watch for the "FROM" keyword and set a flag once it's been seen."""
            isInteresting = lambda token: \
                isinstance(token, IdentifierList) or isinstance(token, Identifier) or isinstance(token, Function)

            found = []

            # Search for columns before a "FROM" clause.
            for token in parsed.tokens:
                if str(token).lower() == 'from':
                    break

                if isInteresting(token):
                    found.append(token)

            if len(found) == 0:
                # Search for columns after a "RETURNING" clause.
                active = False

                # Build list of tokens, making sure to break down everything in the `WHERE` clause.
                for token in _tokensWithSubTokensFor(Where):
                    # Attempt to find any fields listed after a `RETURNING` clause.
                    #logging.info('>>>>>>>> {}/{}'.format(str(token), type(token)))
                    if str(token).lower() == 'returning':
                        active = True

                    if active and isInteresting(token):
                        found.append(token)

            #logging.info(u'SELECTING FOUND {0}'.format(map(lambda x: str(x), found)))
            return found

        selecting = _findSelecting()

        if len(selecting) is 0:
            # Maybe there is a wildcard?
            wildcards = filter(lambda t: t.ttype is Wildcard, parsed.tokens)
            if len(wildcards) == 0:
                raise Exception('Failed to find any columns in the select statement: {0}'.format(sql))

            # A wildcard results in all columns being included.
            return (map(lambda tup: '"{0}"'.format(tup[0]), describe(table)), {})

        columns = dict(map(lambda tup: (tup[0].lower(), tup[0]), describe(table)))

        # `lambda x: x` used to fill out the generator so the contents can be iterated over multiple times.
        flatIdentifiers = map(lambda x: x, flatten(map(
            lambda s: s.get_identifiers() if isinstance(s, IdentifierList) else s,
            selecting
        )))

        def joiner(column):
            """Transform a sqlparse column into a SELECT-clause fragment."""
            pIdent = parseIdentifier(str(column))
            return '{0}{1}'.format(
                columns[column.value.strip('"')] if column.value.strip('"') in columns else column.value,
                ' AS "{0}"'.format(pIdent['alias']) if pIdent['alias'] is not None else ''
                #c.get_alias()) if hasattr(c, 'has_alias') and c.has_alias() else ''
            )

        joinedOut = map(joiner, flatIdentifiers)

        def columnAliasMapper(column, replacePeriods=False):
            """Given an identifier, resolves to a column/alias tuple."""
            pIdent = parseIdentifier(str(column))
            value = column.value.strip('"')
            if replacePeriods is True:
                value = value.replace('"."', '_')
            a = columns[value] if value in columns else column.value
            b = '"{0}"'.format((pIdent['alias'] if pIdent['alias'] is not None else a).strip('"')
                #(column.get_alias() if hasattr(column, 'has_alias') and column.has_alias() else a).strip('"')
            )
            return (a, b)

        columnsToAliases = dict(
            map(columnAliasMapper, flatIdentifiers) +
            map(lambda c: columnAliasMapper(c, True), flatIdentifiers)
        )
        #logging.info(u'_findColumns :: joinedOut={0}\ncolumnsToAliases={1}'.format(joinedOut, columnsToAliases))
        return (joinedOut, columnsToAliases)

    def _toDbLinkT(identifiers, table, listOfReferencedTables=None):
        """
        Take parsed SQL identifiers (e.g. "id" part of "select id from auth_user") targeted towards an existing table
        and deduce what the t(...) statement should look like, generate and return it.
        """
        annotatedIdents = map(lambda c: parseIdentifier(c, table, listOfReferencedTables), identifiers)

        description = map(
            lambda identifier: (
                identifier['alias'] if identifier['alias'] is not None else identifier['column'],
                identifier['type']
            ),
            annotatedIdents
        )
        identifierNames = map(lambda x: x[0], description)

        #logging.info(u'_toDbLinkT :: annotatedIdents={0}'.format(annotatedIdents))
        #logging.info(u'_toDbLinkT :: description={0}, identifierNames={1}'.format(description, identifierNames))
        dbLinkT = tableDescriptionToDbLinkT(description, identifierNames)
        return dbLinkT

    def _remapFunctionIdentifiers(identifiers, table, listOfReferencedTables, stripFunctions=False):
        """
        For distributed queries to return correct results, count(*) needs to
        be remapped to sum(*) in the outermost query.
        """
        remapped = []
        for identifier in identifiers:

            p = parseIdentifier(identifier, table, listOfReferencedTables)

            #logging.info('........identifier={}'.format(p))
            identifier = p['alias'] if p['alias'] is not None else p['column']

            # Add quoting if appropriate.
            stripped = identifier.strip('"')
            if not stripped.endswith('*'):
                identifier = '"{0}"'.format(stripped)
            del stripped

            if stripFunctions is False and p['function'] is not None and \
                p['function'].lower() in _aggregateFunctionTransformMappings.keys():
                # Apply any remapping.
                p['function'] = _aggregateFunctionTransformMappings[p['function']]
                remapped.append('{0}({1}) {2}'.format(
                    p['function'].upper(),
                    identifier if identifier != '*' else '"{0}"'.format(identifier),
                    identifier if identifier != '*' else ''
                ).strip())

            else:
                remapped.append('{0}'.format(identifier))

        return remapped

    def _prepareDbLinkQuery(sql, extraIdentifiers):
        """
        Double-quotes strings inside the dblink query.

        @param extraIdentifiers list of extra tokens to append to select clause.
        """
        # @FIXME This breaks for queries with incidential '%s' substrings, e.g.: .. LIKE '%super%'

        def positionalCallback(match):
            """
            Regex callback to determine the %s position and apply additional
            quotes if appropriate depending on the arg type.
            """
            try:
                #logging.debug('sql={0}'.format(sql))
                #logging.debug('args={0}'.format(args))
                #logging.debug('pos={0}'.format(positionalCallback.position))
                if not any(map(lambda t: isinstance(args[positionalCallback.position], t), (int, long, bool))):
                    # Add extra set of single quotes, which will become ''arg''
                    # once the db adds additional quotes.
                    return "''{0}''".format(match.group(0))
                return match.group(0)

            finally:
                positionalCallback.position += 1

        positionalCallback.position = 0

        # First, change all existing single quotes to 2 single quotes.
        dbLinkSql = sql.replace("'", "''")

        if len(args) > 0:
            # Then add 2 single quotes around any %s string arguments.
            dbLinkSql = _stringArgumentFinder.sub(positionalCallback, dbLinkSql)

        return re.sub(r'([\n ])FROM([\n ])', r', {0}\1FROM\2'.format(', '.join(extraIdentifiers)), dbLinkSql, 1) \
            if len(extraIdentifiers) > 0 else dbLinkSql

    def _prepareGroupingTail(identifiers, table, listOfReferencedTables, outerWhereTail):
        """Identify and extract grouping clause to generate outer query grouping clause."""
        # For counts or sums where that was the only thing queried, chop off the
        # "where" portion of the outermost query.
        #logging.info('OOOOOOOOOOOOUTER WHERE TAIL={}'.format(outerWhereTail))
        initial = 'GROUP BY'
        whereTail = outerWhereTail or initial
        nextToken = ' ' if outerWhereTail else ', '

        if len(identifiers) == 1:
            ident = parseIdentifier(identifiers[0], table, listOfReferencedTables)
            if ident['function'] == 'count' and includeShardInfo is True:
                whereTail += '{0}"shard"'.format(nextToken)

        else:
            # List of parsed identifiers.
            pids = map(lambda i: parseIdentifier(i, table, listOfReferencedTables), identifiers)
            # List of aggregate function names.
            aggregates = _sqlFunctionTypeMappings.keys()
            # Check for aggregate function mixed with fields, and create
            # appropriate group-by clause.
            containsAggregate = len(filter(lambda pi: pi['function'] in aggregates, pids)) > 0
            #logging.info('PIDS={}'.format(pids))
            #logging.info('ADDING {}'.format(
            #    ', '.join(map(lambda pi: pi['column'], filter(lambda pi: pi['function'] not in aggregates, pids)))
            #))
            if containsAggregate is True:
                whereTail += '{0}{1}'.format(
                    nextToken,
                    ', '.join(map(lambda pi: pi['column'], filter(lambda pi: pi['function'] not in aggregates, pids)))
                )

        #logging.info('!!!!!!!!!!!! {}'.format(whereTail))
        return whereTail if whereTail != initial else ''

    table = _findTable(parsed)
    listOfReferencedTables = _findReferencedTables(parsed)

    # NB: @var columnsToAliases Dict of column name to alias.  Used to generate a proper outer tail.
    identifiers, columnsToAliases = _findColumns(parsed, table)
    #logging.info(u'columnsToAliases={0}'.format(columnsToAliases))

    outerWhereTail, extraIdentifiers = _findWhereTail(parsed)

    # Create inner identifiers set.
    innerIdentifiers = \
        map(lambda t: t.value, filter(lambda t: _remapTokenToAlias(t) not in identifiers, extraIdentifiers))

    dbLinkT = _toDbLinkT(identifiers + innerIdentifiers, table, listOfReferencedTables)

    stdArgs = (identifiers, table, listOfReferencedTables)

    # Sometimes count(*) needs to be remapped to sum(*) in the outermost query.
    remappedIdentifiers = _remapFunctionIdentifiers(*stdArgs) + (['shard'] if includeShardInfo is True else [])

    groupingTail = _prepareGroupingTail(*stdArgs, outerWhereTail=outerWhereTail)

    # Get SQL with single quotes -> double single quotes.
    dbLinkSql = _prepareDbLinkQuery(sql, innerIdentifiers)
    #logging.info('usePersistentDbLink={}'.format(usePersistentDbLink))

    multiShardSql = '\nUNION\n'.join(
        map(
            lambda shard: '''SELECT *{maybeSelectShardId} FROM ''' \
                '''dblink('{connectionString}', '{dbLinkSql}') AS {tClause}'''.format(
                maybeSelectShardId=''', '{0}' AS "shard"'''.format(shard) if includeShardInfo is True else '',
                # Generate the dblink connection string if not using persistent, otherwise just use the connection name.
                connectionString=getPsqlConnectionString(shard) if not usePersistentDbLink else shard,
                dbLinkSql=dbLinkSql,
                tClause=dbLinkT
            ),
            shards
        )
    )

    if len(innerIdentifiers) > 0:
        # Sometimes count(*) needs to be remapped to sum(*) in the outermost query.
        outerRemappedIdentifiers = \
            _remapFunctionIdentifiers(*stdArgs, stripFunctions=True) + (['shard'] if includeShardInfo is True else [])

        distributedSql = 'SELECT {outerRemapped}\n' \
            'FROM (SELECT {remapped}, {inner} FROM (\n{multiShardSql}\n) q0 {tail}) q1'.format(
            outerRemapped=', '.join(outerRemappedIdentifiers),
            remapped=', '.join(remappedIdentifiers),
            inner=', '.join(map(lambda i: i.replace('"."', '_'), innerIdentifiers)),
            multiShardSql=multiShardSql,
            tail=groupingTail
        ).strip()

    else:
        distributedSql = 'SELECT {remapped} FROM (\n{multiShardSql}\n) q0 {tail}'.format(
            remapped=', '.join(remappedIdentifiers),
            multiShardSql=multiShardSql,
            tail=groupingTail,
        ).strip()

    #distributedSql = 'SELECT {remapped} FROM (\n{multiShardSql}\n) q0 {tail0} {tail1}'.format(
    #    remapped=', '.join(remappedIdentifiers),
    #    multiShardSql=multiShardSql,
    #    tail0=maybeGroupingTail if 'GROUP BY' not in outerWhereTail.upper() else '',
    #    tail1=outerWhereTail
    #).strip()

    #finishedTs = time.time()
    #logging.info(u'distributedSelect took {0}'.format(finishedTs - startedTs))
    logging.debug(u'IN: {0}'.format(sql))
    logging.debug(u'OUT: {0}'.format(distributedSql))

    #from django_util.log_errors import print_stack
    #logging.debug('[distributedSelect stack]')
    #logging.debug(print_stack())

    return (distributedSql % (args * len(shards))).replace('%', '%%'), tuple()


# Some aggregate functions require remapping in the outermost part of the distributed query to produce the expected
# combined result.  e.g. count -> sum
_aggregateFunctionTransformMappings = {
    'count': 'sum',
}

_aggregateFunctionTypeMappings = {
    # NB: <T> is used to indicate the same as the underlying type of the input.
    'avg': 'numeric',
    'bit_and': '<T>',
    'bit_or': '<T>',
    'bool_and': 'bool',
    'bool_or': 'bool',
    'count': 'bigint',
    'every': 'bool',
    'max': '<T>',
    'min': '<T>',
    'string_agg': '<T>',
    'sum': 'numeric',
    'to_char': 'character varying',
    'xmlagg': 'xml',
}

# Recognized functions:
_sqlFunctionTypeMappings = dict(
    {
        'to_char': 'character varying',
        'array_agg': 'bigint[]', # NB: actually returns array[T] (Not fully supported, bigint[] is just a common case).
    }.items() + _aggregateFunctionTypeMappings.items()
)

_identifierParserRe = re.compile(
    r'''
        ^\s*
        (?P<identifier>(?:[a-zA-Z0-9_]+\()?(?P<column>.*?)(?:\))?)
        (?:\s+(?:as\s+)?(?P<alias>(?:[a-z0-9_]+|"[^"]+"?)))?
        \s*$
    ''',
    re.I | re.X
)

_functionParserRe = re.compile(
    r'''^(?P<function>{0})\(\s*(?P<arg1>.*?)(?P<rest>(?:\s*,\s*.*?\s*)*)\)$''' \
        .format('|'.join(_sqlFunctionTypeMappings.keys())),
    re.I
)

_tableColumnRe = re.compile(r'(?P<table>"?[a-z0-9_]+"?)\.(?P<column>"?[a-z0-9_]+"?)(?: .*)?', re.I)

def parseIdentifier(identifierFragment, table=None, listOfReferencedTables=None):
    """
    Parse an identifier (e.g. the `avg(score) myScore` portion of the statement `select avg(score) myScore from x` into
    it's constituent parts.

    NB: In instances where there is ambiguity with regard to what the return type will be, we default to
        'character varying'.  See the postgresql documentation for more info:
        http://www.postgresql.org/docs/9.2/static/functions-aggregate.html

    @param identifierFragment str containing SQL fragment to parse.

    @param table Optional str name of table to use to match columns with return type.

    @param listOfReferencedTables list of dictionaries of 'table' and 'alias' keys, matching the format returned by
        _findReferencedTables().

    @return dict containing the parsed identifier.
        e.g. {'column': 'score', alias: 'myScore', 'type': 'bigint'}
    """
    from .reflect import describe, plFunctionReturnType

    if listOfReferencedTables is None:
        listOfReferencedTables = []

    m = _identifierParserRe.match(identifierFragment)
    if m is None:
        raise Exception('No identifer found in "{0}"'.format(identifierFragment))

    out = {'function': None}

    out['identifier'], out['column'], out['alias'] = map(pgStripDoubleQuotes, m.groups())
    #logging.info(u'in={}, column={}, alias={}'.format(identifierFragment, out['column'], out['alias']))

    def _findColumn(name):
        """Try to find a specific column name from the table description."""
        # Test for table.column or "table"."column"-style column name:
        #logging.info('NAME={}'.format(name))
        tableColumnMatch = _tableColumnRe.match(out['column'])
        if tableColumnMatch is not None:
            name = tableColumnMatch.group('column').replace('"', '')
            _table = tableColumnMatch.group('table').replace('"', '')

            # Resolve prefix containing candidate table alias.
            for ref in listOfReferencedTables:
                if ref['alias'] == _table:
                    _table = ref['table'].strip('"')
                    break

        else:
            _table = None

        if table is None and _table is None:
            return None

        column = filter(lambda c: c[0].lower() == name.lower(), describe(pgStripDoubleQuotes(_table or table)))

        if len(column) > 0:
            out['column'] = '{0}{1}'.format(
                '"{0}".'.format(_table) if _table is not None else '',
                '"{0}"'.format(column[0][0].replace('"', ''))
            )
            out['type'] = column[0][1]

        return column[0] if len(column) > 0 else None

    def _attemptTypeInference():
        """Infer the identifiers return type."""
        aggregateTest = _functionParserRe.match(out['identifier'])
        if aggregateTest is None:
            return

        out['function'], arg1, rest = map(pgStripDoubleQuotes, aggregateTest.groups())

        out['function'] = out['function'].lower()

        out['args'] = '{0}{1}'.format(arg1, rest)

        # @FIXME Assuming this will contain the column of interest is very
        # naiive; not a safe assumption.
        found = _findColumn(out['column'])
        if found is None and _findColumn(arg1) is not None:
            out['column'] = arg1

        # Function return type inference/lookup.
        if out['function'] in _aggregateFunctionTypeMappings:
            out['type'] = _sqlFunctionTypeMappings[out['function']]

        else:
            # If not in _aggregateFunctionTypeMappings, try to query for the return type.
            returnType = plFunctionReturnType(out['function'])
            if len(returnType) > 0:
                out['type'] = returnType[0][0]

        if 'type' not in out:
            logging.warn(u'[WARN] distributed.parseIdentifier type inference failed, out={0}'.format(out))

    _attemptTypeInference()

    # Do our best to infer the type if the attempt failed or resulted in
    # a '<T>'.
    found = _findColumn(out['identifier'])
    if 'type' not in out or out['type'] == '<T>':
        # Try to find the column type from the description of the table.
        if found is not None:
            out['type'] = found[1]
        else:
            # Default.
            out['type'] = 'character varying'

    # @TODO Add support for inferring `1 as q` as bigint,
    # 'someval' as character varying, etc.

    # NB: For our purposes, the column will always be referred to by the full auto-generated alias (with underscores)
    # rather than the table.column.
    out['column'] = out['column'].replace('"."', '_')
    return out


def multiShardExec(sql):
    """Execute a statement across all shards."""
    from sh_util.sharding import ShardedResource
    from . import db_exec
    for connectionName in ShardedResource.allShardConnectionNames():
        db_exec(sql, using=connectionName)


if __name__ == '__main__':
    import doctest
    doctest.testmod()

