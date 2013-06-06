# -*- coding: utf-8 -*-

"""Postgres-specific distributed operations tools."""

__author__ = 'Jay Taylor [@jtaylor]'

import logging
import re, settings


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
    from . import connections, getPsqlConnectionString

    if len(handles) == 0 and len(custom) == 0:
        logging.warn('pgConnectPersistentDbLinks invoked with no handles, no action taken')
        return

    connectionNames = connections()

    alreadyConnected = pgGetPersistentConnectionHandles(using=using) or []

    for c in handles:
        assert c in connectionNames, 'Connection "{0}" was not found in connections ({1})'.format(c, connectionNames)
        if c not in alreadyConnected:
            psqlConnectionString = getPsqlConnectionString(c)
            pgConnectPersistentDbLink(using, c, psqlConnectionString)

    for handle, psqlConnectionString in custom.items():
        if handle not in alreadyConnected:
            pgConnectPersistentDbLink(using, handle, psqlConnectionString)


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

    logging.info(u'usePersistentDbLink={0}'.format(usePersistentDbLink))

    if usePersistentDbLink is not False:
        resolvedConnections = _resolveConnectionsOrShards(connections)
        logging.info(u'Resolved connections: {0}'.format(resolvedConnections))

        # If the number of connections is 1, then the query does not need to use dblink.
        if len(resolvedConnections) != 1:
            pgConnectPersistentDbLinks(
                using,
                *(resolvedConnections if isinstance(resolvedConnections, list) else []),
                **(resolvedConnections if isinstance(resolvedConnections, dict) else {})
            )

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

    if args is None:
        args = tuple()

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

    def _findWhereTail(parsed, columnsToAliases):
        """
        @param parsed sqlparse result

        @param columnsToAliases Dict of column name to alias.  Used to generate a proper outer tail.

        @return str including the `where` clause and everything after it.
        """
        seenInterestingKeyword = False
        innerTokens = []
        outerTokens = []
        for token in _tokensWithSubTokensFor(Where, IdentifierList): #parsed.tokens:
            # WHERE or GROUP BY keywrods..
            if seenInterestingKeyword is not True and str(token).lower() in ('group', 'limit', 'order'):
                seenInterestingKeyword = True

            if seenInterestingKeyword:
                innerTokens.append(token.value.replace('"."', '_'))
                outerTokens.append(token.value)

        innerTail = ''.join(innerTokens)

        #logging.info(columnsToAliases)
        def remapTokenToAlias(token):
            """Takes a token and produces the aliased name of the field when applicable."""
            #logging.info('CANDIDATE IS: &{}&'.format(token))
            if token in columnsToAliases:
                #logging.info(u'FOUND A MATCH!!! {}'.format(token))
                return columnsToAliases[token]
            #else:
            #    logging.info(u'NOMATCHFOUNDFOR: {}'.format(token))
            return token

        # Strip offsets and limits from the outermost where tail (should retain only order by clauses).
        outerTail = _offsetLimitRe.sub('', ''.join(map(remapTokenToAlias, outerTokens)).replace('\n', ' ')).strip()
        #logging.info(u'outerTail->{0}'.format(outerTail))

        return (innerTail, outerTail)

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
            """
            Watch for the "FROM" keyword and set a flag when it's been seen.
            """
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

        joinedOut = map(
            lambda c: '{0}{1}'.format(
                columns[c.value.strip('"')] if c.value.strip('"') in columns else c.value,
                ' AS "{0}"'.format(c.get_alias()) if hasattr(c, 'has_alias') and c.has_alias() else ''
            ),
            flatIdentifiers
        )

        def columnAliasMapper(column):
            """Given an identifier, resolves to a column/alias tuple."""
            value = column.value.strip('"')
            a = columns[value] if value in columns else column.value
            b = '"{0}"'.format(
                (column.get_alias() if hasattr(column, 'has_alias') and column.has_alias() else a).strip('"')
            )
            return (a, b)

        columnsToAliases = dict(map(columnAliasMapper, flatIdentifiers))
        #logging.info('out={0}\n{1}'.format(joinedOut, columnsToAliases))
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
        #logging.debug(annotatedIdents)
        identifierNames = map(lambda x: x[0], description)

        #logging.debug('!!! {0}'.format(description))
        dbLinkT = tableDescriptionToDbLinkT(description, identifierNames)
        return dbLinkT

    def _remapFunctionIdentifiers(identifiers, table, listOfReferencedTables):
        """
        For distributed queries to return correct results, count(*) needs to
        be remapped to sum(*) in the outermost query.
        """
        remapped = []
        for identifier in identifiers:

            p = parseIdentifier(identifier, table, listOfReferencedTables)

            identifier = p['alias'] if p['alias'] is not None else p['column']

            # Add quoting if appropriate.
            stripped = identifier.strip('"')
            if not stripped.endswith('*'):
                identifier = '"{0}"'.format(stripped)
            del stripped

            if p['function'] is not None and p['function'].lower() in _aggregateFunctions.keys():
                # Apply any function remappings below.
                if p['function'].lower() == 'count':
                    p['function'] = 'sum'

                remapped.append('{0}({1})'.format(p['function'].upper(), identifier))

            else:
                remapped.append(
                    '{0}'.format(identifier)#if p['function'] is None else '{0}({1})'.format(p['function'], p['column'])
                )

        return remapped

    def _prepareDbLinkQuery(sql):
        """Double quote strings inside the dblink query."""
        # @FIXME This breaks for queries with incidential '%s' substrings, e.g.: .. LIKE '%super%'

        def positionalCallback(match):
            """
            Regex callback to determine the %s position and apply additional
            quotes if appropriate depending on the arg type.
            """
            try:
                logging.debug('sql={0}'.format(sql))
                logging.debug('args={0}'.format(args))
                logging.debug('pos={0}'.format(positionalCallback.position))
                if not any(map(
                    lambda t: isinstance(args[positionalCallback.position], t),
                    (int, long, bool)
                )):
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

        return dbLinkSql

    def _prepareGroupingTail(identifiers, table, listOfReferencedTables):
        """d"""
        # For counts or sums where that was the only thing queried, chop off the
        # "where" portion of the outermost query.
        whereTail = ''
        if len(identifiers) == 1:
            ident = parseIdentifier(
                identifiers[0],
                table,
                listOfReferencedTables
            )
            if ident['function'] == 'count' and includeShardInfo is True:
                whereTail = 'GROUP BY "shard"'

        else:
            # List of parsed identifiers.
            pids = map(
                lambda i: parseIdentifier(i, table, listOfReferencedTables),
                identifiers
            )
            # List of aggregate function names.
            aggregates = _aggregateFunctions.keys()
            # Check for aggregate function mixed with fields, and create
            # appropriate group-by clause.
            containsAggregate = len(filter(
                lambda pi: pi['function'] in aggregates,
                pids
            )) > 0
            if containsAggregate is True:
                whereTail = 'GROUP BY {0}'.format(', '.join(map(
                    lambda pi: pi['column'],
                    filter(lambda pi: pi['function'] not in aggregates, pids)
                )))

        return whereTail

    table = _findTable(parsed)
    listOfReferencedTables = _findReferencedTables(parsed)
    identifiers, columnsToAliases = _findColumns(parsed, table)
    innerWhereTail, outerWhereTail = _findWhereTail(parsed, columnsToAliases)
    dbLinkT = _toDbLinkT(identifiers, table, listOfReferencedTables)

    stdArgs = (identifiers, table, listOfReferencedTables)

    # Sometimes count(*) needs to be remapped to sum(*) in the outermost query.
    remappedIdentifiers = _remapFunctionIdentifiers(*stdArgs) + (['shard'] if includeShardInfo is True else [])

    maybeGroupingTail = _prepareGroupingTail(*stdArgs)

    # Get SQL with single quotes -> double single quotes.
    dbLinkSql = _prepareDbLinkQuery(sql)
    logging.info('usePersistentDbLink={}'.format(usePersistentDbLink))

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

    distributedSql = 'SELECT {remapped} FROM (\n{multiShardSql}\n) q0 {tail0} {tail1}'.format(
        remapped=', '.join(remappedIdentifiers),
        multiShardSql=multiShardSql,
        tail0=maybeGroupingTail,
        tail1=outerWhereTail
    ).strip()

    logging.debug('IN: {0}'.format(sql))
    logging.debug('OUT: {0}'.format(distributedSql))

    #from django_util.log_errors import print_stack
    #logging.debug('[distributedSelect stack]')
    #logging.debug(print_stack())

    return (distributedSql % (args * len(shards))).replace('%', '%%'), tuple()


_aggregateFunctions = {
    # NB: <T> is used to indicate the same as the underlying type of the input.
#    'array_agg': '', # Not currently supported
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
    'xmlagg': 'xml',
}

_identifierParser = re.compile(r'''^\s*(?P<identifier>.*?)(?:\s+(?:as\s+)?(?P<alias>[^ ]+?))?\s*$''', re.I)

_aggregateParser = re.compile(
    r'''^(P<function>{0})\(\s*(?P<arg1>.*?)(?P<rest>(?:\s*,\s*.*?\s*)*)\)$''' \
        .format('|'.join(_aggregateFunctions.keys())),
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

    m = _identifierParser.match(identifierFragment)
    if m is None:
        raise Exception('No identifer found in "{0}"'.format(identifierFragment))

    out = {'function': None}

    out['column'], out['alias'] = map(pgStripDoubleQuotes, m.groups())

    def _findColumn(name):
        """Try to find a specific column name from the table description."""
        # Test for table.column or "table"."column"-style column name:
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
        aggregateTest = _aggregateParser.match(out['column'])
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
        if out['function'] in _aggregateFunctions:
            out['type'] = _aggregateFunctions[out['function']]

        else:
            # If not in _aggregateFunctions, try to query for the return type.
            returnType = plFunctionReturnType(out['function'])
            if len(returnType) > 0:
                out['type'] = returnType[0][0]

        if 'type' not in out:
            logging.warn(u'[WARN] distributed.parseIdentifier type inference failed, out={0}'.format(out))

    _attemptTypeInference()

    # Do our best to infer the type if the attempt failed or resulted in
    # a '<T>'.
    found = _findColumn(out['column'])
    if 'type' not in out or out['type'] == '<T>':
        # Try to find the column type from the description of the table.
        if found is not None:
            out['type'] = found[1]
        else:
            # Default.
            out['type'] = 'character varying'

    # @TODO Add support for inferring `1 as q` as bigint,
    # 'someval' as character varying, etc.

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

