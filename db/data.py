# -*- coding: utf-8 -*-

"""
Postgres-specific data tooling, primarily to assist with operational management of logical and physical sharding.

Including:
    - Table data sync/replication
    - User and logical shard shard relocation

NB: Things won't work right if any of the connection names are integers, or if the string of the connection name
resolves to truthy for str.isdigit().
"""

__author__ = 'Jay Taylor [@jtaylor]'

import simplejson as json, re, settings, time
import logging
from collections import OrderedDict
from ..functional import memoize
from ..sharding import ShardedResource, coerceIdToShardName, ShardEvent
from ..memcache import attemptMemcacheFlush
from ..s3 import uploadFile
from . import db_exec, db_query, connections, getPsqlConnectionString
from .reflect import describe, discoverDependencies, findTablesWithUserIdColumn, getPrimaryKeyColumns
from .distributed import tableDescriptionToDbLinkT
# NB: Don't use cStringIO because it is unable to work with UTF-8.
from StringIO import StringIO
from ..text import toSingleLine


# Logical shard S3 backup path.
s3MigrationBackupPath = '/logicalShardMigrations'

def _baseBackupFileName(logicalShardId, ts):
    """@return string containing a base backup filename."""
    return '{0}/id-{1}_{2}'.format(s3MigrationBackupPath, logicalShardId, int(ts))


# Used to cleanup SQL queries sometimes (not always guaranteed to be safe
# WRT messing up your SQL query, discretion required).
_spacesRe = re.compile(r'\s+', re.M)
toSingleLine = lambda s: _spacesRe.sub(' ', s).strip()


class MigrateUserError(Exception):
    """General user migration error."""


class MigrateUserStaleReadError(MigrateUserError):
    """Stale-data read error."""


def shouldTableBeIgnoredForUserOperations(table):
    """@return True if user-specific data does not live in specified table, otherwise False."""
    return table in settings.STATIC_TABLES or table in settings.SHARDING_IGNORE_TABLES


def doesTheTableDataDiffer(table, source1, source2):
    """
    Determine if the table data differs across hosts (shards).

    NB: Hardcoded not to work on tables with more than 100,000 rows.  This should never be used for tables that may grow
    to that size (or even close to that).

    @return True if the data differs between source1 and source2, otherwise False.
    """
    countSql = 'SELECT COUNT(*) FROM "{0}"'.format(table)

    count1 = db_query(countSql, using=source1)[0][0]
    count2 = db_query(countSql, using=source2)[0][0]

    # NEVER USE THIS ON TABLES WITH MORE THAN 100,000 ROWS!
    assert count1 < 100001 and count2 < 100001

    if count1 != count2:
        return True

    # Dynamically lookup PK and generate order clause.
    orderBy = ', '.join(map(
        '"{0}"'.format,
        getPrimaryKeyColumns(table, source1)
    ))

    dataSql = 'SELECT * FROM "{0}" ORDER BY {1} DESC'.format(table, orderBy)

    data1 = db_query(dataSql, using=source1)
    data2 = db_query(dataSql, using=source2)

    return data1 != data2


def replicateTable(table, source, destination):
    """
    Replicate a static table from one database connection to another.  The destination table will pull the data
    directly from the source db's table.

    @param table str Table name.
    @param source str Source connection name.
    @parm destination str Destination connection name.
    """
    # Only operate on static tables defined in settings.
    assert table in settings.STATIC_TABLES

    # Validate source and destination connection names.
    assert source in connections() and destination in connections()

    # Check to see if the table data matches in both locations.
    # If it does, then no further work is required.
    differ = doesTheTableDataDiffer(table, source, destination)
    if not differ:
        return

    logging.info(u'Replicating table {0} from {1} -> {2}'.format(table, source, destination))

    # Let the refresh begin!
    connectionString = getPsqlConnectionString(source)

    description = describe(table, using=destination)

    columns = map(lambda d: '"{0}"'.format(d[0]), description)

    dbLinkT = tableDescriptionToDbLinkT(description)

    sql = '''
        INSERT INTO "{table}" ({columns}) SELECT {columns} FROM dblink(
            '{connectionString}',
            'SELECT {columns} FROM "{table}"'
        ) AS {dbLinkT}
    '''.format(
        table=table,
        columns=', '.join(columns),
        connectionString=connectionString,
        dbLinkT=dbLinkT
    )
    #print sql

    try:
        db_exec('BEGIN', using=destination)
        db_exec('SET CONSTRAINTS ALL DEFERRED', using=destination)
        # NB: Truncate wouldn't work here, because TRUNCATE is a DDL statement.
        # @see
        db_exec('DELETE FROM "{0}"'.format(table), using=destination)
        db_exec(sql, using=destination)
        db_exec('COMMIT', using=destination)

    except Exception, e:
        errorMessage = u'[ERROR] replicateTable caught exception with table={0} source={1} destination={2}: {3}' \
            .format(table, source, destination, e)
        logging.error(errorMessage)
        db_exec('ROLLBACK', using=destination)

        from ..mail import sendEmail
        sendEmail(
            subject='[URGENT] Table sync error on "{0}"'.format(table),
            body=errorMessage,
            fromAddress='devops@sendhub.com',
            toAddress='devops@sendhub.com'
        )


def autoDbLinkInsert(table, dbLinkSql, sourceConnectionString, using='default', pk=None):
    """
    Automatically generate and execute the autoDb part of the SQL statement to insert a remote dataset for a
    particular SELECT query.

    @param table str Name of table
    @param dbLinkSql str  <SELECT X FROM Y clause> for table.
    @param sourceConnectionString str psql-style connection string for the source database.
    @param using str Django connection name -- should be the destination host.
    @param pk str Optional string containing the primary key column name, or None to enable auto-detection.
    """
    if sourceConnectionString in connections():
        sourceConnectionString = getPsqlConnectionString(sourceConnectionString)

    dbLinkSql = toSingleLine(dbLinkSql)
    dbLinkT = tableDescriptionToDbLinkT(describe(table))

    try:
        db_exec('SAVEPOINT auto_db_link_insert', using=using)

        sql = '''
            INSERT INTO "{table}"
            SELECT * FROM dblink(
                '{connectionString}',
                '{dbLinkSql}'
            ) AS {dbLinkT}
        '''.format(table=table, connectionString=sourceConnectionString, dbLinkSql=dbLinkSql, dbLinkT=dbLinkT)

        db_exec(sql, using=using)
        db_exec('RELEASE SAVEPOINT auto_db_link_insert', using=using)

    except Exception, e:
        excStr = str(e)

        if 'duplicate key value violates unique constraint' in excStr:
            logging.warn(u'Naiive autoDbLinkInsert failed, attempting again with pk exclusion..')
            logging.warn(u'Exception was: {0}/{1}'.format(type(e), e))

            db_exec('ROLLBACK TO auto_db_link_insert', using=using)
            db_exec('SAVEPOINT auto_db_link_insert', using=using)

            # NB: Tables with multiple column PK's are not supported.
            pk = pk or getPrimaryKeyColumns(table, using=using)[0]

            # NB: Notice the where clause -- to avoid potential duplicates.
            sql = '''
                INSERT INTO "{table}"
                SELECT * FROM dblink(
                    '{connectionString}',
                    '{dbLinkSql}'
                ) AS {dbLinkT}
                WHERE "{pk}" NOT IN (SELECT "{pk}" FROM "{table}")
            '''.format(
                table=table,
                connectionString=sourceConnectionString,
                dbLinkSql=dbLinkSql,
                dbLinkT=dbLinkT,
                pk=pk
            )

            db_exec(sql, using=using)
            db_exec('RELEASE SAVEPOINT auto_db_link_insert', using=using)

        else:
            db_exec('RELEASE SAVEPOINT auto_db_link_insert', using=using)
            raise e


def tableRowCounts(tableColumnPairs, userIdOrUserIds, using):
    """
    Get counts for each table with the user-id filter applied.  Executes a single query to get the results as
    list((table, count)).

    @param tableColumnPairs list of tuples of table/column pairs (where the column contains the user id).
    @param userIdOrUserIds mixed int user-id or list of user-ids.
    @param using str Connection name.

    @return dict of table -> matching row count
    """
    # True if userIdOrUserIds is an iterable, otherwise False.
    isIterable = isinstance(userIdOrUserIds, (set, list))

    sql = ' UNION '.join(map(
        lambda (table, column): toSingleLine(
            '''
                SELECT '{table}' "table", COUNT(*) "count"
                FROM "{table}"
                WHERE "{userIdColumn}" {op} {idOrIds}
            '''.format(
                table=table.strip('"').strip("'"),
                userIdColumn=column.strip('"'),
                op='IN' if isIterable else '=',
                idOrIds='({0})'.format(','.join(map(str, userIdOrUserIds)) if isIterable else int(userIdOrUserIds))
            )
        ),
        filter(lambda (table, column): not shouldTableBeIgnoredForUserOperations(table), tableColumnPairs)
    ))

    return dict(db_query(sql, using=using))


def scrubTables(using):
    """d"""
    statements = [
        '''
        DELETE FROM "main_phonenumber" WHERE "id" IN (
            SELECT "pn"."id"
            FROM "main_phonenumber" "pn"
                LEFT JOIN "main_extendeduser" "eu" ON "eu"."twilio_phone_number_id" = "pn"."id"
                LEFT JOIN "main_sendhubphonenumber" "spn" ON "spn"."twilioPhoneNumber_id" = "pn"."id"
            WHERE "eu"."twilio_phone_number_id" IS NULL AND "spn"."twilioPhoneNumber_id" IS NULL
        )
        ''',
    ]

    for statement in statements:
        db_exec(statement, using=using)


def setLogicalShardStatus(logicalShardId, status):
    """Set the status field for a logical shard."""
    db_exec('''BEGIN''', using=settings.PRIMARY_SHARD_CONNECTION)
    db_exec(
        '''UPDATE "LogicalShard" SET "status" = %s WHERE "id" = %s''',
        (status, logicalShardId),
        using=settings.PRIMARY_SHARD_CONNECTION
    )
    db_exec('''COMMIT''', using=settings.PRIMARY_SHARD_CONNECTION)


def setLogicalShardPhysicalShardId(logicalShardId, physicalShardId, status=None):
    """Set a new physicalShardId for a logical shard."""
    db_exec('''BEGIN''', using=settings.PRIMARY_SHARD_CONNECTION)

    if status is None:
        db_exec(
            '''UPDATE "LogicalShard" SET "physicalShardId" = %s WHERE "id" = %s''',
            (physicalShardId, logicalShardId),
            using=settings.PRIMARY_SHARD_CONNECTION
        )
    else:
        db_exec(
            '''UPDATE "LogicalShard" SET "physicalShardId" = %s, "status" = %s WHERE "id" = %s''',
            (physicalShardId, status, logicalShardId),
            using=settings.PRIMARY_SHARD_CONNECTION
        )

    db_exec('''COMMIT''', using=settings.PRIMARY_SHARD_CONNECTION)



def _physicalShardId(logicalShardId):
    """Lookup a physical shard id for a logical shard id."""
    res = db_query(
        '''SELECT "physicalShardId" FROM "LogicalShard" WHERE "id" = %s''',
        (logicalShardId,),
        using=settings.PRIMARY_SHARD_CONNECTION
    )
    return res[0][0] if len(res) > 0 else None


def _logicalShardUserIds(logicalShardId, physicalShardId=None):
    """
    Get all the user-ids in a logical shard.

    @return list(int) of user-ids.
    """
    res = db_query(
        '''SELECT "id" FROM "auth_user" WHERE "id" %% %s = %s''',
        (settings.NUM_LOGICAL_SHARDS, logicalShardId),
        using='shard_{0}'.format(physicalShardId or _physicalShardId(logicalShardId))
    )

    userIds = map(lambda tup: tup[0], res)

    return userIds


def migrateLogicalShard(logicalShardId, destinationShard):
    """Move all records for a logical shard to the specified physcial shard."""
    physicalShardId = _physicalShardId(logicalShardId)
    assert physicalShardId is not None

    sourceShard = coerceIdToShardName(physicalShardId)
    assert sourceShard != destinationShard

    userIds = _logicalShardUserIds(logicalShardId, physicalShardId)

    setLogicalShardStatus(logicalShardId, 'RELOCATING')

    try:
        # Keep track of initial counts.
        preSourceCounts = tableRowCounts(_userIdTableColumnPairs(), userIds, using=sourceShard)

        #migrateUsers(userIds, sourceShard, destinationShard)
        startedTs = _dumpAndCopyLogicalShard(logicalShardId, destinationShard, sourceShard, userIds)
        duration = int(time.time() - startedTs)

        startedCountsTs = time.time()
        postSourceCounts = tableRowCounts(_userIdTableColumnPairs(), userIds, using=sourceShard)
        postDestinationCounts = tableRowCounts(_userIdTableColumnPairs(), userIds, using=destinationShard)
        finishedCountsTs = time.time()
        logging.info(u'Tail-end src/dest counts took {0} seconds'.format(int(startedCountsTs - finishedCountsTs)))

        message = u'duration={0}s\nnumUsers={1}\npreSourceCounts={2}\npostSourceCounts={3}\npostDestinationCounts={4}' \
            .format(duration, len(userIds), preSourceCounts, postSourceCounts, postDestinationCounts)
        logging.info(message)

        baseFileName = _baseBackupFileName(logicalShardId, startedTs)

        if preSourceCounts != postSourceCounts or preSourceCounts != postDestinationCounts:
            logging.warn(u'FAILED: Logical shard migration failed due to count mis-match!')
            fileName = '{0}.failed'.format(baseFileName)
            logging.info(u'Deleting copied data from destination shard {0}'.format(destinationShard))
            deleteUsers(userIds, destinationShard)

        else:
            logging.info(u'SUCCEEDED: pre/post source/destination counts all match')
            fileName = '{0}.succeeded'.format(baseFileName)
            newPhysicalShardId = ShardedResource.shardNameToId(destinationShard)
            logging.info(
                u'Updating LogicalShard table to point id={0} at physicalShardId={1}' \
                .format(logicalShardId, newPhysicalShardId)
            )
            setLogicalShardPhysicalShardId(logicalShardId, newPhysicalShardId, 'OK')
            attemptMemcacheFlush()
            deleteUsers(userIds, sourceShard)

        url = uploadFile(fileName, message)
        logging.info(u'Stored migration run note at {0}'.format(url))

    except AssertionError, e:
        logging.warn(
            u'Assertion failed while migrating userIds={0} from {1} to {2}: {3}'
            .format(userIds, sourceShard, destinationShard, e)
        )


def _dumpAndCopyLogicalShard(logicalShardId, destinationShard, using=None, userIds=None):
    """
    Dump and migrate a logical shard.

    @return int Started timestamp in epoch format (# of seconds since 1970).
    """
    startedTs = time.time()
    dump = _dumpLogicalShard(logicalShardId=logicalShardId, using=using, userIds=userIds)
    dumpFinishedTs = time.time()
    dumpDuration = int(dumpFinishedTs - startedTs)
    logging.info(u'LogicalShard dump phase for id={0} took {1} seconds'.format(logicalShardId, dumpDuration))

    sqlStatements = _backupDumpAndConvertToSqlList(dump, logicalShardId, startedTs, dumpFinishedTs)

    copyStartedTs = time.time()

    numStatements = len(sqlStatements)
    logging.info(u'Executing {0} SQL insert statements on {1}'.format(numStatements, destinationShard))

    for i, statement in enumerate(sqlStatements):
        statement = statement.replace('%', '%%')
        logging.info(u'Executing SQL statement {0}/{1}: {2}..'.format(i + 1, numStatements, statement[0:64]))
        db_exec(statement, using=destinationShard)

    copyFinishedTs = time.time()

    copyDuration = int(copyFinishedTs - copyStartedTs)
    logging.info(u'LogicalShard copy phase for id={0} took {1} seconds'.format(logicalShardId, copyDuration))

    duration = int(copyFinishedTs - startedTs)
    logging.info(u'Dump and copy for logicalShardId={0} took {1} seconds'.format(logicalShardId, duration))

    return int(startedTs)


def _dump2SqlString(dump, logicalShardId, startedTs, finishedTs):
    """Convert a logical shard dump to a string of SQL statements."""
    buf = StringIO()
    buf.write(u'-- Dump of LogicalShard {0} on {1}\n'.format(logicalShardId, int(startedTs)))
    for key in dump:
        buf.write(u'\n\n-- table = {0}\n'.format(key))
        for statement in dump[key]:
            buf.write(u'{0}\n'.format(statement))
    out = buf.getvalue()
    buf.close()
    return out


def _dump2SqlList(dump):
    """Convert a logical shard dump to a list of SQL statements."""
    out = []
    for key in dump:
        for statement in dump[key]:
            out.append(statement)
    return out


def _backupDumpAndConvertToSqlList(dump, logicalShardId, startedTs, finishedTs):
    """Backup a logical shard dump in two formats -- as an SQL string and as a JSON list of discrete statements."""
    baseFileName = _baseBackupFileName(logicalShardId, startedTs)

    # Upload SQL string to S3.
    sqlString = _dump2SqlString(dump, logicalShardId, startedTs, finishedTs)
    sqlStringUrl = uploadFile(baseFileName + '.sql', sqlString)
    logging.info(u'Uploaded SQL string dump of logicalShard {0}, sqlStringUrl={1}'.format(logicalShardId, sqlStringUrl))

    # Upload JSON-serialized SQL list to S3.
    sqlList = _dump2SqlList(dump)
    sqlListUrl = uploadFile(baseFileName + '.json', json.dumps(sqlList))
    logging.info(u'Uploaded JSON list dump of logicalShard {0}, sqlStringUrl={1}'.format(logicalShardId, sqlListUrl))
    return sqlList


def _dumpLogicalShard(logicalShardId, using=None, userIds=None):
    """Dump all data for a logical shard."""
    if using is None:
        physicalShardId = _physicalShardId(logicalShardId)
        using = coerceIdToShardName(physicalShardId)

    if userIds is None:
        if 'physicalShardId' not in vars():
            physicalShardId = _physicalShardId(logicalShardId)
        userIds = _logicalShardUserIds(logicalShardId, physicalShardId)

    assert len(userIds) > 0, 'No users found for logicalShard={0}'.format(logicalShardId)

    return dumpUsers(userIds, using)


seedTableColumnPairs = (
    ('auth_user', 'id'),
    ('main_extendeduser', 'user_id'),
    ('main_usermessage', 'user_id'),
    ('main_thread', 'userId'),
    ('main_contact', 'user_id'),
    ('main_group', 'user_id'),
)

preMigrationSql = (
    'ALTER TABLE "main_contact" DISABLE TRIGGER "main_contact_trigger";',
    'BEGIN;',
    'SET CONSTRAINTS ALL DEFERRED;',
)

postMigrationSql = (
    'SET CONSTRAINTS ALL IMMEDIATE;',
    'COMMIT;',
    'ALTER TABLE "main_contact" ENABLE TRIGGER "main_contact_trigger";',
)


@memoize
def _userIdTableColumnPairs():
    """@return list of <table,column> pairs for tables with user-id columns."""
    # Uniqify set of items while retaining original list order.
    return list(OrderedDict.fromkeys(list(seedTableColumnPairs) + findTablesWithUserIdColumn()))


def _verifyTheseUsersExistInShard(userIds, using):
    """Assert that all user-ids exist in the specified database."""
    inUserIds = ','.join(map(str, userIds))

    # Verify that the requested users exist on the sourceShard indicated.
    userCheck = db_query('''SELECT count(*) FROM "auth_user" WHERE "id" IN ({0})'''.format(inUserIds), using=using)
    assert userCheck[0][0] == len(userIds), 'not all userIds in ({0}) not found on {1}'.format(userIds, using)


def dumpUsers(userIds, using):
    """
    Dump complete user records to dict of a list of insert statement lists.

    @param userIds list of int.
    @param using mixed str or int Source connection name or shard id.

    @return dict of <table, list of insert statement lists>.
    """
    from .select2insert import select2multiInsert

    logging.info('Dumping users ({0}) from {1}'.format(userIds, using))

    using = coerceIdToShardName(using)

    _verifyTheseUsersExistInShard(userIds, using)

    inUserIds = ','.join(map(str, userIds))

    # Keep track of inserts on a per-table basis.
    inserts = OrderedDict()
    inserts['__pre__'] = list(preMigrationSql)

    def collectInserts(table, whereClause):
        """
        Given a table and where-clause, appends the list of inserts for the matching records from that table to a
        corresponding key for that table in the ``inserts`` dict.
        """
        sql = select2multiInsert(table=table, description=describe(table), using=using, whereClause=whereClause)
        if sql is not None:
            if table not in inserts:
                inserts[table] = []
            inserts[table].append(sql)

    def collectRecords(sourceTable, sourcePkColumn, innerTable, innerColumn, innerUserIdColumn):
        """Generic way to move rows containing ``userIds`` from one shard to another."""
        if shouldTableBeIgnoredForUserOperations(sourceTable):
            logging.debug(u'Skipping copy to static table: {0}'.format(sourceTable))
            return

        collectInserts(
            sourceTable,
            whereClause='"{pk}" IN (' \
                'SELECT "{innerColumn}" FROM "{innerTable}" WHERE "{innerUserIdColumn}" in ({userIds})' \
            ')'.format(
                pk=sourcePkColumn,
                innerColumn=innerColumn,
                innerTable=innerTable,
                innerUserIdColumn=innerUserIdColumn,
                userIds=inUserIds
            )
        )

    # Uniqify set of items while retaining original list order.
    userIdTableColumnPairs = _userIdTableColumnPairs()

    dependencies = discoverDependencies(map(lambda x: x[0], userIdTableColumnPairs), using=using)

    populatedTables = []

    for table, userIdColumn in userIdTableColumnPairs:
        logging.debug(u'(1) TABLE={0}'.format(table))

        if shouldTableBeIgnoredForUserOperations(table):
            logging.debug(u'Skipping dump from static table: {0}'.format(table))
            continue

        if table in populatedTables:
            logging.info(u'Skipping dump from already populated table: {0}'.format(table))
            continue

        if table in _additionalRelations:
            for fkTable, fkColumn, sourceTable in _additionalRelations[table]:
                sourcePkColumn = getPrimaryKeyColumns(sourceTable, using=using)[0]
                collectRecords(sourceTable, sourcePkColumn, fkTable, fkColumn, userIdColumn)

        # Collect relevant records from the table.
        collectInserts(table, '''"{0}" IN ({1})'''.format(userIdColumn, inUserIds))
        populatedTables.append(table)

    # Backfill dependent tables.
    for table, userIdColumn in userIdTableColumnPairs:
        logging.debug(u'(2) TABLE={0}'.format(table))

        if shouldTableBeIgnoredForUserOperations(table):
            logging.debug(u'Dependencies backfiller is skipping static table: {0}'.format(table))
            continue

        # If there are additional dependencies, insert them as well.
        if table in dependencies:
            unpopulatedTables = \
                filter(lambda (col, fkTable, fkCol): fkTable not in populatedTables, dependencies[table])

            for column, fkTable, fkColumn in unpopulatedTables:
                collectRecords(fkTable, fkColumn, table, column, userIdColumn)
                populatedTables.append(fkTable)

    inserts['__post__'] = list(postMigrationSql)

    return inserts


def migrateUsers(userIds, sourceShard, destinationShard):
    """
    Migrate all records for a particular set of user-ids from one physical shard to another.

    @param userIds list of int.
    @param sourceShard str Source connection name.
    @param sourceShard str Destination connection name.
    """
    sourceShard = coerceIdToShardName(sourceShard)
    destinationShard = coerceIdToShardName(destinationShard)

    def genCopyPreCommitCb(mySource, myDestination):
        """Pre-commit callback for copyUser()."""
        def copyPreCommitCb():
            # Lambda function to commit the copy.
            deletePreCommitCb = lambda: db_exec('COMMIT', using=myDestination)

            # Seal the deal.
            # Delete the user.
            deleteUsers(userIds, mySource, preCommitCb=deletePreCommitCb)

        return copyPreCommitCb

    #copyUsers(userIds, sourceShard, destinationShard, copyPreCommitCb, True)
    preCommitCb = genCopyPreCommitCb(sourceShard, destinationShard)
    copyUsers(userIds, sourceShard, destinationShard, preCommitCb=preCommitCb, commitDestinationShard=False)

    # Notify subscribers about update.
    shardId = destinationShard[destinationShard.rindex('_') + 1:]
    se = ShardEvent()
    map(lambda userId: se.publish('movedUser', {'userId': userId, 'shardId': shardId}), userIds)


def migrateUser(userId, sourceShard, destinationShard):
    return migrateUsers([userId], sourceShard, destinationShard)


# dict((table, tuple(fkTable, fkColumn, sourceTable), ..)))
_additionalRelations = {
    'main_receipt': [('main_receipt', 'shortlink_id', 'main_shortlink'),],
    'main_usermessage': [('main_usermessage', 'shortlink_id', 'main_shortlink'),],
    'main_extendeduser': [('main_extendeduser', 'twilio_phone_number_id', 'main_phonenumber'),('main_extendeduser', 'entitlement_id', 'main_entitlement'),],
    'main_groupshare': [('main_groupshare', 'invitation_ptr_id', 'main_invitation'),],
}

def copyUsers(userIds, sourceShard, destinationShard, **kw):
    """
    Migrate all records for a particular user-id from one physical shard to another.

    @param userId int
    @param sourceShard str Source connection name.
    @param sourceShard str Destination connection name.
    @param **kw Dict of optional arguments, including:
        ``preCommitCb`` mixed Function or None Defaults to None.  Function to invoke before the copy is committed.
        ``commitDestinationShard`` bool Defaults to True.  Whether or not to commit the changes to the destination
            shard -- you may instead handle the commit operation yourself in the pre-commit callback.
        ``deactivateTriggers`` bool Defaults to True.  Flag to determine whether or not triggers will be disabled.
        ``manageTransactions`` bool Defaults to True.  Flat to determine whether or not the function will manage the
            transaction.
    """
    preCommitCb = kw.get('preCommitCb', None)
    commitDestinationShard = kw.get('commitDestinationShard', True)
    deactivateTriggers = kw.get('deactivateTriggers', True)
    manageTransactions = kw.get('manageTransactions', True)

    def ifManagingTransactionsThenExec(sql, using):
        """Will only execute the statement if ``manageTransactions`` is True."""
        if manageTransactions is True:
            db_exec(sql, using=using)

    inUserIds = ','.join(map(str, userIds))

    _verifyTheseUsersExistInShard(userIds, sourceShard)

    def remotelyFillTable(sourceTable, sourcePkColumn, innerTable, innerColumn, innerUserIdColumn):
        """Generic way to move rows containing ``userIds`` from one shard to another."""
        if shouldTableBeIgnoredForUserOperations(sourceTable):
            logging.debug(u'Skipping copy to static table: {0}'.format(sourceTable))
            return

        dbLinkSql = toSingleLine(
            '''
                SELECT * FROM "{sourceTable}" WHERE "{pk}" IN (
                    SELECT "{innerColumn}" FROM "{innerTable}" WHERE "{innerUserIdColumn}" in ({userIds})
                )
            '''.format(
                sourceTable=sourceTable,
                pk=sourcePkColumn,
                innerColumn=innerColumn,
                innerTable=innerTable,
                innerUserIdColumn=innerUserIdColumn,
                userIds=inUserIds
            )
        )

        # Insert relevant records from the table.
        autoDbLinkInsert(sourceTable, dbLinkSql, sourceShard, destinationShard)

    # Uniqify set of items while retaining original list order.
    userIdTableColumnPairs = _userIdTableColumnPairs()

    sourceCountsInitial = tableRowCounts(userIdTableColumnPairs, userIds, using=sourceShard)

    dependencies = discoverDependencies(map(lambda x: x[0], userIdTableColumnPairs), using=sourceShard)

    if deactivateTriggers is True:
        # Disable all triggers.
        #db_exec('SELECT fn_modify_all_trigger_states(FALSE)', using=destinationShard)
        db_exec('ALTER TABLE "main_contact" DISABLE TRIGGER "main_contact_trigger"', using=destinationShard)

    ifManagingTransactionsThenExec('BEGIN', using=destinationShard)

    # NB: About set constraints all deferred:
    # http://www.postgresql.org/docs/devel/static/sql-set-constraints.html
    ifManagingTransactionsThenExec('SET CONSTRAINTS ALL DEFERRED', using=destinationShard)

    populatedTables = []

    ordering = []

    userIdTableColumnPairsCopy = list(userIdTableColumnPairs)
    savePoint = 0
    n = 0

    #for table, userIdColumn in userIdTableColumnPairs:
    while len(userIdTableColumnPairsCopy) > 0:
        n += 1
        if n > len(userIdTableColumnPairs) * 2:
            raise Exception('Dependency cycle detected')

        table, userIdColumn = userIdTableColumnPairsCopy.pop(0)
        logging.debug(u'TABLE={0}'.format(table))

        if shouldTableBeIgnoredForUserOperations(table):
            logging.debug(u'Skipping copy to static table: {0}'.format(table))
            continue

        if table in populatedTables:
            logging.info(u'Skipping copy to already populated table: {0}'.format(table))
            continue

        try:
            savePoint += 1
            db_exec('SAVEPOINT save{0}'.format(savePoint), using=destinationShard)

            if table in _additionalRelations:
                for fkTable, fkColumn, sourceTable in _additionalRelations[table]:
                    sourcePkColumn = getPrimaryKeyColumns(sourceTable, using=destinationShard)[0]
                    remotelyFillTable(sourceTable, sourcePkColumn, fkTable, fkColumn, userIdColumn)

            dbLinkSql = '''SELECT * FROM "{0}" WHERE "{1}" IN ({2})'''.format(table, userIdColumn, inUserIds)

            # Insert relevant records from the table.
            autoDbLinkInsert(table, dbLinkSql, sourceShard, destinationShard)
            populatedTables.append(table)
            db_exec('RELEASE SAVEPOINT save{0}'.format(savePoint), using=destinationShard)
            n = 0

        except Exception, e:
            logging.info(u'Caught exception, will handle with it: {0}'.format(e))
            db_exec('ROLLBACK TO save{0}'.format(savePoint), using=destinationShard)
            userIdTableColumnPairsCopy.append((table, userIdColumn))

        ordering.append((table, userIdColumn))

    userIdTableColumnPairsCopy = list(userIdTableColumnPairs)
    savePoint = 0

    # Backfill dependent tables.
    #for table, userIdColumn in userIdTableColumnPairs:
    while len(userIdTableColumnPairsCopy) > 0:
        n += 1
        if n > len(userIdTableColumnPairs) * 2:
            raise Exception('Dependency cycle detected')

        table, userIdColumn = userIdTableColumnPairsCopy.pop(0)

        if shouldTableBeIgnoredForUserOperations(table):
            logging.debug(u'Dependencies backfiller is skipping static table: {0}'.format(table))
            continue

        try:
            savePoint += 1
            db_exec('SAVEPOINT save{0}'.format(savePoint), using=destinationShard)

            # If there are additional dependencies, insert them as well.
            if table in dependencies:
                unpopulatedTables = filter(
                    lambda (col, fkTable, fkCol): fkTable not in populatedTables,
                    dependencies[table]
                )

                for column, fkTable, fkColumn in unpopulatedTables:
                    remotelyFillTable(fkTable, fkColumn, table, column, userIdColumn)
                    populatedTables.append(fkTable)
            db_exec('RELEASE SAVEPOINT save{0}'.format(savePoint), using=destinationShard)
            n = 0

        except Exception, e:
            logging.info(u'Caught exception, will handle with it: {0}'.format(e))
            db_exec('ROLLBACK TO save{0}'.format(savePoint), using=destinationShard)
            userIdTableColumnPairsCopy.append((table, userIdColumn))

    destinationCountsVerify = tableRowCounts(userIdTableColumnPairs, userIds, using=destinationShard)
    sourceCountsVerify = tableRowCounts(userIdTableColumnPairs, userIds, using=sourceShard)

    if destinationCountsVerify == sourceCountsInitial and destinationCountsVerify == sourceCountsVerify:
        # Before proceeding, set constraints to all immediate.
        # Will be applied retroactively, raising issues before more work is performed.
        # @see http://postgresql.org/docs/devel/static/sql-set-constraints.html
        ifManagingTransactionsThenExec('SET CONSTRAINTS ALL IMMEDIATE', using=destinationShard)

        if preCommitCb is not None:
            preCommitCb()

        if commitDestinationShard is True:
            ifManagingTransactionsThenExec('COMMIT', using=destinationShard)

        if deactivateTriggers is True:
            # Re-enable all triggers.
            #db_exec('SELECT fn_modify_all_trigger_states(TRUE)', using=destinationShard)
            db_exec('ALTER TABLE "main_contact" ENABLE TRIGGER "main_contact_trigger"', using=destinationShard)

    else:
        ifManagingTransactionsThenExec('ROLLBACK', using=destinationShard)

        if deactivateTriggers is True:
            # Re-enable all triggers.
            #db_exec('SELECT fn_modify_all_trigger_states(TRUE)', using=destinationShard)
            db_exec('ALTER TABLE "main_contact" ENABLE TRIGGER "main_contact_trigger"', using=destinationShard)

        raise MigrateUserStaleReadError(
            'Aborted migration of userIds={0} from {1} to {2} due to changed ' \
            'source data\n\nsourceCountsInitial={3}\n\n' \
            'destinationCountsVerify={4}\n\nsourceCountsVerify={5}'.format(
                userIds,
                sourceShard,
                destinationShard,
                sourceCountsInitial,
                destinationCountsVerify,
                sourceCountsVerify
            )
        )


def copyUser(userId, sourceShard, destinationShard, **kw):
    """Copy a single user."""
    return copyUsers([userId], sourceShard, destinationShard, **kw)


def deleteUsers(userIds, using, **kw):
    """
    Completely delete a user and all of their data from a shard.

    @param userIds
    @param using str Database connection handle to use.
    @param **kw Dict of optional arguments, including:
        ''preCommitCb mixed Function or None.  Pre-commit callback function, invoked immediately before COMMIT.
        ``manageTransactions`` bool Defaults to True.  Flat to determine whether or not the function will manage the
            transaction.
    """
    preCommitCb = kw.get('preCommitCb', None)
    manageTransactions = kw.get('manageTransactions', True)

    def ifManagingTransactionsThenExec(sql, using):
        """Will only execute the statement if ``manageTransactions`` is True."""
        if manageTransactions is True:
            db_exec(sql, using=using)

    inUserIds = ','.join(map(str, userIds))

    userIdTableColumnPairs = findTablesWithUserIdColumn(using=using)

    dependencies = discoverDependencies(map(lambda x: x[0], userIdTableColumnPairs), using=using)

    clearedTables = []

    origLen = len(userIdTableColumnPairs)
    n = 0 # Count number of iterations since last success.

    ifManagingTransactionsThenExec('BEGIN', using=using)

    # NB: About set constraints all deferred:
    # http://www.postgresql.org/docs/devel/static/sql-set-constraints.html
    ifManagingTransactionsThenExec('SET CONSTRAINTS ALL DEFERRED', using=using)

    # Temporary hacks.
    sqls = [
        '''
        DELETE FROM "main_voicemailtranscription" WHERE "voiceMail_id" IN (
            SELECT "id" FROM "main_voicemail" WHERE "user_id" IN ({0})
        )
        ''',
        '''
        DELETE FROM "main_groupshare" WHERE "invitation_ptr_id" IN (
                SELECT "id" FROM "main_invitation" WHERE "user_id" IN ({0})
            )
        ''',
        '''
        DELETE FROM "main_groupshare" WHERE "invitation_ptr_id" IN (
            SELECT "id" FROM "main_invitation" WHERE "owner_id" IN ({0})
        )
        ''',
        '''
        DELETE FROM "main_sendhubinvitation" WHERE "invitation_ptr_id" IN (
            SELECT "id" FROM "main_invitation" WHERE "user_id" IN ({0})
        )
        ''',
        '''
        DELETE FROM "main_sendhubinvitation" WHERE "invitation_ptr_id" IN (
            SELECT "id" FROM "main_invitation" WHERE "owner_id" IN ({0})
        )
        ''',
        '''
        DELETE FROM "main_enterpriseinvitation" WHERE "invitation_ptr_id" IN (
            SELECT "id" FROM "main_invitation" WHERE "user_id" IN ({0})
        )
        ''',
        '''
        DELETE FROM "main_enterpriseinvitation" WHERE "invitation_ptr_id" IN (
            SELECT "id" FROM "main_invitation" WHERE "owner_id" IN ({0})
        )
        ''',
        '''
        DELETE FROM "main_invitation" WHERE "owner_id" IN ({0})
        ''',
        '''
        DELETE FROM "main_usermessage_contacts" WHERE "contact_id" IN (
            SELECT "id" FROM "main_contact" WHERE "user_id" IN ({0})
        )
        ''',
        '''
        DELETE FROM "main_contact_groups" WHERE "contact_id" IN (
            SELECT "id" FROM "main_contact" WHERE "user_id" IN ({0})
        )
        ''',
        '''
        DELETE FROM "main_contactparent" WHERE "contact_id" IN (
            SELECT "id" FROM "main_contact" WHERE "user_id" IN ({0})
        )
        ''',
        '''
        DELETE FROM "main_usermessage_groups" WHERE "group_id" IN (
            SELECT "id" FROM "main_group" WHERE "user_id" IN ({0})
        )
        ''',
        '''
        DELETE FROM "main_groupshortcode" WHERE "group_id" IN (
            SELECT "id" FROM "main_group" WHERE "user_id" IN ({0})
        )
        ''',
        '''
        DELETE FROM "main_callobservation" WHERE "voiceCall" IN (
            SELECT "id" FROM "main_voicecall" WHERE "user_id" IN ({0})
        )
        ''',
        '''
        DELETE FROM "main_voicecallrating" WHERE "voiceCall" IN (
            SELECT "id" FROM "main_voicecall" WHERE "user_id" IN ({0})
        )
        ''',
        '''
        DELETE FROM "main_phonenumber" WHERE "id" IN (
            SELECT "twilio_phone_number_id" FROM "main_extendeduser" WHERE "user_id" IN ({0})
        )
        ''',
        '''
        DELETE FROM "main_entitlement" WHERE "id" IN (
            SELECT "entitlement_id" FROM "main_extendeduser" WHERE "user_id" IN ({0})
        )
        ''',
        '''
        DELETE FROM "main_receipt" WHERE "group_id" IN (
            SELECT "id" FROM "main_group" WHERE "user_id" IN ({0})
        )
        ''',
    ]
    map(lambda sql: db_exec(toSingleLine(sql.format(inUserIds)), using=using), sqls)
    del sqls

    savePoint = 0

    while len(userIdTableColumnPairs) > 0:
        logging.info(u'Number of table,column pairs remaining: {0}'.format(len(userIdTableColumnPairs)))
        n += 1
        if n > origLen * 2:
            logging.info(u'userIdTableColumnPairs={0}'.format(userIdTableColumnPairs))
            raise Exception('Dependency cycle detected')

        table, userIdColumn = userIdTableColumnPairs.pop(0)

        if shouldTableBeIgnoredForUserOperations(table):
            logging.debug(u'[{0}] Skipping deletion from static table: {1}'.format(using, table))
            continue

        logging.info(u'[{0}] Deleting from table: {1}'.format(using, table))

        try:
            savePoint += 1
            db_exec('SAVEPOINT save{0}'.format(savePoint), using=using)

            if table in _additionalRelations:
                for fkTable, fkColumn, sourceTable in _additionalRelations[table]:
                    if shouldTableBeIgnoredForUserOperations(fkTable):
                        logging.debug(u'[{0}] Skipping deletion from static table: {1}'.format(using, sourceTable))
                        continue

                    logging.info(u'[{0}] Deleting from subtable: {1}'.format(using, sourceTable))

                    deleteSql = toSingleLine(
                        '''
                            DELETE FROM "{sourceTable}" WHERE "{pk}" IN (
                                SELECT "{fkColumn}" FROM "{fkTable}" WHERE "{userIdColumn}" IN ({userIds})
                            )
                        '''.format(
                            sourceTable=sourceTable,
                            pk=getPrimaryKeyColumns(sourceTable)[0],
                            fkColumn=fkColumn,
                            fkTable=fkTable,
                            userIdColumn=userIdColumn,
                            userIds=inUserIds
                        )
                    )
                    db_exec(deleteSql, using=using)

            if table in dependencies:
                # If there are additional dependents, delete them first.
                for column, fkTable, fkColumn in dependencies[table]:
                    if shouldTableBeIgnoredForUserOperations(fkTable):
                        logging.debug(u'[{0}] Skipping deletion from static table: {1}'.format(using, fkTable))
                        continue

                    logging.info(u'[{0}] Deleting from subtable: {1}'.format(using, fkTable))

                    deleteSql = toSingleLine(
                        '''
                            DELETE FROM "{fkTable}" WHERE "{fkColumn}" IN (
                                SELECT "{column}" FROM "{table}" WHERE "{userIdColumn}" IN ({userIds})
                            )
                        '''.format(
                            fkTable=fkTable,
                            fkColumn=fkColumn,
                            column=column,
                            table=table,
                            userIdColumn=userIdColumn,
                            userIds=inUserIds
                        )
                    )
                    db_exec(deleteSql, using=using)

            deleteSql = toSingleLine(
                '''DELETE FROM "{0}" WHERE "{1}" IN ({2})'''.format(table, userIdColumn, inUserIds)
            )
            db_exec(deleteSql, using=using)

            clearedTables.append(table)

            db_exec('RELEASE SAVEPOINT save{0}'.format(savePoint), using=using)
            # Reset cycle detector counter.
            n = 0

        except Exception, e:
            logging.info(
                u'[{0}] Dealing with IntegrityError -----\n{1}----- for table={2}/userIdColumn={3}'
                .format(using, e, table, userIdColumn)
            )
            db_exec('ROLLBACK TO save{0}'.format(savePoint), using=using)
            userIdTableColumnPairs.append((table, userIdColumn))
            if 'waits for ShareLock on transaction' in str(e):
                raise e

    try:
        # Set constraints to all immediate, which will be applied retroactively
        # (raising any problems BEFORE commits have happened).
        # @see http://postgresql.org/docs/devel/static/sql-set-constraints.html
        ifManagingTransactionsThenExec('SET CONSTRAINTS ALL IMMEDIATE', using=using)

        if preCommitCb is not None:
            logging.info(u'deleteUser invoking pre-commit callback')
            preCommitCb()

        logging.info(u'Committing deletion on {0}'.format(using))
        ifManagingTransactionsThenExec('COMMIT', using=using)

        return True

    except Exception, e:
        ifManagingTransactionsThenExec('ROLLBACK', using=using)
        raise MigrateUserError(e.message)


def deleteUser(userId, using, **kw):
    """Delete a single user."""
    return deleteUsers([userId], using, **kw)

