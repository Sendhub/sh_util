# -*- coding: utf-8 -*-

"""
Postgres-specific data tooling.

Including:
    - User deletion
"""

import re
import settings
import logging
from . import db_exec
from .reflect import discoverDependencies
from .reflect import findTablesWithUserIdColumn
from .reflect import getPrimaryKeyColumns

# Used to cleanup SQL queries sometimes (not always guaranteed to be safe
# WRT messing up your SQL query, discretion required).
_spacesRe = re.compile(r'\s+', re.M)
_toSingleLine = lambda s: _spacesRe.sub(' ', s).strip()


class DeleteUserError(Exception):
    """General user migration error."""


def shouldTableBeIgnoredForUserOperations(table):
    """
    @return True if user-specific data does not live in specified table,
        otherwise False.
    """
    return table in settings.STATIC_TABLES


seedTableColumnPairs = (
    ('auth_user', 'id'),
    ('main_extendeduser', 'user_id'),
    ('main_usermessage', 'user_id'),
    ('main_thread', 'userId'),
    ('main_contact', 'user_id'),
    ('main_group', 'user_id'),
)

# dict((table, tuple(fkTable, fkColumn, sourceTable), ..)))
_additionalRelations = {
    'main_receipt': [
        ('main_receipt', 'shortlink_id', 'main_shortlink'),
        ],
    'main_usermessage': [
        ('main_usermessage', 'shortlink_id', 'main_shortlink'),
        ],
    'main_extendeduser': [
        ('main_extendeduser', 'twilio_phone_number_id', 'main_phonenumber'),
        ('main_extendeduser', 'entitlement_id', 'main_entitlement'),
        ],
    'main_groupshare': [
        ('main_groupshare', 'invitation_ptr_id', 'main_invitation'),
        ],
    'main_sendhubemailverification': [
        ('main_sendhubemailverification',
         'invitation_ptr_id',
         'main_invitation'),
        ],
    'main_sendhubphonenumber': [
        ('main_sendhubphonenumber',
         'twilioPhoneNumber_id',
         'main_phonenumber')
        ]
}


def deleteUsers(userIds, using='default', **kw):
    """
    Completely delete a user and all of their data.

    @param userIds
    @param using str Database connection handle to use.
    @param **kw Dict of optional arguments, including:
        ''preCommitCb mixed Function or None.
            Pre-commit callback function, invoked immediately before COMMIT.

        ``manageTransactions`` bool Defaults to True.
            Flat to determine whether or not the function will manage the
            transaction.
    """
    preCommitCb = kw.get('preCommitCb', None)
    manageTransactions = kw.get('manageTransactions', True)

    def ifManagingTransactionsThenExec(sql, using):
        """
        Will only execute the statement if ``manageTransactions`` is True.
        """
        if manageTransactions is True:
            db_exec(sql, using=using)

    inUserIds = ','.join(map(str, userIds))

    userIdTableColumnPairs = findTablesWithUserIdColumn(using=using)

    dependencies = discoverDependencies(
        map(lambda x: x[0], userIdTableColumnPairs), using=using)

    clearedTables = []

    origLen = len(userIdTableColumnPairs)
    n = 0  # Count number of iterations since last success.

    ifManagingTransactionsThenExec('BEGIN', using=using)

    # NB: About set constraints all deferred:
    # http://www.postgresql.org/docs/devel/static/sql-set-constraints.html
    ifManagingTransactionsThenExec('SET CONSTRAINTS ALL DEFERRED', using=using)

    # Temporary hacks.
    sqls = [
        '''
        DELETE FROM main_shortlink
        WHERE id
        IN (
            SELECT shortlink_id FROM main_usermessage WHERE user_id IN ({0})
            UNION
            SELECT shortlink_id FROM main_receipt WHERE "userId" IN ({0})
        );
        ''',
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
        DELETE FROM "main_sendhubemailverification" WHERE "invitation_ptr_id"
        IN (
            SELECT "id" FROM "main_invitation" WHERE "user_id" IN ({0})
        )
        ''',
        '''
        DELETE FROM "main_sendhubemailverification" WHERE "invitation_ptr_id"
        IN (
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
            SELECT "twilio_phone_number_id" FROM "main_extendeduser"
            WHERE "user_id" IN ({0})
        )
        ''',
        '''
        DELETE FROM "main_entitlement" WHERE "id" IN (
            SELECT "entitlement_id" FROM "main_extendeduser"
            WHERE "user_id" IN ({0})
        )
        ''',
        '''
        DELETE FROM "main_receipt" WHERE "group_id" IN (
            SELECT "id" FROM "main_group" WHERE "user_id" IN ({0})
        )
        ''',
    ]
    map(lambda sql: db_exec(
        _toSingleLine(sql.format(inUserIds)), using=using), sqls)
    del sqls

    savePoint = 0

    while len(userIdTableColumnPairs) > 0:
        logging.info(u'Number of table,column pairs remaining: {0}'.format(
            len(userIdTableColumnPairs)))
        n += 1
        if n > origLen * 2:
            logging.info(u'userIdTableColumnPairs={0}'.format(
                userIdTableColumnPairs))
            raise Exception('Dependency cycle detected')

        table, userIdColumn = userIdTableColumnPairs.pop(0)

        if shouldTableBeIgnoredForUserOperations(table):
            logging.debug(
                u'[{0}] Skipping deletion from static '
                u'table: {1}'.format(using, table))
            continue

        logging.info(u'[{0}] Deleting from table: {1}'.format(using, table))

        try:
            savePoint += 1
            db_exec('SAVEPOINT save{0}'.format(savePoint), using=using)

            if table in _additionalRelations:
                for fkTable, fkColumn, sourceTable in \
                        _additionalRelations[table]:
                    if shouldTableBeIgnoredForUserOperations(fkTable):
                        logging.debug(u'[{0}] Skipping deletion from static '
                                      u'table: {1}'.format(using, sourceTable))
                        continue

                    logging.info(u'[{0}] Deleting from subtable: {1}'.format(
                        using, sourceTable))

                    deleteSql = _toSingleLine(
                        '''
                            DELETE FROM "{sourceTable}" WHERE "{pk}" IN (
                                SELECT "{fkColumn}" FROM "{fkTable}"
                                WHERE "{userIdColumn}" IN ({userIds})
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
                        logging.debug(
                            u'[{0}] Skipping deletion from static '
                            u'table: {1}'.format(using, fkTable))
                        continue

                    logging.info(u'[{0}] Deleting from subtable: {1}'.format(
                        using, fkTable))

                    deleteSql = _toSingleLine(
                        '''
                            DELETE FROM "{fkTable}" WHERE "{fkColumn}" IN (
                                SELECT "{column}" FROM "{table}"
                                WHERE "{userIdColumn}" IN ({userIds})
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

            deleteSql = _toSingleLine(
                '''DELETE FROM "{0}" WHERE "{1}" IN ({2})'''.format(
                    table, userIdColumn, inUserIds)
            )
            db_exec(deleteSql, using=using)

            clearedTables.append(table)

            db_exec('RELEASE SAVEPOINT save{0}'.format(savePoint), using=using)
            # Reset cycle detector counter.
            n = 0

        except Exception, e:
            logging.info(
                u'[{0}] Dealing with IntegrityError -----\n{1}----- for '
                u'table={2}/userIdColumn={3}'
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
        ifManagingTransactionsThenExec(
            'SET CONSTRAINTS ALL IMMEDIATE', using=using)

        if preCommitCb is not None:
            logging.info(u'deleteUser invoking pre-commit callback')
            preCommitCb()

        logging.info(u'Committing deletion on {0}'.format(using))
        ifManagingTransactionsThenExec('COMMIT', using=using)

        return True

    except Exception, e:
        ifManagingTransactionsThenExec('ROLLBACK', using=using)
        raise DeleteUserError(e.message)


def deleteUser(userId, using='default', **kw):
    """Delete a single user."""
    return deleteUsers([userId], using, **kw)
