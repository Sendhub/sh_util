"""
Postgres-specific data tooling, primarily to assist with operational
management of logical and physical sharding.

Including:
    - Table data sync/replication
    - User and logical shard shard relocation

NB: Things won't work right if any of the connection names are integers,
or if the string of the connection name
resolves to truthy for str.isdigit().
"""

__author__ = "Jay Taylor [@jtaylor]"

import logging
import re
import time
from collections import OrderedDict
from io import StringIO

import settings
import simplejson as sjson

from ..functional import memoize
from ..memcache import attempt_memcache_flush
from ..s3 import upload_file
from ..sharding import ShardedResource, ShardEvent, coerce_id_to_shard_name
from . import connections, db_exec, db_query, get_psql_connection_string
from .distributed import table_description_to_db_link_t
from .reflect import (
    describe,
    discover_dependencies,
    find_tables_with_user_id_column,
    get_primary_key_columns,
    update_primary_key_id,
)

s3MigrationBackupPath = "/logicalShardMigrations"

_SET_CONSTRAINTS_ALL_DEFERRED = "SET CONSTRAINTS ALL DEFERRED"
_RELEASE_SAVEPOINT_AUTO_DB_LINK_INSERT = "RELEASE SAVEPOINT auto_db_link_insert"
_SKIPPING_COPY_TO_STATIC_TABLE_MSG = "Skipping copy to static table: %s"
_DEPENDENCY_CYCLE_DETECTED_MSG = "Dependency cycle detected"
_SKIPPING_DELETION_FROM_STATIC_TABLE_MSG = "[%s] Skipping deletion from static table: %s"


def _base_backup_file_name(logical_shard_id, ts):
    """@return string containing a base backup filename."""
    return f"{s3MigrationBackupPath}/id-{logical_shard_id}_{int(ts)}"


# Used to cleanup SQL queries sometimes (not always guaranteed to be safe
# WRT messing up your SQL query, discretion required).
_spacesRe = re.compile(r"\s+", re.M)


def to_single_line(s):
    """to single line"""
    return _spacesRe.sub(" ", s).strip()


class MigrateUserError(Exception):
    """General user migration error."""


class MigrateUserStaleReadError(MigrateUserError):
    """Stale-data read error."""


class UserIdResolutionError(Exception):
    """Raised when a user-id cannot be resolved for a set of thread members."""


class MaxRetriesExceededError(Exception):
    """Raised when a dump/copy operation exhausts its retry budget."""


class DependencyCycleError(Exception):
    """Raised when table dependency ordering detects a cycle."""


def should_table_be_ignored_for_user_operations(table):
    """
    @return True if user-specific data does not live in
    specified table, otherwise False.
    """
    return table in settings.STATIC_TABLES or table in settings.SHARDING_IGNORE_TABLES


def does_the_table_data_differ(table, source1, source2):
    """
    Determine if the table data differs across hosts (shards).

    NB: Hardcoded not to work on tables with more than 100,000 rows.
    This should never be used for tables that may grow
    to that size (or even close to that).

    @return True if the data differs between source1 and source2,
    otherwise False.
    """
    count_sql = f'SELECT COUNT(*) FROM "{table}"'

    count1 = db_query(count_sql, using=source1)[0][0]
    count2 = db_query(count_sql, using=source2)[0][0]

    # NEVER USE THIS ON TABLES WITH MORE THAN 100,000 ROWS!
    assert count1 < 100001 and count2 < 100001

    if count1 != count2:
        return True

    # Dynamically lookup PK and generate order clause.
    order_by = ", ".join(map('"{}"'.format, get_primary_key_columns(table, source1)))

    data_sql = f'SELECT * FROM "{table}" ORDER BY {order_by} DESC'

    data1 = db_query(data_sql, using=source1)
    data2 = db_query(data_sql, using=source2)

    return data1 != data2


def replicate_table(table, source, destination):
    """
    Replicate a static table from one database connection to another.
    The destination table will pull the data
    directly from the source db's table.

    :@param table: str Table name.
    :@param source: str Source connection name.
    :@parm destination: str Destination connection name.
    """
    # Only operate on static tables defined in settings.
    assert table in settings.STATIC_TABLES

    # Validate source and destination connection names.
    assert source in connections() and destination in connections()

    # Check to see if the table data matches in both locations.
    # If it does, then no further work is required.
    differ = does_the_table_data_differ(table, source, destination)
    if not differ:
        return

    logging.info(f"Replicating table {table} from {source} -> {destination}")

    # Let the refresh begin!
    connection_string = get_psql_connection_string(source)

    description = describe(table, using=destination)

    columns = [f'"{d[0]}"' for d in description]

    db_link_t = table_description_to_db_link_t(description)

    sql = f'''INSERT INTO "{table}" ({columns}) SELECT {columns} FROM dblink('{connection_string}', 'SELECT {columns} FROM "{table}"') AS {db_link_t}'''

    try:
        db_exec("BEGIN", using=destination)
        db_exec(_SET_CONSTRAINTS_ALL_DEFERRED, using=destination)
        # NB: Truncate wouldn't work here, because TRUNCATE is a DDL statement.
        # @see
        db_exec(f'DELETE FROM "{table}"', using=destination)
        db_exec(sql, using=destination)
        db_exec("COMMIT", using=destination)

    except Exception as e:
        error_message = f"[ERROR] replicate_table caught exception with table={table} source={source} destination={destination}: {e}"

        logging.error(error_message)
        db_exec("ROLLBACK", using=destination)

        from ..mail import send_email

        send_email(subject=f'[URGENT] Table sync error on "{table}"', body=error_message, from_address="devops@sendhub.com", to_address="devops@sendhub.com")


def auto_db_link_insert(table, db_link_sql, source_connection_string, using="default", pk=None):
    """
    Automatically generate and execute the autoDb part of the SQL statement
    to insert a remote dataset for a
    particular SELECT query.

    @param table str Name of table
    @param db_link_sql str  <SELECT X FROM Y clause> for table.
    @param source_connection_string str psql-style connection string for the source database.
    @param using str Django connection name -- should be the destination host.
    @param pk str Optional string containing the primary key column name, or None to enable auto-detection.
    """
    if source_connection_string in connections():
        source_connection_string = get_psql_connection_string(source_connection_string)  # noqa

    db_link_sql = to_single_line(db_link_sql)
    db_link_t = table_description_to_db_link_t(describe(table))

    try:
        db_exec("SAVEPOINT auto_db_link_insert", using=using)

        sql = f''' INSERT INTO "{table}" SELECT * FROM dblink( '{source_connection_string}', '{db_link_sql}' ) AS {db_link_t} '''

        db_exec(sql, using=using)
        db_exec(_RELEASE_SAVEPOINT_AUTO_DB_LINK_INSERT, using=using)

    except Exception as exp_err:
        exc_str = str(exp_err)

        if "duplicate key value violates unique constraint" in exc_str:
            logging.warning("Naiive auto_db_link_insert failed, attempting again with pk exclusion..")
            logging.warning(f"Exception was: {type(exp_err)}/{exp_err}")

            db_exec("ROLLBACK TO auto_db_link_insert", using=using)
            db_exec("SAVEPOINT auto_db_link_insert", using=using)

            # NB: Tables with multiple column PK's are not supported.
            pk = pk or get_primary_key_columns(table, using=using)[0]

            # NB: Notice the where clause -- to avoid potential duplicates.
            sql = f'''INSERT INTO "{table}" SELECT * FROM dblink( '{source_connection_string}', '{db_link_sql}' ) AS {db_link_t} WHERE "{pk}" NOT IN (SELECT "{pk}" FROM "{table}")'''

            db_exec(sql, using=using)
            db_exec(_RELEASE_SAVEPOINT_AUTO_DB_LINK_INSERT, using=using)

        else:
            db_exec(_RELEASE_SAVEPOINT_AUTO_DB_LINK_INSERT, using=using)
            raise exp_err


def table_row_counts(table_column_pairs, user_id_or_user_ids, using):
    """
    Get counts for each table with the user-id filter applied.
    Executes a single query to get the results as list((table, count)).

    @param table_column_pairs list of tuples of table/column pairs (where the column contains the user id).
    @param user_id_or_user_ids mixed int user-id or list of user-ids.
    @param using str Connection name.

    @return dict of table -> matching row count
    """
    # True if user_id_or_user_ids is an iterable, otherwise False.
    is_iterable = isinstance(user_id_or_user_ids, (set, list))

    sql = " UNION ".join(
        [
            to_single_line(
                """
                SELECT '{table}' "table", COUNT(*) "count"
                FROM "{table}"
                WHERE "{user_id_column}" {op} {idOrIds}
            """.format(
                    table=table_column[0].strip('"').strip("'"),
                    user_id_column=table_column[1].strip('"'),
                    op="IN" if is_iterable else "=",
                    idOrIds="({})".format(",".join(map(str, user_id_or_user_ids)) if is_iterable else int(user_id_or_user_ids)),
                )
            )
            for table_column in [table for table in table_column_pairs if not should_table_be_ignored_for_user_operations(table)]
        ]
    )  # noqa

    return dict(db_query(sql, using=using))


def scrub_tables(using):
    """d"""
    statements = [
        """
        DELETE FROM "main_phonenumber" WHERE "id" IN (
            SELECT "pn"."id"
            FROM "main_phonenumber" "pn"
                LEFT JOIN "main_extendeduser" "eu" ON "eu"."twilio_phone_number_id" = "pn"."id"
                LEFT JOIN "main_sendhubphonenumber" "spn" ON "spn"."twilioPhoneNumber_id" = "pn"."id"
            WHERE "eu"."twilio_phone_number_id" IS NULL AND "spn"."twilioPhoneNumber_id" IS NULL
        )
        """,
    ]

    for statement in statements:
        db_exec(statement, using=using)


def set_logical_shard_status(logical_shard_id, status):
    """Set the status field for a logical shard."""
    db_exec("""BEGIN""", using=settings.PRIMARY_SHARD_CONNECTION)
    db_exec("""UPDATE "LogicalShard" SET "status" = %s WHERE "id" = %s""", (status, logical_shard_id), using=settings.PRIMARY_SHARD_CONNECTION)
    db_exec("""COMMIT""", using=settings.PRIMARY_SHARD_CONNECTION)


def set_logical_shard_physical_shard_id(logical_shard_id, physical_shard_id, status=None):
    """Set a new physical_shard_id for a logical shard."""
    db_exec("""BEGIN""", using=settings.PRIMARY_SHARD_CONNECTION)

    if status is None:
        db_exec("""UPDATE "LogicalShard" SET "physical_shard_id" = %s WHERE "id" = %s""", (physical_shard_id, logical_shard_id), using=settings.PRIMARY_SHARD_CONNECTION)
    else:
        db_exec("""UPDATE "LogicalShard" SET "physical_shard_id" = %s, "status" = %s WHERE "id" = %s""", (physical_shard_id, status, logical_shard_id), using=settings.PRIMARY_SHARD_CONNECTION)

    db_exec("""COMMIT""", using=settings.PRIMARY_SHARD_CONNECTION)


def _physical_shard_id(logical_shard_id):
    """Lookup a physical shard id for a logical shard id."""
    res = db_query("""SELECT "physical_shard_id" FROM "LogicalShard" WHERE "id" = %s""", (logical_shard_id,), using=settings.PRIMARY_SHARD_CONNECTION)
    return res[0][0] if len(res) > 0 else None


def _logical_shard_user_ids(logical_shard_id, physical_shard_id=None):
    """
    Get all the user-ids in a logical shard.

    @return list(int) of user-ids.
    """
    res = db_query(
        """SELECT "id" FROM "auth_user" WHERE "id" %% %s = %s""", (settings.NUM_LOGICAL_SHARDS, logical_shard_id), using=f"shard_{physical_shard_id or _physical_shard_id(logical_shard_id)}"
    )

    user_ids = [tup[0] for tup in res]

    return user_ids


def _cleanup_straggler_short_links(connection_name):
    """Cleanup orphaned shortlinks."""
    logging.info(f"Cleaning up orphaned straggler shortlinks on connection={connection_name}")
    return db_exec(
        """
        DELETE FROM "main_shortlink"
        WHERE "id" IN (
            SELECT "s"."id" FROM "main_shortlink" "s"
                LEFT JOIN "main_usermessage" "um" ON "um"."shortlink_id" = "s"."id"
                LEFT JOIN "main_receipt" "r" ON "r"."shortlink_id" = "s"."id"
            WHERE "s"."used" IS NOT NULL AND "r"."id" IS NULL AND "um"."id" IS NULL
        )
        """,
        using=connection_name,
    )


def _automatic_duplicate_recovery(logical_shard_id, source_connection_name, destination_connection_name):
    """
    To be invoked at the end of `migrate_logical_shard()` regardless of the outcome.
    """
    logging.info(
        f"_automatic_duplicate_recovery :: invoked with logical_shard_id={logical_shard_id}, source_connection_name={source_connection_name}, destination_connection_name={destination_connection_name}"
    )
    db_exec("ROLLBACK", using=source_connection_name)
    db_exec("ROLLBACK", using=destination_connection_name)
    test = db_query(
        """
        SELECT au1.id
        FROM auth_user au1
        JOIN (SELECT id FROM dblink('{0}', 'SELECT id FROM auth_user WHERE id
        %% {1} = {2}') AS t(id bigint)) au2 on au1.id = au2.id
        WHERE au1.id %% {1} = {2}
        """.format(get_psql_connection_string(destination_connection_name), settings.NUM_LOGICAL_SHARDS, logical_shard_id),
        using=source_connection_name,
    )
    if len(test) > 0:
        logging.warning("Dupe user_ids detected, affected ids: %s", str(", ".join(["(user-id={}, ls_id={})".format(tup[0], tup[0] % settings.NUM_LOGICAL_SHARDS) for tup in test])))  # noqa

        logging.warning("Logical shard migration failed, removing duplicate entries from the destination shard")

        physical_shard_id = re.sub(r"\D", "", source_connection_name)

        assert physical_shard_id.isdigit(), f'Failed to extract physical_shard_id from source connection name "{source_connection_name}"'

        delete_users([x[0] for x in test], using=destination_connection_name)
        _cleanup_straggler_short_links(destination_connection_name)

        db_exec(
            'UPDATE "LogicalShard" SET "physical_shard_id" = %s WHERE "id" = %s',
            (
                physical_shard_id,
                logical_shard_id,
            ),
            using=settings.PRIMARY_SHARD_CONNECTION,
        )

        attempt_memcache_flush()


def migrate_logical_shard(logical_shard_id, destination_shard, **kw):
    """Move all records for a logical shard to the specified physcial shard."""
    physical_shard_id = _physical_shard_id(logical_shard_id)
    assert physical_shard_id is not None

    source_shard = coerce_id_to_shard_name(physical_shard_id)
    assert source_shard != destination_shard

    user_ids = _logical_shard_user_ids(logical_shard_id, physical_shard_id)

    set_logical_shard_status(logical_shard_id, "RELOCATING")

    try:
        # Keep track of initial counts.
        pre_source_counts = table_row_counts(_user_id_table_column_pairs(), user_ids, using=source_shard)

        # migrateUsers(userIds, source_shard, destination_shard)
        started_ts = _dump_and_copy_logical_shard_wrapper(logical_shard_id, destination_shard, source_shard, user_ids, **kw)
        duration = int(time.time() - started_ts)

        started_counts_ts = time.time()
        post_source_counts = table_row_counts(_user_id_table_column_pairs(), user_ids, using=source_shard)
        post_destination_counts = table_row_counts(_user_id_table_column_pairs(), user_ids, using=destination_shard)
        finished_counts_ts = time.time()
        logging.info(f"Tail-end src/dest counts took {int(started_counts_ts - finished_counts_ts)} seconds")

        message = f"duration={duration}s\nnumUsers={len(user_ids)}\npreSourceCounts={pre_source_counts}\npostSourceCounts={post_source_counts}\npostDestinationCounts={post_destination_counts}"
        logging.info(message)

        base_file_name = _base_backup_file_name(logical_shard_id, started_ts)

        if pre_source_counts != post_source_counts or pre_source_counts != post_destination_counts:
            logging.warning("FAILED: Logical shard migration failed due to count mis-match!")
            file_name = f"{base_file_name}.failed"
            logging.info(f"Deleting copied data from destination shard {destination_shard}")
            delete_users(user_ids, destination_shard, **kw)

        else:
            logging.info("SUCCEEDED: pre/post source/destination counts all match")
            file_name = f"{base_file_name}.succeeded"
            new_physical_shard_id = ShardedResource.shard_name_to_id(destination_shard)  # noqa
            logging.info(f"Updating LogicalShard table to point id={logical_shard_id} at physical_shard_id={new_physical_shard_id}")

            set_logical_shard_physical_shard_id(logical_shard_id, new_physical_shard_id, "OK")
            attempt_memcache_flush()
            delete_users(user_ids, source_shard, **kw)

        url = upload_file(file_name, message)
        logging.info(f"Stored migration run note at {url}")

    except AssertionError as e:
        logging.warning(f"Assertion failed while migrating userIds={user_ids} from {source_shard} to {destination_shard}: {e}")

    finally:
        _automatic_duplicate_recovery(logical_shard_id, source_shard, destination_shard)


class AutomaticErrorResolver:
    """
    Base automatic migration error resolver class.

    NB: All AutomaticErrorResolvers must have a `.run()` method.
    """

    def __init__(self, using, regex_str):
        """
        @param using str Db connection name to resolve conflict on (where data will be altered).
        @param regex_str str Regular expression string to be used.
        """
        self.using = using
        self.regex_str = regex_str
        self.match = None

    def matches(self, exc):
        """Determine if a particular exception matches the regular expression of this AutomaticErrorResolver."""
        self.match = re.match(self.regex_str, str(exc).replace("\n", " "))
        if not self.match:
            return False
        return True

    def validate_runnability(self):
        """
        Ensure this resolver is in a ready-to-run state.
        All children should invoke this method before starting `.run()` to validate their state is good.
        """
        assert self.match is not None, "Error: AutomaticErrorResolver instance method `.run()` invoked after `.matches()` failed: no match was found in the first place"

    def run(self):
        """run"""
        raise NotImplementedError("All children of AutomaticErrorResolver must implement their own `run()` method")


class DuplicateMixPanelIdResolver(AutomaticErrorResolver):
    """Duplicate mix panel id resolver"""

    def __init__(self, source_shard, destination_shard):
        regex_str = r""".*duplicate key value violates unique constraint "main_extendeduser_mixpanelid_key".*DETAIL: *Key \(mixpanelid\)=\((.+)\) already exists\..*"""
        super().__init__(destination_shard, regex_str)

    def run(self):
        """Verify that the state of `destination_shard` is as expected, and if so, update the conflicting mixpanelid to something new"""
        self.validate_runnability()
        found_value = self.match.group(1)
        db_exec("ROLLBACK", using=self.using)
        num_rows = db_query('SELECT count(*) FROM "main_extendeduser" WHERE "mixpanelid" = %s', (found_value,), using=self.using)[0][0]
        assert num_rows == 1, f"Expected to find 1 row in main_extendeduser where mixpanelid={found_value} on {self.using}, but instead found {num_rows}"
        import uuid

        new_value = str(uuid.uuid4())
        db_exec("BEGIN", using=self.using)
        db_exec(
            'UPDATE "main_extendeduser" SET "mixpanelid" = %s WHERE "mixpanelid" = %s',
            (
                new_value,
                found_value,
            ),
            using=self.using,
        )
        db_exec("COMMIT", using=self.using)
        logging.info('DuplicateMixPanelIdResolver :: updated "%s" to "%s"', str(found_value), str(new_value))


class DuplicateUsernameResolver(AutomaticErrorResolver):
    """duplicate user name resolver"""

    def __init__(self, source_shard, destination_shard):
        regex_str = r""".*duplicate key value violates unique constraint
                    "username".*DETAIL: *Key \(username\)=\((.+)\)
                    already exists\..*"""
        super().__init__(destination_shard, regex_str)

    def run(self):
        """Handles cases where the username is something
        like 'openiduser12'."""
        self.validate_runnability()
        found_value = self.match.group(1)
        assert re.match(r"^\d{10,11}$", found_value) is None, 'Unable to automatically rename user with username "{}"'.format(found_value)
        db_exec("ROLLBACK", using=self.using)
        num_rows = db_query('SELECT count(*) FROM "auth_user" WHERE "username" = %s', (found_value,), using=self.using)[0][0]
        assert num_rows == 1, "Expected to find 1 row in auth_user where username={} on {}, but instead found {}".format(found_value, self.using, num_rows)
        new_value = found_value + found_value[-1]
        db_exec("BEGIN", using=self.using)
        db_exec(
            'UPDATE "auth_user" SET "username" = %s WHERE "username" = %s',
            (
                new_value,
                found_value,
            ),
            using=self.using,
        )
        db_exec("COMMIT", using=self.using)
        logging.info('DuplicateUsernameResolver :: updated "%s" to "%s"', str(found_value), str(new_value))


class DuplicateIdResolver(AutomaticErrorResolver):
    """duplicate id resolver"""

    def __init__(self, source_shard, destination_shard):
        regex_str = r""".*duplicate key value violates unique constraint
                    "(main_usermessage|main_shortlink|main_receipt|main_thread|
                    main_phonenumber|main_userphonenumber|main_voicecall|
                    tastypie_apikey|django_openid_auth_useropenid|
                    main_usermessageshortcode).+".*DETAIL:
                    *Key \(id\)=\(([0-9]+)\) already exists\..*"""
        super().__init__(destination_shard, regex_str)

    def run(self):
        """Updates the duplicate id to a new value."""
        self.validate_runnability()
        table = self.match.group(1)
        current_id = self.match.group(2)
        assert current_id.isdigit(), f"Extracted currentId={current_id}, was expecting a number"
        current_id = int(current_id)
        db_exec("ROLLBACK", using=self.using)
        db_exec("BEGIN", using=self.using)
        new_id = db_query("""SELECT sh_next_id('{}_id_seq')""".format(table), using=self.using)[0][0]
        logging.info('DuplicateIdResolver :: updating "%s" to "%s" on connection=%s', str(current_id), str(new_id), str(self.using))
        update_primary_key_id(table, current_id, new_id, using=self.using)
        db_exec("COMMIT", using=self.using)


class ContactGroupsOverlapResolver(AutomaticErrorResolver):
    """Fix mis-matched contact group membership."""

    def __init__(self, source_shard, destination_shard):
        regex_str = r""".*insert or update on table "main_contact_groups"
                    violates foreign key constraint "[^"]+".*DETAIL:
                    *Key \(group_id\)=\(([0-9]+)\) is not present in
                    table "main_group"\..*"""
        super().__init__(source_shard, regex_str)

    def run(self):
        """Updates the offending contacts-groups records to remove contacts
        from groups where the contact's user-id differs from the
        group's user-id."""
        self.validate_runnability()
        group_id = int(self.match.group(1))
        db_exec("ROLLBACK", using=self.using)
        # Find actual group owner user-id.
        db_exec("BEGIN", using=self.using)
        user_id = db_query('SELECT "user_id" FROM "main_group" WHERE "id" = %s', (group_id,), using=self.using)[0][0]
        db_exec(
            """
            DELETE FROM "main_contact_groups"
            WHERE
                "group_id" = %s AND
                "contact_id" IN (
                    SELECT "c"."id"
                    FROM "main_contact" "c"
                        JOIN "main_contact_groups" "cg" ON
                        "cg"."contact_id" = "c"."id"
                    WHERE "cg"."group_id" = %s AND "c"."user_id" != %s
                )
            """,
            (
                group_id,
                group_id,
                user_id,
            ),
            using=self.using,
        )
        logging.info("ContactGroupsOverlapResolver :: fixed main_contact_groups for group_id=%s on connection=%s", str(group_id), str(self.using))
        db_exec("COMMIT", using=self.using)


class ReceiptOverlapResolver(AutomaticErrorResolver):
    """Fix mis-matched receipts."""

    def __init__(self, source_shard, destination_shard):
        regex_str = r""".*insert or update on table "main_receipt" violates
                    foreign key constraint "[^"]+".*DETAIL:
                    *Key \((contact|group)_id\)=\(([0-9]+)\) is not present
                    in table "main_(contact|group)"\..*"""
        super().__init__(source_shard, regex_str)

    def run(self):
        """Updates the offending receipt and related records
        to belong to the correct user-id."""
        self.validate_runnability()
        table = self.match.group(1)
        current_id = int(self.match.group(2))
        db_exec("ROLLBACK", using=self.using)
        db_exec("BEGIN", using=self.using)
        # Find actual object owner's user-id.
        user_id = db_query('SELECT "user_id" FROM "main_{}" WHERE "id" = %s'.format(table), (current_id,), using=self.using)[0][0]
        db_exec(
            """
            UPDATE "main_thread"
            SET "user_id" = {user_id}
            WHERE "latestUserMessageId" IN (
                SELECT "um"."id" FROM "main_usermessage" "um" JOIN
                "main_receipt" "r" ON "r"."message_id" = "um"."id"
                WHERE "r"."{table}_id" = {current_id}
            )
            """.format(table=table, current_id=current_id, user_id=user_id),
            using=self.using,
        )
        db_exec(
            """
            UPDATE "main_usermessage"
            SET "user_id" = {user_id}
            WHERE "id" IN (
                SELECT "um"."id" FROM "main_usermessage" "um" JOIN
                "main_receipt" "r" ON "r"."message_id" = "um"."id"
                WHERE "r"."{table}_id" = {current_id}
            )
            """.format(table=table, current_id=current_id, user_id=user_id),
            using=self.using,
        )
        db_exec(
            """UPDATE "main_receipt" SET "user_id" = {user_id} WHERE
            "{table}_id" = {current_id}""".format(table=table, current_id=current_id, user_id=user_id),
            using=self.using,
        )
        logging.info("ReceiptOverlapResolver :: fixed mis-matched receipt for %s_id=%s/user_id=%s on connection=%s", str(table), str(current_id), str(user_id), str(self.using))
        db_exec("COMMIT", using=self.using)


def _find_and_validate_user_id_for_thread_members(match, members_json, using):
    """Given a Thread.membersJson field value, resolve the
    members to a single user-id."""
    user_ids_c, user_ids_g = None, None
    contact_ids, group_ids = sjson.loads(members_json)
    assert len(contact_ids) + len(group_ids) != 0, f"threadId={match.group(1)} somehow had no members at all"
    if len(contact_ids) > 0:
        user_ids_c = db_query(
            """SELECT DISTINCT "user_id" FROM "main_contact"
                            WHERE "id" IN ({})""".format(",".join(map(str, contact_ids))),
            using=using,
        )
        assert len(user_ids_c) == 1, "Expected to find a single user-id for contactIds={}, but instead found {}".format(contact_ids, len(user_ids_c))
    if len(group_ids) > 0:
        user_ids_g = db_query(
            """SELECT DISTINCT "user_id" FROM "main_group"
                            WHERE "id" IN ({})""".format(",".join(map(str, group_ids))),
            using=using,
        )
        assert len(user_ids_g) == 1, "Expected to find a single user-id for groupIds={}, but instead found {}".format(group_ids, len(user_ids_g))
    if "user_ids_c" in vars() and "user_ids_g" in vars():
        assert user_ids_c[0][0] == user_ids_g[0][0], "user-id for contacts/groups in membersJson={} did not match: {}, {}".format(members_json, user_ids_c[0], user_ids_g[0])
        return user_ids_c[0][0]
    elif "user_ids_c" in vars():
        return user_ids_c[0][0]
    elif "user_ids_g" in vars():
        return user_ids_g[0][0]
    else:
        raise UserIdResolutionError("Failed to resolve any user-ids for membersJson={}".format(members_json))


class ThreadOverlapResolver(AutomaticErrorResolver):
    """Fix mis-matched threads."""

    def __init__(self, source_shard, destination_shard):
        regex_str = r""".*insert or update on table "main_usermessage" violates
                    foreign key constraint "threadId_.*".*DETAIL:
                    *Key \(threadId\)=\(([0-9]+)\) is not present in
                    table "main_thread"\..*"""
        super().__init__(source_shard, regex_str)

    def run(self):
        """Updates the offending threadId and associated records
        to reference the correct user-id."""
        self.validate_runnability()
        thread_id = int(self.match.group(1))
        db_exec("ROLLBACK", using=self.using)
        db_exec("BEGIN", using=self.using)
        # Find actual object owner's user-id.
        incorrect_user_id, members_json = db_query('SELECT "user_id", "membersJson" FROM "main_thread" WHERE "id" = %s', (thread_id,), using=self.using)[0]
        user_id = _find_and_validate_user_id_for_thread_members(self.match, members_json, self.using)

        if incorrect_user_id == user_id:
            db_exec(
                """UPDATE "main_thread" SET "latestUserMessageId" = NULL
                    WHERE "id" = %s""",
                (thread_id,),
                using=self.using,
            )
            logging.info("ThreadOverlapResolver :: fixed mis-matched thread for threadId=%s, nulled out latestUserMessageId onconnection=%s", str(thread_id), str(self.using))

        else:
            db_exec(
                """UPDATE "main_receipt" SET "user_id" = %s WHERE
                    "message_id" IN (SELECT "id" FROM "main_usermessage"
                    WHERE "threadId" = %s)""",
                (
                    user_id,
                    thread_id,
                ),
                using=self.using,
            )
            db_exec(
                """UPDATE "main_usermessage" SET "user_id" = %s
                    WHERE "threadId" = %s""",
                (
                    user_id,
                    thread_id,
                ),
                using=self.using,
            )
            db_exec(
                """UPDATE "main_thread" SET "user_id" = %s WHERE
                    "id" = %s""",
                (
                    user_id,
                    thread_id,
                ),
                using=self.using,
            )
            logging.info(
                "ThreadOverlapResolver :: fixed mis-matched thread for threadId=%s, incorrectUserId=%s correctUserId=%s on connection=%s",
                str(thread_id),
                str(incorrect_user_id),
                str(user_id),
                str(self.using),
            )
        db_exec("COMMIT", using=self.using)


class BlockMismatchResolver(AutomaticErrorResolver):
    """Fix mis-matched receipts."""

    def __init__(self, source_shard, destination_shard):
        regex_str = r""".*insert or update on table "main_block" violates
                    foreign key constraint "message_id.*".*DETAIL:
                    *Key \(message_id\)=\(([0-9]+)\) is not present in
                    table "main_usermessage"\..*"""
        super().__init__(source_shard, regex_str)

    def run(self):
        """Updates the offending related block records to
        belong to the correct user-id."""
        self.validate_runnability()
        user_message_id = int(self.match.group(1))
        db_exec("ROLLBACK", using=self.using)
        db_exec("BEGIN", using=self.using)
        user_message_ids = []
        user_id = None
        # Find all blocks from the conflicting user message id.
        blocks = db_query(
            """SELECT * FROM "main_block" WHERE "blocked_user_id" =
            (SELECT "blocked_user_id" FROM "main_block" WHERE
            "message_id" = %s LIMIT 1)""",
            (user_message_id,),
            using=self.using,
            as_dict=True,
        )
        for block in blocks:
            # Require that the blocked user-id matches the contact user-id.
            contact_user_id = db_query(
                """SELECT "user_id" FROM
                                     "main_contact" WHERE "id" = %s""",
                (block["contact_id"],),
                using=self.using,
            )[0][0]
            assert block["blocked_user_id"] == contact_user_id, "Bad block with id={}, blocked_user_id={} but contactId={} user-id was {}".format(
                block["id"], block["blocked_user_id"], block["contact_id"], contact_user_id
            )
            user_message_ids.append(str(block["message_id"]))
            if user_id is None:
                user_id = block["blocked_user_id"]

        if user_id is None:
            logging.warning("BlockMismatchResolver :: Unexpectedly failed to find block(s) for user with block originatingfrom message_id=%s", str(user_message_id))
            return

        db_exec(
            """UPDATE "main_usermessage" SET "user_id" = %s
                WHERE "id" IN ({})""".format(",".join(user_message_ids)),
            (user_id,),
            using=self.using,
        )
        db_exec(
            """UPDATE "main_receipt" SET "user_id" = %s
                WHERE "id" IN ({})""".format(",".join(user_message_ids)),
            (user_id,),
            using=self.using,
        )
        db_exec(
            """UPDATE "main_thread" SET "user_id" = %s
            WHERE "id" IN (SELECT "threadId" FROM "main_usermessage"
            WHERE "id" IN ({}))""".format(",".join(user_message_ids)),
            (user_id,),
            using=self.using,
        )
        logging.info("BlockMismatchResolver :: fixed mis-matched block records for userMessageId=%s/user_id=%s on connection=%s", str(user_message_id), str(user_id), str(self.using))
        db_exec("COMMIT", using=self.using)


class ThreadMismatchResolver(AutomaticErrorResolver):
    """Fix mis-matched threads."""

    def __init__(self, source_shard, destination_shard):
        regex_str = r""".*insert or update on table "main_thread" violates
                    foreign key constraint "latestUserMessageId_.*".*DETAIL:
                    +Key \(latestUserMessageId\)=\(([0-9]+)\) is not present
                    in table "main_usermessage"\..*"""
        super().__init__(source_shard, regex_str)

    def run(self):
        """Updates the offending related block records to belong to the
        correct user-id."""
        self.validate_runnability()
        user_message_id = int(self.match.group(1))
        db_exec("ROLLBACK", using=self.using)
        db_exec("BEGIN", using=self.using)

        user_id0, members_json = db_query(
            """SELECT "user_id", "membersJson" FROM
                               "main_thread" WHERE
                               "latestUserMessageId" = %s""",
            (user_message_id,),
            using=self.using,
        )[0]
        user_id = _find_and_validate_user_id_for_thread_members(self.match, members_json, self.using)
        assert user_id0 == user_id, "Thread with membersJson={} is too borked to handle automatically".format(members_json)

        db_exec(
            """UPDATE "main_receipt" SET "user_id" = %s WHERE
                "message_id" = %s""",
            (
                user_id,
                user_message_id,
            ),
            using=self.using,
        )
        db_exec(
            """UPDATE "main_usermessage" SET "user_id" = %s WHERE
                "id" = %s""",
            (
                user_id,
                user_message_id,
            ),
            using=self.using,
        )
        unintelligible_receipt_ids = [
            str(row[0])
            for row in db_query(
                """
            SELECT "r"."id"
            FROM "main_receipt" "r"
                JOIN "main_contact" "c" ON "c"."id" = "r"."contact_id"
            WHERE "r"."message_id" = %s AND "r"."user_id" = %s
            AND "c"."user_id" != "r"."user_id"
            """,
                (
                    user_message_id,
                    user_id,
                ),
                using=self.using,
            )
        ]
        if len(unintelligible_receipt_ids) > 0:
            logging.info("ThreadMismatchResolver :: found %s unintelligible receipts, ids=%s", str(len(unintelligible_receipt_ids)), str(unintelligible_receipt_ids))
            db_exec(
                """DELETE FROM "main_receipt" WHERE "message_id" = %s
                    AND "id" IN ({})""".format(",".join(unintelligible_receipt_ids)),
                (user_message_id,),
                using=self.using,
            )
        logging.info("ThreadMismatchResolver :: fixed mismatched thread with lastestUserMessageId=%s/user_id=%s on connection=%s", str(user_message_id), str(user_id), str(self.using))
        db_exec("COMMIT", using=self.using)


class MismatchedContactOrGroupResolver(AutomaticErrorResolver):
    """Fix mis-matched threads."""

    def __init__(self, source_shard, destination_shard):
        regex_str = r""".*insert or update on table
                    "main_usermessage_(contact|group)s" violates foreign key
                    constraint "main_usermessage_(?:contact|group)s_
                    (?:contact|group)_id_fk".*DETAIL:
                    *Key \((?:contact|group)_id\)=\(([0-9]+)\) is not present
                    in table "main_(?:contact|group)"\..*"""
        super().__init__(source_shard, regex_str)

    def run(self):
        """Updates the offending usermessages to belong
        to the correct user-id."""
        self.validate_runnability()
        object_type = self.match.group(1)
        assert object_type in ("contact", "group"), f"Unrecognized object type: {object_type}"
        object_id = int(self.match.group(2))
        db_exec("ROLLBACK", using=self.using)
        db_exec("BEGIN", using=self.using)

        user_id = db_query(
            """SELECT "user_id" FROM "main_{}"
                          WHERE "id" = %s""".format(object_type),
            (object_id,),
            using=self.using,
        )[0][0]
        bad_user_message_ids = [
            str(row[0])
            for row in db_query(
                """SELECT "um"."id" FROM "main_usermessage_{0}s" "t"
            JOIN "main_usermessage" "um" ON "um"."id" = "t"."usermessage_id"
            WHERE "t"."{0}_id" = %s AND "um"."user_id" != %s""".format(object_type),
                (
                    object_id,
                    user_id,
                ),
                using=self.using,
            )
        ]
        db_exec(
            """UPDATE "main_usermessage" SET "user_id" = %s WHERE
                "id" IN ({})""".format(",".join(bad_user_message_ids)),
            (user_id,),
            using=self.using,
        )
        logging.info(
            "MismatchedContactOrGroupResolver :: fixed mismatched usermessages for %sId=%s to belong to user_id=%s on connection=%s", str(object_type), str(object_id), str(user_id), str(self.using)
        )
        db_exec("COMMIT", using=self.using)


class ReceiptMismatchResolver(AutomaticErrorResolver):
    """Fix mis-matched threads."""

    def __init__(self, source_shard, destination_shard):
        regex_str = r""".*insert or update on table "main_receipt" violates
                    foreign key constraint
                    "main_receipt__message_id_fk".*DETAIL:
                    Key \(message_id\)=\(([0-9]+)\) is not present in
                    table "main_usermessage"\..*"""
        super().__init__(source_shard, regex_str)

    def run(self):
        """Updates the offending usermessages to belong to
        the correct user-id."""
        self.validate_runnability()
        user_message_id = int(self.match.group(1))
        db_exec("ROLLBACK", using=self.using)
        db_exec("BEGIN", using=self.using)

        incorrect_user_id = db_query(
            """SELECT "user_id" FROM "main_receipt"
                                   WHERE "message_id" = %s LIMIT 1""",
            (user_message_id,),
            using=self.using,
        )[0][0]
        correct_user_id = db_query(
            """
            SELECT "user_id" FROM "main_contact" WHERE
            "id" = (SELECT "contact_id" FROM "main_receipt"
            WHERE "message_id" = %s)
            UNION
            SELECT "user_id" FROM "main_group"
            WHERE "id" = (SELECT "group_id" FROM "main_receipt"
            WHERE "message_id" = %s)
            """,
            (
                user_message_id,
                user_message_id,
            ),
            using=self.using,
        )[0][0]
        assert incorrect_user_id != correct_user_id, 'The "good" user-id must not match the incorrect one, but they did ({} == {})'.format(correct_user_id, incorrect_user_id)
        db_exec(
            """UPDATE "main_usermessage" SET "user_id" = %s
                WHERE "id" = %s""",
            (correct_user_id, user_message_id),
            using=self.using,
        )
        logging.info(
            "ReceiptMismatchResolver :: fixed mismatched userMessageId=%s, correct user_id=%s, incorrect user_id=%s on connection=%s",
            str(user_message_id),
            str(correct_user_id),
            str(incorrect_user_id),
            str(self.using),
        )
        db_exec("COMMIT", using=self.using)


_automaticErrorResolvers = (
    DuplicateMixPanelIdResolver,
    DuplicateUsernameResolver,
    DuplicateIdResolver,
    ContactGroupsOverlapResolver,
    ReceiptOverlapResolver,
    ThreadOverlapResolver,
    BlockMismatchResolver,
    ThreadMismatchResolver,
    MismatchedContactOrGroupResolver,
)


def _find_automatic_error_resolver(source_shard, destination_shard, exc):
    """
    Attempt to find a matching automatic resolver.

    @param exc Exception to match against.

    @return Matching AutomaticErrorResolver instance or None.
    """
    for resolver_class in _automaticErrorResolvers:
        instance = resolver_class(source_shard, destination_shard)
        if instance.matches(exc):
            logging.info("_findAutomaticErrorResolver :: Found matching resolver: %s", str(instance.__class__.__name__))
            return instance
    return None


MAX_DUMP_COPY_ERRORS = 10


def _dump_and_copy_logical_shard_wrapper(logical_shard_id, destination_shard, using, user_ids=None, **kw):
    """Automatically attempts to handle recognized error cases."""
    if "attemptCount" not in kw or "lastException" not in kw:
        # Seed counter during first attempt.
        kw["attemptCount"] = 1
        kw["lastException"] = None

    if kw["attemptCount"] > MAX_DUMP_COPY_ERRORS:
        if "lastException" in kw and kw["lastException"] is not None:
            raise kw["lastException"]
        else:
            raise MaxRetriesExceededError("Max number of dump/copy retries exceeded")

    try:
        return _dump_and_copy_logical_shard(logical_shard_id, destination_shard, using, user_ids, **kw)

    except Exception as e:
        # If the same exception occurs twice in a row, don't
        # keep trying to resolve it the same
        # way (astronomically unlikely to work).
        if "lastException" in kw and kw["lastException"] is not None and str(e) == str(kw["lastException"]):
            logging.error("Got the same exact exception twice, aborting operation")
            raise

        logging.info("_dumpAndCopyLogicalShard :: Caught exception: %s, will try to resolve automatically..", str(e))
        resolver = _find_automatic_error_resolver(using, destination_shard, e)
        if resolver is None:
            logging.error("_dumpAndCopyLogicalShard :: Automatic resolution could not be found")
            raise

        resolver.run()
        # Rollback on the destination shard connection to
        # establish a known transaction state (no txn in progress).
        db_exec("ROLLBACK", using=destination_shard)
        kw["attemptCount"] += 1
        kw["lastException"] = e

        return _dump_and_copy_logical_shard_wrapper(logical_shard_id, destination_shard, using, user_ids, **kw)


def _dump_and_copy_logical_shard(logical_shard_id, destination_shard, using=None, user_ids=None, **kw):
    """
    Dump and copy a logical shard.

    @return int Started timestamp in epoch format (# of seconds since 1970).
    """
    started_ts = time.time()
    dump = _dump_logical_shard(logical_shard_id=logical_shard_id, using=using, user_ids=user_ids, **kw)
    dump_finished_ts = time.time()
    dump_duration = int(dump_finished_ts - started_ts)
    logging.info("LogicalShard dump phase for id=%s took %s seconds", str(logical_shard_id), str(dump_duration))

    sql_statements = _backup_dump_and_convert_to_sql_list(dump, logical_shard_id, started_ts, dump_finished_ts)

    copy_started_ts = time.time()

    num_statements = len(sql_statements)
    logging.info("Executing %s SQL insert statements on %s", str(num_statements), str(destination_shard))

    for i, statement in enumerate(sql_statements):
        statement = statement.replace("%", "%%")
        logging.info("Executing SQL statement %s/%s: %s..", str(i + 1), str(num_statements), str(statement[0:64]))
        db_exec(statement, using=destination_shard)

    copy_finished_ts = time.time()

    copy_duration = int(copy_finished_ts - copy_started_ts)
    logging.info("LogicalShard copy phase for id=%s took %s seconds", str(logical_shard_id), str(copy_duration))

    duration = int(copy_finished_ts - started_ts)
    logging.info("Dump and copy for logical_shard_id=%s took %s seconds", str(logical_shard_id), str(duration))

    return int(started_ts)


def _dump2_sql_string(dump, logical_shard_id, started_ts, finished_ts=None):
    """Convert a logical shard dump to a string of SQL statements."""
    buf = StringIO()
    buf.write("-- Dump of LogicalShard {} on {}\n".format(logical_shard_id, int(started_ts)))
    if finished_ts is not None:
        buf.write("-- Dump of LogicalShard {} finished on {}\n".format(logical_shard_id, int(finished_ts)))
    for key in dump:
        buf.write(f"\n\n-- table = {key}\n")
        for statement in dump[key]:
            buf.write(f"{statement}\n")
    out = buf.getvalue()
    buf.close()
    return out


def _dump2_sql_list(dump):
    """Convert a logical shard dump to a list of SQL statements."""
    out = []
    for key in dump:
        for statement in dump[key]:
            out.append(statement)
    return out


def _backup_dump_and_convert_to_sql_list(dump, logical_shard_id, started_ts, finished_ts):
    """
    Backup a logical shard dump in two formats -- as an SQL string
    and as a JSON list of discrete statements.
    """
    base_file_name = _base_backup_file_name(logical_shard_id, started_ts)

    # Upload SQL string to S3.
    sql_string = _dump2_sql_string(dump, logical_shard_id, started_ts, finished_ts)
    sql_string_url = upload_file(base_file_name + ".sql", sql_string)
    logging.info("Uploaded SQL string dump of logicalShard %s, sqlStringUrl=%s", str(logical_shard_id), str(sql_string_url))

    # Upload JSON-serialized SQL list to S3.
    sql_list = _dump2_sql_list(dump)
    sql_list_url = upload_file(base_file_name + ".json", sjson.dumps(sql_list))
    logging.info("Uploaded JSON list dump of logicalShard %s, sqlStringUrl=%s", str(logical_shard_id), str(sql_list_url))
    return sql_list


def _dump_logical_shard(logical_shard_id, using=None, user_ids=None, **kw):
    """Dump all data for a logical shard."""
    if using is None:
        physical_shard_id = _physical_shard_id(logical_shard_id)
        using = coerce_id_to_shard_name(physical_shard_id)

    if user_ids is None:
        if "physical_shard_id" not in vars():
            physical_shard_id = _physical_shard_id(logical_shard_id)
        user_ids = _logical_shard_user_ids(logical_shard_id, physical_shard_id)

    assert len(user_ids) > 0, f"No users found for logicalShard={logical_shard_id}"

    return dump_users(user_ids, using, **kw)


seedTableColumnPairs = (
    ("auth_user", "id"),
    ("main_extendeduser", "user_id"),
    ("main_usermessage", "user_id"),
    ("main_thread", "user_id"),
    ("main_contact", "user_id"),
    ("main_group", "user_id"),
)

preMigrationSql = [
    "BEGIN;",
    "SET CONSTRAINTS ALL DEFERRED;",
]

postMigrationSql = [
    "SET CONSTRAINTS ALL IMMEDIATE;",
    "COMMIT;",
]


@memoize
def _user_id_table_column_pairs():
    """@return list of <table,column> pairs for tables with user-id columns."""
    # Uniqify set of items while retaining original list order.
    return list(OrderedDict.fromkeys(list(seedTableColumnPairs) + find_tables_with_user_id_column()))  # noqa


def _verify_these_users_exist_in_shard(user_ids, using):
    """Assert that all user-ids exist in the specified database."""
    in_user_ids = ",".join(map(str, user_ids))

    # Verify that the requested users exist on the source_shard indicated.
    user_check = db_query(
        """SELECT count(*) FROM "auth_user"
                         WHERE "id" IN ({})""".format(in_user_ids),
        using=using,
    )
    assert user_check[0][0] == len(user_ids), f"not all userIds in ({user_ids}) not found on {using}"


def dump_users(user_ids, using, **kw):
    """
    Dump complete user records to dict of a list of insert statement lists.

    @param userIds list of int.
    @param using mixed str or int Source connection name or shard id.

    @return dict of <table, list of insert statement lists>.
    """
    from .select2insert import select2multi_insert

    logging.info("Dumping users (%s) from %s", str(user_ids), str(using))

    deactivate_triggers = kw.get("deactivateTriggers", True)

    using = coerce_id_to_shard_name(using)

    _verify_these_users_exist_in_shard(user_ids, using)

    in_user_ids = ",".join(map(str, user_ids))

    # Keep track of inserts on a per-table basis.
    inserts = OrderedDict()
    inserts["__pre__"] = ['ALTER TABLE "main_contact" DISABLE TRIGGER "main_contact_trigger";'] if deactivate_triggers else []  # noqa
    inserts["__pre__"] += preMigrationSql

    def collect_inserts(table, where_clause):
        """
        Given a table and where-clause, appends the list of inserts
        for the matching records from that table to a
        corresponding key for that table in the ``inserts`` dict.
        """
        sql = select2multi_insert(table=table, description=describe(table), using=using, where_clause=where_clause)
        if sql is not None:
            if table not in inserts:
                inserts[table] = []
            inserts[table].append(sql)

    def collect_records(source_table, source_pk_column, inner_table, inner_column, inner_user_id_column):
        """
        Generic way to move rows containing ``userIds``
        from one shard to another.
        """
        if should_table_be_ignored_for_user_operations(source_table):
            logging.debug(_SKIPPING_COPY_TO_STATIC_TABLE_MSG, str(source_table))
            return

        collect_inserts(
            source_table,
            where_clause='"{pk}" IN (SELECT "{inner_column}" FROM "{inner_table}" WHERE "{inner_user_id_column}" in ({user_ids}))'.format(
                pk=source_pk_column, inner_column=inner_column, inner_table=inner_table, inner_user_id_column=inner_user_id_column, user_ids=in_user_ids
            ),
        )

    # Uniqify set of items while retaining original list order.
    user_id_table_column_pairs = _user_id_table_column_pairs()

    dependencies = discover_dependencies([x[0] for x in user_id_table_column_pairs], using=using)  # noqa

    populated_tables = []

    for table, user_id_column in user_id_table_column_pairs:
        logging.debug("(1) TABLE=%s", str(table))

        if should_table_be_ignored_for_user_operations(table):
            logging.debug("Skipping dump from static table: %s", str(table))
            continue

        if table in populated_tables:
            logging.info("Skipping dump from already populated table: %s", str(table))
            continue

        if table in _additionalRelations:
            for fk_table, fk_column, source_table in _additionalRelations[table]:
                source_pk_column = get_primary_key_columns(source_table, using=using)[0]
                collect_records(source_table, source_pk_column, fk_table, fk_column, user_id_column)

        # Collect relevant records from the table.
        collect_inserts(table, f'''"{user_id_column}" IN ({in_user_ids})''')
        populated_tables.append(table)

    # Backfill dependent tables.
    for table, user_id_column in user_id_table_column_pairs:
        logging.debug("(2) TABLE=%s", str(table))

        if should_table_be_ignored_for_user_operations(table):
            logging.debug("Dependencies backfiller is skipping static table: %s", str(table))
            continue

        # If there are additional dependencies, insert them as well.
        if table in dependencies:
            unpopulated_tables = [fk_table for fk_table in dependencies[table] if fk_table not in populated_tables]  # noqa

            for column, fk_table, fk_column in unpopulated_tables:
                collect_records(fk_table, fk_column, table, column, user_id_column)
                populated_tables.append(fk_table)

    inserts["__post__"] = postMigrationSql
    if deactivate_triggers:
        inserts["__post__"].append('ALTER TABLE "main_contact" ENABLE TRIGGER "main_contact_trigger";')

    return inserts


def migrate_users(user_ids, source_shard, destination_shard, **kw):
    """
    Migrate all records for a particular set of user-ids
    from one physical shard to another.

    @param userIds list of int.
    @param sourceShard str Source connection name.
    @param source_shard str Destination connection name.
    """
    source_shard = coerce_id_to_shard_name(source_shard)
    destination_shard = coerce_id_to_shard_name(destination_shard)

    def gen_copy_pre_commit_cb(my_source, my_destination):
        """Pre-commit callback for copyUser()."""

        def copy_pre_commit_cb():
            # Lambda function to commit the copy.
            deletePreCommitCb = lambda: db_exec("COMMIT", using=my_destination)  # noqa

            # Seal the deal.
            # Delete the user.
            delete_users(user_ids, my_source, pre_commit_cb=deletePreCommitCb, **kw)

        return copy_pre_commit_cb

    # copyUsers(userIds, source_shard, destination_shard, copyPreCommitCb,True)
    pre_commit_cb = gen_copy_pre_commit_cb(source_shard, destination_shard)
    copy_users(user_ids, source_shard, destination_shard, pre_commit_cb=pre_commit_cb, commit_destination_shard=False, **kw)

    # Notify subscribers about update.
    shard_id = destination_shard[destination_shard.rindex("_") + 1 :]
    se = ShardEvent()
    [se.publish("movedUser", {"user_id": user_id, "shardId": shard_id}) for user_id in user_ids]


def migrate_user(user_id, source_shard, destination_shard, **kw):
    """migrate user"""
    return migrate_users([user_id], source_shard, destination_shard, **kw)


# dict((table, tuple(fkTable, fkColumn, sourceTable), ..)))
_additionalRelations = {
    "main_receipt": [
        ("main_receipt", "shortlink_id", "main_shortlink"),
    ],
    "main_usermessage": [
        ("main_usermessage", "shortlink_id", "main_shortlink"),
    ],
    "main_extendeduser": [
        ("main_extendeduser", "twilio_phone_number_id", "main_phonenumber"),
        ("main_extendeduser", "entitlement_id", "main_entitlement"),
    ],
    "main_groupshare": [
        ("main_groupshare", "invitation_ptr_id", "main_invitation"),
    ],
}


def copy_users(user_ids, source_shard, destination_shard, **kw):
    """
    Migrate all records for a particular user-id from one
    physical shard to another.

    @param userId int
    @param sourceShard str Source connection name.
    @param source_shard str Destination connection name.
    @param **kw Dict of optional arguments, including:
        ``preCommitCb`` mixed Function or None Defaults to None.
        Function to invoke before the copy is committed.
        ``commitDestinationShard`` bool Defaults to True.
        Whether or not to commit the changes to the destination
            shard -- you may instead handle the commit operation
            yourself in the pre-commit callback.
        ``deactivateTriggers`` bool Defaults to True.  Flag to determine
        whether or not triggers will be disabled.
        ``manageTransactions`` bool Defaults to True.  Flat to determine
        whether or not the function will manage the
            transaction.
    """
    pre_commit_cb = kw.get("preCommitCb", None)
    commit_destination_shard = kw.get("commitDestinationShard", True)
    deactivate_triggers = kw.get("deactivateTriggers", True)
    manage_transactions = kw.get("manageTransactions", True)

    def if_managing_transactions_then_exec(sql, using):
        """
        Will only execute the statement if
        ``manageTransactions`` is True.
        """
        if manage_transactions is True:
            db_exec(sql, using=using)

    in_user_ids = ",".join(map(str, user_ids))

    _verify_these_users_exist_in_shard(user_ids, source_shard)

    def remotely_fill_table(source_table, source_pk_column, inner_table, inner_column, inner_user_id_column):
        """
        Generic way to move rows containing ``userIds``
        from one shard to another.
        """
        if should_table_be_ignored_for_user_operations(source_table):
            logging.debug(_SKIPPING_COPY_TO_STATIC_TABLE_MSG, str(source_table))
            return

        db_link_sql = to_single_line(
            """
                SELECT * FROM "{source_table}" WHERE "{pk}" IN (
                    SELECT "{inner_column}" FROM "{inner_table}"
                    WHERE "{inner_user_id_column}" in ({user_ids})
                )
            """.format(source_table=source_table, pk=source_pk_column, inner_column=inner_column, inner_table=inner_table, inner_user_id_column=inner_user_id_column, user_ids=in_user_ids)
        )

        # Insert relevant records from the table.
        auto_db_link_insert(source_table, db_link_sql, source_shard, destination_shard)

    # Uniqify set of items while retaining original list order.
    user_id_table_column_pairs = _user_id_table_column_pairs()

    source_counts_initial = table_row_counts(user_id_table_column_pairs, user_ids, using=source_shard)

    dependencies = discover_dependencies([x[0] for x in user_id_table_column_pairs], using=source_shard)  # noqa

    if deactivate_triggers is True:
        # Disable all triggers.
        # db_exec('SELECT fn_modify_all_trigger_states(FALSE)',
        # using=destination_shard)
        db_exec('ALTER TABLE "main_contact" DISABLE TRIGGER "main_contact_trigger"', using=destination_shard)

    if_managing_transactions_then_exec("BEGIN", using=destination_shard)

    # NB: About set constraints all deferred:
    # http://www.postgresql.org/docs/devel/static/sql-set-constraints.html
    if_managing_transactions_then_exec(_SET_CONSTRAINTS_ALL_DEFERRED, using=destination_shard)

    populated_tables = []

    ordering = []

    user_id_table_column_pairs_copy = list(user_id_table_column_pairs)
    save_point = 0
    n = 0

    # for table, userIdColumn in userIdTableColumnPairs:
    while len(user_id_table_column_pairs_copy) > 0:
        n += 1
        if n > len(user_id_table_column_pairs) * 2:
            raise DependencyCycleError(_DEPENDENCY_CYCLE_DETECTED_MSG)

        table, user_id_column = user_id_table_column_pairs_copy.pop(0)
        logging.debug("TABLE=%s", str(table))

        if should_table_be_ignored_for_user_operations(table):
            logging.debug(_SKIPPING_COPY_TO_STATIC_TABLE_MSG, str(table))
            continue

        if table in populated_tables:
            logging.info("Skipping copy to already populated table: %s", str(table))
            continue

        try:
            save_point += 1
            db_exec("SAVEPOINT save{}".format(save_point), using=destination_shard)

            if table in _additionalRelations:
                for fk_table, fk_column, source_table in _additionalRelations[table]:
                    source_pk_column = get_primary_key_columns(source_table, using=destination_shard)[0]  # noqa
                    remotely_fill_table(source_table, source_pk_column, fk_table, fk_column, user_id_column)

            db_link_sql = """SELECT * FROM "{}"
                        WHERE "{}" IN ({})""".format(table, user_id_column, in_user_ids)

            # Insert relevant records from the table.
            auto_db_link_insert(table, db_link_sql, source_shard, destination_shard)
            populated_tables.append(table)
            db_exec("RELEASE SAVEPOINT save{}".format(save_point), using=destination_shard)
            n = 0

        except Exception as e:
            logging.info("Caught exception, will handle with it: %s", str(e))
            db_exec("ROLLBACK TO save{}".format(save_point), using=destination_shard)
            user_id_table_column_pairs_copy.append((table, user_id_column))

        ordering.append((table, user_id_column))

    user_id_table_column_pairs_copy = list(user_id_table_column_pairs)
    save_point = 0

    # Backfill dependent tables.
    # for table, userIdColumn in userIdTableColumnPairs:
    while len(user_id_table_column_pairs_copy) > 0:
        n += 1
        if n > len(user_id_table_column_pairs) * 2:
            raise DependencyCycleError(_DEPENDENCY_CYCLE_DETECTED_MSG)

        table, user_id_column = user_id_table_column_pairs_copy.pop(0)

        if should_table_be_ignored_for_user_operations(table):
            logging.debug("Dependencies backfiller is skipping static table: %s", str(table))
            continue

        try:
            save_point += 1
            db_exec("SAVEPOINT save{}".format(save_point), using=destination_shard)

            # If there are additional dependencies, insert them as well.
            if table in dependencies:
                unpopulated_tables = [fk_table for fk_table in dependencies[table] if fk_table not in populated_tables]

                for column, fk_table, fk_column in unpopulated_tables:
                    remotely_fill_table(fk_table, fk_column, table, column, user_id_column)
                    populated_tables.append(fk_table)
            db_exec("RELEASE SAVEPOINT save{}".format(save_point), using=destination_shard)
            n = 0

        except Exception as e:
            logging.info("Caught exception, will handle with it: %s", str(e))
            db_exec("ROLLBACK TO save{}".format(save_point), using=destination_shard)
            user_id_table_column_pairs_copy.append((table, user_id_column))

    destination_counts_verify = table_row_counts(user_id_table_column_pairs, user_ids, using=destination_shard)
    source_counts_verify = table_row_counts(user_id_table_column_pairs, user_ids, using=source_shard)

    if destination_counts_verify == source_counts_initial and destination_counts_verify == source_counts_verify:
        # Before proceeding, set constraints to all immediate.
        # Will be applied retroactively, raising issues
        # before more work is performed.
        # @see http://postgresql.org/docs/devel/static/sql-set-constraints.html
        if_managing_transactions_then_exec("SET CONSTRAINTS ALL IMMEDIATE", using=destination_shard)

        if pre_commit_cb is not None:
            pre_commit_cb()

        if commit_destination_shard is True:
            if_managing_transactions_then_exec("COMMIT", using=destination_shard)

        if deactivate_triggers is True:
            # Re-enable all triggers.
            # db_exec('SELECT fn_modify_all_trigger_states(TRUE)',
            #         using=destination_shard)
            db_exec('ALTER TABLE "main_contact" ENABLE TRIGGER "main_contact_trigger"', using=destination_shard)

    else:
        if_managing_transactions_then_exec("ROLLBACK", using=destination_shard)

        if deactivate_triggers is True:
            # Re-enable all triggers.
            # db_exec('SELECT fn_modify_all_trigger_states(TRUE)',
            # using=destination_shard)
            db_exec('ALTER TABLE "main_contact" ENABLE TRIGGER "main_contact_trigger"', using=destination_shard)

        raise MigrateUserStaleReadError(
            "Aborted migration of userIds={} from {} to {} due to changed source data\n\nsourceCountsInitial={}\n\ndestinationCountsVerify={}\n\nsourceCountsVerify={}".format(
                user_ids, source_shard, destination_shard, source_counts_initial, destination_counts_verify, source_counts_verify
            )
        )


def copy_user(user_id, source_shard, destination_shard, **kw):
    """Copy a single user."""
    return copy_users([user_id], source_shard, destination_shard, **kw)


def delete_users(user_ids, using, **kw):
    """
    Completely delete a user and all of their data from a shard.

    @param userIds
    @param using str Database connection handle to use.
    @param **kw Dict of optional arguments, including:
        ''preCommitCb mixed Function or None.  Pre-commit callback function,
        invoked immediately before COMMIT.
        ``manageTransactions`` bool Defaults to True.  Flat to determine
        whether or not the function will manage the
            transaction.
    """
    pre_commit_cb = kw.get("preCommitCb", None)
    manage_transactions = kw.get("manageTransactions", True)

    def if_managing_transactions_then_exec(sql, using):
        """
        Will only execute the statement if ``manageTransactions`` is True.
        """
        if manage_transactions is True:
            db_exec(sql, using=using)

    in_user_ids = ",".join(map(str, user_ids))

    user_id_table_column_pairs = find_tables_with_user_id_column(using=using)

    dependencies = discover_dependencies([x[0] for x in user_id_table_column_pairs], using=using)  # noqa

    cleared_tables = []

    orig_len = len(user_id_table_column_pairs)
    n = 0  # Count number of iterations since last success.

    if_managing_transactions_then_exec("BEGIN", using=using)

    # NB: About set constraints all deferred:
    # http://www.postgresql.org/docs/devel/static/sql-set-constraints.html
    if_managing_transactions_then_exec(_SET_CONSTRAINTS_ALL_DEFERRED, using=using)

    # Temporary hacks.
    sqls = [
        """
        DELETE FROM "main_voicemailtranscription" WHERE "voiceMail_id" IN (
            SELECT "id" FROM "main_voicemail" WHERE "user_id" IN ({0})
        )
        """,
        """
        DELETE FROM "main_groupshare" WHERE "invitation_ptr_id" IN (
                SELECT "id" FROM "main_invitation" WHERE "user_id" IN ({0})
            )
        """,
        """
        DELETE FROM "main_groupshare" WHERE "invitation_ptr_id" IN (
            SELECT "id" FROM "main_invitation" WHERE "owner_id" IN ({0})
        )
        """,
        """
        DELETE FROM "main_sendhubinvitation" WHERE "invitation_ptr_id" IN (
            SELECT "id" FROM "main_invitation" WHERE "user_id" IN ({0})
        )
        """,
        """
        DELETE FROM "main_sendhubinvitation" WHERE "invitation_ptr_id" IN (
            SELECT "id" FROM "main_invitation" WHERE "owner_id" IN ({0})
        )
        """,
        """
        DELETE FROM "main_enterpriseinvitation" WHERE "invitation_ptr_id" IN (
            SELECT "id" FROM "main_invitation" WHERE "user_id" IN ({0})
        )
        """,
        """
        DELETE FROM "main_enterpriseinvitation" WHERE "invitation_ptr_id" IN (
            SELECT "id" FROM "main_invitation" WHERE "owner_id" IN ({0})
        )
        """,
        """
        DELETE FROM "main_invitation" WHERE "owner_id" IN ({0})
        """,
        """
        DELETE FROM "main_usermessage_contacts" WHERE "contact_id" IN (
            SELECT "id" FROM "main_contact" WHERE "user_id" IN ({0})
        )
        """,
        """
        DELETE FROM "main_contact_groups" WHERE "contact_id" IN (
            SELECT "id" FROM "main_contact" WHERE "user_id" IN ({0})
        )
        """,
        """
        DELETE FROM "main_contactparent" WHERE "contact_id" IN (
            SELECT "id" FROM "main_contact" WHERE "user_id" IN ({0})
        )
        """,
        """
        DELETE FROM "main_usermessage_groups" WHERE "group_id" IN (
            SELECT "id" FROM "main_group" WHERE "user_id" IN ({0})
        )
        """,
        """
        DELETE FROM "main_groupshortcode" WHERE "group_id" IN (
            SELECT "id" FROM "main_group" WHERE "user_id" IN ({0})
        )
        """,
        """
        DELETE FROM "main_callobservation" WHERE "voiceCall" IN (
            SELECT "id" FROM "main_voicecall" WHERE "user_id" IN ({0})
        )
        """,
        """
        DELETE FROM "main_voicecallrating" WHERE "voiceCall" IN (
            SELECT "id" FROM "main_voicecall" WHERE "user_id" IN ({0})
        )
        """,
        """
        DELETE FROM "main_phonenumber" WHERE "id" IN (
            SELECT "twilio_phone_number_id" FROM "main_extendeduser"
            WHERE "user_id" IN ({0})
        )
        """,
        """
        DELETE FROM "main_entitlement" WHERE "id" IN (
            SELECT "entitlement_id" FROM "main_extendeduser"
            WHERE "user_id" IN ({0})
        )
        """,
        """
        DELETE FROM "main_receipt" WHERE "group_id" IN (
            SELECT "id" FROM "main_group" WHERE "user_id" IN ({0})
        )
        """,
    ]
    [db_exec(to_single_line(sql.format(in_user_ids)), using=using) for sql in sqls]
    del sqls

    save_point = 0

    while len(user_id_table_column_pairs) > 0:
        logging.info("Number of table,column pairs remaining: %s", str(len(user_id_table_column_pairs)))
        n += 1
        if n > orig_len * 2:
            logging.info("userIdTableColumnPairs=%s", str(user_id_table_column_pairs))
            raise DependencyCycleError(_DEPENDENCY_CYCLE_DETECTED_MSG)

        table, user_id_column = user_id_table_column_pairs.pop(0)

        if should_table_be_ignored_for_user_operations(table):
            logging.debug(_SKIPPING_DELETION_FROM_STATIC_TABLE_MSG, str(using), str(table))
            continue

        logging.info("[%s] Deleting from table: %s", str(using), str(table))

        try:
            save_point += 1
            db_exec(f"SAVEPOINT save{save_point}", using=using)

            if table in _additionalRelations:
                for fk_table, fk_column, source_table in _additionalRelations[table]:
                    if should_table_be_ignored_for_user_operations(fk_table):
                        logging.debug(_SKIPPING_DELETION_FROM_STATIC_TABLE_MSG, str(using), str(source_table))
                        continue

                    logging.info("[%s] Deleting from subtable: %s", str(using), str(source_table))

                    delete_sql = to_single_line(
                        """
                            DELETE FROM "{source_table}" WHERE "{pk}" IN (
                                SELECT "{fk_column}" FROM "{fk_table}"
                                WHERE "{user_id_column}" IN ({user_ids})
                            )
                        """.format(source_table=source_table, pk=get_primary_key_columns(source_table)[0], fk_column=fk_column, fk_table=fk_table, user_id_column=user_id_column, user_ids=in_user_ids)
                    )
                    db_exec(delete_sql, using=using)

            if table in dependencies:
                # If there are additional dependents, delete them first.
                for column, fk_table, fk_column in dependencies[table]:
                    if should_table_be_ignored_for_user_operations(fk_table):
                        logging.debug(_SKIPPING_DELETION_FROM_STATIC_TABLE_MSG, str(using), str(fk_table))
                        continue

                    logging.info("[%s] Deleting from subtable: %s", str(using), str(fk_table))

                    delete_sql = to_single_line(
                        """
                            DELETE FROM "{fk_table}" WHERE "{fk_column}" IN (
                                SELECT "{column}" FROM "{table}"
                                WHERE "{user_id_column}" IN ({user_ids})
                            )
                        """.format(fk_table=fk_table, fk_column=fk_column, column=column, table=table, user_id_column=user_id_column, user_ids=in_user_ids)
                    )
                    db_exec(delete_sql, using=using)

            delete_sql = to_single_line(
                """DELETE FROM "{}" WHERE
                "{}" IN ({})""".format(table, user_id_column, in_user_ids)
            )
            db_exec(delete_sql, using=using)

            cleared_tables.append(table)

            db_exec(f"RELEASE SAVEPOINT save{save_point}", using=using)
            # Reset cycle detector counter.
            n = 0

        except Exception as e:
            logging.info("[%s] Dealing with IntegrityError -----\n%s----- for table=%s/userIdColumn=%s", str(using), str(e), str(table), str(user_id_column))
            db_exec(f"ROLLBACK TO save{save_point}", using=using)
            user_id_table_column_pairs.append((table, user_id_column))
            if "waits for ShareLock on transaction" in str(e):
                raise e

    try:
        # Set constraints to all immediate, which will be applied retroactively
        # (raising any problems BEFORE commits have happened).
        # @see http://postgresql.org/docs/devel/static/sql-set-constraints.html
        if_managing_transactions_then_exec("SET CONSTRAINTS ALL IMMEDIATE", using=using)

        if pre_commit_cb is not None:
            logging.info("deleteUser invoking pre-commit callback")
            pre_commit_cb()

        logging.info("Committing deletion on %s", str(using))
        if_managing_transactions_then_exec("COMMIT", using=using)

        return True

    except Exception as exp_err:
        if_managing_transactions_then_exec("ROLLBACK", using=using)
        raise MigrateUserError(str(exp_err)) from exp_err


def delete_user(user_id, using, **kw):
    """Delete a single user."""
    return delete_users([user_id], using, **kw)
