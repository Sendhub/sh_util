"""Unit tests for ``sh_util.db.data``.

Covers Postgres-specific data tooling for shard replication and user
copy/dump/delete/migration, plus the automatic error-resolution machinery
used during logical-shard migrations.

Driver boundary: ``data.py`` does ``from . import connections, db_exec,
db_query, get_psql_connection_string`` and ``from .reflect import describe,
discover_dependencies, find_tables_with_user_id_column,
get_primary_key_columns, update_primary_key_id``, plus
``table_description_to_db_link_t`` (``.distributed``), ``ShardedResource``/
``ShardEvent``/``coerce_id_to_shard_name`` (``..sharding``),
``attempt_memcache_flush`` (``..memcache``), and ``upload_file`` (``..s3``).
Every test patches these as attributes of ``data_module`` (they are bound
names in its namespace, not sub-modules). Two collaborators are imported
locally inside function bodies rather than at module scope --
``sh_util.db.select2insert.select2multi_insert`` (inside ``dump_users``,
patched by dotted path) and ``sh_util.mail.send_email`` (inside
``replicate_table``, patched by seeding ``sys.modules`` directly, since the
real ``sh_util.mail`` package imports Django at module scope and Django
isn't installed here -- ``mock.patch()`` would need to import it for real
just to reach the attribute). No test hits a real database, memcache, or S3.

Known pre-existing quirks in the vendored source pinned by tests below
(not fixed, per task constraints):
  * ``AutomaticErrorResolver.matches()`` collapses real newlines in the
    exception text to spaces before matching, but most subclasses embed
    literal (unescaped) newlines in their multi-line ``regex_str`` -- so
    ``matches()`` can never succeed for those subclasses. Only
    ``DuplicateMixPanelIdResolver`` uses a single-line regex and can match.
  * ``_find_and_validate_user_id_for_thread_members`` no longer crashes
    unconditionally (fixed SonarQube S3862: ``user_ids_c, user_ids_g =
    None`` is now ``= None, None``), but still isn't fully exercised here --
    callers (``ThreadOverlapResolver``/``ThreadMismatchResolver``) mock it
    directly to exercise their own surrounding branch logic.
  * ``table_row_counts`` filters "ignored" tables by testing the whole
    ``(table, column)`` tuple against ``settings.STATIC_TABLES`` /
    ``SHARDING_IGNORE_TABLES``, not the table name alone, so in practice the
    filter only fires on an exact tuple match.
"""

import re
import sys
from unittest import mock

import pytest
from sh_util.db import data as data_module


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_memoized_cache():
    """``_user_id_table_column_pairs`` is memoized at decoration time; clear
    its cache before and after every test so a mocked
    ``find_tables_with_user_id_column`` return value from one test can't
    leak into another via the cache."""
    data_module._user_id_table_column_pairs._cached.clear()
    yield
    data_module._user_id_table_column_pairs._cached.clear()


@pytest.fixture(autouse=True)
def _settings_defaults(monkeypatch):
    """``SHARDING_IGNORE_TABLES`` and ``PRIMARY_SHARD_CONNECTION`` are not
    defined anywhere in this repo's settings (only ``STATIC_TABLES`` and
    ``NUM_LOGICAL_SHARDS`` are, via ``src/config.py``), so every test needs
    them provided. ``raising=False`` lets monkeypatch create attributes that
    don't already exist and still clean them up afterwards."""
    monkeypatch.setattr(data_module.settings, "SHARDING_IGNORE_TABLES", (), raising=False)
    monkeypatch.setattr(data_module.settings, "PRIMARY_SHARD_CONNECTION", "primary_shard", raising=False)
    monkeypatch.setattr(data_module.settings, "STATIC_TABLES", (), raising=False)
    monkeypatch.setattr(data_module.settings, "NUM_LOGICAL_SHARDS", 4096, raising=False)


# ---------------------------------------------------------------------------
# to_single_line / should_table_be_ignored_for_user_operations
# ---------------------------------------------------------------------------


class TestToSingleLine:
    def test_collapses_whitespace_and_strips(self):
        assert data_module.to_single_line("  SELECT  *\n FROM  t \n") == "SELECT * FROM t"

    def test_no_whitespace_is_unchanged(self):
        assert data_module.to_single_line("SELECT") == "SELECT"


class TestShouldTableBeIgnoredForUserOperations:
    def test_static_table_is_ignored(self, monkeypatch):
        monkeypatch.setattr(data_module.settings, "STATIC_TABLES", ("main_group",), raising=False)
        assert data_module.should_table_be_ignored_for_user_operations("main_group") is True

    def test_sharding_ignore_table_is_ignored(self, monkeypatch):
        monkeypatch.setattr(data_module.settings, "SHARDING_IGNORE_TABLES", ("main_ignored",), raising=False)
        assert data_module.should_table_be_ignored_for_user_operations("main_ignored") is True

    def test_other_table_is_not_ignored(self):
        assert data_module.should_table_be_ignored_for_user_operations("auth_user") is False


# ---------------------------------------------------------------------------
# does_the_table_data_differ
# ---------------------------------------------------------------------------


class TestDoesTheTableDataDiffer:
    def test_returns_true_when_counts_differ(self):
        with mock.patch.object(data_module, "db_query", side_effect=[[(5,)], [(3,)]]) as db_query:
            result = data_module.does_the_table_data_differ("t", "source1", "source2")
        assert result is True
        assert db_query.call_count == 2
        first_call = db_query.call_args_list[0]
        assert first_call.args[0] == 'SELECT COUNT(*) FROM "t"'
        assert first_call.kwargs == {"using": "source1"}
        second_call = db_query.call_args_list[1]
        assert second_call.kwargs == {"using": "source2"}

    def test_returns_false_when_counts_and_data_match(self):
        with (
            mock.patch.object(data_module, "db_query", side_effect=[[(2,)], [(2,)], [("a",)], [("a",)]]) as db_query,
            mock.patch.object(data_module, "get_primary_key_columns", return_value=["id"]),
        ):
            result = data_module.does_the_table_data_differ("t", "source1", "source2")
        assert result is False
        data_sql_call = db_query.call_args_list[2]
        assert data_sql_call.args[0] == 'SELECT * FROM "t" ORDER BY "id" DESC'

    def test_returns_true_when_data_differs(self):
        with (
            mock.patch.object(data_module, "db_query", side_effect=[[(2,)], [(2,)], [("a",)], [("b",)]]),
            mock.patch.object(data_module, "get_primary_key_columns", return_value=["id"]),
        ):
            assert data_module.does_the_table_data_differ("t", "source1", "source2") is True

    def test_raises_assertion_error_when_row_count_too_large(self):
        with mock.patch.object(data_module, "db_query", side_effect=[[(100001,)], [(0,)]]):
            with pytest.raises(AssertionError):
                data_module.does_the_table_data_differ("t", "source1", "source2")

    def test_multi_column_primary_key_order_by(self):
        with (
            mock.patch.object(data_module, "db_query", side_effect=[[(1,)], [(1,)], [("x",)], [("x",)]]) as db_query,
            mock.patch.object(data_module, "get_primary_key_columns", return_value=["a", "b"]),
        ):
            data_module.does_the_table_data_differ("t", "s1", "s2")
        assert db_query.call_args_list[2].args[0] == 'SELECT * FROM "t" ORDER BY "a", "b" DESC'


# ---------------------------------------------------------------------------
# replicate_table
# ---------------------------------------------------------------------------


class TestReplicateTable:
    def test_raises_if_table_is_not_static(self, monkeypatch):
        monkeypatch.setattr(data_module.settings, "STATIC_TABLES", (), raising=False)
        with pytest.raises(AssertionError):
            data_module.replicate_table("main_group", "s1", "s2")

    def test_raises_if_connection_names_invalid(self, monkeypatch):
        monkeypatch.setattr(data_module.settings, "STATIC_TABLES", ("t",), raising=False)
        with mock.patch.object(data_module, "connections", return_value=["s1"]):
            with pytest.raises(AssertionError):
                data_module.replicate_table("t", "s1", "s2")

    def test_no_op_when_data_does_not_differ(self, monkeypatch):
        monkeypatch.setattr(data_module.settings, "STATIC_TABLES", ("t",), raising=False)
        with (
            mock.patch.object(data_module, "connections", return_value=["s1", "s2"]),
            mock.patch.object(data_module, "does_the_table_data_differ", return_value=False) as differ,
            mock.patch.object(data_module, "db_exec") as db_exec,
        ):
            data_module.replicate_table("t", "s1", "s2")
        differ.assert_called_once_with("t", "s1", "s2")
        db_exec.assert_not_called()

    def test_replicates_when_data_differs(self, monkeypatch):
        monkeypatch.setattr(data_module.settings, "STATIC_TABLES", ("t",), raising=False)
        with (
            mock.patch.object(data_module, "connections", return_value=["s1", "s2"]),
            mock.patch.object(data_module, "does_the_table_data_differ", return_value=True),
            mock.patch.object(data_module, "get_psql_connection_string", return_value="pg://s1"),
            mock.patch.object(data_module, "describe", return_value=[("id", "integer")]),
            mock.patch.object(data_module, "table_description_to_db_link_t", return_value='t("id" integer)'),
            mock.patch.object(data_module, "db_exec") as db_exec,
        ):
            data_module.replicate_table("t", "s1", "s2")

        calls = [c.args[0] for c in db_exec.call_args_list]
        assert calls[0] == "BEGIN"
        assert calls[1] == "SET CONSTRAINTS ALL DEFERRED"
        assert calls[2] == 'DELETE FROM "t"'
        assert 'INSERT INTO "t"' in calls[3]
        assert "dblink('pg://s1'" in calls[3]
        assert 't("id" integer)' in calls[3]
        assert calls[4] == "COMMIT"
        for c in db_exec.call_args_list:
            assert c.kwargs == {"using": "s2"}

    def test_exception_rolls_back_and_sends_email(self, monkeypatch):
        monkeypatch.setattr(data_module.settings, "STATIC_TABLES", ("t",), raising=False)
        # `replicate_table`'s except-block does `from ..mail import send_email`
        # locally. The real `sh_util.mail` package imports Django at module
        # scope, which isn't installed in this environment, so it can't be
        # imported (even just to patch an attribute on it) -- pre-seed
        # sys.modules with a stand-in instead, which the local import will
        # find already cached and use as-is.
        fake_mail_module = mock.MagicMock(name="sh_util.mail")
        with (
            mock.patch.dict(sys.modules, {"sh_util.mail": fake_mail_module}),
            mock.patch.object(data_module, "connections", return_value=["s1", "s2"]),
            mock.patch.object(data_module, "does_the_table_data_differ", return_value=True),
            mock.patch.object(data_module, "get_psql_connection_string", return_value="pg://s1"),
            mock.patch.object(data_module, "describe", return_value=[("id", "integer")]),
            mock.patch.object(data_module, "table_description_to_db_link_t", return_value='t("id" integer)'),
            mock.patch.object(data_module, "db_exec", side_effect=[None, None, None, Exception("boom"), None]) as db_exec,
        ):
            data_module.replicate_table("t", "s1", "s2")

        assert db_exec.call_args_list[-1].args[0] == "ROLLBACK"
        send_email = fake_mail_module.send_email
        send_email.assert_called_once()
        _, kwargs = send_email.call_args
        assert "URGENT" in kwargs["subject"]
        assert "boom" in kwargs["body"]
        assert kwargs["from_address"] == "devops@sendhub.com"
        assert kwargs["to_address"] == "devops@sendhub.com"


# ---------------------------------------------------------------------------
# auto_db_link_insert
# ---------------------------------------------------------------------------


class TestAutoDbLinkInsert:
    def test_happy_path_uses_raw_connection_string(self):
        with (
            mock.patch.object(data_module, "connections", return_value=[]),
            mock.patch.object(data_module, "describe", return_value=[("id", "integer")]),
            mock.patch.object(data_module, "table_description_to_db_link_t", return_value="t(\"id\" integer)"),
            mock.patch.object(data_module, "db_exec", side_effect=[None, None, None]) as db_exec,
        ):
            data_module.auto_db_link_insert("t", "SELECT * FROM t", "pg://raw", using="dest")

        assert db_exec.call_count == 3
        assert db_exec.call_args_list[0] == mock.call("SAVEPOINT auto_db_link_insert", using="dest")
        insert_sql = db_exec.call_args_list[1].args[0]
        assert 'INSERT INTO "t"' in insert_sql
        assert "dblink( 'pg://raw'" in insert_sql
        assert db_exec.call_args_list[2] == mock.call("RELEASE SAVEPOINT auto_db_link_insert", using="dest")

    def test_resolves_connection_name_via_get_psql_connection_string(self):
        with (
            mock.patch.object(data_module, "connections", return_value=["source"]),
            mock.patch.object(data_module, "get_psql_connection_string", return_value="pg://resolved") as get_conn,
            mock.patch.object(data_module, "describe", return_value=[("id", "integer")]),
            mock.patch.object(data_module, "table_description_to_db_link_t", return_value='t("id" integer)'),
            mock.patch.object(data_module, "db_exec"),
        ):
            data_module.auto_db_link_insert("t", "SELECT * FROM t", "source", using="dest")

        get_conn.assert_called_once_with("source")

    def test_default_using_is_default_connection(self):
        with (
            mock.patch.object(data_module, "connections", return_value=[]),
            mock.patch.object(data_module, "describe", return_value=[("id", "integer")]),
            mock.patch.object(data_module, "table_description_to_db_link_t", return_value='t("id" integer)'),
            mock.patch.object(data_module, "db_exec") as db_exec,
        ):
            data_module.auto_db_link_insert("t", "SELECT * FROM t", "pg://raw")

        for c in db_exec.call_args_list:
            assert c.kwargs.get("using") == "default"

    def test_duplicate_key_retries_with_explicit_pk(self):
        dup_exc = Exception('duplicate key value violates unique constraint "t_pkey"')
        with (
            mock.patch.object(data_module, "connections", return_value=[]),
            mock.patch.object(data_module, "describe", return_value=[("id", "integer")]),
            mock.patch.object(data_module, "table_description_to_db_link_t", return_value='t("id" integer)'),
            mock.patch.object(data_module, "get_primary_key_columns") as get_pk,
            mock.patch.object(data_module, "db_exec", side_effect=[None, dup_exc, None, None, None, None]) as db_exec,
        ):
            data_module.auto_db_link_insert("t", "SELECT * FROM t", "pg://raw", using="dest", pk="explicit_pk")

        get_pk.assert_not_called()
        assert db_exec.call_count == 6
        assert db_exec.call_args_list[2] == mock.call("ROLLBACK TO auto_db_link_insert", using="dest")
        assert db_exec.call_args_list[3] == mock.call("SAVEPOINT auto_db_link_insert", using="dest")
        retry_sql = db_exec.call_args_list[4].args[0]
        assert '"explicit_pk" NOT IN' in retry_sql
        assert db_exec.call_args_list[5] == mock.call("RELEASE SAVEPOINT auto_db_link_insert", using="dest")

    def test_duplicate_key_auto_detects_pk_when_not_supplied(self):
        dup_exc = Exception('duplicate key value violates unique constraint "t_pkey"')
        with (
            mock.patch.object(data_module, "connections", return_value=[]),
            mock.patch.object(data_module, "describe", return_value=[("id", "integer")]),
            mock.patch.object(data_module, "table_description_to_db_link_t", return_value='t("id" integer)'),
            mock.patch.object(data_module, "get_primary_key_columns", return_value=["auto_pk"]) as get_pk,
            mock.patch.object(data_module, "db_exec", side_effect=[None, dup_exc, None, None, None, None]) as db_exec,
        ):
            data_module.auto_db_link_insert("t", "SELECT * FROM t", "pg://raw", using="dest")

        get_pk.assert_called_once_with("t", using="dest")
        retry_sql = db_exec.call_args_list[4].args[0]
        assert '"auto_pk" NOT IN' in retry_sql

    def test_non_duplicate_exception_releases_savepoint_and_reraises(self):
        other_exc = Exception("connection reset")
        with (
            mock.patch.object(data_module, "connections", return_value=[]),
            mock.patch.object(data_module, "describe", return_value=[("id", "integer")]),
            mock.patch.object(data_module, "table_description_to_db_link_t", return_value='t("id" integer)'),
            mock.patch.object(data_module, "db_exec", side_effect=[None, other_exc, None]) as db_exec,
        ):
            with pytest.raises(Exception, match="connection reset"):
                data_module.auto_db_link_insert("t", "SELECT * FROM t", "pg://raw", using="dest")

        assert db_exec.call_count == 3
        assert db_exec.call_args_list[2] == mock.call("RELEASE SAVEPOINT auto_db_link_insert", using="dest")


# ---------------------------------------------------------------------------
# table_row_counts
# ---------------------------------------------------------------------------


class TestTableRowCounts:
    def test_builds_union_query_for_single_user_id(self):
        with mock.patch.object(data_module, "db_query", return_value=[("t1", 3)]) as db_query:
            result = data_module.table_row_counts([("t1", "user_id")], 5, using="shard_1")
        assert result == {"t1": 3}
        sql, kwargs = db_query.call_args
        assert kwargs == {"using": "shard_1"}
        assert '"t1"' in sql[0]
        assert 'WHERE "user_id" = (5)' in sql[0]

    def test_builds_in_clause_for_multiple_user_ids(self):
        with mock.patch.object(data_module, "db_query", return_value=[("t1", 2), ("t2", 1)]) as db_query:
            result = data_module.table_row_counts([("t1", "user_id"), ("t2", "user_id")], [1, 2], using="shard_1")
        assert result == {"t1": 2, "t2": 1}
        sql = db_query.call_args[0][0]
        assert 'WHERE "user_id" IN (1,2)' in sql
        assert " UNION " in sql

    def test_ignored_tables_are_excluded_by_exact_tuple_match(self, monkeypatch):
        # NB: should_table_be_ignored_for_user_operations() is invoked with the
        # whole (table, column) tuple here (not the table name), a pre-existing
        # quirk -- so only an exact tuple match in STATIC_TABLES filters it out.
        monkeypatch.setattr(data_module.settings, "STATIC_TABLES", (("static_t", "user_id"),), raising=False)
        with mock.patch.object(data_module, "db_query", return_value=[]) as db_query:
            data_module.table_row_counts([("static_t", "user_id"), ("t1", "user_id")], 5, using="shard_1")
        sql = db_query.call_args[0][0]
        assert "static_t" not in sql
        assert '"t1"' in sql


# ---------------------------------------------------------------------------
# scrub_tables
# ---------------------------------------------------------------------------


class TestScrubTables:
    def test_executes_hardcoded_cleanup_statement(self):
        with mock.patch.object(data_module, "db_exec") as db_exec:
            data_module.scrub_tables("shard_1")
        db_exec.assert_called_once()
        sql, kwargs = db_exec.call_args
        assert "main_phonenumber" in sql[0]
        assert kwargs == {"using": "shard_1"}


# ---------------------------------------------------------------------------
# set_logical_shard_status / set_logical_shard_physical_shard_id
# ---------------------------------------------------------------------------


class TestSetLogicalShardStatus:
    def test_wraps_update_in_a_transaction(self, monkeypatch):
        monkeypatch.setattr(data_module.settings, "PRIMARY_SHARD_CONNECTION", "primary_shard", raising=False)
        with mock.patch.object(data_module, "db_exec") as db_exec:
            data_module.set_logical_shard_status(7, "OK")

        assert db_exec.call_args_list[0] == mock.call("BEGIN", using="primary_shard")
        assert db_exec.call_args_list[1] == mock.call('UPDATE "LogicalShard" SET "status" = %s WHERE "id" = %s', ("OK", 7), using="primary_shard")
        assert db_exec.call_args_list[2] == mock.call("COMMIT", using="primary_shard")


class TestSetLogicalShardPhysicalShardId:
    def test_without_status_updates_physical_shard_id_only(self, monkeypatch):
        monkeypatch.setattr(data_module.settings, "PRIMARY_SHARD_CONNECTION", "primary_shard", raising=False)
        with mock.patch.object(data_module, "db_exec") as db_exec:
            data_module.set_logical_shard_physical_shard_id(7, 3)

        assert db_exec.call_args_list[0] == mock.call("BEGIN", using="primary_shard")
        assert db_exec.call_args_list[1] == mock.call('UPDATE "LogicalShard" SET "physical_shard_id" = %s WHERE "id" = %s', (3, 7), using="primary_shard")
        assert db_exec.call_args_list[2] == mock.call("COMMIT", using="primary_shard")

    def test_with_status_updates_both_fields(self, monkeypatch):
        monkeypatch.setattr(data_module.settings, "PRIMARY_SHARD_CONNECTION", "primary_shard", raising=False)
        with mock.patch.object(data_module, "db_exec") as db_exec:
            data_module.set_logical_shard_physical_shard_id(7, 3, status="OK")

        assert db_exec.call_args_list[1] == mock.call('UPDATE "LogicalShard" SET "physical_shard_id" = %s, "status" = %s WHERE "id" = %s', (3, "OK", 7), using="primary_shard")


# ---------------------------------------------------------------------------
# _physical_shard_id / _logical_shard_user_ids / _cleanup_straggler_short_links
# ---------------------------------------------------------------------------


class TestPhysicalShardId:
    def test_returns_value_when_found(self, monkeypatch):
        monkeypatch.setattr(data_module.settings, "PRIMARY_SHARD_CONNECTION", "primary_shard", raising=False)
        with mock.patch.object(data_module, "db_query", return_value=[(3,)]) as db_query:
            result = data_module._physical_shard_id(7)
        assert result == 3
        db_query.assert_called_once_with('SELECT "physical_shard_id" FROM "LogicalShard" WHERE "id" = %s', (7,), using="primary_shard")

    def test_returns_none_when_not_found(self):
        with mock.patch.object(data_module, "db_query", return_value=[]):
            assert data_module._physical_shard_id(7) is None


class TestLogicalShardUserIds:
    def test_uses_supplied_physical_shard_id(self, monkeypatch):
        monkeypatch.setattr(data_module.settings, "NUM_LOGICAL_SHARDS", 4096, raising=False)
        with (
            mock.patch.object(data_module, "db_query", return_value=[(1,), (2,)]) as db_query,
            mock.patch.object(data_module, "_physical_shard_id") as physical_shard_id,
        ):
            result = data_module._logical_shard_user_ids(7, physical_shard_id=3)
        assert result == [1, 2]
        physical_shard_id.assert_not_called()
        assert db_query.call_args.kwargs == {"using": "shard_3"}

    def test_looks_up_physical_shard_id_when_not_supplied(self):
        with (
            mock.patch.object(data_module, "db_query", return_value=[]),
            mock.patch.object(data_module, "_physical_shard_id", return_value=9) as physical_shard_id,
        ):
            data_module._logical_shard_user_ids(7)
        physical_shard_id.assert_called_once_with(7)


class TestCleanupStragglerShortLinks:
    def test_deletes_orphaned_shortlinks(self):
        with mock.patch.object(data_module, "db_exec", return_value="deleted") as db_exec:
            result = data_module._cleanup_straggler_short_links("shard_1")
        assert result == "deleted"
        sql, kwargs = db_exec.call_args
        assert "main_shortlink" in sql[0]
        assert kwargs == {"using": "shard_1"}


# ---------------------------------------------------------------------------
# _automatic_duplicate_recovery
# ---------------------------------------------------------------------------


class TestAutomaticDuplicateRecovery:
    def test_no_op_when_no_duplicates_found(self):
        with (
            mock.patch.object(data_module, "db_exec") as db_exec,
            mock.patch.object(data_module, "get_psql_connection_string", return_value="pg://dest"),
            mock.patch.object(data_module, "db_query", return_value=[]) as db_query,
            mock.patch.object(data_module, "delete_users") as delete_users,
            mock.patch.object(data_module, "attempt_memcache_flush") as flush,
        ):
            data_module._automatic_duplicate_recovery(7, "shard_1", "shard_2")

        assert db_exec.call_args_list[0] == mock.call("ROLLBACK", using="shard_1")
        assert db_exec.call_args_list[1] == mock.call("ROLLBACK", using="shard_2")
        db_query.assert_called_once()
        delete_users.assert_not_called()
        flush.assert_not_called()

    def test_removes_duplicates_when_found(self, monkeypatch):
        monkeypatch.setattr(data_module.settings, "PRIMARY_SHARD_CONNECTION", "primary_shard", raising=False)
        with (
            mock.patch.object(data_module, "db_exec") as db_exec,
            mock.patch.object(data_module, "get_psql_connection_string", return_value="pg://dest"),
            mock.patch.object(data_module, "db_query", return_value=[(42,)]),
            mock.patch.object(data_module, "delete_users") as delete_users,
            mock.patch.object(data_module, "_cleanup_straggler_short_links") as cleanup,
            mock.patch.object(data_module, "attempt_memcache_flush") as flush,
        ):
            data_module._automatic_duplicate_recovery(7, "shard_1", "shard_2")

        delete_users.assert_called_once_with([42], using="shard_2")
        cleanup.assert_called_once_with("shard_2")
        flush.assert_called_once()
        update_call = db_exec.call_args_list[-1]
        assert update_call.args[0] == 'UPDATE "LogicalShard" SET "physical_shard_id" = %s WHERE "id" = %s'
        assert update_call.args[1] == ("1", 7)
        assert update_call.kwargs == {"using": "primary_shard"}

    def test_raises_when_source_connection_name_has_no_digits(self):
        with (
            mock.patch.object(data_module, "db_exec"),
            mock.patch.object(data_module, "get_psql_connection_string", return_value="pg://dest"),
            mock.patch.object(data_module, "db_query", return_value=[(42,)]),
            mock.patch.object(data_module, "delete_users"),
            mock.patch.object(data_module, "_cleanup_straggler_short_links"),
            mock.patch.object(data_module, "attempt_memcache_flush"),
        ):
            with pytest.raises(AssertionError):
                data_module._automatic_duplicate_recovery(7, "no_digits_here", "shard_2")


# ---------------------------------------------------------------------------
# migrate_logical_shard
# ---------------------------------------------------------------------------


class TestMigrateLogicalShard:
    def test_raises_when_physical_shard_id_not_found(self):
        with mock.patch.object(data_module, "_physical_shard_id", return_value=None):
            with pytest.raises(AssertionError):
                data_module.migrate_logical_shard(7, "shard_2")

    def test_raises_when_source_equals_destination(self):
        with (
            mock.patch.object(data_module, "_physical_shard_id", return_value=2),
            mock.patch.object(data_module, "coerce_id_to_shard_name", return_value="shard_2"),
        ):
            with pytest.raises(AssertionError):
                data_module.migrate_logical_shard(7, "shard_2")

    def test_succeeds_when_counts_match(self):
        with (
            mock.patch.object(data_module, "_physical_shard_id", return_value=1),
            mock.patch.object(data_module, "coerce_id_to_shard_name", return_value="shard_1"),
            mock.patch.object(data_module, "_logical_shard_user_ids", return_value=[1, 2]),
            mock.patch.object(data_module, "_user_id_table_column_pairs", return_value=[("auth_user", "id")]),
            mock.patch.object(data_module, "set_logical_shard_status") as set_status,
            mock.patch.object(data_module, "table_row_counts", return_value={"auth_user": 2}) as row_counts,
            mock.patch.object(data_module, "_dump_and_copy_logical_shard_wrapper", return_value=1000) as dump_and_copy,
            mock.patch.object(data_module, "ShardedResource") as sharded_resource,
            mock.patch.object(data_module, "set_logical_shard_physical_shard_id") as set_physical,
            mock.patch.object(data_module, "attempt_memcache_flush") as flush,
            mock.patch.object(data_module, "delete_users") as delete_users,
            mock.patch.object(data_module, "upload_file", return_value="https://s3/x") as upload_file,
            mock.patch.object(data_module, "_automatic_duplicate_recovery") as recovery,
        ):
            sharded_resource.shard_name_to_id.return_value = 2
            data_module.migrate_logical_shard(7, "shard_2")

        set_status.assert_called_once_with(7, "RELOCATING")
        dump_and_copy.assert_called_once()
        assert row_counts.call_count == 3
        set_physical.assert_called_once_with(7, 2, "OK")
        flush.assert_called_once()
        delete_users.assert_called_once()
        assert delete_users.call_args.args[1] == "shard_1"
        upload_file.assert_called_once()
        recovery.assert_called_once_with(7, "shard_1", "shard_2")

    def test_deletes_from_destination_when_counts_mismatch(self):
        with (
            mock.patch.object(data_module, "_physical_shard_id", return_value=1),
            mock.patch.object(data_module, "coerce_id_to_shard_name", return_value="shard_1"),
            mock.patch.object(data_module, "_logical_shard_user_ids", return_value=[1, 2]),
            mock.patch.object(data_module, "_user_id_table_column_pairs", return_value=[("auth_user", "id")]),
            mock.patch.object(data_module, "set_logical_shard_status"),
            mock.patch.object(data_module, "table_row_counts", side_effect=[{"auth_user": 2}, {"auth_user": 1}, {"auth_user": 2}]),
            mock.patch.object(data_module, "_dump_and_copy_logical_shard_wrapper", return_value=1000),
            mock.patch.object(data_module, "set_logical_shard_physical_shard_id") as set_physical,
            mock.patch.object(data_module, "attempt_memcache_flush") as flush,
            mock.patch.object(data_module, "delete_users") as delete_users,
            mock.patch.object(data_module, "upload_file", return_value="https://s3/x"),
            mock.patch.object(data_module, "_automatic_duplicate_recovery") as recovery,
        ):
            data_module.migrate_logical_shard(7, "shard_2")

        set_physical.assert_not_called()
        flush.assert_not_called()
        delete_users.assert_called_once()
        assert delete_users.call_args.args[1] == "shard_2"
        recovery.assert_called_once_with(7, "shard_1", "shard_2")

    def test_assertion_error_inside_try_is_caught_and_logged(self):
        with (
            mock.patch.object(data_module, "_physical_shard_id", return_value=1),
            mock.patch.object(data_module, "coerce_id_to_shard_name", return_value="shard_1"),
            mock.patch.object(data_module, "_logical_shard_user_ids", return_value=[1, 2]),
            mock.patch.object(data_module, "_user_id_table_column_pairs", return_value=[("auth_user", "id")]),
            mock.patch.object(data_module, "set_logical_shard_status"),
            mock.patch.object(data_module, "table_row_counts", side_effect=AssertionError("boom")),
            mock.patch.object(data_module, "_automatic_duplicate_recovery") as recovery,
        ):
            # Should not raise -- the AssertionError is caught inside migrate_logical_shard.
            data_module.migrate_logical_shard(7, "shard_2")

        recovery.assert_called_once_with(7, "shard_1", "shard_2")


# ---------------------------------------------------------------------------
# AutomaticErrorResolver base class
# ---------------------------------------------------------------------------


class TestAutomaticErrorResolverBase:
    def test_matches_sets_match_and_returns_true(self):
        resolver = data_module.AutomaticErrorResolver("s1", r"^boom (?P<x>\d+)$")
        assert resolver.matches(Exception("boom 5")) is True
        assert resolver.match is not None

    def test_matches_returns_false_and_leaves_match_none_on_miss(self):
        resolver = data_module.AutomaticErrorResolver("s1", r"^nomatch$")
        assert resolver.matches(Exception("something else")) is False
        assert resolver.match is None

    def test_matches_collapses_real_newlines_before_matching(self):
        resolver = data_module.AutomaticErrorResolver("s1", r"^line1 line2$")
        assert resolver.matches(Exception("line1\nline2")) is True

    def test_validate_runnability_raises_without_a_prior_match(self):
        resolver = data_module.AutomaticErrorResolver("s1", r"^x$")
        with pytest.raises(AssertionError):
            resolver.validate_runnability()

    def test_validate_runnability_passes_after_a_match(self):
        resolver = data_module.AutomaticErrorResolver("s1", r"^x$")
        resolver.matches(Exception("x"))
        resolver.validate_runnability()

    def test_run_is_not_implemented(self):
        resolver = data_module.AutomaticErrorResolver("s1", r"^x$")
        with pytest.raises(NotImplementedError):
            resolver.run()


class TestMultiLineResolverRegexesNeverMatch:
    """Pins the pre-existing bug described in the module docstring above:
    every subclass except DuplicateMixPanelIdResolver defines its regex_str
    across multiple physical (unescaped) lines, so it embeds literal newline
    characters that can never appear in the space-normalized exception text
    matches() actually tests against."""

    @pytest.mark.parametrize(
        "cls",
        [
            data_module.DuplicateUsernameResolver,
            data_module.DuplicateIdResolver,
            data_module.ContactGroupsOverlapResolver,
            data_module.ReceiptOverlapResolver,
            data_module.ThreadOverlapResolver,
            data_module.BlockMismatchResolver,
            data_module.ThreadMismatchResolver,
            data_module.MismatchedContactOrGroupResolver,
            data_module.ReceiptMismatchResolver,
        ],
    )
    def test_never_matches_even_a_plausible_message(self, cls):
        resolver = cls("shard_1", "shard_2")
        # Constructed to contain all the literal substrings the pattern
        # needs, joined with real newlines exactly as postgres would emit --
        # but matches() normalizes those away first, so this always misses.
        plausible = " ".join(part.strip() for part in resolver.regex_str.strip(".*").split("\n"))
        plausible = plausible.replace("\\", "").replace("(.+)", "12345").replace("([0-9]+)", "1")
        assert resolver.matches(Exception(plausible)) is False


class TestDuplicateMixPanelIdResolver:
    def test_regex_is_single_line_and_matches(self):
        resolver = data_module.DuplicateMixPanelIdResolver("shard_1", "shard_2")
        assert resolver.using == "shard_2"
        exc = Exception('duplicate key value violates unique constraint "main_extendeduser_mixpanelid_key" DETAIL:  Key (mixpanelid)=(abc-123) already exists.')
        assert resolver.matches(exc) is True
        assert resolver.match.group(1) == "abc-123"

    def test_run_updates_the_conflicting_mixpanelid(self):
        resolver = data_module.DuplicateMixPanelIdResolver("shard_1", "shard_2")
        exc = Exception('duplicate key value violates unique constraint "main_extendeduser_mixpanelid_key" DETAIL:  Key (mixpanelid)=(abc-123) already exists.')
        resolver.matches(exc)

        with (
            mock.patch.object(data_module, "db_exec") as db_exec,
            mock.patch.object(data_module, "db_query", return_value=[(1,)]) as db_query,
            mock.patch("uuid.uuid4", return_value="new-uuid"),
        ):
            resolver.run()

        assert db_exec.call_args_list[0] == mock.call("ROLLBACK", using="shard_2")
        db_query.assert_called_once_with('SELECT count(*) FROM "main_extendeduser" WHERE "mixpanelid" = %s', ("abc-123",), using="shard_2")
        assert db_exec.call_args_list[1] == mock.call("BEGIN", using="shard_2")
        update_call = db_exec.call_args_list[2]
        assert update_call.args[0] == 'UPDATE "main_extendeduser" SET "mixpanelid" = %s WHERE "mixpanelid" = %s'
        assert update_call.args[1] == ("new-uuid", "abc-123")
        assert db_exec.call_args_list[3] == mock.call("COMMIT", using="shard_2")

    def test_run_raises_when_row_count_is_not_exactly_one(self):
        resolver = data_module.DuplicateMixPanelIdResolver("shard_1", "shard_2")
        exc = Exception('duplicate key value violates unique constraint "main_extendeduser_mixpanelid_key" DETAIL:  Key (mixpanelid)=(abc-123) already exists.')
        resolver.matches(exc)

        with (
            mock.patch.object(data_module, "db_exec"),
            mock.patch.object(data_module, "db_query", return_value=[(0,)]),
        ):
            with pytest.raises(AssertionError):
                resolver.run()

    def test_run_raises_without_a_prior_match(self):
        resolver = data_module.DuplicateMixPanelIdResolver("shard_1", "shard_2")
        with pytest.raises(AssertionError):
            resolver.run()


def _resolver_with_injected_match(cls, source_shard, destination_shard, pattern, text):
    """Manually inject a match, bypassing the broken multi-line matches()
    pathway (see TestMultiLineResolverRegexesNeverMatch), so each resolver's
    run() body -- the part that actually does work -- stays testable."""
    resolver = cls(source_shard, destination_shard)
    resolver.match = re.match(pattern, text)
    assert resolver.match is not None
    return resolver


class TestDuplicateUsernameResolver:
    def test_using_is_destination_shard(self):
        resolver = data_module.DuplicateUsernameResolver("shard_1", "shard_2")
        assert resolver.using == "shard_2"

    def test_run_updates_username(self):
        resolver = _resolver_with_injected_match(data_module.DuplicateUsernameResolver, "shard_1", "shard_2", r"(.+)", "openiduser12x")

        with (
            mock.patch.object(data_module, "db_exec") as db_exec,
            mock.patch.object(data_module, "db_query", return_value=[(1,)]) as db_query,
        ):
            resolver.run()

        db_query.assert_called_once_with('SELECT count(*) FROM "auth_user" WHERE "username" = %s', ("openiduser12x",), using="shard_2")
        update_call = db_exec.call_args_list[2]
        assert update_call.args[1] == ("openiduser12xx", "openiduser12x")

    def test_run_rejects_purely_numeric_usernames(self):
        resolver = _resolver_with_injected_match(data_module.DuplicateUsernameResolver, "shard_1", "shard_2", r"(.+)", "1234567890")
        with pytest.raises(AssertionError):
            resolver.run()

    def test_run_raises_when_row_count_is_not_exactly_one(self):
        resolver = _resolver_with_injected_match(data_module.DuplicateUsernameResolver, "shard_1", "shard_2", r"(.+)", "someuser")
        with (
            mock.patch.object(data_module, "db_exec"),
            mock.patch.object(data_module, "db_query", return_value=[(2,)]),
        ):
            with pytest.raises(AssertionError):
                resolver.run()


class TestDuplicateIdResolver:
    def test_run_updates_primary_key_id(self):
        resolver = _resolver_with_injected_match(data_module.DuplicateIdResolver, "shard_1", "shard_2", r"(\w+)_(\d+)", "main_shortlink_42")

        with (
            mock.patch.object(data_module, "db_exec") as db_exec,
            mock.patch.object(data_module, "db_query", return_value=[(99,)]) as db_query,
            mock.patch.object(data_module, "update_primary_key_id") as update_pk,
        ):
            resolver.run()

        assert db_exec.call_args_list[0] == mock.call("ROLLBACK", using="shard_2")
        assert db_exec.call_args_list[1] == mock.call("BEGIN", using="shard_2")
        db_query.assert_called_once_with("""SELECT sh_next_id('main_shortlink_id_seq')""", using="shard_2")
        update_pk.assert_called_once_with("main_shortlink", 42, 99, using="shard_2")
        assert db_exec.call_args_list[2] == mock.call("COMMIT", using="shard_2")

    def test_run_raises_when_current_id_is_not_numeric(self):
        resolver = _resolver_with_injected_match(data_module.DuplicateIdResolver, "shard_1", "shard_2", r"(\w+)_(\w+)", "main_shortlink_abc")
        with pytest.raises(AssertionError):
            resolver.run()


class TestContactGroupsOverlapResolver:
    def test_using_is_source_shard(self):
        resolver = data_module.ContactGroupsOverlapResolver("shard_1", "shard_2")
        assert resolver.using == "shard_1"

    def test_run_removes_mismatched_group_membership(self):
        resolver = _resolver_with_injected_match(data_module.ContactGroupsOverlapResolver, "shard_1", "shard_2", r"(\d+)", "9")

        with (
            mock.patch.object(data_module, "db_exec") as db_exec,
            mock.patch.object(data_module, "db_query", return_value=[(5,)]) as db_query,
        ):
            resolver.run()

        db_query.assert_called_once_with('SELECT "user_id" FROM "main_group" WHERE "id" = %s', (9,), using="shard_1")
        delete_call = db_exec.call_args_list[2]
        assert 'DELETE FROM "main_contact_groups"' in delete_call.args[0]
        assert delete_call.args[1] == (9, 9, 5)
        assert db_exec.call_args_list[-1] == mock.call("COMMIT", using="shard_1")


class TestReceiptOverlapResolver:
    def test_run_reassigns_receipt_ownership(self):
        resolver = _resolver_with_injected_match(data_module.ReceiptOverlapResolver, "shard_1", "shard_2", r"(contact|group)_(\d+)", "contact_9")

        with (
            mock.patch.object(data_module, "db_exec") as db_exec,
            mock.patch.object(data_module, "db_query", return_value=[(5,)]) as db_query,
        ):
            resolver.run()

        db_query.assert_called_once_with('SELECT "user_id" FROM "main_contact" WHERE "id" = %s', (9,), using="shard_1")
        sql_calls = [c.args[0] for c in db_exec.call_args_list]
        assert any("main_thread" in s for s in sql_calls)
        assert any("main_usermessage" in s and "SET \"user_id\"" in s for s in sql_calls)
        assert any(s.startswith('UPDATE "main_receipt"') for s in sql_calls)
        assert sql_calls[0] == "ROLLBACK"
        assert sql_calls[-1] == "COMMIT"


class TestFindAndValidateUserIdForThreadMembers:
    def test_empty_members_json_raises_value_error(self):
        # `user_ids_c, user_ids_g = None, None` no longer raises TypeError
        # (fixed SonarQube S3862); an empty members_json ("{}" -> {}) now
        # fails later at `contact_ids, group_ids = sjson.loads(members_json)`
        # since a 0-key dict can't unpack into two variables.
        with pytest.raises(ValueError, match="not enough values to unpack"):
            data_module._find_and_validate_user_id_for_thread_members(mock.Mock(), "{}", "shard_1")


class TestThreadOverlapResolver:
    def test_run_nulls_out_latest_message_when_user_matches(self):
        resolver = _resolver_with_injected_match(data_module.ThreadOverlapResolver, "shard_1", "shard_2", r"(\d+)", "3")

        with (
            mock.patch.object(data_module, "db_exec") as db_exec,
            mock.patch.object(data_module, "db_query", return_value=[(5, "{}")]),
            mock.patch.object(data_module, "_find_and_validate_user_id_for_thread_members", return_value=5),
        ):
            resolver.run()

        sql_calls = [c.args[0] for c in db_exec.call_args_list]
        assert any("latestUserMessageId" in s and "NULL" in s for s in sql_calls)
        assert sql_calls[-1] == "COMMIT"

    def test_run_reassigns_ownership_when_user_mismatches(self):
        resolver = _resolver_with_injected_match(data_module.ThreadOverlapResolver, "shard_1", "shard_2", r"(\d+)", "3")

        with (
            mock.patch.object(data_module, "db_exec") as db_exec,
            mock.patch.object(data_module, "db_query", return_value=[(5, "{}")]),
            mock.patch.object(data_module, "_find_and_validate_user_id_for_thread_members", return_value=99),
        ):
            resolver.run()

        sql_calls = [c.args[0] for c in db_exec.call_args_list]
        assert any(s.startswith('UPDATE "main_receipt"') for s in sql_calls)
        assert any(s.startswith('UPDATE "main_usermessage"') for s in sql_calls)
        assert any(s.startswith('UPDATE "main_thread"') and "SET \"user_id\"" in s for s in sql_calls)


class TestBlockMismatchResolver:
    def test_run_returns_early_when_no_blocks_found(self):
        resolver = _resolver_with_injected_match(data_module.BlockMismatchResolver, "shard_1", "shard_2", r"(\d+)", "3")

        with (
            mock.patch.object(data_module, "db_exec") as db_exec,
            mock.patch.object(data_module, "db_query", return_value=[]),
        ):
            resolver.run()

        sql_calls = [c.args[0] for c in db_exec.call_args_list]
        assert "COMMIT" not in sql_calls

    def test_run_updates_blocks_when_found(self):
        resolver = _resolver_with_injected_match(data_module.BlockMismatchResolver, "shard_1", "shard_2", r"(\d+)", "3")
        blocks = [{"id": 1, "blocked_user_id": 5, "contact_id": 10, "message_id": 20}]

        def db_query_side_effect(sql, *args, **kwargs):
            if sql.strip().startswith('SELECT * FROM "main_block"'):
                return blocks
            return [(5,)]

        with (
            mock.patch.object(data_module, "db_exec") as db_exec,
            mock.patch.object(data_module, "db_query", side_effect=db_query_side_effect),
        ):
            resolver.run()

        sql_calls = [c.args[0] for c in db_exec.call_args_list]
        assert sql_calls[-1] == "COMMIT"
        assert any(s.startswith('UPDATE "main_usermessage"') for s in sql_calls)
        assert any(s.startswith('UPDATE "main_receipt"') for s in sql_calls)
        assert any(s.startswith('UPDATE "main_thread"') for s in sql_calls)

    def test_run_raises_when_blocked_user_id_mismatches_contact_owner(self):
        resolver = _resolver_with_injected_match(data_module.BlockMismatchResolver, "shard_1", "shard_2", r"(\d+)", "3")
        blocks = [{"id": 1, "blocked_user_id": 5, "contact_id": 10, "message_id": 20}]

        def db_query_side_effect(sql, *args, **kwargs):
            if sql.strip().startswith('SELECT * FROM "main_block"'):
                return blocks
            return [(999,)]

        with (
            mock.patch.object(data_module, "db_exec"),
            mock.patch.object(data_module, "db_query", side_effect=db_query_side_effect),
        ):
            with pytest.raises(AssertionError):
                resolver.run()


class TestThreadMismatchResolver:
    def test_run_deletes_unintelligible_receipts_when_present(self):
        resolver = _resolver_with_injected_match(data_module.ThreadMismatchResolver, "shard_1", "shard_2", r"(\d+)", "3")

        def db_query_side_effect(sql, *args, **kwargs):
            if "membersJson" in sql:
                return [(5, "{}")]
            return [(11,), (12,)]

        with (
            mock.patch.object(data_module, "db_exec") as db_exec,
            mock.patch.object(data_module, "db_query", side_effect=db_query_side_effect),
            mock.patch.object(data_module, "_find_and_validate_user_id_for_thread_members", return_value=5),
        ):
            resolver.run()

        sql_calls = [c.args[0] for c in db_exec.call_args_list]
        assert any(s.startswith("DELETE FROM") for s in sql_calls)
        assert sql_calls[-1] == "COMMIT"

    def test_run_skips_deletion_when_no_unintelligible_receipts(self):
        resolver = _resolver_with_injected_match(data_module.ThreadMismatchResolver, "shard_1", "shard_2", r"(\d+)", "3")

        def db_query_side_effect(sql, *args, **kwargs):
            if "membersJson" in sql:
                return [(5, "{}")]
            return []

        with (
            mock.patch.object(data_module, "db_exec") as db_exec,
            mock.patch.object(data_module, "db_query", side_effect=db_query_side_effect),
            mock.patch.object(data_module, "_find_and_validate_user_id_for_thread_members", return_value=5),
        ):
            resolver.run()

        sql_calls = [c.args[0] for c in db_exec.call_args_list]
        assert not any(s.startswith("DELETE FROM") for s in sql_calls)

    def test_run_raises_when_user_ids_disagree(self):
        resolver = _resolver_with_injected_match(data_module.ThreadMismatchResolver, "shard_1", "shard_2", r"(\d+)", "3")

        with (
            mock.patch.object(data_module, "db_exec"),
            mock.patch.object(data_module, "db_query", return_value=[(5, "{}")]),
            mock.patch.object(data_module, "_find_and_validate_user_id_for_thread_members", return_value=999),
        ):
            with pytest.raises(AssertionError):
                resolver.run()


class TestMismatchedContactOrGroupResolver:
    def test_run_reassigns_usermessage_ownership(self):
        resolver = _resolver_with_injected_match(data_module.MismatchedContactOrGroupResolver, "shard_1", "shard_2", r"(contact|group)_(\d+)", "group_11")

        def db_query_side_effect(sql, *args, **kwargs):
            if sql.strip().startswith('SELECT "user_id"'):
                return [(5,)]
            return [(101,), (102,)]

        with (
            mock.patch.object(data_module, "db_exec") as db_exec,
            mock.patch.object(data_module, "db_query", side_effect=db_query_side_effect),
        ):
            resolver.run()

        sql_calls = [c.args[0] for c in db_exec.call_args_list]
        update_call = [c for c in db_exec.call_args_list if c.args[0].startswith('UPDATE "main_usermessage"')][0]
        assert "101" in update_call.args[0] and "102" in update_call.args[0]
        assert sql_calls[-1] == "COMMIT"

    def test_run_raises_for_unrecognized_object_type(self):
        resolver = _resolver_with_injected_match(data_module.MismatchedContactOrGroupResolver, "shard_1", "shard_2", r"(\w+)_(\d+)", "widget_11")
        with pytest.raises(AssertionError):
            resolver.run()


class TestReceiptMismatchResolver:
    def test_run_fixes_mismatched_receipt_owner(self):
        resolver = _resolver_with_injected_match(data_module.ReceiptMismatchResolver, "shard_1", "shard_2", r"(\d+)", "3")

        with (
            mock.patch.object(data_module, "db_exec") as db_exec,
            mock.patch.object(data_module, "db_query", side_effect=[[(1,)], [(2,)]]),
        ):
            resolver.run()

        update_call = db_exec.call_args_list[2]
        assert update_call.args[0] == 'UPDATE "main_usermessage" SET "user_id" = %s\n                WHERE "id" = %s'
        assert update_call.args[1] == (2, 3)
        assert db_exec.call_args_list[-1] == mock.call("COMMIT", using="shard_1")

    def test_run_raises_when_ids_already_match(self):
        resolver = _resolver_with_injected_match(data_module.ReceiptMismatchResolver, "shard_1", "shard_2", r"(\d+)", "3")

        with (
            mock.patch.object(data_module, "db_exec"),
            mock.patch.object(data_module, "db_query", side_effect=[[(1,)], [(1,)]]),
        ):
            with pytest.raises(AssertionError):
                resolver.run()


class TestFindAutomaticErrorResolver:
    def test_finds_matching_resolver(self):
        exc = Exception('duplicate key value violates unique constraint "main_extendeduser_mixpanelid_key" DETAIL:  Key (mixpanelid)=(abc-123) already exists.')
        resolver = data_module._find_automatic_error_resolver("shard_1", "shard_2", exc)
        assert isinstance(resolver, data_module.DuplicateMixPanelIdResolver)

    def test_returns_none_when_nothing_matches(self):
        resolver = data_module._find_automatic_error_resolver("shard_1", "shard_2", Exception("totally unrelated error"))
        assert resolver is None


# ---------------------------------------------------------------------------
# _dump_and_copy_logical_shard_wrapper / _dump_and_copy_logical_shard
# ---------------------------------------------------------------------------


class TestDumpAndCopyLogicalShardWrapper:
    def test_returns_result_on_first_success(self):
        with mock.patch.object(data_module, "_dump_and_copy_logical_shard", return_value=1000) as inner:
            result = data_module._dump_and_copy_logical_shard_wrapper(7, "shard_2", "shard_1")
        assert result == 1000
        inner.assert_called_once()

    def test_resolves_error_and_retries(self):
        exc = Exception('duplicate key value violates unique constraint "main_extendeduser_mixpanelid_key" DETAIL:  Key (mixpanelid)=(abc-123) already exists.')
        with (
            mock.patch.object(data_module, "_dump_and_copy_logical_shard", side_effect=[exc, 1000]),
            mock.patch.object(data_module, "db_exec") as db_exec,
            mock.patch.object(data_module, "db_query", return_value=[(1,)]),
            mock.patch("uuid.uuid4", return_value="new-uuid"),
        ):
            result = data_module._dump_and_copy_logical_shard_wrapper(7, "shard_2", "shard_1")
        assert result == 1000
        assert mock.call("ROLLBACK", using="shard_2") in db_exec.call_args_list

    def test_raises_when_no_resolver_found(self):
        with mock.patch.object(data_module, "_dump_and_copy_logical_shard", side_effect=Exception("totally unrelated error")):
            with pytest.raises(Exception, match="totally unrelated error"):
                data_module._dump_and_copy_logical_shard_wrapper(7, "shard_2", "shard_1")

    def test_raises_last_exception_after_exceeding_max_retries(self):
        # Each retry must raise a *distinct* message -- otherwise the
        # "identical exception twice" short-circuit (tested separately below)
        # fires first instead of the MAX_DUMP_COPY_ERRORS counter.
        counter = {"n": 0}

        def raise_varying_duplicate(*args, **kwargs):
            counter["n"] += 1
            raise Exception(f'duplicate key value violates unique constraint "main_extendeduser_mixpanelid_key" DETAIL:  Key (mixpanelid)=(val-{counter["n"]}) already exists.')

        with (
            mock.patch.object(data_module, "_dump_and_copy_logical_shard", side_effect=raise_varying_duplicate),
            mock.patch.object(data_module, "db_exec"),
            mock.patch.object(data_module, "db_query", return_value=[(1,)]),
            mock.patch("uuid.uuid4", side_effect=[f"uuid-{i}" for i in range(20)]),
        ):
            with pytest.raises(Exception, match="duplicate key value"):
                data_module._dump_and_copy_logical_shard_wrapper(7, "shard_2", "shard_1")
        # The retry-count guard runs *before* each call, so exactly
        # MAX_DUMP_COPY_ERRORS calls happen (the (MAX+1)'th is blocked).
        assert counter["n"] == data_module.MAX_DUMP_COPY_ERRORS

    def test_aborts_immediately_on_repeated_identical_exception(self):
        exc = Exception("same error text")
        with mock.patch.object(data_module, "_dump_and_copy_logical_shard", side_effect=exc):
            with pytest.raises(Exception, match="same error text"):
                data_module._dump_and_copy_logical_shard_wrapper(7, "shard_2", "shard_1", attemptCount=1, lastException=Exception("same error text"))


class TestDumpAndCopyLogicalShard:
    def test_dumps_and_executes_each_statement(self):
        dump = {"auth_user": ['INSERT INTO "auth_user" VALUES (1);']}
        with (
            mock.patch.object(data_module, "_dump_logical_shard", return_value=dump),
            mock.patch.object(data_module, "upload_file", return_value="https://s3/x"),
            mock.patch.object(data_module, "db_exec") as db_exec,
        ):
            started_ts = data_module._dump_and_copy_logical_shard(7, "shard_2", using="shard_1", user_ids=[1])
        assert isinstance(started_ts, int)
        db_exec.assert_called_once_with('INSERT INTO "auth_user" VALUES (1);', using="shard_2")

    def test_percent_signs_are_escaped_before_execution(self):
        dump = {"auth_user": ["UPDATE t SET x = '50%' WHERE id=1;"]}
        with (
            mock.patch.object(data_module, "_dump_logical_shard", return_value=dump),
            mock.patch.object(data_module, "upload_file", return_value="https://s3/x"),
            mock.patch.object(data_module, "db_exec") as db_exec,
        ):
            data_module._dump_and_copy_logical_shard(7, "shard_2", using="shard_1", user_ids=[1])
        assert db_exec.call_args.args[0] == "UPDATE t SET x = '50%%' WHERE id=1;"


# ---------------------------------------------------------------------------
# _dump2_sql_string / _dump2_sql_list / _backup_dump_and_convert_to_sql_list
# ---------------------------------------------------------------------------


class TestBaseBackupFileName:
    def test_builds_expected_path(self):
        assert data_module._base_backup_file_name(7, 1000.4) == "/logicalShardMigrations/id-7_1000"


class TestDump2SqlString:
    def test_formats_all_statements_grouped_by_table(self):
        dump = {"auth_user": ["INSERT 1;", "INSERT 2;"], "main_group": ["INSERT 3;"]}
        out = data_module._dump2_sql_string(dump, 7, 1000)
        assert "-- table = auth_user" in out
        assert "INSERT 1;" in out
        assert "INSERT 2;" in out
        assert "-- table = main_group" in out
        assert "INSERT 3;" in out


class TestDump2SqlList:
    def test_flattens_all_statements(self):
        dump = {"auth_user": ["INSERT 1;", "INSERT 2;"], "main_group": ["INSERT 3;"]}
        assert data_module._dump2_sql_list(dump) == ["INSERT 1;", "INSERT 2;", "INSERT 3;"]

    def test_empty_dump_yields_empty_list(self):
        assert data_module._dump2_sql_list({}) == []


class TestBackupDumpAndConvertToSqlList:
    def test_uploads_sql_and_json_forms(self):
        dump = {"auth_user": ["INSERT 1;"]}
        with mock.patch.object(data_module, "upload_file", return_value="https://s3/x") as upload_file:
            result = data_module._backup_dump_and_convert_to_sql_list(dump, 7, 1000, 1010)

        assert result == ["INSERT 1;"]
        assert upload_file.call_count == 2
        sql_call, json_call = upload_file.call_args_list
        assert sql_call.args[0] == "/logicalShardMigrations/id-7_1000.sql"
        assert "INSERT 1;" in sql_call.args[1]
        assert "-- Dump of LogicalShard 7 finished on 1010" in sql_call.args[1]
        assert json_call.args[0] == "/logicalShardMigrations/id-7_1000.json"
        assert "INSERT 1;" in json_call.args[1]


# ---------------------------------------------------------------------------
# _dump_logical_shard
# ---------------------------------------------------------------------------


class TestDumpLogicalShard:
    def test_uses_supplied_using_and_user_ids(self):
        with (
            mock.patch.object(data_module, "_physical_shard_id") as physical_shard_id,
            mock.patch.object(data_module, "_logical_shard_user_ids") as logical_shard_user_ids,
            mock.patch.object(data_module, "dump_users", return_value={"k": []}) as dump_users,
        ):
            result = data_module._dump_logical_shard(7, using="shard_1", user_ids=[1, 2])

        physical_shard_id.assert_not_called()
        logical_shard_user_ids.assert_not_called()
        dump_users.assert_called_once_with([1, 2], "shard_1")
        assert result == {"k": []}

    def test_resolves_using_and_user_ids_when_omitted(self):
        with (
            mock.patch.object(data_module, "_physical_shard_id", return_value=3) as physical_shard_id,
            mock.patch.object(data_module, "coerce_id_to_shard_name", return_value="shard_3"),
            mock.patch.object(data_module, "_logical_shard_user_ids", return_value=[1, 2]) as logical_shard_user_ids,
            mock.patch.object(data_module, "dump_users", return_value={}),
        ):
            data_module._dump_logical_shard(7)

        physical_shard_id.assert_called_once_with(7)
        logical_shard_user_ids.assert_called_once_with(7, 3)

    def test_recomputes_physical_shard_id_when_using_supplied_but_user_ids_omitted(self):
        # `using` is supplied, so the first `if using is None:` block never
        # runs and never sets the local `physical_shard_id` -- so the second
        # block's `"physical_shard_id" not in vars()` guard must be True and
        # it must look the id up itself rather than reusing an unset local.
        with (
            mock.patch.object(data_module, "_physical_shard_id", return_value=3) as physical_shard_id,
            mock.patch.object(data_module, "_logical_shard_user_ids", return_value=[1, 2]) as logical_shard_user_ids,
            mock.patch.object(data_module, "dump_users", return_value={}),
        ):
            data_module._dump_logical_shard(7, using="shard_1")

        physical_shard_id.assert_called_once_with(7)
        logical_shard_user_ids.assert_called_once_with(7, 3)

    def test_raises_when_no_users_found(self):
        with (
            mock.patch.object(data_module, "_physical_shard_id", return_value=3),
            mock.patch.object(data_module, "coerce_id_to_shard_name", return_value="shard_3"),
            mock.patch.object(data_module, "_logical_shard_user_ids", return_value=[]),
        ):
            with pytest.raises(AssertionError):
                data_module._dump_logical_shard(7)


# ---------------------------------------------------------------------------
# _user_id_table_column_pairs
# ---------------------------------------------------------------------------


class TestUserIdTableColumnPairs:
    def test_merges_seed_pairs_with_discovered_pairs_uniquely(self):
        with mock.patch.object(data_module, "find_tables_with_user_id_column", return_value=[("auth_user", "id"), ("main_extra", "user_id")]):
            result = data_module._user_id_table_column_pairs()
        assert result[0] == ("auth_user", "id")
        assert ("main_extra", "user_id") in result
        assert result.count(("auth_user", "id")) == 1


# ---------------------------------------------------------------------------
# _verify_these_users_exist_in_shard
# ---------------------------------------------------------------------------


class TestVerifyTheseUsersExistInShard:
    def test_passes_when_all_users_found(self):
        with mock.patch.object(data_module, "db_query", return_value=[(2,)]) as db_query:
            data_module._verify_these_users_exist_in_shard([1, 2], using="shard_1")
        db_query.assert_called_once_with('SELECT count(*) FROM "auth_user"\n                         WHERE "id" IN (1,2)', using="shard_1")

    def test_raises_when_some_users_missing(self):
        with mock.patch.object(data_module, "db_query", return_value=[(1,)]):
            with pytest.raises(AssertionError):
                data_module._verify_these_users_exist_in_shard([1, 2], using="shard_1")


# ---------------------------------------------------------------------------
# dump_users
# ---------------------------------------------------------------------------


class TestDumpUsers:
    def _patch_common(self, pairs, dependencies=None, select_sql="INSERT ...;"):
        return (
            mock.patch.object(data_module, "coerce_id_to_shard_name", side_effect=lambda x: x),
            mock.patch.object(data_module, "_verify_these_users_exist_in_shard"),
            mock.patch.object(data_module, "_user_id_table_column_pairs", return_value=pairs),
            mock.patch.object(data_module, "discover_dependencies", return_value=dependencies or {}),
            mock.patch.object(data_module, "describe", return_value=[("id", "integer")]),
            mock.patch("sh_util.db.select2insert.select2multi_insert", return_value=select_sql),
        )

    def test_happy_path_collects_inserts_per_table(self):
        pairs = [("auth_user", "id")]
        patches = self._patch_common(pairs)
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5] as select2multi_insert:
            result = data_module.dump_users([1, 2], "shard_1")

        assert result["__pre__"][0] == 'ALTER TABLE "main_contact" DISABLE TRIGGER "main_contact_trigger";'
        assert "BEGIN;" in result["__pre__"]
        assert result["auth_user"] == ["INSERT ...;"]
        assert "COMMIT;" in result["__post__"]
        assert 'ALTER TABLE "main_contact" ENABLE TRIGGER "main_contact_trigger";' in result["__post__"]
        select2multi_insert.assert_called_once()
        _, kwargs = select2multi_insert.call_args
        assert kwargs["table"] == "auth_user"
        assert '"id" IN (1,2)' in kwargs["where_clause"]

    def test_deactivate_triggers_false_skips_trigger_statements(self):
        pairs = [("auth_user", "id")]
        patches = self._patch_common(pairs)
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5]:
            result = data_module.dump_users([1], "shard_1", deactivateTriggers=False)

        assert result["__pre__"] == list(data_module.preMigrationSql)
        assert result["__post__"] == list(data_module.postMigrationSql)

    def test_static_table_is_skipped_entirely(self, monkeypatch):
        monkeypatch.setattr(data_module.settings, "STATIC_TABLES", ("auth_user",), raising=False)
        pairs = [("auth_user", "id")]
        patches = self._patch_common(pairs)
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5] as select2multi_insert:
            result = data_module.dump_users([1], "shard_1")

        assert "auth_user" not in result
        select2multi_insert.assert_not_called()

    def test_none_sql_from_select2multi_insert_is_not_recorded(self):
        pairs = [("auth_user", "id")]
        patches = self._patch_common(pairs, select_sql=None)
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5]:
            result = data_module.dump_users([1], "shard_1")

        assert "auth_user" not in result

    def test_additional_relations_are_dumped_first(self):
        pairs = [("main_receipt", "user_id")]
        with (
            mock.patch.object(data_module, "coerce_id_to_shard_name", side_effect=lambda x: x),
            mock.patch.object(data_module, "_verify_these_users_exist_in_shard"),
            mock.patch.object(data_module, "_user_id_table_column_pairs", return_value=pairs),
            mock.patch.object(data_module, "discover_dependencies", return_value={}),
            mock.patch.object(data_module, "describe", return_value=[("id", "integer")]),
            mock.patch.object(data_module, "get_primary_key_columns", return_value=["id"]),
            mock.patch("sh_util.db.select2insert.select2multi_insert", return_value="INSERT ...;") as select2multi_insert,
        ):
            result = data_module.dump_users([1], "shard_1")

        tables_dumped = [c.kwargs["table"] for c in select2multi_insert.call_args_list]
        assert tables_dumped[0] == "main_shortlink"
        assert "main_receipt" in tables_dumped
        assert result["main_shortlink"] == ["INSERT ...;"]

    def test_additional_relation_source_table_ignored_is_skipped(self, monkeypatch):
        monkeypatch.setattr(data_module.settings, "STATIC_TABLES", ("main_shortlink",), raising=False)
        pairs = [("main_receipt", "user_id")]
        with (
            mock.patch.object(data_module, "coerce_id_to_shard_name", side_effect=lambda x: x),
            mock.patch.object(data_module, "_verify_these_users_exist_in_shard"),
            mock.patch.object(data_module, "_user_id_table_column_pairs", return_value=pairs),
            mock.patch.object(data_module, "discover_dependencies", return_value={}),
            mock.patch.object(data_module, "describe", return_value=[("id", "integer")]),
            mock.patch.object(data_module, "get_primary_key_columns", return_value=["id"]),
            mock.patch("sh_util.db.select2insert.select2multi_insert", return_value="INSERT ...;") as select2multi_insert,
        ):
            result = data_module.dump_users([1], "shard_1")

        tables_dumped = [c.kwargs["table"] for c in select2multi_insert.call_args_list]
        assert "main_shortlink" not in tables_dumped
        assert "main_shortlink" not in result

    def test_already_populated_table_is_skipped_on_second_occurrence(self):
        # `_user_id_table_column_pairs` is mocked directly here, so its usual
        # (real) de-duplication doesn't apply -- a duplicate entry exercises
        # dump_users' own "already populated" guard.
        pairs = [("auth_user", "id"), ("auth_user", "id")]
        with (
            mock.patch.object(data_module, "coerce_id_to_shard_name", side_effect=lambda x: x),
            mock.patch.object(data_module, "_verify_these_users_exist_in_shard"),
            mock.patch.object(data_module, "_user_id_table_column_pairs", return_value=pairs),
            mock.patch.object(data_module, "discover_dependencies", return_value={}),
            mock.patch.object(data_module, "describe", return_value=[("id", "integer")]),
            mock.patch("sh_util.db.select2insert.select2multi_insert", return_value="INSERT ...;") as select2multi_insert,
        ):
            data_module.dump_users([1], "shard_1")

        assert select2multi_insert.call_count == 1

    def test_dependencies_are_backfilled(self):
        pairs = [("auth_user", "id")]
        dependencies = {"auth_user": {("id", "main_extra", "auth_user_id")}}
        with (
            mock.patch.object(data_module, "coerce_id_to_shard_name", side_effect=lambda x: x),
            mock.patch.object(data_module, "_verify_these_users_exist_in_shard"),
            mock.patch.object(data_module, "_user_id_table_column_pairs", return_value=pairs),
            mock.patch.object(data_module, "discover_dependencies", return_value=dependencies),
            mock.patch.object(data_module, "describe", return_value=[("id", "integer")]),
            mock.patch("sh_util.db.select2insert.select2multi_insert", return_value="INSERT ...;") as select2multi_insert,
        ):
            result = data_module.dump_users([1], "shard_1")

        tables_dumped = [c.kwargs["table"] for c in select2multi_insert.call_args_list]
        assert "main_extra" in tables_dumped
        assert result["main_extra"] == ["INSERT ...;"]


# ---------------------------------------------------------------------------
# copy_users / copy_user
# ---------------------------------------------------------------------------


class TestCopyUsers:
    def _patch_common(self, pairs, dependencies=None, row_counts=None):
        row_counts = row_counts if row_counts is not None else [{"auth_user": 1}] * 3
        return (
            mock.patch.object(data_module, "_verify_these_users_exist_in_shard"),
            mock.patch.object(data_module, "_user_id_table_column_pairs", return_value=pairs),
            mock.patch.object(data_module, "table_row_counts", side_effect=row_counts),
            mock.patch.object(data_module, "discover_dependencies", return_value=dependencies or {}),
            mock.patch.object(data_module, "db_exec"),
            mock.patch.object(data_module, "auto_db_link_insert"),
        )

    def test_happy_path_commits_when_counts_match(self):
        pairs = [("auth_user", "id")]
        patches = self._patch_common(pairs)
        with patches[0], patches[1], patches[2], patches[3], patches[4] as db_exec, patches[5] as auto_insert:
            pre_commit_cb = mock.Mock()
            data_module.copy_users([1, 2], "shard_1", "shard_2", preCommitCb=pre_commit_cb)

        auto_insert.assert_called_once_with("auth_user", mock.ANY, "shard_1", "shard_2")
        pre_commit_cb.assert_called_once()
        sql_calls = [c.args[0] for c in db_exec.call_args_list]
        assert "COMMIT" in sql_calls
        assert 'ALTER TABLE "main_contact" ENABLE TRIGGER "main_contact_trigger"' in sql_calls

    def test_rolls_back_and_raises_when_counts_mismatch(self):
        pairs = [("auth_user", "id")]
        patches = self._patch_common(pairs, row_counts=[{"auth_user": 2}, {"auth_user": 1}, {"auth_user": 2}])
        with patches[0], patches[1], patches[2], patches[3], patches[4] as db_exec, patches[5]:
            with pytest.raises(data_module.MigrateUserStaleReadError):
                data_module.copy_users([1, 2], "shard_1", "shard_2")

        sql_calls = [c.args[0] for c in db_exec.call_args_list]
        assert "ROLLBACK" in sql_calls

    def test_ignored_table_is_skipped(self, monkeypatch):
        monkeypatch.setattr(data_module.settings, "STATIC_TABLES", ("static_t",), raising=False)
        pairs = [("static_t", "id")]
        patches = self._patch_common(pairs)
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5] as auto_insert:
            data_module.copy_users([1], "shard_1", "shard_2")

        auto_insert.assert_not_called()

    def test_deactivate_triggers_false_skips_trigger_statements(self):
        pairs = [("auth_user", "id")]
        patches = self._patch_common(pairs)
        with patches[0], patches[1], patches[2], patches[3], patches[4] as db_exec, patches[5]:
            data_module.copy_users([1], "shard_1", "shard_2", deactivateTriggers=False)

        sql_calls = [c.args[0] for c in db_exec.call_args_list]
        assert not any("TRIGGER" in s for s in sql_calls)

    def test_manage_transactions_false_skips_transaction_statements(self):
        pairs = [("auth_user", "id")]
        patches = self._patch_common(pairs)
        with patches[0], patches[1], patches[2], patches[3], patches[4] as db_exec, patches[5]:
            data_module.copy_users([1], "shard_1", "shard_2", manageTransactions=False)

        sql_calls = [c.args[0] for c in db_exec.call_args_list]
        assert "BEGIN" not in sql_calls
        assert "COMMIT" not in sql_calls

    def test_commit_destination_shard_false_skips_commit(self):
        pairs = [("auth_user", "id")]
        patches = self._patch_common(pairs)
        with patches[0], patches[1], patches[2], patches[3], patches[4] as db_exec, patches[5]:
            data_module.copy_users([1], "shard_1", "shard_2", commitDestinationShard=False)

        sql_calls = [c.args[0] for c in db_exec.call_args_list]
        assert "COMMIT" not in sql_calls
        assert "BEGIN" in sql_calls

    def test_additional_relations_are_copied_first(self):
        pairs = [("main_receipt", "user_id")]
        with (
            mock.patch.object(data_module, "_verify_these_users_exist_in_shard"),
            mock.patch.object(data_module, "_user_id_table_column_pairs", return_value=pairs),
            mock.patch.object(data_module, "table_row_counts", side_effect=[{"main_receipt": 1}] * 3),
            mock.patch.object(data_module, "discover_dependencies", return_value={}),
            mock.patch.object(data_module, "get_primary_key_columns", return_value=["id"]),
            mock.patch.object(data_module, "db_exec"),
            mock.patch.object(data_module, "auto_db_link_insert") as auto_insert,
        ):
            data_module.copy_users([1], "shard_1", "shard_2")

        tables_inserted = [c.args[0] for c in auto_insert.call_args_list]
        assert tables_inserted[0] == "main_shortlink"
        assert "main_receipt" in tables_inserted

    def test_additional_relation_source_table_ignored_is_skipped(self, monkeypatch):
        monkeypatch.setattr(data_module.settings, "STATIC_TABLES", ("main_shortlink",), raising=False)
        pairs = [("main_receipt", "user_id")]
        with (
            mock.patch.object(data_module, "_verify_these_users_exist_in_shard"),
            mock.patch.object(data_module, "_user_id_table_column_pairs", return_value=pairs),
            mock.patch.object(data_module, "table_row_counts", side_effect=[{"main_receipt": 1}] * 3),
            mock.patch.object(data_module, "discover_dependencies", return_value={}),
            mock.patch.object(data_module, "get_primary_key_columns", return_value=["id"]),
            mock.patch.object(data_module, "db_exec"),
            mock.patch.object(data_module, "auto_db_link_insert") as auto_insert,
        ):
            data_module.copy_users([1], "shard_1", "shard_2")

        tables_inserted = [c.args[0] for c in auto_insert.call_args_list]
        assert "main_shortlink" not in tables_inserted
        assert tables_inserted == ["main_receipt"]

    def test_already_populated_table_is_skipped_on_second_occurrence(self):
        # Direct mock of `_user_id_table_column_pairs` bypasses its usual
        # (real) de-duplication, so a duplicate entry exercises copy_users'
        # own first-loop "already populated" guard.
        pairs = [("auth_user", "id"), ("auth_user", "id")]
        patches = self._patch_common(pairs)
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5] as auto_insert:
            data_module.copy_users([1], "shard_1", "shard_2")

        assert auto_insert.call_count == 1

    def test_dependencies_are_backfilled(self):
        pairs = [("auth_user", "id")]
        dependencies = {"auth_user": {("id", "main_extra", "auth_user_id")}}
        patches = self._patch_common(pairs, dependencies=dependencies)
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5] as auto_insert:
            data_module.copy_users([1], "shard_1", "shard_2")

        tables_inserted = [c.args[0] for c in auto_insert.call_args_list]
        assert "main_extra" in tables_inserted

    def test_retries_on_transient_error_then_succeeds(self):
        pairs = [("auth_user", "id")]
        patches = self._patch_common(pairs)
        with patches[0], patches[1], patches[2], patches[3], patches[4] as db_exec, patches[5] as auto_insert:
            auto_insert.side_effect = [Exception("transient"), None]
            data_module.copy_users([1], "shard_1", "shard_2")

        assert auto_insert.call_count == 2
        sql_calls = [c.args[0] for c in db_exec.call_args_list]
        assert any(s.startswith("ROLLBACK TO save") for s in sql_calls)

    def test_raises_dependency_cycle_when_always_failing(self):
        pairs = [("auth_user", "id")]
        patches = self._patch_common(pairs)
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5] as auto_insert:
            auto_insert.side_effect = Exception("always fails")
            with pytest.raises(Exception, match="Dependency cycle detected"):
                data_module.copy_users([1], "shard_1", "shard_2")


class TestCopyUser:
    def test_delegates_to_copy_users_with_single_element_list(self):
        with mock.patch.object(data_module, "copy_users", return_value="ok") as copy_users:
            result = data_module.copy_user(1, "shard_1", "shard_2", deactivateTriggers=False)
        copy_users.assert_called_once_with([1], "shard_1", "shard_2", deactivateTriggers=False)
        assert result == "ok"


# ---------------------------------------------------------------------------
# delete_users / delete_user
# ---------------------------------------------------------------------------


class TestDeleteUsers:
    def test_happy_path_returns_true(self):
        pairs = [("auth_user", "id")]
        pre_commit_cb = mock.Mock()
        with (
            mock.patch.object(data_module, "find_tables_with_user_id_column", return_value=pairs),
            mock.patch.object(data_module, "discover_dependencies", return_value={}),
            mock.patch.object(data_module, "db_exec") as db_exec,
        ):
            result = data_module.delete_users([1, 2], "shard_1", preCommitCb=pre_commit_cb)

        assert result is True
        pre_commit_cb.assert_called_once()
        sql_calls = [c.args[0] for c in db_exec.call_args_list]
        assert "BEGIN" in sql_calls
        assert "COMMIT" in sql_calls
        assert any(s.startswith('DELETE FROM "auth_user"') for s in sql_calls)

    def test_ignored_table_is_skipped(self, monkeypatch):
        monkeypatch.setattr(data_module.settings, "STATIC_TABLES", ("static_t",), raising=False)
        pairs = [("static_t", "id")]
        with (
            mock.patch.object(data_module, "find_tables_with_user_id_column", return_value=pairs),
            mock.patch.object(data_module, "discover_dependencies", return_value={}),
            mock.patch.object(data_module, "db_exec") as db_exec,
        ):
            data_module.delete_users([1], "shard_1")

        sql_calls = [c.args[0] for c in db_exec.call_args_list]
        assert not any("static_t" in s for s in sql_calls)

    def test_manage_transactions_false_skips_transaction_statements(self):
        pairs = [("auth_user", "id")]
        with (
            mock.patch.object(data_module, "find_tables_with_user_id_column", return_value=pairs),
            mock.patch.object(data_module, "discover_dependencies", return_value={}),
            mock.patch.object(data_module, "db_exec") as db_exec,
        ):
            data_module.delete_users([1], "shard_1", manageTransactions=False)

        sql_calls = [c.args[0] for c in db_exec.call_args_list]
        assert "BEGIN" not in sql_calls
        assert "COMMIT" not in sql_calls

    def test_additional_relations_are_deleted_first(self):
        pairs = [("main_receipt", "user_id")]
        with (
            mock.patch.object(data_module, "find_tables_with_user_id_column", return_value=pairs),
            mock.patch.object(data_module, "discover_dependencies", return_value={}),
            mock.patch.object(data_module, "get_primary_key_columns", return_value=["id"]),
            mock.patch.object(data_module, "db_exec") as db_exec,
        ):
            data_module.delete_users([1], "shard_1")

        sql_calls = [c.args[0] for c in db_exec.call_args_list]
        assert any('DELETE FROM "main_shortlink"' in s for s in sql_calls)

    def test_additional_relation_fk_table_ignored_is_skipped(self, monkeypatch):
        # NB: delete_users' _additionalRelations guard checks
        # `should_table_be_ignored_for_user_operations(fk_table)`, but every
        # real _additionalRelations entry has fk_table == the outer table
        # key -- which the per-table loop has *already* verified is not
        # ignored just to reach this code. So with the real mapping this
        # branch is unreachable; inject a synthetic entry where fk_table
        # actually differs from the outer table to exercise it directly.
        monkeypatch.setattr(data_module.settings, "STATIC_TABLES", ("some_ignored_fk_table",), raising=False)
        monkeypatch.setattr(data_module, "_additionalRelations", {"main_receipt": [("some_ignored_fk_table", "shortlink_id", "main_shortlink")]})
        pairs = [("main_receipt", "user_id")]
        with (
            mock.patch.object(data_module, "find_tables_with_user_id_column", return_value=pairs),
            mock.patch.object(data_module, "discover_dependencies", return_value={}),
            mock.patch.object(data_module, "get_primary_key_columns", return_value=["id"]),
            mock.patch.object(data_module, "db_exec") as db_exec,
        ):
            data_module.delete_users([1], "shard_1")

        sql_calls = [c.args[0] for c in db_exec.call_args_list]
        assert not any('DELETE FROM "main_shortlink"' in s for s in sql_calls)
        assert any('DELETE FROM "main_receipt"' in s for s in sql_calls)

    def test_dependency_fk_table_ignored_is_skipped(self, monkeypatch):
        monkeypatch.setattr(data_module.settings, "STATIC_TABLES", ("main_extra",), raising=False)
        pairs = [("auth_user", "id")]
        dependencies = {"auth_user": [("id", "main_extra", "auth_user_id")]}
        with (
            mock.patch.object(data_module, "find_tables_with_user_id_column", return_value=pairs),
            mock.patch.object(data_module, "discover_dependencies", return_value=dependencies),
            mock.patch.object(data_module, "db_exec") as db_exec,
        ):
            data_module.delete_users([1], "shard_1")

        sql_calls = [c.args[0] for c in db_exec.call_args_list]
        assert not any('DELETE FROM "main_extra"' in s for s in sql_calls)
        assert any('DELETE FROM "auth_user"' in s for s in sql_calls)

    def test_dependencies_are_deleted_before_the_table(self):
        pairs = [("auth_user", "id")]
        dependencies = {"auth_user": [("id", "main_extra", "auth_user_id")]}
        with (
            mock.patch.object(data_module, "find_tables_with_user_id_column", return_value=pairs),
            mock.patch.object(data_module, "discover_dependencies", return_value=dependencies),
            mock.patch.object(data_module, "db_exec") as db_exec,
        ):
            data_module.delete_users([1], "shard_1")

        sql_calls = [c.args[0] for c in db_exec.call_args_list]
        assert any('DELETE FROM "main_extra"' in s for s in sql_calls)

    def test_share_lock_exception_propagates_immediately(self):
        pairs = [("auth_user", "id")]

        def db_exec_side_effect(sql, *args, **kwargs):
            if sql.startswith('DELETE FROM "auth_user"'):
                raise Exception("waits for ShareLock on transaction 123")
            return None

        with (
            mock.patch.object(data_module, "find_tables_with_user_id_column", return_value=pairs),
            mock.patch.object(data_module, "discover_dependencies", return_value={}),
            mock.patch.object(data_module, "db_exec", side_effect=db_exec_side_effect),
        ):
            with pytest.raises(Exception, match="waits for ShareLock"):
                data_module.delete_users([1], "shard_1")

    def test_dependency_cycle_detected_when_delete_always_fails(self):
        pairs = [("auth_user", "id")]

        def db_exec_side_effect(sql, *args, **kwargs):
            if sql.startswith('DELETE FROM "auth_user"'):
                raise Exception("generic failure")
            return None

        with (
            mock.patch.object(data_module, "find_tables_with_user_id_column", return_value=pairs),
            mock.patch.object(data_module, "discover_dependencies", return_value={}),
            mock.patch.object(data_module, "db_exec", side_effect=db_exec_side_effect),
        ):
            with pytest.raises(Exception, match="Dependency cycle detected"):
                data_module.delete_users([1], "shard_1")

    def test_final_phase_exception_is_wrapped_and_rolled_back(self):
        pairs = [("auth_user", "id")]
        pre_commit_cb = mock.Mock(side_effect=Exception("callback boom"))
        with (
            mock.patch.object(data_module, "find_tables_with_user_id_column", return_value=pairs),
            mock.patch.object(data_module, "discover_dependencies", return_value={}),
            mock.patch.object(data_module, "db_exec") as db_exec,
        ):
            with pytest.raises(data_module.MigrateUserError, match="callback boom"):
                data_module.delete_users([1], "shard_1", preCommitCb=pre_commit_cb)

        sql_calls = [c.args[0] for c in db_exec.call_args_list]
        assert "ROLLBACK" in sql_calls


class TestDeleteUser:
    def test_delegates_to_delete_users_with_single_element_list(self):
        with mock.patch.object(data_module, "delete_users", return_value=True) as delete_users:
            result = data_module.delete_user(1, "shard_1", manageTransactions=False)
        delete_users.assert_called_once_with([1], "shard_1", manageTransactions=False)
        assert result is True


# ---------------------------------------------------------------------------
# migrate_users / migrate_user
# ---------------------------------------------------------------------------


class TestMigrateUsers:
    def test_copies_then_publishes_moved_user_events(self):
        with (
            mock.patch.object(data_module, "coerce_id_to_shard_name", side_effect=lambda x: x),
            mock.patch.object(data_module, "copy_users") as copy_users,
            mock.patch.object(data_module, "ShardEvent") as shard_event_cls,
        ):
            shard_event_instance = shard_event_cls.return_value
            data_module.migrate_users([1, 2], "shard_1", "shard_2")

        copy_users.assert_called_once()
        args, kwargs = copy_users.call_args
        assert args == ([1, 2], "shard_1", "shard_2")
        assert kwargs["commit_destination_shard"] is False
        assert callable(kwargs["pre_commit_cb"])

        assert shard_event_instance.publish.call_count == 2
        published_shard_ids = {c.args[1]["shardId"] for c in shard_event_instance.publish.call_args_list}
        assert published_shard_ids == {"2"}

    def test_pre_commit_cb_deletes_from_source_and_commits_destination(self):
        with (
            mock.patch.object(data_module, "coerce_id_to_shard_name", side_effect=lambda x: x),
            mock.patch.object(data_module, "copy_users") as copy_users,
            mock.patch.object(data_module, "ShardEvent"),
            mock.patch.object(data_module, "delete_users") as delete_users,
            mock.patch.object(data_module, "db_exec") as db_exec,
        ):
            data_module.migrate_users([1, 2], "shard_1", "shard_2")
            pre_commit_cb = copy_users.call_args.kwargs["pre_commit_cb"]
            pre_commit_cb()

            delete_users.assert_called_once()
            args, kwargs = delete_users.call_args
            assert args == ([1, 2], "shard_1")
            assert "pre_commit_cb" in kwargs

            # Invoking delete_users' pre_commit_cb should commit the destination shard.
            inner_cb = kwargs["pre_commit_cb"]
            inner_cb()
            db_exec.assert_called_once_with("COMMIT", using="shard_2")


class TestMigrateUsersKwargMismatchBug:
    """Pins a third pre-existing bug (distinct from the two documented in the
    module docstring): a snake_case/camelCase kwarg mismatch that makes
    ``migrate_users``' delete-from-source step dead code in production.

    ``migrate_users`` calls
    ``copy_users(..., pre_commit_cb=pre_commit_cb, commit_destination_shard=False, **kw)``
    and its inner callback calls
    ``delete_users(user_ids, my_source, pre_commit_cb=deletePreCommitCb, **kw)``
    -- both using snake_case keyword names. But ``copy_users`` only reads
    ``kw.get("preCommitCb", None)`` / ``kw.get("commitDestinationShard", True)``,
    and ``delete_users`` only reads ``kw.get("preCommitCb", None)`` -- both
    camelCase. The snake_case keys land in ``**kw`` under different names than
    what's read back out, so both callbacks silently fall back to their
    defaults: ``pre_commit_cb`` is always ``None`` (the delete-from-source
    callback is never invoked) and ``commit_destination_shard`` stays ``True``
    (the destination commits immediately regardless of the ``False`` that was
    "passed"). Net effect: ``migrate_users`` copies data to the destination
    shard and commits it, but *never* deletes it from the source shard -- the
    delete step central to "migrate" semantics never runs.

    This is exercised here with ``copy_users``/``delete_users`` themselves
    unmocked (only the driver boundary below them is mocked), so the bug is
    demonstrated end-to-end rather than merely re-deriving the dict lookups.
    """

    def test_delete_from_source_never_runs_end_to_end(self):
        pairs = [("auth_user", "id")]
        delete_sql_calls = []
        commit_calls = []

        def db_exec_spy(sql, *args, **kwargs):
            if sql.startswith('DELETE FROM "'):
                delete_sql_calls.append((sql, kwargs.get("using")))
            if sql == "COMMIT":
                commit_calls.append(kwargs.get("using"))
            return None

        with (
            mock.patch.object(data_module, "coerce_id_to_shard_name", side_effect=lambda x: x),
            mock.patch.object(data_module, "_verify_these_users_exist_in_shard"),
            mock.patch.object(data_module, "_user_id_table_column_pairs", return_value=pairs),
            mock.patch.object(data_module, "find_tables_with_user_id_column", return_value=pairs),
            mock.patch.object(data_module, "table_row_counts", return_value={"auth_user": 1}),
            mock.patch.object(data_module, "discover_dependencies", return_value={}),
            mock.patch.object(data_module, "auto_db_link_insert"),
            mock.patch.object(data_module, "ShardEvent"),
            mock.patch.object(data_module, "db_exec", side_effect=db_exec_spy),
        ):
            # copy_users and delete_users are intentionally NOT mocked here --
            # only their own driver-boundary collaborators are -- so the real
            # kwarg plumbing between migrate_users -> copy_users -> (via the
            # pre-commit callback, if it fired) -> delete_users runs for real.
            data_module.migrate_users([1], "shard_1", "shard_2")

        # The destination commit happens unconditionally: commit_destination_shard
        # silently stayed True (copy_users never saw "commitDestinationShard").
        assert commit_calls == ["shard_2"]
        # But no DELETE statement ever ran against the source shard: pre_commit_cb
        # silently stayed None (copy_users never saw "preCommitCb"), so the
        # delete-from-source callback -- and therefore delete_users itself --
        # was never invoked at all.
        assert not any(using == "shard_1" for _, using in delete_sql_calls)


class TestMigrateUser:
    def test_delegates_to_migrate_users_with_single_element_list(self):
        with mock.patch.object(data_module, "migrate_users", return_value="ok") as migrate_users:
            result = data_module.migrate_user(1, "shard_1", "shard_2")
        migrate_users.assert_called_once_with([1], "shard_1", "shard_2")
        assert result == "ok"
