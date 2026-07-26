"""Unit tests for ``sh_util.db.drivers.dj`` (the Django db driver).

Django is not installed in this environment, but every Django reference in
``dj.py`` is a local ``from django.db import connections`` inside a function
body, not a module-level import -- the module itself imports cleanly (as
verified for this task). Each test that needs the Django import to resolve
pre-seeds ``sys.modules["django"]``/``sys.modules["django.db"]`` with a fake
stand-in module (via ``monkeypatch.setitem``, auto-restored), so the local
import finds it already cached rather than trying to load the real package.
``get_real_shard_connection_name``'s "default" branch does
``from ...sharding import ShardedResource`` (a real, already-importable
sh_util module), which is patched directly instead. No test hits a real
Django ORM, database, or the network.
"""

import sys
import types
from unittest import mock

import pytest
from sh_util.db.drivers import dj as dj_module


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _install_fake_django_connections(monkeypatch, connections_obj):
    """Seed sys.modules so ``from django.db import connections`` resolves to
    ``connections_obj`` without needing the real django package installed."""
    django_db_module = types.ModuleType("django.db")
    django_db_module.connections = connections_obj
    django_module = types.ModuleType("django")
    django_module.db = django_db_module

    monkeypatch.setitem(sys.modules, "django", django_module)
    monkeypatch.setitem(sys.modules, "django.db", django_db_module)


# ---------------------------------------------------------------------------
# connections
# ---------------------------------------------------------------------------


class TestConnections:
    def test_returns_the_django_connections_registry(self, monkeypatch):
        fake_registry = mock.Mock(name="connections_registry")
        _install_fake_django_connections(monkeypatch, fake_registry)

        assert dj_module.connections() is fake_registry


# ---------------------------------------------------------------------------
# switch_default_database
# ---------------------------------------------------------------------------


class TestSwitchDefaultDatabase:
    def test_swaps_connection_and_settings_entry(self, monkeypatch):
        fake_connections = {"default": "old-conn", "shard_1": "new-conn"}
        _install_fake_django_connections(monkeypatch, fake_connections)
        monkeypatch.setattr(
            dj_module.settings,
            "DATABASES",
            {"default": {"NAME": "old_db"}, "shard_1": {"NAME": "new_db"}},
            raising=False,
        )

        dj_module.switch_default_database("shard_1")

        assert fake_connections["default"] == "new-conn"
        assert dj_module.settings.DATABASES["default"] == {"NAME": "new_db"}


# ---------------------------------------------------------------------------
# _dictfetchall
# ---------------------------------------------------------------------------


class TestDictFetchAll:
    def test_zips_column_names_from_cursor_description(self):
        cursor = mock.Mock()
        cursor.description = [("id", None), ("name", None)]
        cursor.fetchall.return_value = [(1, "a"), (2, "b")]

        assert dj_module._dictfetchall(cursor) == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]

    def test_empty_result_set(self):
        cursor = mock.Mock()
        cursor.description = [("id", None)]
        cursor.fetchall.return_value = []

        assert dj_module._dictfetchall(cursor) == []


# ---------------------------------------------------------------------------
# get_real_shard_connection_name
# ---------------------------------------------------------------------------


class TestGetRealShardConnectionName:
    def test_non_default_using_is_returned_unchanged(self):
        assert dj_module.get_real_shard_connection_name("shard_7") == "shard_7"

    def test_default_resolves_via_sharded_resource(self):
        with mock.patch("sh_util.sharding.ShardedResource.get_current_shard", return_value="shard_9"):
            assert dj_module.get_real_shard_connection_name("default") == "shard_9"


# ---------------------------------------------------------------------------
# db_query
# ---------------------------------------------------------------------------


class TestDbQuery:
    def _connections_with_cursor(self, fetchall_return=None):
        cursor = mock.Mock(name="cursor")
        if fetchall_return is not None:
            cursor.fetchall.return_value = fetchall_return
        conn = mock.Mock(name="connection")
        conn.cursor.return_value = cursor
        connections_registry = {"default": conn}
        return connections_registry, cursor

    def test_returns_fetchall_rows(self, monkeypatch):
        connections_registry, cursor = self._connections_with_cursor([(1,), (2,)])
        with mock.patch.object(dj_module, "connections", return_value=connections_registry):
            result = dj_module.db_query("SELECT * FROM t", using="default")

        assert result == [(1,), (2,)]
        cursor.execute.assert_called_once_with("SELECT * FROM t", ())
        cursor.close.assert_called_once()

    def test_as_dict_true_uses_dictfetchall(self, monkeypatch):
        connections_registry, cursor = self._connections_with_cursor()
        cursor.description = [("id", None)]
        cursor.fetchall.return_value = [(1,), (2,)]
        with mock.patch.object(dj_module, "connections", return_value=connections_registry):
            result = dj_module.db_query("SELECT * FROM t", using="default", as_dict=True)

        assert result == [{"id": 1}, {"id": 2}]

    def test_args_are_passed_through_to_cursor_execute(self, monkeypatch):
        connections_registry, cursor = self._connections_with_cursor([])
        with mock.patch.object(dj_module, "connections", return_value=connections_registry):
            dj_module.db_query("SELECT * FROM t WHERE id = %s", (5,), using="default")

        cursor.execute.assert_called_once_with("SELECT * FROM t WHERE id = %s", (5,))

    def test_exception_during_execute_is_logged_and_returns_none(self, monkeypatch):
        connections_registry, cursor = self._connections_with_cursor()
        cursor.execute.side_effect = Exception("boom")
        with mock.patch.object(dj_module, "connections", return_value=connections_registry):
            result = dj_module.db_query("SELECT * FROM t", using="default")

        assert result is None
        cursor.close.assert_called_once()

    def test_debug_true_still_returns_results(self, monkeypatch):
        connections_registry, cursor = self._connections_with_cursor([(1,)])
        with mock.patch.object(dj_module, "connections", return_value=connections_registry):
            result = dj_module.db_query("SELECT * FROM t", using="default", debug=True)

        assert result == [(1,)]

    def test_cursor_is_always_closed_even_on_success(self, monkeypatch):
        connections_registry, cursor = self._connections_with_cursor([])
        with mock.patch.object(dj_module, "connections", return_value=connections_registry):
            dj_module.db_query("SELECT * FROM t", using="default")
        cursor.close.assert_called_once()


# ---------------------------------------------------------------------------
# db_exec
# ---------------------------------------------------------------------------


class TestDbExec:
    def _connections_with_cursor(self, execute_return="OK"):
        cursor = mock.Mock(name="cursor")
        cursor.execute.return_value = execute_return
        conn = mock.Mock(name="connection")
        conn.cursor.return_value = cursor
        return {"default": conn}, cursor

    def test_returns_cursor_execute_result(self, monkeypatch):
        connections_registry, cursor = self._connections_with_cursor("result-token")
        with mock.patch.object(dj_module, "connections", return_value=connections_registry):
            result = dj_module.db_exec('UPDATE "t" SET "x" = 1', using="default")

        assert result == "result-token"
        cursor.execute.assert_called_once_with('UPDATE "t" SET "x" = 1', ())
        cursor.close.assert_called_once()

    def test_args_passed_through(self, monkeypatch):
        connections_registry, cursor = self._connections_with_cursor()
        with mock.patch.object(dj_module, "connections", return_value=connections_registry):
            dj_module.db_exec('UPDATE "t" SET "x" = %s', (5,), using="default")

        cursor.execute.assert_called_once_with('UPDATE "t" SET "x" = %s', (5,))

    def test_debug_true_still_executes_and_returns(self, monkeypatch):
        connections_registry, cursor = self._connections_with_cursor("ok")
        with mock.patch.object(dj_module, "connections", return_value=connections_registry):
            result = dj_module.db_exec("COMMIT", using="default", debug=True)

        assert result == "ok"

    def test_cursor_is_always_closed(self, monkeypatch):
        connections_registry, cursor = self._connections_with_cursor()
        with mock.patch.object(dj_module, "connections", return_value=connections_registry):
            dj_module.db_exec("COMMIT", using="default")
        cursor.close.assert_called_once()


# ---------------------------------------------------------------------------
# get_psql_connection_string
# ---------------------------------------------------------------------------


class TestGetPsqlConnectionString:
    def test_raises_assertion_when_connection_name_unknown(self, monkeypatch):
        monkeypatch.setattr(dj_module.settings, "DATABASES", {"default": {}}, raising=False)
        with pytest.raises(AssertionError, match="Requested connection missing"):
            dj_module.get_psql_connection_string("nope")

    def test_secure_true_appends_sslmode_require(self, monkeypatch):
        monkeypatch.setattr(
            dj_module.settings,
            "DATABASES",
            {"default": {"NAME": "sendhub_db", "USER": "u", "PASSWORD": "p", "HOST": "h", "PORT": "5433"}},
            raising=False,
        )
        out = dj_module.get_psql_connection_string("default", secure=True)
        assert out == "dbname=sendhub_db user=u password=p host=h port=5433 sslmode=require"

    def test_secure_false_omits_sslmode(self, monkeypatch):
        monkeypatch.setattr(
            dj_module.settings,
            "DATABASES",
            {"default": {"NAME": "sendhub_db", "USER": "u", "PASSWORD": "p", "HOST": "h", "PORT": "5433"}},
            raising=False,
        )
        out = dj_module.get_psql_connection_string("default", secure=False)
        assert out == "dbname=sendhub_db user=u password=p host=h port=5433"

    def test_missing_and_empty_keys_are_filtered_out(self, monkeypatch):
        # NAME present, USER present, PASSWORD empty string, HOST missing
        # entirely, PORT is None -- only NAME and USER should survive.
        monkeypatch.setattr(
            dj_module.settings,
            "DATABASES",
            {"default": {"NAME": "sendhub_db", "USER": "u", "PASSWORD": "", "PORT": None}},
            raising=False,
        )
        out = dj_module.get_psql_connection_string("default", secure=False)
        assert out == "dbname=sendhub_db user=u"
