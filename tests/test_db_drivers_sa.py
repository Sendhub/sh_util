"""Unit tests for ``sh_util.db.drivers.sa`` (the SqlAlchemy db driver).

Driver boundary: ``sa.py`` resolves its connection registry through two
locally-scoped import chains rather than module-level imports:
``connections()`` tries ``from src.app import app as flask_app`` then falls
back to ``from app import app as flask_app``; ``db_query``/``db_exec`` try
``from src.database import ScopedSessions``, then ``from src.app import
ScopedSessions``, then ``from app import ScopedSessions``. Real ``src/app.py``
and ``src/database.py`` modules exist in this repo but require a live DB/
Flask context to import cleanly, so every test here pre-seeds
``sys.modules`` (via ``monkeypatch.setitem``, auto-restored) with fake stand-
ins for whichever import path it wants to exercise -- setting a name to
``None`` forces Python's import system to raise ``ImportError`` for it,
which is how the fallback branches are reached. No test hits a real
database, Flask app, or the network.

Known pre-existing quirks in the vendored source pinned by tests below (not
fixed, per task constraints):
  * ``sql_and_args_to_text``'s ``?`` placeholder substitution keeps the
    literal ``?`` in the output (``next_bind_sub`` uses the matched ``?`` as
    the ``prefix``), producing ``?:argN`` instead of replacing the ``?``
    with the bind name -- only the ``%s`` form is replaced cleanly.

Fixed since: ``get_psql_connection_string``'s dead first assignment to
``out`` (SonarQube S1854) has been removed; behavior is unchanged since it
was never read before being overwritten.
"""

import sys
import types
from unittest import mock

import pytest
from sh_util.db.drivers import sa as sa_module


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _install_module(monkeypatch, name, module_or_none):
    """Seed ``sys.modules[name]`` for the duration of a test. Passing
    ``None`` forces the next ``import``/``from ... import`` of that name to
    raise ImportError, which is how sa.py's fallback except-branches are
    reached without needing the real (DB-backed) module to exist."""
    monkeypatch.setitem(sys.modules, name, module_or_none)


def _fake_module(name, **attrs):
    mod = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(mod, k, v)
    return mod


def _install_scoped_sessions(monkeypatch, sessions):
    """Make ``from src.database import ScopedSessions`` resolve to
    ``sessions`` (a plain dict of name -> session-factory callable)."""
    _install_module(monkeypatch, "src.database", _fake_module("src.database", ScopedSessions=sessions))


# ---------------------------------------------------------------------------
# _normalize_sql_and_args
# ---------------------------------------------------------------------------


class TestNormalizeSqlAndArgs:
    def test_plain_sql_and_args_pass_through_unchanged(self):
        assert sa_module._normalize_sql_and_args("SELECT 1", (1, 2)) == ("SELECT 1", (1, 2))

    def test_tuple_form_unpacks_sql_and_args(self):
        assert sa_module._normalize_sql_and_args(("SELECT 1", (1, 2))) == ("SELECT 1", (1, 2))

    def test_tuple_form_with_no_extra_args_is_fine(self):
        assert sa_module._normalize_sql_and_args(("SELECT 1", None)) == ("SELECT 1", None)

    def test_wrong_length_tuple_raises(self):
        with pytest.raises(ValueError, match="must be in the form"):
            sa_module._normalize_sql_and_args(("a", "b", "c"))

    def test_args_provided_twice_raises(self):
        with pytest.raises(ValueError, match="provided twice"):
            sa_module._normalize_sql_and_args(("SELECT 1", (1,)), (2,))


# ---------------------------------------------------------------------------
# sql_and_args_to_text
# ---------------------------------------------------------------------------


class TestSqlAndArgsToText:
    def test_no_args_returns_plain_text_clause(self):
        clause = sa_module.sql_and_args_to_text("SELECT * FROM t")
        assert str(clause) == "SELECT * FROM t"

    def test_percent_s_placeholders_are_bound_by_position(self):
        clause = sa_module.sql_and_args_to_text("SELECT * FROM t WHERE id = %s AND age > %s", (5, 21))
        assert str(clause) == "SELECT * FROM t WHERE id = :arg0 AND age > :arg1"
        assert clause.compile().params == {"arg0": 5, "arg1": 21}

    def test_question_mark_placeholder_keeps_literal_question_mark(self):
        # Pinned pre-existing quirk: the "?" itself is kept as a prefix, so
        # the output is "?:arg0" rather than a clean ":arg0" substitution.
        clause = sa_module.sql_and_args_to_text("SELECT * FROM t WHERE name = ?", ("bob",))
        assert str(clause) == "SELECT * FROM t WHERE name = ?:arg0"
        assert clause.compile().params == {"arg0": "bob"}

    def test_mixed_percent_s_and_question_mark(self):
        clause = sa_module.sql_and_args_to_text("SELECT * FROM t WHERE id = %s AND name = ?", (5, "bob"))
        assert str(clause) == "SELECT * FROM t WHERE id = :arg0 AND name = ?:arg1"
        assert clause.compile().params == {"arg0": 5, "arg1": "bob"}

    def test_tuple_form_input_is_normalized_first(self):
        clause = sa_module.sql_and_args_to_text(("SELECT * FROM t WHERE id = %s", (7,)))
        assert str(clause) == "SELECT * FROM t WHERE id = :arg0"
        assert clause.compile().params == {"arg0": 7}


# ---------------------------------------------------------------------------
# connections
# ---------------------------------------------------------------------------


class TestConnections:
    def test_prefers_src_app_package_import(self, monkeypatch):
        fake_app = mock.Mock()
        fake_app.engines = {"default": mock.Mock(name="engine")}
        _install_module(monkeypatch, "src.app", _fake_module("src.app", app=fake_app))
        assert sa_module.connections() is fake_app.engines

    def test_falls_back_to_top_level_app_module_on_import_error(self, monkeypatch):
        # Force "from src.app import app as flask_app" to raise ImportError.
        _install_module(monkeypatch, "src.app", None)
        fake_app = mock.Mock()
        fake_app.engines = {"default": mock.Mock(name="engine")}
        _install_module(monkeypatch, "app", _fake_module("app", app=fake_app))
        assert sa_module.connections() is fake_app.engines


# ---------------------------------------------------------------------------
# switch_default_database
# ---------------------------------------------------------------------------


class TestSwitchDefaultDatabase:
    def test_is_a_no_op(self):
        assert sa_module.switch_default_database("shard_1") is None


# ---------------------------------------------------------------------------
# get_real_shard_connection_name
# ---------------------------------------------------------------------------


class TestGetRealShardConnectionName:
    def test_non_default_using_is_returned_unchanged(self):
        assert sa_module.get_real_shard_connection_name("shard_3") == "shard_3"

    def test_default_resolves_via_settings_when_configured(self, monkeypatch):
        monkeypatch.setattr(sa_module.settings, "DATABASE_DEFAULT_SHARD", "shard_primary", raising=False)
        assert sa_module.get_real_shard_connection_name("default") == "shard_primary"

    def test_default_falls_back_to_first_connection_when_setting_absent(self, monkeypatch):
        monkeypatch.delattr(sa_module.settings, "DATABASE_DEFAULT_SHARD", raising=False)
        with mock.patch.object(sa_module, "connections", return_value={"shard_a": object(), "shard_b": object()}):
            assert sa_module.get_real_shard_connection_name("default") == "shard_a"

    def test_default_resolves_to_none_when_no_connections_at_all(self, monkeypatch):
        monkeypatch.delattr(sa_module.settings, "DATABASE_DEFAULT_SHARD", raising=False)
        with mock.patch.object(sa_module, "connections", return_value={}):
            assert sa_module.get_real_shard_connection_name("default") is None


# ---------------------------------------------------------------------------
# _dictfetchall
# ---------------------------------------------------------------------------


class TestDictFetchAll:
    def test_zips_column_names_with_row_values(self):
        result_proxy = mock.Mock()
        result_proxy.keys.return_value = ["id", "name"]
        result_proxy.fetchall.return_value = [(1, "a"), (2, "b")]
        assert sa_module._dictfetchall(result_proxy) == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]

    def test_empty_result_set(self):
        result_proxy = mock.Mock()
        result_proxy.keys.return_value = ["id"]
        result_proxy.fetchall.return_value = []
        assert sa_module._dictfetchall(result_proxy) == []


# ---------------------------------------------------------------------------
# db_query
# ---------------------------------------------------------------------------


class TestDbQuery:
    def _session_factory(self, execute_return=None):
        session = mock.Mock(name="session")
        session.execute.return_value = execute_return if execute_return is not None else mock.Mock(name="result_proxy")
        factory = mock.Mock(name="factory", return_value=session)
        return factory, session

    def test_simple_query_returns_fetchall_rows(self, monkeypatch):
        factory, session = self._session_factory()
        session.execute.return_value.fetchall.return_value = [(1,), (2,)]
        _install_scoped_sessions(monkeypatch, {"default": factory})

        result = sa_module.db_query("SELECT * FROM t", using="default", force=True)

        assert result == [(1,), (2,)]
        session.execute.assert_called_once()
        session.execute.return_value.close.assert_called_once()

    def test_as_dict_true_uses_dictfetchall(self, monkeypatch):
        factory, session = self._session_factory()
        session.execute.return_value.keys.return_value = ["id"]
        session.execute.return_value.fetchall.return_value = [(1,), (2,)]
        _install_scoped_sessions(monkeypatch, {"default": factory})

        result = sa_module.db_query("SELECT * FROM t", using="default", force=True, as_dict=True)

        assert result == [{"id": 1}, {"id": 2}]

    def test_query_with_params_binds_values_into_the_clause_itself(self, monkeypatch):
        # NB: sql_and_args_to_text() always returns a single TextClause (with
        # bindparams already attached via .bindparams()), never a
        # (clause, params) tuple -- so db_query's "if params:" branch never
        # actually fires and execute() is always called with just the clause.
        factory, session = self._session_factory()
        session.execute.return_value.fetchall.return_value = []
        _install_scoped_sessions(monkeypatch, {"default": factory})

        sa_module.db_query("SELECT * FROM t WHERE id = %s", (5,), using="default", force=True)

        (clause,), kwargs = session.execute.call_args
        assert kwargs == {}
        assert str(clause) == "SELECT * FROM t WHERE id = :arg0"
        assert clause.compile().params == {"arg0": 5}

    def test_force_false_resolves_shard_via_get_real_shard_connection_name(self, monkeypatch):
        factory, session = self._session_factory()
        session.execute.return_value.fetchall.return_value = []
        _install_scoped_sessions(monkeypatch, {"resolved": factory})

        with mock.patch.object(sa_module, "get_real_shard_connection_name", return_value="resolved") as resolver:
            sa_module.db_query("SELECT 1", using="default", force=False)

        resolver.assert_called_once_with("default")

    def test_unconfigured_connection_falls_back_to_first_available(self, monkeypatch):
        factory, session = self._session_factory()
        session.execute.return_value.fetchall.return_value = []
        _install_scoped_sessions(monkeypatch, {"only_one": factory})

        result = sa_module.db_query("SELECT 1", using="missing", force=True)

        assert result == []
        session.execute.assert_called_once()

    def test_raises_runtime_error_when_no_sessions_configured_at_all(self, monkeypatch):
        _install_scoped_sessions(monkeypatch, {})
        with pytest.raises(RuntimeError, match="No database sessions configured"):
            sa_module.db_query("SELECT 1", using="default", force=True)

    def test_debug_true_does_not_change_result(self, monkeypatch, caplog):
        factory, session = self._session_factory()
        session.execute.return_value.fetchall.return_value = [(1,)]
        _install_scoped_sessions(monkeypatch, {"default": factory})

        import logging

        caplog.set_level(logging.DEBUG)
        result = sa_module.db_query("SELECT 1", using="default", force=True, debug=True)

        assert result == [(1,)]

    def test_tuple_sql_form_is_normalized(self, monkeypatch):
        factory, session = self._session_factory()
        session.execute.return_value.fetchall.return_value = []
        _install_scoped_sessions(monkeypatch, {"default": factory})

        sa_module.db_query(("SELECT * FROM t WHERE id = %s", (9,)), using="default", force=True)

        (clause,), _kwargs = session.execute.call_args
        assert clause.compile().params == {"arg0": 9}

    def test_falls_back_through_the_full_import_chain_to_top_level_app(self, monkeypatch):
        # Force both "from src.database import ScopedSessions" and
        # "from src.app import ScopedSessions" to raise ImportError, so
        # db_query walks the entire try/except/except fallback chain down
        # to "from app import ScopedSessions".
        _install_module(monkeypatch, "src.database", None)
        _install_module(monkeypatch, "src.app", None)
        factory, session = self._session_factory()
        session.execute.return_value.fetchall.return_value = [(1,)]
        _install_module(monkeypatch, "app", _fake_module("app", ScopedSessions={"default": factory}))

        result = sa_module.db_query("SELECT 1", using="default", force=True)

        assert result == [(1,)]

    def test_uses_tuple_return_from_sql_and_args_to_text(self, monkeypatch):
        # sql_and_args_to_text() never actually returns a (clause, params)
        # tuple in practice (see module docstring), but db_query() still
        # branches on isinstance(ret, tuple) -- exercise that branch and the
        # resulting params-truthy execute() call directly.
        factory, session = self._session_factory()
        session.execute.return_value.fetchall.return_value = []
        _install_scoped_sessions(monkeypatch, {"default": factory})
        fake_clause = mock.Mock(name="clause")

        with mock.patch.object(sa_module, "sql_and_args_to_text", return_value=(fake_clause, {"p": 1})):
            sa_module.db_query("SELECT 1", using="default", force=True)

        session.execute.assert_called_once_with(fake_clause, {"p": 1})


# ---------------------------------------------------------------------------
# db_exec
# ---------------------------------------------------------------------------


class TestDbExec:
    def _session_factory(self):
        session = mock.Mock(name="session")
        factory = mock.Mock(name="factory", return_value=session)
        return factory, session

    def test_begin_calls_session_begin(self, monkeypatch):
        factory, session = self._session_factory()
        _install_scoped_sessions(monkeypatch, {"default": factory})

        sa_module.db_exec("BEGIN", using="default", force=True)

        session.begin.assert_called_once()

    def test_begin_swallows_invalid_request_error(self, monkeypatch):
        from sqlalchemy.exc import InvalidRequestError

        factory, session = self._session_factory()
        session.begin.side_effect = InvalidRequestError()
        _install_scoped_sessions(monkeypatch, {"default": factory})

        # Should not raise.
        sa_module.db_exec("begin", using="default", force=True)

    def test_rollback_calls_session_rollback(self, monkeypatch):
        factory, session = self._session_factory()
        _install_scoped_sessions(monkeypatch, {"default": factory})

        sa_module.db_exec("ROLLBACK", using="default", force=True)

        session.rollback.assert_called_once()

    def test_commit_calls_session_commit(self, monkeypatch):
        factory, session = self._session_factory()
        _install_scoped_sessions(monkeypatch, {"default": factory})

        sa_module.db_exec(" commit ;", using="default", force=True)

        session.commit.assert_called_once()

    def test_other_sql_executes_without_params(self, monkeypatch):
        factory, session = self._session_factory()
        _install_scoped_sessions(monkeypatch, {"default": factory})

        sa_module.db_exec('UPDATE "t" SET "x" = 1', using="default", force=True)

        (clause,), kwargs = session.execute.call_args
        assert str(clause) == 'UPDATE "t" SET "x" = 1'
        assert kwargs == {}

    def test_other_sql_executes_with_params(self, monkeypatch):
        factory, session = self._session_factory()
        _install_scoped_sessions(monkeypatch, {"default": factory})

        sa_module.db_exec('UPDATE "t" SET "x" = %s', (5,), using="default", force=True)

        (clause,), _kwargs = session.execute.call_args
        assert clause.compile().params == {"arg0": 5}

    def test_force_false_resolves_shard(self, monkeypatch):
        factory, session = self._session_factory()
        _install_scoped_sessions(monkeypatch, {"resolved": factory})

        with mock.patch.object(sa_module, "get_real_shard_connection_name", return_value="resolved") as resolver:
            sa_module.db_exec("COMMIT", using="default", force=False)

        resolver.assert_called_once_with("default")

    def test_unconfigured_connection_falls_back(self, monkeypatch):
        factory, session = self._session_factory()
        _install_scoped_sessions(monkeypatch, {"only_one": factory})

        sa_module.db_exec("COMMIT", using="missing", force=True)

        session.commit.assert_called_once()

    def test_raises_runtime_error_when_no_sessions_configured(self, monkeypatch):
        _install_scoped_sessions(monkeypatch, {})
        with pytest.raises(RuntimeError, match="No database sessions configured"):
            sa_module.db_exec("COMMIT", using="default", force=True)

    def test_debug_true_still_executes(self, monkeypatch):
        factory, session = self._session_factory()
        _install_scoped_sessions(monkeypatch, {"default": factory})

        sa_module.db_exec("COMMIT", using="default", force=True, debug=True)

        session.commit.assert_called_once()

    def test_falls_back_through_the_full_import_chain_to_top_level_app(self, monkeypatch):
        _install_module(monkeypatch, "src.database", None)
        _install_module(monkeypatch, "src.app", None)
        factory, session = self._session_factory()
        _install_module(monkeypatch, "app", _fake_module("app", ScopedSessions={"default": factory}))

        sa_module.db_exec("COMMIT", using="default", force=True)

        session.commit.assert_called_once()

    def test_uses_tuple_return_from_sql_and_args_to_text(self, monkeypatch):
        factory, session = self._session_factory()
        _install_scoped_sessions(monkeypatch, {"default": factory})
        fake_clause = mock.Mock(name="clause")

        with mock.patch.object(sa_module, "sql_and_args_to_text", return_value=(fake_clause, {"p": 1})):
            sa_module.db_exec('UPDATE "t" SET "x" = 1', using="default", force=True)

        session.execute.assert_called_once_with(fake_clause, {"p": 1})


# ---------------------------------------------------------------------------
# get_psql_connection_string
# ---------------------------------------------------------------------------


class TestGetPsqlConnectionString:
    def _engine_with_url(self, **url_attrs):
        engine = mock.Mock()
        url = mock.Mock()
        for k, v in url_attrs.items():
            setattr(url, k, v)
        engine.url = url
        return engine

    def test_raises_assertion_when_connection_name_unknown(self, monkeypatch):
        monkeypatch.setattr(sa_module.settings, "DATABASE_URLS", ["default"], raising=False)
        with pytest.raises(AssertionError):
            sa_module.get_psql_connection_string("nope")

    def test_secure_true_appends_sslmode_require(self, monkeypatch):
        monkeypatch.setattr(sa_module.settings, "DATABASE_URLS", ["default"], raising=False)
        engine = self._engine_with_url(database="sendhub_db", username="u", password="p", host="h", port="5433")
        with mock.patch.object(sa_module, "connections", return_value={"default": engine}):
            out = sa_module.get_psql_connection_string("default", secure=True)
        assert out == "dbname=sendhub_db user=u password=p host=h port=5433 sslmode=require"

    def test_secure_false_omits_sslmode(self, monkeypatch):
        monkeypatch.setattr(sa_module.settings, "DATABASE_URLS", ["default"], raising=False)
        engine = self._engine_with_url(database="sendhub_db", username="u", password="p", host="h", port="5433")
        with mock.patch.object(sa_module, "connections", return_value={"default": engine}):
            out = sa_module.get_psql_connection_string("default", secure=False)
        assert out == "dbname=sendhub_db user=u password=p host=h port=5433"

    def test_falls_back_to_default_dbname_and_port_when_absent(self, monkeypatch):
        monkeypatch.setattr(sa_module.settings, "DATABASE_URLS", ["default"], raising=False)
        engine = self._engine_with_url(database=None, username="u", password=None, host="h", port=None)
        with mock.patch.object(sa_module, "connections", return_value={"default": engine}):
            out = sa_module.get_psql_connection_string("default", secure=False)
        assert out == "dbname=sendhub user=u password=None host=h port=5432"
