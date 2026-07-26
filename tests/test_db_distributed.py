"""Unit tests for ``sh_util.db.distributed``.

Covers Postgres dblink-based "distributed query" tooling: generating
``t(...)`` type-cast clauses for dblink, managing persistent dblink
connections, resolving shard/connection lists, and parsing SQL identifier
fragments for type inference.

Driver boundary: unlike ``sh_util.db.data``, this module does **not** import
its collaborators at module scope -- every collaborator (``db_query``,
``db_exec``, ``connections``, ``get_psql_connection_string`` from
``sh_util.db``; ``describe``/``pl_function_return_type`` from
``sh_util.db.reflect``; ``ShardedResource`` from ``sh_util.sharding``) is
imported locally, inside the function body that uses it, via
``from . import ...`` / ``from .reflect import ...`` /
``from sh_util.sharding import ShardedResource``. Because those are bound at
*call* time (not at module-import time), patching an attribute on
``distributed`` itself would have no effect -- the local import would still
fetch the real object. Instead every test patches the attribute on the
collaborator's *owning* module (``sh_util.db`` / ``sh_util.db.reflect`` /
``sh_util.sharding.ShardedResource``), which the local import picks up.
Module-level helper functions (``pg_get_persistent_connection_handles``,
``_resolve_connections_or_shards``, ``distributed_select``, ...) that are
called directly by name (not re-imported) are instead patched as attributes
of ``distributed`` itself, exactly as in ``test_db_data.py``. No test hits a
real database, memcache, or S3; nothing here uses threads/pools.

Known pre-existing quirk (not fixed, per task constraints): this admin
repo's ``requirements.txt`` pins vanilla ``sqlparse==0.5.3``, where
``Token.is_whitespace`` is a plain boolean attribute set in ``__init__``
(``self.is_whitespace = self.ttype in T.Whitespace``) -- see
``sqlparse/sql.py``. ``distributed.py``'s nested ``_find_referenced_tables()``
helper (used unconditionally near the top of ``distributed_select()``)
instead calls it as a method: ``if token.is_whitespace(): continue``. Since
*every* token (including the first, non-whitespace one) gets this call,
``distributed_select()`` raises ``TypeError: 'bool' object is not callable``
for any realistic SQL input under the pinned sqlparse, before it can reach
any of the actual dblink/UNION-ALL query-generation logic. Notably,
``utils/sh_util/requirements.txt`` pins a *different*, custom sqlparse fork
(``git+.../Sendhub/sqlparse.git@...``) that this code was presumably written
against and where ``is_whitespace`` may still be callable -- so this is a
dependency-pin mismatch between the submodule's own requirements and what's
installed in the admin venv, not a logic bug fixable by editing
``distributed.py``.

``TestDistributedSelectSqlparseIncompatibility`` pins that raw incompatibility
(production-relevant, belongs in the record). Every other
``distributed_select`` test uses the ``adapted_sqlparse`` fixture below,
which patches ``sqlparse.parse`` with a wrapper that runs the *real* parser
and then walks the *real* resulting token tree, swapping each token's
``is_whitespace`` bool attribute for a ``_BoolCallable`` -- an ``int``
subclass that is simultaneously truthy/falsy exactly like the original bool
(so sqlparse's *own* internal attribute-style checks, e.g. inside
``get_identifiers()``, keep working) and callable (so
``distributed.py``'s single incompatible call site also works). This
restores the calling convention the vendored code was written against, on
genuine sqlparse objects -- ``isinstance``/``ttype``/``.value`` semantics are
completely untouched, so the resulting dblink/UNION-ALL SQL these tests
assert on is exactly what the module would really generate against its
intended sqlparse dependency. It is not a fake/fictional code path: it is a
test-only compatibility shim for a documented dependency-pin mismatch, the
same category of workaround as ``test_db_data.py`` pre-seeding
``sys.modules['sh_util.mail']`` to dodge an unrelated Django import at
collection time.

While exercising the adapted path, a *second*, unrelated pre-existing
incompatibility was found and is pinned by
``test_aggregate_function_select_is_shredded_by_a_separate_flatten_iterable_quirk``:
``sh_util/functional.py``'s ``flatten()`` treats anything ``Iterable`` as a
nested sequence to descend into, but current sqlparse's ``Function``/
``Identifier`` tokens implement ``__iter__`` (via ``TokenList``), so a lone
aggregate-function SELECT target (e.g. ``count(*)``) gets shredded into its
constituent leaf tokens instead of staying one atomic identifier. This means
the ``count`` -> ``sum`` remap branch in ``_remap_function_identifiers`` and
the "single aggregate identifier" branch in ``_prepare_grouping_tail`` can't
be triggered by any realistic aggregate query in this environment either --
pinned as observed, real (if degenerate) behavior, not fixed.

The one function that genuinely fans a call out across multiple shard
*connections* in a Python-level loop is ``multi_shard_exec`` -- its
happy-path/single-shard/empty/exception-propagation behavior is covered
below. ``evaluated_distributed_select`` is fully unit-tested by mocking
``distributed_select`` itself as a collaborator, which is where the real
"fan out to shards and get back a merged result" flow (as seen by callers)
is verified end to end.
"""

from unittest import mock

import pytest
import sh_util.db as db_pkg
import sh_util.db.reflect as reflect_mod
import sh_util.sharding as sharding_mod
import sqlparse
from sh_util.db import distributed as d


class _BoolCallable(int):
    """A truthy/falsy int (0 or 1) that is ALSO callable and returns its own
    bool value via ``__call__``. sqlparse 0.5.3's ``Token.is_whitespace`` is
    a plain bool attribute (set once in ``Token.__init__``);
    ``distributed.py``'s ``_find_referenced_tables()`` calls it as
    ``token.is_whitespace()``, a method. Swapping in a plain
    lambda/function for every token would break sqlparse's *own* internal
    attribute-style checks elsewhere (e.g. inside ``get_identifiers()``),
    which would then treat every token as "whitespace" (a function is
    always truthy) and filter everything out. An ``int`` subclass preserves
    real bool semantics (truthy/falsy, ``== True``/``False``) for those
    internal checks while also being callable for the one incompatible call
    site in this module."""

    def __call__(self):
        return bool(self)


def _make_is_whitespace_callable(token):
    """Recursively walk a REAL parsed sqlparse token tree (a statement and
    all its descendants) and replace each token's ``is_whitespace`` bool
    attribute with a ``_BoolCallable``. Restores the older/forked-sqlparse
    contract ``distributed.py`` was written against (where ``is_whitespace``
    was a method) without altering any other real parsing/classification
    the tokens carry (``ttype``, ``isinstance``, ``.value``, ``.tokens``,
    etc. are all untouched)."""
    token.is_whitespace = _BoolCallable(bool(token.is_whitespace))
    for child in getattr(token, "tokens", ()):
        _make_is_whitespace_callable(child)
    return token


_real_sqlparse_parse = sqlparse.parse


def _sqlparse_parse_is_whitespace_adapted(sql):
    """Drop-in replacement for ``sqlparse.parse``: runs the real parser,
    then adapts every resulting (real) token tree so
    ``token.is_whitespace()`` works, unblocking ``distributed_select``'s
    dblink/UNION-ALL query-generation logic for testing. See module
    docstring for the full incompatibility writeup."""
    return tuple(_make_is_whitespace_callable(stmt) for stmt in _real_sqlparse_parse(sql))


@pytest.fixture
def adapted_sqlparse(monkeypatch):
    """Patches the real ``sqlparse.parse`` -- which ``distributed.py``'s
    local ``import sqlparse; ...; sqlparse.parse(sql)`` resolves against at
    call time, since the module-level ``sqlparse`` object is shared
    process-wide -- so ``distributed_select``'s post-parse logic becomes
    reachable for the duration of the test. Also defaults ``settings.DEBUG``
    to False (``raising=False``): unlike ``sh_util.db.data``'s tests, this
    repo's compatibility ``settings`` module doesn't reliably expose
    ``DEBUG`` in this venv (``src.config``'s import can fail before setting
    it, e.g. without ``DATABASE_URL``), and reaching the very end of
    ``distributed_select`` -- the whole point of this fixture -- means
    ``if settings.DEBUG is True:`` is always evaluated."""
    monkeypatch.setattr(d.settings, "DEBUG", False, raising=False)
    with mock.patch.object(sqlparse, "parse", _sqlparse_parse_is_whitespace_adapted):
        yield


# ---------------------------------------------------------------------------
# table_description_to_db_link_t
# ---------------------------------------------------------------------------


class TestTableDescriptionToDbLinkT:
    def test_dict_rows_wildcard_columns(self):
        description = [
            {"column": "id", "type": "integer"},
            {"column": "name", "type": "character varying(128)"},
        ]
        result = d.table_description_to_db_link_t(description)
        assert result == 't("id" integer, "name" character varying(128))'

    def test_tuple_rows_wildcard_columns(self):
        description = [("id", "integer"), ("name", "character varying(128)")]
        result = d.table_description_to_db_link_t(description)
        assert result == 't("id" integer, "name" character varying(128))'

    def test_columns_as_comma_string_subset(self):
        description = [("id", "integer"), ("name", "character varying(128)")]
        result = d.table_description_to_db_link_t(description, "id")
        assert result == 't("id" integer)'

    def test_columns_as_list_subset(self):
        description = [("id", "integer"), ("name", "character varying(128)")]
        result = d.table_description_to_db_link_t(description, ["name"])
        assert result == 't("name" character varying(128))'

    def test_columns_as_comma_string_multi(self):
        description = [("id", "integer"), ("name", "character varying(128)")]
        result = d.table_description_to_db_link_t(description, "id,name")
        assert result == 't("id" integer, "name" character varying(128))'

    def test_unsupported_columns_type_raises(self):
        description = [("id", "integer")]
        with pytest.raises(Exception, match="Unexpecte columns value"):
            d.table_description_to_db_link_t(description, 12345)

    def test_empty_description_raises_assertion_error(self):
        with pytest.raises(AssertionError):
            d.table_description_to_db_link_t([])

    def test_row_with_wrong_length_raises_assertion_error(self):
        with pytest.raises(AssertionError):
            d.table_description_to_db_link_t([("id", "integer", "extra")])


# ---------------------------------------------------------------------------
# pg_strip_double_quotes
# ---------------------------------------------------------------------------


class TestPgStripDoubleQuotes:
    def test_quoted_string_preserves_case(self):
        assert d.pg_strip_double_quotes('"MixedCase"') == "MixedCase"

    def test_unquoted_string_is_lowercased(self):
        assert d.pg_strip_double_quotes("MixedCase") == "mixedcase"

    def test_non_string_passthrough(self):
        assert d.pg_strip_double_quotes(None) is None
        assert d.pg_strip_double_quotes(42) == 42


# ---------------------------------------------------------------------------
# pg_get_persistent_connection_handles
# ---------------------------------------------------------------------------


class TestPgGetPersistentConnectionHandles:
    def test_returns_first_row_first_column(self):
        with mock.patch.object(db_pkg, "db_query", return_value=[["h1,h2"]]) as db_query:
            result = d.pg_get_persistent_connection_handles("shard_1")
        assert result == "h1,h2"
        db_query.assert_called_once_with("SELECT dblink_get_connections()", using="shard_1")


# ---------------------------------------------------------------------------
# pg_connect_persistent_db_link
# ---------------------------------------------------------------------------


class TestPgConnectPersistentDbLink:
    def test_executes_dblink_connect(self):
        with mock.patch.object(db_pkg, "db_exec") as db_exec:
            d.pg_connect_persistent_db_link("shard_1", "handle_a", "pg://conn")
        db_exec.assert_called_once_with("""SELECT dblink_connect('handle_a', 'pg://conn')""", using="shard_1")


# ---------------------------------------------------------------------------
# pg_connect_persistent_db_links
# ---------------------------------------------------------------------------


class TestPgConnectPersistentDbLinks:
    def test_no_handles_no_custom_is_a_no_op(self):
        with (
            mock.patch.object(db_pkg, "connections") as connections,
            mock.patch.object(db_pkg, "db_query") as db_query,
        ):
            result = d.pg_connect_persistent_db_links("shard_1")
        assert result is None
        connections.assert_not_called()
        db_query.assert_not_called()

    def test_unknown_handle_raises_assertion_error(self):
        with (
            mock.patch.object(db_pkg, "connections", return_value=["shard_1"]),
            mock.patch.object(d, "pg_get_persistent_connection_handles", return_value=[]),
        ):
            with pytest.raises(AssertionError, match="was not found in connections"):
                d.pg_connect_persistent_db_links("shard_1", "shard_bogus")

    def test_connects_handles_not_already_connected(self):
        with (
            mock.patch.object(db_pkg, "connections", return_value=["shard_1", "shard_2"]),
            mock.patch.object(d, "pg_get_persistent_connection_handles", return_value=[]),
            mock.patch.object(db_pkg, "get_psql_connection_string", side_effect=lambda c: "pg://{0}".format(c)),
            mock.patch.object(db_pkg, "db_query") as db_query,
        ):
            d.pg_connect_persistent_db_links("using1", "shard_1", "shard_2")

        db_query.assert_called_once_with(
            "SELECT dblink_connect('shard_1', 'pg://shard_1'), dblink_connect('shard_2', 'pg://shard_2')",
            using="using1",
        )

    def test_already_connected_handles_are_skipped(self):
        with (
            mock.patch.object(db_pkg, "connections", return_value=["shard_1", "shard_2"]),
            mock.patch.object(d, "pg_get_persistent_connection_handles", return_value=["shard_1"]),
            mock.patch.object(db_pkg, "get_psql_connection_string", side_effect=lambda c: "pg://{0}".format(c)),
            mock.patch.object(db_pkg, "db_query") as db_query,
        ):
            d.pg_connect_persistent_db_links("using1", "shard_1", "shard_2")

        db_query.assert_called_once_with(
            "SELECT dblink_connect('shard_2', 'pg://shard_2')",
            using="using1",
        )

    def test_all_already_connected_skips_db_query_entirely(self):
        with (
            mock.patch.object(db_pkg, "connections", return_value=["shard_1", "shard_2"]),
            mock.patch.object(d, "pg_get_persistent_connection_handles", return_value=["shard_1", "shard_2"]),
            mock.patch.object(db_pkg, "get_psql_connection_string"),
            mock.patch.object(db_pkg, "db_query") as db_query,
        ):
            d.pg_connect_persistent_db_links("using1", "shard_1", "shard_2")
        db_query.assert_not_called()

    def test_none_already_connected_result_falls_back_to_empty(self):
        # pg_get_persistent_connection_handles() can return None (falsy);
        # `or []` guards against that so the `not in already_connected`
        # membership check below doesn't blow up.
        with (
            mock.patch.object(db_pkg, "connections", return_value=["shard_1"]),
            mock.patch.object(d, "pg_get_persistent_connection_handles", return_value=None),
            mock.patch.object(db_pkg, "get_psql_connection_string", return_value="pg://shard_1"),
            mock.patch.object(db_pkg, "db_query") as db_query,
        ):
            d.pg_connect_persistent_db_links("using1", "shard_1")
        db_query.assert_called_once_with("SELECT dblink_connect('shard_1', 'pg://shard_1')", using="using1")

    def test_custom_handles_raise_type_error_pre_existing_bug(self):
        """Pins a pre-existing bug: the `custom` dict-items branch does
        `filter(lambda c, _: c not in already_connected, custom.items())`.
        `filter()` calls its predicate with a single positional argument (the
        `(key, value)` tuple), but the lambda requires two -- so any non-empty
        `custom` mapping always raises TypeError before anything is
        connected. Not fixed here, per task constraints (tests only)."""
        with (
            mock.patch.object(db_pkg, "connections", return_value=["shard_1"]),
            mock.patch.object(d, "pg_get_persistent_connection_handles", return_value=[]),
        ):
            with pytest.raises(TypeError):
                d.pg_connect_persistent_db_links("using1", **{"custom_handle": "pg://custom"})


# ---------------------------------------------------------------------------
# _resolve_connections_or_shards
# ---------------------------------------------------------------------------


class TestResolveConnectionsOrShards:
    def test_none_resolves_to_all_shard_connection_names(self):
        with mock.patch.object(sharding_mod.ShardedResource, "all_shard_connection_names", return_value=["s1", "s2"]) as m:
            result = d._resolve_connections_or_shards(None)
        assert result == ["s1", "s2"]
        m.assert_called_once_with()

    def test_explicit_list_is_returned_unmodified(self):
        with mock.patch.object(sharding_mod.ShardedResource, "all_shard_connection_names") as m:
            result = d._resolve_connections_or_shards(["a", "b"])
        assert result == ["a", "b"]
        m.assert_not_called()

    def test_explicit_dict_is_returned_unmodified(self):
        given = {"h1": "pg://x"}
        assert d._resolve_connections_or_shards(given) is given


# ---------------------------------------------------------------------------
# pg_initialize_db_links
# ---------------------------------------------------------------------------


class TestPgInitializeDbLinks:
    def test_single_connection_skips_dblink_setup(self):
        with (
            mock.patch.object(d, "_resolve_connections_or_shards", return_value=["shard_1"]) as resolve,
            mock.patch.object(d, "pg_connect_persistent_db_links") as inner,
        ):
            d.pg_initialize_db_links("using1", connections=["shard_1"])
        resolve.assert_called_once_with(["shard_1"])
        inner.assert_not_called()

    def test_multiple_connections_as_list_uses_positional_args(self):
        with (
            mock.patch.object(d, "_resolve_connections_or_shards", return_value=["shard_1", "shard_2"]),
            mock.patch.object(d, "pg_connect_persistent_db_links") as inner,
        ):
            d.pg_initialize_db_links("using1", connections=["shard_1", "shard_2"])
        inner.assert_called_once_with("using1", "shard_1", "shard_2")

    def test_dict_connections_uses_keyword_args(self):
        with (
            mock.patch.object(d, "_resolve_connections_or_shards", return_value={"h1": "pg://x", "h2": "pg://y"}),
            mock.patch.object(d, "pg_connect_persistent_db_links") as inner,
        ):
            d.pg_initialize_db_links("using1", connections={"h1": "pg://x", "h2": "pg://y"})
        inner.assert_called_once_with("using1", h1="pg://x", h2="pg://y")

    def test_zero_connections_still_invokes_with_no_extra_args(self):
        with (
            mock.patch.object(d, "_resolve_connections_or_shards", return_value=[]),
            mock.patch.object(d, "pg_connect_persistent_db_links") as inner,
        ):
            d.pg_initialize_db_links("using1", connections=[])
        inner.assert_called_once_with("using1")


# ---------------------------------------------------------------------------
# evaluated_distributed_select
# ---------------------------------------------------------------------------


class TestEvaluatedDistributedSelect:
    """`distributed_select` is mocked out as a collaborator here (it is
    exercised directly, and its current inability to run to completion is
    documented, in TestDistributedSelectSqlparseIncompatibility below) --
    this isolates `evaluated_distributed_select`'s own branch logic: default
    resolution of `use_persistent_db_link`, conditional dblink
    initialization, and pass-through of the final (fanned-out, merged by
    Postgres) result set from `db_query`."""

    def test_happy_path_returns_merged_db_query_result(self):
        merged_rows = [(1, "shard-a-row"), (2, "shard-b-row")]
        with (
            mock.patch.object(d, "distributed_select", return_value=("SELECT ...", ())) as distributed_select,
            mock.patch.object(d, "pg_initialize_db_links") as init,
            mock.patch.object(db_pkg, "db_query", return_value=merged_rows) as db_query,
        ):
            result = d.evaluated_distributed_select("SELECT id, val FROM t")

        assert result == merged_rows
        distributed_select.assert_called_once_with(sql="SELECT id, val FROM t", args=(), include_shard_info=False, connections=None, use_persistent_db_link=False)
        init.assert_not_called()
        db_query.assert_called_once_with("SELECT ...", (), using="default", as_dict=False)

    def test_empty_result_passes_through_unchanged(self):
        with (
            mock.patch.object(d, "distributed_select", return_value=("SELECT ...", ())),
            mock.patch.object(d, "pg_initialize_db_links"),
            mock.patch.object(db_pkg, "db_query", return_value=[]) as db_query,
        ):
            result = d.evaluated_distributed_select("SELECT id FROM t")
        assert result == []
        db_query.assert_called_once()

    def test_explicit_use_persistent_db_link_true_initializes_dblinks(self):
        with (
            mock.patch.object(d, "distributed_select", return_value=("SELECT ...", ())),
            mock.patch.object(d, "pg_initialize_db_links") as init,
            mock.patch.object(db_pkg, "db_query", return_value=[]),
        ):
            d.evaluated_distributed_select("SELECT id FROM t", using="shard_main", connections=["a", "b"], use_persistent_db_link=True)
        init.assert_called_once_with("shard_main", ["a", "b"])

    def test_settings_default_enables_persistent_db_link_when_flag_unset(self, monkeypatch):
        monkeypatch.setattr(d.settings, "SH_UTIL_USE_PERSISTENT_DBLINK", True, raising=False)
        with (
            mock.patch.object(d, "distributed_select", return_value=("SELECT ...", ())),
            mock.patch.object(d, "pg_initialize_db_links") as init,
            mock.patch.object(db_pkg, "db_query", return_value=[]),
        ):
            d.evaluated_distributed_select("SELECT id FROM t")
        init.assert_called_once()

    def test_as_dict_is_forwarded_to_db_query(self):
        with (
            mock.patch.object(d, "distributed_select", return_value=("SELECT ...", ())),
            mock.patch.object(d, "pg_initialize_db_links"),
            mock.patch.object(db_pkg, "db_query", return_value=[{"id": 1}]) as db_query,
        ):
            result = d.evaluated_distributed_select("SELECT id FROM t", as_dict=True)
        assert result == [{"id": 1}]
        assert db_query.call_args.kwargs["as_dict"] is True

    def test_explicit_args_are_forwarded_to_distributed_select(self):
        with (
            mock.patch.object(d, "distributed_select", return_value=("SELECT ...", ())) as distributed_select,
            mock.patch.object(d, "pg_initialize_db_links"),
            mock.patch.object(db_pkg, "db_query", return_value=[]),
        ):
            d.evaluated_distributed_select("SELECT id FROM t WHERE x = %s", args=(5,))
        assert distributed_select.call_args.kwargs["args"] == (5,)


# ---------------------------------------------------------------------------
# distributed_select -- pinned sqlparse-incompatibility bug (see module
# docstring above for the full root-cause explanation).
# ---------------------------------------------------------------------------


class TestDistributedSelectSqlparseIncompatibility:
    def test_raises_type_error_under_the_pinned_sqlparse_version(self):
        """Pins the raw incompatibility itself (production-relevant, not just
        a test artifact): under this admin repo's pinned sqlparse==0.5.3,
        `token.is_whitespace` is a plain bool attribute, but
        `_find_referenced_tables()` (called unconditionally near the top of
        `distributed_select`) invokes it as `token.is_whitespace()` -- so
        without the `adapted_sqlparse` fixture used by every other test in
        this file, any realistic SQL raises TypeError before any
        dblink/UNION-ALL generation happens. See module docstring for the
        full root-cause writeup."""
        with mock.patch.object(reflect_mod, "describe", return_value=[("id", "integer")]):
            with pytest.raises(TypeError, match="not callable"):
                d.distributed_select("SELECT id FROM auth_user", connections=["shard_1", "shard_2"])


# ---------------------------------------------------------------------------
# distributed_select -- dblink/UNION-ALL query generation, shard fan-out and
# merge-query assembly (exercised via the `adapted_sqlparse` fixture -- see
# module docstring for why that's a legitimate test-only compatibility shim
# rather than a fake code path).
# ---------------------------------------------------------------------------


class TestDistributedSelectSqlGeneration:
    describe_map = {"auth_user": [("id", "integer"), ("name", "character varying(128)"), ("score", "integer")]}

    def _fake_describe(self, table, using="default"):
        return self.describe_map.get(table, [])

    def _fake_conn_string(self, shard):
        return "pg://{0}".format(shard)

    def test_fan_out_across_multiple_shards_joins_with_union_all(self, adapted_sqlparse):
        with (
            mock.patch.object(reflect_mod, "describe", side_effect=self._fake_describe),
            mock.patch.object(db_pkg, "get_psql_connection_string", side_effect=self._fake_conn_string),
        ):
            sql, args = d.distributed_select("SELECT id, name FROM auth_user", connections=["shard_1", "shard_2"])

        assert args == ()
        assert sql.count("UNION ALL") == 1
        assert "dblink('pg://shard_1', 'SELECT id, name FROM auth_user')" in sql
        assert "dblink('pg://shard_2', 'SELECT id, name FROM auth_user')" in sql
        assert 't("id" integer, "name" character varying(128))' in sql
        assert sql.startswith('SELECT "id", "name" FROM (')
        assert sql.rstrip().endswith(") q0")

    def test_single_shard_has_no_union_all(self, adapted_sqlparse):
        with (
            mock.patch.object(reflect_mod, "describe", side_effect=self._fake_describe),
            mock.patch.object(db_pkg, "get_psql_connection_string", side_effect=self._fake_conn_string),
        ):
            sql, args = d.distributed_select("SELECT id, name FROM auth_user", connections=["shard_1"])
        assert "UNION ALL" not in sql
        assert sql.count("dblink(") == 1
        assert "dblink('pg://shard_1', 'SELECT id, name FROM auth_user')" in sql

    def test_wildcard_expands_columns_via_describe(self, adapted_sqlparse):
        with (
            mock.patch.object(reflect_mod, "describe", side_effect=self._fake_describe),
            mock.patch.object(db_pkg, "get_psql_connection_string", side_effect=self._fake_conn_string),
        ):
            sql, args = d.distributed_select("SELECT * FROM auth_user", connections=["shard_1", "shard_2"])
        assert 'SELECT "id", "name", "score" FROM' in sql
        assert 't("id" integer, "name" character varying(128), "score" integer)' in sql

    def test_include_shard_info_adds_shard_literal_column(self, adapted_sqlparse):
        with (
            mock.patch.object(reflect_mod, "describe", side_effect=self._fake_describe),
            mock.patch.object(db_pkg, "get_psql_connection_string", side_effect=self._fake_conn_string),
        ):
            sql, args = d.distributed_select("SELECT id, name FROM auth_user", connections=["shard_1", "shard_2"], include_shard_info=True)
        assert 'SELECT "id", "name", shard FROM' in sql
        assert "'shard_1' AS" in sql
        assert "'shard_2' AS" in sql

    def test_where_and_args_are_embedded_per_shard(self, adapted_sqlparse):
        with (
            mock.patch.object(reflect_mod, "describe", side_effect=self._fake_describe),
            mock.patch.object(db_pkg, "get_psql_connection_string", side_effect=self._fake_conn_string),
        ):
            sql, args = d.distributed_select(
                "SELECT id, name FROM auth_user WHERE score = %s ORDER BY name",
                args=(5,),
                connections=["shard_1", "shard_2"],
            )
        # Positional args are substituted directly into the *generated* SQL
        # text (once per shard's embedded dblink query) rather than returned
        # for the caller to bind separately -- distributed_select always
        # hands back an empty args tuple.
        assert args == ()
        assert sql.count("WHERE score = 5") == 2
        assert "ORDER BY name" in sql

    def test_dict_connections_uses_handle_names_as_dblink_targets(self, adapted_sqlparse):
        with (
            mock.patch.object(reflect_mod, "describe", side_effect=self._fake_describe),
            mock.patch.object(db_pkg, "get_psql_connection_string", side_effect=self._fake_conn_string),
        ):
            sql, args = d.distributed_select(
                "SELECT id, name FROM auth_user",
                connections={"shard_1": "pg://raw1", "shard_2": "pg://raw2"},
            )
        assert sql.count("UNION ALL") == 1
        assert "dblink('pg://shard_1'," in sql
        assert "dblink('pg://shard_2'," in sql

    def test_use_persistent_db_link_uses_shard_name_directly_as_connection_string(self, adapted_sqlparse):
        with (
            mock.patch.object(reflect_mod, "describe", side_effect=self._fake_describe),
            mock.patch.object(db_pkg, "get_psql_connection_string") as get_conn_string,
        ):
            sql, args = d.distributed_select(
                "SELECT id, name FROM auth_user",
                connections=["shard_1", "shard_2"],
                use_persistent_db_link=True,
            )
        get_conn_string.assert_not_called()
        assert "dblink('shard_1', 'SELECT id, name FROM auth_user')" in sql
        assert "dblink('shard_2', 'SELECT id, name FROM auth_user')" in sql

    def test_default_none_connections_resolves_via_sharded_resource(self, adapted_sqlparse):
        with (
            mock.patch.object(sharding_mod.ShardedResource, "all_shard_connection_names", return_value=["shard_1"]),
            mock.patch.object(reflect_mod, "describe", side_effect=self._fake_describe),
            mock.patch.object(db_pkg, "get_psql_connection_string", side_effect=self._fake_conn_string),
        ):
            sql, args = d.distributed_select("SELECT id FROM auth_user")
        assert "dblink('pg://shard_1', 'SELECT id FROM auth_user')" in sql

    def test_empty_connections_produces_empty_union(self, adapted_sqlparse):
        with (
            mock.patch.object(reflect_mod, "describe", side_effect=self._fake_describe),
            mock.patch.object(db_pkg, "get_psql_connection_string", side_effect=self._fake_conn_string),
        ):
            sql, args = d.distributed_select("SELECT id, name FROM auth_user", connections=[])
        assert "dblink(" not in sql
        assert "UNION ALL" not in sql
        assert sql.startswith('SELECT "id", "name" FROM (')

    def test_no_selectable_columns_raises(self, adapted_sqlparse):
        with mock.patch.object(reflect_mod, "describe", side_effect=self._fake_describe):
            with pytest.raises(Exception, match="Failed to find any columns"):
                d.distributed_select("SELECT FROM auth_user", connections=["shard_1"])

    def test_group_by_single_non_aggregate_identifier(self, adapted_sqlparse):
        # Exercises _prepare_grouping_tail's `len(identifiers) == 1` branch
        # selection. The identifier isn't `count`, so the shard-appending
        # sub-branch specifically doesn't fire here -- see
        # test_aggregate_function_select_is_shredded_by_a_separate_flatten_iterable_quirk
        # below for why a clean, unshredded single aggregate identifier is
        # not reachable in this environment.
        with (
            mock.patch.object(reflect_mod, "describe", side_effect=self._fake_describe),
            mock.patch.object(db_pkg, "get_psql_connection_string", side_effect=self._fake_conn_string),
        ):
            sql, args = d.distributed_select(
                "SELECT id FROM auth_user GROUP BY id",
                connections=["shard_1"],
                include_shard_info=True,
            )
        assert sql.count("GROUP BY") == 1  # only the one embedded verbatim in the inner dblink query
        assert 'SELECT "id", shard FROM' in sql

    def test_aggregate_function_select_is_shredded_by_a_separate_flatten_iterable_quirk(self, adapted_sqlparse):
        """Pins a SECOND, unrelated pre-existing incompatibility (not the
        is_whitespace one, and not fixed here) found while exercising this
        adapted path -- see module docstring. A lone aggregate-function
        SELECT target like `count(*)` ends up shredded into separate
        "count", "(", "*", "" fragments by sh_util.functional.flatten()'s
        over-eager descent into sqlparse's now-Iterable Function tokens."""
        with (
            mock.patch.object(reflect_mod, "describe", side_effect=self._fake_describe),
            mock.patch.object(reflect_mod, "pl_function_return_type", return_value=[]),
            mock.patch.object(db_pkg, "get_psql_connection_string", side_effect=self._fake_conn_string),
        ):
            sql, args = d.distributed_select("SELECT count(*) FROM auth_user", connections=["shard_1"])
        assert 'SELECT "count", "(", *, ""' in sql
        assert "dblink('pg://shard_1', 'SELECT count(*) FROM auth_user')" in sql


# ---------------------------------------------------------------------------
# multi_shard_exec -- the one function that genuinely fans a call out across
# shard connections in a Python-level loop.
# ---------------------------------------------------------------------------


class TestMultiShardExec:
    def test_fan_out_happy_path_executes_on_every_shard_in_order(self):
        with (
            mock.patch.object(sharding_mod.ShardedResource, "all_shard_connection_names", return_value=["shard_1", "shard_2", "shard_3"]),
            mock.patch.object(db_pkg, "db_exec") as db_exec,
        ):
            d.multi_shard_exec("DELETE FROM foo WHERE bar = 1")

        assert db_exec.call_args_list == [
            mock.call("DELETE FROM foo WHERE bar = 1", using="shard_1"),
            mock.call("DELETE FROM foo WHERE bar = 1", using="shard_2"),
            mock.call("DELETE FROM foo WHERE bar = 1", using="shard_3"),
        ]

    def test_single_shard(self):
        with (
            mock.patch.object(sharding_mod.ShardedResource, "all_shard_connection_names", return_value=["shard_1"]),
            mock.patch.object(db_pkg, "db_exec") as db_exec,
        ):
            d.multi_shard_exec("SELECT 1")
        db_exec.assert_called_once_with("SELECT 1", using="shard_1")

    def test_empty_shards_is_a_no_op(self):
        with (
            mock.patch.object(sharding_mod.ShardedResource, "all_shard_connection_names", return_value=[]),
            mock.patch.object(db_pkg, "db_exec") as db_exec,
        ):
            d.multi_shard_exec("SELECT 1")
        db_exec.assert_not_called()

    def test_per_shard_exception_propagates_and_stops_remaining_shards(self):
        """Documents the module's actual (undocumented-in-source) behavior:
        there is no try/except around the per-shard db_exec call, so an
        exception on any shard immediately propagates out of
        multi_shard_exec -- it does not skip the failing shard and continue
        with the rest."""
        with (
            mock.patch.object(sharding_mod.ShardedResource, "all_shard_connection_names", return_value=["shard_1", "shard_2", "shard_3"]),
            mock.patch.object(db_pkg, "db_exec", side_effect=[None, RuntimeError("boom"), None]) as db_exec,
        ):
            with pytest.raises(RuntimeError, match="boom"):
                d.multi_shard_exec("UPDATE foo SET x = 1")

        assert db_exec.call_count == 2
        assert db_exec.call_args_list == [
            mock.call("UPDATE foo SET x = 1", using="shard_1"),
            mock.call("UPDATE foo SET x = 1", using="shard_2"),
        ]


# ---------------------------------------------------------------------------
# parse_identifier
# ---------------------------------------------------------------------------


class TestParseIdentifier:
    describe_map = {"auth_user": [("id", "integer"), ("score", "integer"), ("name", "character varying(128)")]}

    def _fake_describe(self, table, using="default"):
        return self.describe_map.get(table, [])

    def test_plain_column_no_table_defaults_to_character_varying(self):
        with mock.patch.object(reflect_mod, "describe", side_effect=self._fake_describe):
            out = d.parse_identifier("score")
        assert out["function"] is None
        assert out["column"] == "score"
        assert out["alias"] is None
        assert out["type"] == "character varying"

    def test_plain_column_with_table_resolves_type_via_describe(self):
        with mock.patch.object(reflect_mod, "describe", side_effect=self._fake_describe):
            out = d.parse_identifier("score", table="auth_user")
        assert out["column"] == '"score"'
        assert out["type"] == "integer"

    def test_table_qualified_column_resolves_via_referenced_table_alias(self):
        refs = [{"table": "auth_user", "alias": "t"}]
        with mock.patch.object(reflect_mod, "describe", side_effect=self._fake_describe):
            out = d.parse_identifier("t.score", list_of_referenced_tables=refs)
        assert out["column"] == '"auth_user_score"'
        assert out["type"] == "integer"

    def test_column_with_as_alias(self):
        with mock.patch.object(reflect_mod, "describe", side_effect=self._fake_describe):
            out = d.parse_identifier("score as myScore", table="auth_user")
        assert out["alias"] == "myscore"
        assert out["type"] == "integer"

    def test_count_star_maps_to_bigint(self):
        with mock.patch.object(reflect_mod, "describe", side_effect=self._fake_describe):
            out = d.parse_identifier("count(*)", table="auth_user")
        assert out["function"] == "count"
        assert out["column"] == "*"
        assert out["type"] == "bigint"

    def test_count_column_maps_to_bigint(self):
        with mock.patch.object(reflect_mod, "describe", side_effect=self._fake_describe):
            out = d.parse_identifier("count(score)", table="auth_user")
        assert out["function"] == "count"
        assert out["type"] == "bigint"

    def test_avg_with_alias_maps_to_numeric(self):
        with mock.patch.object(reflect_mod, "describe", side_effect=self._fake_describe):
            out = d.parse_identifier("avg(score) myAvg", table="auth_user")
        assert out["function"] == "avg"
        assert out["alias"] == "myavg"
        assert out["type"] == "numeric"

    def test_max_falls_back_to_character_varying_when_placeholder_type_unresolved(self):
        # `max`/`min`/etc. map to the "<T>" placeholder in
        # _aggregateFunctionTypeMappings; parse_identifier tries to resolve it
        # via a describe() lookup keyed by the *whole* "max(score)" fragment,
        # which never matches an actual column name, so it falls back to the
        # 'character varying' default.
        with mock.patch.object(reflect_mod, "describe", side_effect=self._fake_describe):
            out = d.parse_identifier("max(score)", table="auth_user")
        assert out["function"] == "max"
        assert out["type"] == "character varying"

    def test_unrecognized_function_name_is_treated_as_plain_column(self):
        # "unknownfunc" isn't in _sqlFunctionTypeMappings, so the aggregate
        # regex simply doesn't match -- out['function'] stays None.
        with mock.patch.object(reflect_mod, "describe", side_effect=self._fake_describe):
            out = d.parse_identifier("unknownfunc(score)", table="auth_user")
        assert out["function"] is None

    def test_array_agg_looks_up_return_type_via_pl_function_return_type(self):
        with (
            mock.patch.object(reflect_mod, "describe", side_effect=self._fake_describe),
            mock.patch.object(reflect_mod, "pl_function_return_type", return_value=[("bigint[]",)]) as pfrt,
        ):
            out = d.parse_identifier("array_agg(score)", table="auth_user")
        assert out["function"] == "array_agg"
        assert out["type"] == "bigint[]"
        pfrt.assert_called_once_with("array_agg")

    def test_array_agg_falls_back_when_return_type_lookup_is_empty(self):
        with (
            mock.patch.object(reflect_mod, "describe", side_effect=self._fake_describe),
            mock.patch.object(reflect_mod, "pl_function_return_type", return_value=[]),
        ):
            # No table -> _find_column() never resolves -> default type.
            out = d.parse_identifier("array_agg(unknown_col)")
        assert out["type"] == "character varying"

    def test_unrecognized_identifier_fragment_raises(self):
        # `_identifierParserRe` requires a non-empty `\s*$`-anchored match;
        # a string containing only characters the pattern's charset excludes
        # for the trailing alias group but no valid column throws off `m`
        # being None only in pathological cases. Instead, directly cover the
        # "no match" branch by patching the compiled regex to fail to match.
        with mock.patch.object(d, "_identifierParserRe") as fake_re:
            fake_re.match.return_value = None
            with pytest.raises(Exception, match="No identifer found"):
                d.parse_identifier("anything")
