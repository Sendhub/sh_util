"""Unit tests for ``sh_util.db.reflect`` (Postgres metadata reflection).

Driver boundary: every function that touches the database does a local
``from . import db_query`` or ``from . import db_exec`` inside its own body,
resolving to ``sh_util.db``'s already-imported driver functions rather than
an attribute of ``reflect`` itself. Every test patches these via
``mock.patch.object(db_pkg, "db_query"/"db_exec", ...)`` where ``db_pkg`` is
``sh_util.db`` -- the same pattern used in ``test_db_distributed.py``. No
test hits a real database.

Nearly every public function in this module is decorated with
``@memoize`` (``sh_util.functional.memoize``), which caches results forever,
keyed by pickled ``(args, kwargs)``, on the *module-level* function object --
so the cache persists across tests in the same process unless cleared. The
autouse ``_reset_all_memoized_caches`` fixture below clears every memoized
function's ``._cached`` dict before and after each test (mirroring the
``_reset_memoized_cache`` fixture in ``test_db_data.py``), so a mocked
``db_query`` return value from one test can never leak into another via the
cache -- this also gives direct coverage of both the cache-miss path (first
call, real body runs) and the cache-hit path (second call with identical
args returns the cached value without re-invoking the mock).

Known pre-existing quirk in the vendored source pinned by a test below (not
fixed, per task constraints):
  * ``discover_dependencies``'s dedup filter is ``ref[0] not in tables``,
    but ``ref[0]`` is a relation's *column* name (per
    ``referenced_by_tables``'s ``(column, foreignTable, fkColumn)`` return
    shape), not the referencing table name -- so it only filters out a
    relation when its column name happens to collide with one of the
    original table names, not when the referencing table is already known.
"""

from unittest import mock

import pytest
import sh_util.db as db_pkg
import sh_util.db.reflect as reflect_mod


_MEMOIZED_FUNCS = [
    "all_table_names_and_primary_keys",
    "get_primary_key_columns",
    "pl_function_return_type",
    "is_nullable",
    "describe_public",
    "describe",
    "list_tables",
    "find_tables_with_user_id_column",
    "discover_dependencies",
    "all_table_relations",
    "references_tables",
    "referenced_by_tables",
]


@pytest.fixture(autouse=True)
def _reset_all_memoized_caches():
    def _clear():
        for name in _MEMOIZED_FUNCS:
            getattr(reflect_mod, name)._cached.clear()

    _clear()
    yield
    _clear()


# ---------------------------------------------------------------------------
# all_table_names_and_primary_keys / get_primary_key_columns
# ---------------------------------------------------------------------------


class TestAllTableNamesAndPrimaryKeys:
    def test_groups_columns_by_table(self):
        rows = [("t1", "id"), ("t1", "other_id"), ("t2", "pk")]
        with mock.patch.object(db_pkg, "db_query", return_value=rows) as db_query:
            result = reflect_mod.all_table_names_and_primary_keys(using="default")

        assert result == {"t1": ["id", "other_id"], "t2": ["pk"]}
        db_query.assert_called_once()
        assert db_query.call_args.kwargs == {"using": "default"}

    def test_cache_hit_does_not_call_db_query_again(self):
        rows = [("t1", "id")]
        with mock.patch.object(db_pkg, "db_query", return_value=rows) as db_query:
            first = reflect_mod.all_table_names_and_primary_keys(using="default")
            second = reflect_mod.all_table_names_and_primary_keys(using="default")

        assert first == second == {"t1": ["id"]}
        db_query.assert_called_once()


class TestGetPrimaryKeyColumns:
    def test_returns_columns_for_known_table(self):
        with mock.patch.object(db_pkg, "db_query", return_value=[("t1", "id")]):
            assert reflect_mod.get_primary_key_columns("t1", using="default") == ["id"]

    def test_returns_empty_list_for_unknown_table(self):
        with mock.patch.object(db_pkg, "db_query", return_value=[("t1", "id")]):
            assert reflect_mod.get_primary_key_columns("nonexistent_table_xyz", using="default") == []


# ---------------------------------------------------------------------------
# update_primary_key_id
# ---------------------------------------------------------------------------


class TestUpdatePrimaryKeyId:
    def test_raises_when_table_does_not_have_exactly_one_pk_column(self):
        with mock.patch.object(reflect_mod, "get_primary_key_columns", return_value=["a", "b"]):
            with pytest.raises(AssertionError, match="1 primary key"):
                reflect_mod.update_primary_key_id("t", 1, 2, using="default")

    def test_updates_dependent_tables_then_the_table_itself(self):
        deps = {"t": {("fk_col", "child_table", "child_col")}}
        with (
            mock.patch.object(reflect_mod, "get_primary_key_columns", return_value=["id"]),
            mock.patch.object(reflect_mod, "discover_dependencies", return_value=deps),
            mock.patch.object(db_pkg, "db_exec") as db_exec,
        ):
            reflect_mod.update_primary_key_id("t", 5, 9, using="shard_1")

        calls = [c.args[0] for c in db_exec.call_args_list]
        assert calls[0] == "SET CONSTRAINTS ALL DEFERRED"
        assert calls[1] == 'UPDATE "child_table" SET "child_col" = 9 WHERE "child_col" = 5'
        assert calls[2] == 'UPDATE "t" SET "id" = 9 WHERE "id" = 5'
        for c in db_exec.call_args_list:
            assert c.kwargs == {"using": "shard_1"}

    def test_no_dependencies_skips_the_relation_loop(self):
        with (
            mock.patch.object(reflect_mod, "get_primary_key_columns", return_value=["id"]),
            mock.patch.object(reflect_mod, "discover_dependencies", return_value={}),
            mock.patch.object(db_pkg, "db_exec") as db_exec,
        ):
            reflect_mod.update_primary_key_id("t", 5, 9, using="shard_1")

        assert db_exec.call_count == 1
        assert db_exec.call_args.args[0] == 'UPDATE "t" SET "id" = 9 WHERE "id" = 5'


# ---------------------------------------------------------------------------
# pl_function_return_type
# ---------------------------------------------------------------------------


class TestPlFunctionReturnType:
    def test_queries_for_the_function_return_type(self):
        with mock.patch.object(db_pkg, "db_query", return_value=[("integer",)]) as db_query:
            result = reflect_mod.pl_function_return_type("my_func", using="default")

        assert result == [("integer",)]
        sql = db_query.call_args.args[0]
        assert "my_func" in sql
        assert db_query.call_args.kwargs == {"as_dict": False, "using": "default"}


# ---------------------------------------------------------------------------
# is_nullable
# ---------------------------------------------------------------------------


class TestIsNullable:
    def test_true_when_column_is_nullable(self):
        with mock.patch.object(db_pkg, "db_query", return_value=[("YES",)]):
            assert reflect_mod.is_nullable("t1", "col_a", using="default") is True

    def test_false_when_column_is_not_nullable(self):
        with mock.patch.object(db_pkg, "db_query", return_value=[("NO",)]):
            assert reflect_mod.is_nullable("t1", "col_b", using="default") is False

    def test_false_when_column_not_found(self):
        with mock.patch.object(db_pkg, "db_query", return_value=[]):
            assert reflect_mod.is_nullable("t1", "col_missing", using="default") is False

    def test_strips_quotes_from_table_and_apostrophes_from_column(self):
        with mock.patch.object(db_pkg, "db_query", return_value=[]) as db_query:
            reflect_mod.is_nullable('t"1', "col'x", using="default")

        sql = db_query.call_args.args[0]
        assert "'t1'" in sql
        assert "'colx'" in sql


# ---------------------------------------------------------------------------
# describe_public / describe
# ---------------------------------------------------------------------------


class TestDescribePublic:
    def test_groups_columns_by_table(self):
        rows = [("t1", "id", "integer"), ("t1", "name", "text"), ("t2", "id", "integer")]
        with mock.patch.object(db_pkg, "db_query", return_value=rows):
            result = reflect_mod.describe_public(using="default")

        assert result == {
            "t1": [("id", "integer"), ("name", "text")],
            "t2": [("id", "integer")],
        }


class TestDescribe:
    def test_returns_columns_for_known_table(self):
        with mock.patch.object(reflect_mod, "describe_public", return_value={"t1": [("id", "integer")]}):
            assert reflect_mod.describe("t1", using="default") == [("id", "integer")]

    def test_returns_empty_list_for_unknown_table(self):
        with mock.patch.object(reflect_mod, "describe_public", return_value={"t1": [("id", "integer")]}):
            assert reflect_mod.describe("missing_table", using="default") == []


# ---------------------------------------------------------------------------
# list_tables
# ---------------------------------------------------------------------------


class TestListTables:
    def test_returns_flat_list_of_table_names(self):
        with mock.patch.object(db_pkg, "db_query", return_value=[("t1",), ("t2",)]):
            assert reflect_mod.list_tables(using="default") == ["t1", "t2"]


# ---------------------------------------------------------------------------
# find_user_id_column_from_description (pure function, from the docstring)
# ---------------------------------------------------------------------------


class TestFindUserIdColumnFromDescription:
    def test_finds_plain_user_id(self):
        description = (("id",), ("user_id",))
        assert reflect_mod.find_user_id_column_from_description(description) == "user_id"

    def test_finds_user_id_ahead_of_other_columns(self):
        description = (("user_id",), ("someOtherId",))
        assert reflect_mod.find_user_id_column_from_description(description) == "user_id"

    def test_skips_parent_prefixed_column_and_finds_later_match(self):
        description = (("parentUserId",), ("someOtherId",), ("user_id",))
        assert reflect_mod.find_user_id_column_from_description(description) == "user_id"

    def test_finds_camelcase_the_user_id_variant(self):
        description = (("id",), ("theUserId",))
        assert reflect_mod.find_user_id_column_from_description(description) == "theUserId"

    def test_returns_none_when_only_parent_prefixed_candidate_exists(self):
        description = (("parentUserId",), ("id",))
        assert reflect_mod.find_user_id_column_from_description(description) is None

    def test_returns_none_when_only_parent_prefixed_snake_case_candidate_exists(self):
        description = (("id",), ("parent_user_id",))
        assert reflect_mod.find_user_id_column_from_description(description) is None


# ---------------------------------------------------------------------------
# find_tables_with_user_id_column
# ---------------------------------------------------------------------------


class TestFindTablesWithUserIdColumn:
    def test_always_includes_auth_user_and_detected_tables(self):
        with (
            mock.patch.object(reflect_mod, "list_tables", return_value=["main_group", "main_static"]),
            mock.patch.object(
                reflect_mod,
                "describe",
                side_effect=lambda table, using="default": [("user_id", "integer")] if table == "main_group" else [("name", "text")],
            ),
        ):
            result = reflect_mod.find_tables_with_user_id_column(using="default")

        assert result == [("auth_user", "id"), ("main_group", "user_id")]


# ---------------------------------------------------------------------------
# discover_dependencies
# ---------------------------------------------------------------------------


class TestDiscoverDependencies:
    def test_returns_empty_dict_when_no_relations_found(self):
        with mock.patch.object(reflect_mod, "referenced_by_tables", return_value=[]):
            result = reflect_mod.discover_dependencies(["t1"], using="default")
        assert result == {}

    def test_relation_filtered_out_when_column_name_matches_an_original_table_name(self):
        # Pinned pre-existing quirk: the filter is `ref[0] not in tables`,
        # but ref[0] is the relation's *column* name, not the referencing
        # table -- so it only filters out a relation when its column name
        # happens to collide with one of the original table names, not when
        # the referencing table itself is already in `tables`.
        with mock.patch.object(reflect_mod, "referenced_by_tables", return_value=[("t1", "t2", "id")]):
            result = reflect_mod.discover_dependencies(["t1"], using="default")
        assert result == {}

    def test_discovers_and_stabilizes_a_single_external_relation(self):
        # referenced_by_tables always reports the same downstream relation;
        # the recursion should terminate once the discovered set stops
        # growing (second pass finds nothing new).
        with mock.patch.object(reflect_mod, "referenced_by_tables", return_value=[("fk_col", "t2", "id")]) as ref:
            result = reflect_mod.discover_dependencies(["t1"], using="default")

        assert result == {"t1": {("fk_col", "t2", "id")}}
        assert ref.call_count >= 2


# ---------------------------------------------------------------------------
# all_table_relations / references_tables / referenced_by_tables
# ---------------------------------------------------------------------------


class TestAllTableRelations:
    def test_builds_references_and_referenced_by_maps(self):
        rows = [
            ("child_t", "parent_id", "parent_t", "id"),
        ]
        with mock.patch.object(db_pkg, "db_query", return_value=rows):
            references, referenced_by = reflect_mod.all_table_relations(using="default")

        assert references == {"child_t": [("parent_id", "parent_t", "id")]}
        assert referenced_by == {"parent_t": [("id", "child_t", "parent_id")]}


class TestReferencesTables:
    def test_returns_the_tables_a_table_references(self):
        with mock.patch.object(
            reflect_mod,
            "all_table_relations",
            return_value=({"child_t": [("parent_id", "parent_t", "id")]}, {}),
        ):
            assert reflect_mod.references_tables("child_t", using="default") == [("parent_id", "parent_t", "id")]

    def test_returns_empty_list_for_unreferenced_table(self):
        with mock.patch.object(reflect_mod, "all_table_relations", return_value=({}, {})):
            assert reflect_mod.references_tables("lonely_t", using="default") == []


class TestReferencedByTables:
    def test_returns_the_tables_that_reference_a_table(self):
        with mock.patch.object(
            reflect_mod,
            "all_table_relations",
            return_value=({}, {"parent_t": [("id", "child_t", "parent_id")]}),
        ):
            assert reflect_mod.referenced_by_tables("parent_t", using="default") == [("id", "child_t", "parent_id")]

    def test_returns_empty_list_for_unreferenced_table(self):
        with mock.patch.object(reflect_mod, "all_table_relations", return_value=({}, {})):
            assert reflect_mod.referenced_by_tables("lonely_t", using="default") == []
