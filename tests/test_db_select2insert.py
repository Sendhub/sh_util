"""Unit tests for ``sh_util.db.select2insert``.

``select2insert`` is a pure SQL-text transformation with no I/O -- tests
assert on the exact generated SQL string per its docstring contract.
``select2multi_insert`` does ``from . import db_query`` locally (resolving
to ``sh_util.db``'s already-imported ``db_query`` attribute at call time, not
an attribute of ``select2insert`` itself), so it is patched via
``mock.patch.object(db_pkg, "db_query", ...)`` where ``db_pkg`` is the
``sh_util.db`` package module -- the same pattern used in
``test_db_distributed.py``. No test hits a real database or the network.

Known pre-existing quirks in the vendored source pinned by tests below (not
fixed, per task constraints):
  * When ``where_clause`` is supplied without a leading ``"where "``, the
    function prefixes it with ``"WHERE "`` but then concatenates it directly
    onto the table name with no separating space or newline, producing
    ``FROM "t"WHERE id > 5;`` (no space between the closing quote and
    ``WHERE``).
  * The module's own ``select2insert`` doctest example is stale: it asserts
    the old multi-line-wrapped output format, but the current
    implementation returns the same content with only one embedded ``\n``
    (right before the opening paren). Running the module as ``__main__``
    (which calls ``doctest.testmod()``) therefore always reports a failure
    -- ``doctest.testmod()`` never raises for this, it only returns a
    failure count -- so this is exercised purely for line coverage of the
    ``if __name__ == "__main__":`` block, not as a claim that the doctest
    passes.
"""

import runpy
from unittest import mock

import sh_util.db as db_pkg
from sh_util.db import select2insert as select2insert_module


_AUTH_USER_DESCRIPTION = [
    ("id", "bigint"),
    ("username", "character varying(85)"),
    ("first_name", "character varying(120)"),
    ("last_name", "character varying(30)"),
    ("email", "character varying(75)"),
    ("password", "character varying(128)"),
    ("is_staff", "boolean"),
    ("is_active", "boolean"),
    ("is_superuser", "boolean"),
    ("last_login", "timestamp without time zone"),
    ("date_joined", "timestamp without time zone"),
]

_AUTH_USER_EXPECTED_SQL = (
    'SELECT \'INSERT INTO "auth_user" ("id","username","first_name","last_name","email","password","is_staff","is_active","is_superuser","last_login","date_joined") VALUES\n'
    "        (' || quote_nullable(\"id\") || ',' || quote_nullable(\"username\") || ',' || quote_nullable(\"first_name\") || ',' || "
    "quote_nullable(\"last_name\") || ',' || quote_nullable(\"email\") || ',' || quote_nullable(\"password\") || ',' || "
    "quote_nullable(\"is_staff\") || ',' || quote_nullable(\"is_active\") || ',' || quote_nullable(\"is_superuser\") || ',' || "
    "quote_nullable(\"last_login\") || ',' || quote_nullable(\"date_joined\") || ');' FROM \"auth_user\";"
)


# ---------------------------------------------------------------------------
# select2insert
# ---------------------------------------------------------------------------


class TestSelect2Insert:
    def test_matches_the_docstring_contract_example(self):
        assert select2insert_module.select2insert("auth_user", _AUTH_USER_DESCRIPTION) == _AUTH_USER_EXPECTED_SQL

    def test_single_column_no_where_clause(self):
        result = select2insert_module.select2insert("t", [("id", "integer")])
        expected = 'SELECT \'INSERT INTO "t" ("id") VALUES\n        (\' || quote_nullable("id") || \');\' FROM "t";'
        assert result == expected

    def test_where_clause_without_where_prefix_gets_prefixed(self):
        result = select2insert_module.select2insert("t", [("id", "integer")], where_clause="id > 5")
        # Pinned pre-existing quirk: no space is inserted before "WHERE".
        expected = 'SELECT \'INSERT INTO "t" ("id") VALUES\n        (\' || quote_nullable("id") || \');\' FROM "t"WHERE id > 5;'
        assert result == expected

    def test_where_clause_already_prefixed_is_used_verbatim(self):
        result = select2insert_module.select2insert("t", [("id", "integer")], where_clause="WHERE id > 5")
        expected = 'SELECT \'INSERT INTO "t" ("id") VALUES\n        (\' || quote_nullable("id") || \');\' FROM "t"WHERE id > 5;'
        assert result == expected

    def test_where_clause_prefix_check_is_case_insensitive(self):
        result = select2insert_module.select2insert("t", [("id", "integer")], where_clause="where id > 5")
        expected = 'SELECT \'INSERT INTO "t" ("id") VALUES\n        (\' || quote_nullable("id") || \');\' FROM "t"where id > 5;'
        assert result == expected

    def test_none_where_clause_produces_no_where_segment(self):
        result = select2insert_module.select2insert("t", [("a", "int"), ("b", "text")], where_clause=None)
        assert result.endswith('FROM "t";')
        assert "WHERE" not in result


# ---------------------------------------------------------------------------
# select2multi_insert
# ---------------------------------------------------------------------------


class TestSelect2MultiInsert:
    def test_builds_final_insert_sql_from_db_query_rows(self):
        with mock.patch.object(db_pkg, "db_query",return_value=[("(1,'a')",), ("(2,'b')",)]) as db_query:
            result = select2insert_module.select2multi_insert("default", "t", [("id", "int"), ("name", "text")])

        assert result == 'INSERT INTO "t" ("id","name") VALUES (1,'"'"'a'"'"'),(2,'"'"'b'"'"');'
        sql_passed, kwargs = db_query.call_args
        assert kwargs == {"using": "default"}
        assert 'FROM\n        "t";' in sql_passed[0]

    def test_returns_none_when_no_rows_match(self):
        with mock.patch.object(db_pkg, "db_query",return_value=[]):
            result = select2insert_module.select2multi_insert("default", "t", [("id", "int")])

        assert result is None

    def test_where_clause_is_applied_to_the_intermediate_query(self):
        with mock.patch.object(db_pkg, "db_query",return_value=[("(1)",)]) as db_query:
            select2insert_module.select2multi_insert("default", "t", [("id", "int")], where_clause="id = 1")

        sql_passed = db_query.call_args[0][0]
        assert "WHERE id = 1" in sql_passed


# ---------------------------------------------------------------------------
# `if __name__ == "__main__":` block
# ---------------------------------------------------------------------------


class TestDunderMainBlock:
    def test_running_as_a_script_invokes_doctest_without_raising(self, capsys):
        # See the module docstring above re: the stale doctest example --
        # doctest.testmod() reports (but does not raise for) a failure here.
        runpy.run_path(select2insert_module.__file__, run_name="__main__")

        captured = capsys.readouterr()
        assert "1 failure" in captured.out
