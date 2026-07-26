"""Unit tests for ``sh_util.settings``.

Pure constants module: the DB driver name and the tuple of tables treated as
"static" (replicated/shared rather than per-shard).
"""

from sh_util import settings


class TestSettings:
    def test_db_driver_is_sqlalchemy(self):
        assert settings.SH_UTIL_DB_DRIVER == "sqlalchemy"

    def test_static_tables_is_a_tuple(self):
        assert isinstance(settings.STATIC_TABLES, tuple)

    def test_static_tables_contains_expected_entries(self):
        assert "django_site" in settings.STATIC_TABLES
        assert "main_paymentplan" in settings.STATIC_TABLES

    def test_static_tables_are_all_strings(self):
        assert all(isinstance(t, str) for t in settings.STATIC_TABLES)
