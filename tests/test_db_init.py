"""Unit tests for ``sh_util.db`` (package ``__init__``): driver dispatch.

``sh_util.db.__init__`` picks its active driver module once, at import time,
based on ``settings.SH_UTIL_DB_DRIVER`` (``"django"`` -> ``drivers.dj``,
``"sqlalchemy"``/``"sa"`` -> ``drivers.sa``, anything else -> raises). Since
that decision only happens at import time, exercising both branches (plus
the invalid-value error branch) requires ``monkeypatch.setattr`` on
``settings.SH_UTIL_DB_DRIVER`` followed by ``importlib.reload(db_pkg)``.

The test runner (``scripts/run_vendored_submodule_tests.py``) sets
``settings.SH_UTIL_DB_DRIVER = "sqlalchemy"`` before collection, so
``sh_util.db`` is already loaded with the sqlalchemy driver when this module
runs, and every other sh_util test module (this file's siblings included)
assumes that state -- e.g. that ``sh_util.db.db_query`` is
``drivers.sa.db_query``. The autouse ``_restore_sqlalchemy_driver`` fixture
below force-sets ``settings.SH_UTIL_DB_DRIVER`` back to ``"sqlalchemy"`` and
reloads the package after every test in this file (even on failure/error),
so no other test collected in the same session is affected by state left
behind here. django itself is never imported: ``drivers.dj`` only touches
Django inside local function bodies, not at module scope, so reloading with
the "django" driver value succeeds without needing a django stand-in.
"""

import importlib
from unittest import mock

import pytest
import sh_util.db as db_pkg


@pytest.fixture(autouse=True)
def _restore_sqlalchemy_driver():
    yield
    db_pkg.settings.SH_UTIL_DB_DRIVER = "sqlalchemy"
    importlib.reload(db_pkg)


class TestDriverDispatch:
    def test_sqlalchemy_driver_value_dispatches_to_sa(self, monkeypatch):
        monkeypatch.setattr(db_pkg.settings, "SH_UTIL_DB_DRIVER", "sqlalchemy", raising=False)
        importlib.reload(db_pkg)
        assert db_pkg.connections.__module__ == "sh_util.db.drivers.sa"

    def test_sa_alias_also_dispatches_to_sa(self, monkeypatch):
        monkeypatch.setattr(db_pkg.settings, "SH_UTIL_DB_DRIVER", "sa", raising=False)
        importlib.reload(db_pkg)
        assert db_pkg.connections.__module__ == "sh_util.db.drivers.sa"

    def test_driver_value_is_case_insensitive_for_sqlalchemy(self, monkeypatch):
        monkeypatch.setattr(db_pkg.settings, "SH_UTIL_DB_DRIVER", "SqlAlchemy", raising=False)
        importlib.reload(db_pkg)
        assert db_pkg.connections.__module__ == "sh_util.db.drivers.sa"

    def test_django_driver_value_dispatches_to_dj(self, monkeypatch):
        monkeypatch.setattr(db_pkg.settings, "SH_UTIL_DB_DRIVER", "django", raising=False)
        importlib.reload(db_pkg)
        assert db_pkg.connections.__module__ == "sh_util.db.drivers.dj"

    def test_driver_value_is_case_insensitive_for_django(self, monkeypatch):
        monkeypatch.setattr(db_pkg.settings, "SH_UTIL_DB_DRIVER", "Django", raising=False)
        importlib.reload(db_pkg)
        assert db_pkg.connections.__module__ == "sh_util.db.drivers.dj"

    def test_unrecognized_driver_value_raises(self, monkeypatch):
        monkeypatch.setattr(db_pkg.settings, "SH_UTIL_DB_DRIVER", "mongodb", raising=False)
        with pytest.raises(Exception, match="Unrecognized sh_util db driver: mongodb"):
            importlib.reload(db_pkg)

    def test_reload_after_a_bad_value_recovers_cleanly(self, monkeypatch):
        # An invalid value raises during reload, leaving the previously
        # successful driver bindings in place (the raise happens before any
        # `from .drivers.X import *`); reloading again with a good value
        # afterwards must dispatch normally.
        monkeypatch.setattr(db_pkg.settings, "SH_UTIL_DB_DRIVER", "not-a-real-driver", raising=False)
        with pytest.raises(Exception):
            importlib.reload(db_pkg)

        monkeypatch.setattr(db_pkg.settings, "SH_UTIL_DB_DRIVER", "django", raising=False)
        importlib.reload(db_pkg)
        assert db_pkg.connections.__module__ == "sh_util.db.drivers.dj"


class TestTransactionHelpers:
    def test_begin_calls_db_exec_with_begin(self):
        with mock.patch.object(db_pkg, "db_exec") as db_exec:
            db_pkg.begin("shard_1")
        db_exec.assert_called_once_with("BEGIN", using="shard_1")

    def test_commit_calls_db_exec_with_commit(self):
        with mock.patch.object(db_pkg, "db_exec") as db_exec:
            db_pkg.commit("shard_1")
        db_exec.assert_called_once_with("COMMIT", using="shard_1")

    def test_rollback_calls_db_exec_with_rollback(self):
        with mock.patch.object(db_pkg, "db_exec") as db_exec:
            db_pkg.rollback("shard_1")
        db_exec.assert_called_once_with("ROLLBACK", using="shard_1")
