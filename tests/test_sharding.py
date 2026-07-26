"""Unit tests for ``sh_util.sharding``.

Covers shard-name/id math, the ``ShardedConnection`` context manager, the
``ShardEvent`` pub/sub singleton, the module-level shard-list cache
(``_get_shards``), and ``ShardedResource``'s cache-backed shard-lookup and
shard-warming machinery, plus ``ShardedAuthenticationMiddleware`` and the
"movedUser" event subscriber wired up at import time.

Driver boundary: ``sharding.py`` does ``from .db import connections`` and
``from .memcache import get_memcache_client`` at module scope, so those are
bound names in ``sharding``'s own namespace -- every test patches them as
attributes of ``sharding`` itself (``monkeypatch.setattr(sharding, "connections",
...)`` / ``monkeypatch.setattr(sharding, "get_memcache_client", ...)``),
exactly as ``test_db_data.py`` patches ``data_module``'s bound collaborator
names. Two collaborators are instead imported locally, inside the function
body that uses them, so patching ``sharding`` itself would have no effect:
``ShardedConnection.__enter__``/``__exit__`` and
``ShardedAuthenticationMiddleware.process_request`` both do
``from .db import switch_default_database``, patched here on
``sh_util.db`` (as ``db_pkg``); ``_real_user_id_to_physical_shard_id`` does
``from sh_util.db import db_query``, patched the same way.

``ShardedResource.set_shard_id`` is decorated ``@retry(tries=3)`` *above*
``@staticmethod`` (i.e. ``retry()`` wraps the ``staticmethod`` descriptor
itself, not a plain function). This still works when called via the class
(``ShardedResource.set_shard_id(...)``) because Python's ``staticmethod``
objects are directly callable, and ``retry_fn`` -- a plain function -- reads
back unbound through class-attribute access. Its failure path exhausts all 3
retries with real ``time.sleep`` calls by default; ``sh_util.retry``'s
module-level ``_time.sleep`` is monkeypatched to a no-op for that one test so
it runs instantly instead of ~21s.

``_subscribe_to_shard_events``'s inner ``moved_user`` closure and
``ShardedAuthenticationMiddleware.process_request`` both do local
``from django...`` (and, for ``moved_user``, ``from main.models import ...``)
imports. Neither Django nor a ``main`` app is installed in this environment,
so those tests pre-seed ``sys.modules`` with stand-in ``types.ModuleType``
objects carrying just the attributes each import statement needs (``User``,
``SESSION_KEY``, ``get_user``, ``SimpleLazyObject``, ``ExtendedUser``,
``PhoneNumber``) -- the same "seed sys.modules directly" technique
``test_db_data.py`` uses for ``sh_util.mail`` to dodge an unrelated
module-scope Django import. No test hits a real database, memcache, or S3.

Known pre-existing bugs in the vendored source pinned by tests below (not
fixed, per task constraints):
  * ``ShardEvent.unsubscribe`` does
    ``self.subscribers[event].remove(self.subscribers[event].index(fn))``.
    ``list.index(fn)`` returns an integer *position*, but ``list.remove(x)``
    searches by *value*, not position -- so this removes whatever list
    element equals that integer (almost never true for a list of
    callables), which raises ``ValueError``, silently swallowed by the bare
    ``except ValueError: pass``. In practice ``unsubscribe`` therefore never
    actually removes the subscriber. ``TestShardEventUnsubscribeBug`` pins
    this.
  * ``ShardEvent`` subclasses ``Singleton``, whose ``__new__`` correctly
    returns the same cached instance on every call -- but Python still runs
    ``__init__`` on the returned instance regardless of whether it was just
    created or reused, and ``ShardEvent.__init__`` unconditionally does
    ``self.subscribers = {}``. So *any* fresh ``ShardEvent()`` call anywhere
    (not just the first) silently wipes every previously registered
    subscriber for every event, including "movedUser" (subscribed once at
    ``sharding`` import time). Every test in ``TestShardEventPublishSubscribe``
    /``TestShardEventUnsubscribeBug`` therefore grabs exactly one
    ``sharding.ShardEvent()`` reference per test and reuses it, never
    re-instantiating mid-test; ``test_reinstantiating_shard_event_resets_all_subscribers_bug``
    pins the reset behavior directly, and the "movedUser" test below holds a
    single reference across its own ``_subscribe_to_shard_events()`` call
    for the same reason.
"""

import sys
import types
from unittest import mock

import pylibmc
import pytest
import sh_util.db as db_pkg
import sh_util.retry as retry_mod
from sh_util import sharding


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _settings_defaults(monkeypatch):
    """``NUM_LOGICAL_SHARDS`` is defined in ``src/config.py`` (re-exported via
    the ``settings`` compatibility shim) as 4096; pin it explicitly here so
    tests are independent of that value ever changing."""
    monkeypatch.setattr(sharding.settings, "NUM_LOGICAL_SHARDS", 4096, raising=False)


@pytest.fixture(autouse=True)
def _reset_shards_cache():
    """``_get_shards()`` lazily memoizes into the module-level ``_shards``
    global; reset it before and after every test so a mocked ``connections``
    return value (or cached list) from one test can't leak into another."""
    sharding._shards = None
    yield
    sharding._shards = None


# ---------------------------------------------------------------------------
# coerce_id_to_shard_name
# ---------------------------------------------------------------------------


class TestCoerceIdToShardName:
    def test_int_zero_becomes_shard_zero(self):
        assert sharding.coerce_id_to_shard_name(0) == "shard_0"

    def test_positive_int_becomes_shard_name(self):
        assert sharding.coerce_id_to_shard_name(7) == "shard_7"

    def test_large_int_becomes_shard_name(self):
        assert sharding.coerce_id_to_shard_name(999999999) == "shard_999999999"

    def test_digit_string_becomes_shard_name(self):
        assert sharding.coerce_id_to_shard_name("3") == "shard_3"

    def test_negative_int_is_returned_unchanged(self):
        # str(-1) == "-1", and "-1".isdigit() is False (the minus sign isn't
        # a digit), so the negative-id branch falls through untouched.
        assert sharding.coerce_id_to_shard_name(-1) == -1

    def test_non_digit_string_is_returned_unchanged(self):
        assert sharding.coerce_id_to_shard_name("shard_5") == "shard_5"

    def test_empty_string_is_returned_unchanged(self):
        assert sharding.coerce_id_to_shard_name("") == ""


# ---------------------------------------------------------------------------
# user_id_to_logical_shard_id
# ---------------------------------------------------------------------------


class TestUserIdToLogicalShardId:
    def test_zero(self):
        assert sharding.user_id_to_logical_shard_id(0) == 0

    def test_below_shard_count_boundary(self):
        assert sharding.user_id_to_logical_shard_id(4095) == 4095

    def test_exact_shard_count_wraps_to_zero(self):
        assert sharding.user_id_to_logical_shard_id(4096) == 0

    def test_one_past_shard_count_wraps_to_one(self):
        assert sharding.user_id_to_logical_shard_id(4097) == 1

    def test_accepts_string_user_id(self):
        assert sharding.user_id_to_logical_shard_id("10") == 10

    def test_respects_configured_shard_count(self, monkeypatch):
        monkeypatch.setattr(sharding.settings, "NUM_LOGICAL_SHARDS", 10, raising=False)
        assert sharding.user_id_to_logical_shard_id(25) == 5


# ---------------------------------------------------------------------------
# ShardedConnection
# ---------------------------------------------------------------------------


class TestShardedConnectionInit:
    def test_int_id_is_coerced_to_shard_name(self):
        conn = sharding.ShardedConnection(4)
        assert conn.connection_name == "shard_4"
        assert conn.original_shard is None

    def test_digit_string_id_is_coerced_to_shard_name(self):
        conn = sharding.ShardedConnection("4")
        assert conn.connection_name == "shard_4"

    def test_non_digit_name_is_kept_unchanged(self):
        conn = sharding.ShardedConnection("primary_shard")
        assert conn.connection_name == "primary_shard"


class TestShardedConnectionContextManager:
    def test_enter_switches_to_target_shard_and_returns_its_name(self, monkeypatch):
        monkeypatch.setattr(sharding.ShardedResource, "get_current_shard", lambda: "shard_1")
        switch_mock = mock.Mock()
        monkeypatch.setattr(db_pkg, "switch_default_database", switch_mock)

        conn = sharding.ShardedConnection(2)
        result = conn.__enter__()

        assert result == "shard_2"
        assert conn.original_shard == "shard_1"
        switch_mock.assert_called_once_with("shard_2")

    def test_exit_restores_original_shard(self, monkeypatch):
        switch_mock = mock.Mock()
        monkeypatch.setattr(db_pkg, "switch_default_database", switch_mock)

        conn = sharding.ShardedConnection(2)
        conn.original_shard = "shard_1"
        conn.__exit__(None, None, None)

        switch_mock.assert_called_once_with("shard_1")

    def test_used_as_a_with_statement(self, monkeypatch):
        monkeypatch.setattr(sharding.ShardedResource, "get_current_shard", lambda: "shard_9")
        switch_mock = mock.Mock()
        monkeypatch.setattr(db_pkg, "switch_default_database", switch_mock)

        with sharding.ShardedConnection(3) as name:
            assert name == "shard_3"

        assert switch_mock.call_args_list == [mock.call("shard_3"), mock.call("shard_9")]


# ---------------------------------------------------------------------------
# ShardEvent
# ---------------------------------------------------------------------------


class TestShardEventPublishSubscribe:
    def test_publish_notifies_event_specific_subscribers(self):
        event = sharding.ShardEvent()
        received = []
        event.subscribe("pub_specific_event", lambda data: received.append(data))

        event.publish("pub_specific_event", "payload")

        assert received == ["payload"]

    def test_publish_notifies_global_wildcard_subscribers(self):
        event = sharding.ShardEvent()
        received = []
        event.subscribe("*", lambda evt, data: received.append((evt, data)))

        event.publish("pub_wildcard_event", "payload")

        assert (("pub_wildcard_event", "payload")) in received

    def test_publish_with_no_subscribers_is_a_no_op(self):
        event = sharding.ShardEvent()
        # Should not raise even though nothing is subscribed to this event.
        event.publish("pub_nobody_listening_event", "payload")

    def test_subscribe_appends_to_existing_event_list(self):
        event = sharding.ShardEvent()
        calls = []
        event.subscribe("pub_multi_event", lambda data: calls.append("a"))
        event.subscribe("pub_multi_event", lambda data: calls.append("b"))

        event.publish("pub_multi_event", None)

        assert calls == ["a", "b"]


class TestShardEventUnsubscribeBug:
    def test_unsubscribe_unknown_event_is_a_no_op(self):
        event = sharding.ShardEvent()
        # Should not raise for an event nobody ever subscribed to.
        event.unsubscribe("unsub_unknown_event", lambda data: None)

    def test_unsubscribe_does_not_actually_remove_the_subscriber(self):
        """PRE-EXISTING BUG (not fixed, per task constraints) -- see module
        docstring. ``unsubscribe`` calls ``.remove(.index(fn))``, which
        removes by value (an int position), not by the callable itself, so
        it raises a silently-swallowed ``ValueError`` and never actually
        removes anything. This test pins that observed behavior."""
        event = sharding.ShardEvent()
        received = []

        def handler(data):
            received.append(data)

        event.subscribe("unsub_bug_event", handler)
        event.unsubscribe("unsub_bug_event", handler)

        # If unsubscribe worked correctly, `handler` would be gone here.
        assert handler in event.subscribers["unsub_bug_event"]

        event.publish("unsub_bug_event", "still-subscribed")
        assert received == ["still-subscribed"]


class TestShardEventReinstantiationBug:
    def test_reinstantiating_shard_event_resets_all_subscribers(self):
        """PRE-EXISTING BUG (not fixed, per task constraints) -- see module
        docstring. ``Singleton.__new__`` returns the same cached object on
        every ``ShardEvent()`` call, but ``ShardEvent.__init__`` still runs
        on it each time and unconditionally does ``self.subscribers = {}``,
        so identity is preserved but all subscriber state is wiped."""
        event = sharding.ShardEvent()
        event.subscribe("reset_bug_event", lambda data: None)
        assert "reset_bug_event" in event.subscribers

        event_again = sharding.ShardEvent()

        assert event_again is event
        assert event_again.subscribers == {}


# ---------------------------------------------------------------------------
# _get_shards
# ---------------------------------------------------------------------------


class TestGetShards:
    def test_lazily_computes_and_filters_to_shard_connections(self, monkeypatch):
        monkeypatch.setattr(sharding, "connections", mock.Mock(return_value=["shard_1", "shard_2", "default", "shard_10"]))

        assert sharding._get_shards() == ["shard_1", "shard_2", "shard_10"]

    def test_caches_result_after_first_call(self, monkeypatch):
        conn_mock = mock.Mock(return_value=["shard_1"])
        monkeypatch.setattr(sharding, "connections", conn_mock)

        first = sharding._get_shards()
        second = sharding._get_shards()

        assert first == second == ["shard_1"]
        conn_mock.assert_called_once()

    def test_import_error_falls_back_to_empty_list(self, monkeypatch):
        monkeypatch.setattr(sharding, "connections", mock.Mock(side_effect=ImportError))
        assert sharding._get_shards() == []

    def test_attribute_error_falls_back_to_empty_list(self, monkeypatch):
        monkeypatch.setattr(sharding, "connections", mock.Mock(side_effect=AttributeError))
        assert sharding._get_shards() == []


# ---------------------------------------------------------------------------
# ShardedResource.get_current_shard / shard_name_to_id / cache key helpers
# ---------------------------------------------------------------------------


class TestGetCurrentShard:
    def test_returns_the_shard_matching_default(self, monkeypatch):
        monkeypatch.setattr(sharding.ShardedResource, "all_shard_connection_names", lambda: ["shard_1", "shard_2"])
        monkeypatch.setattr(
            sharding.settings,
            "DATABASES",
            {"default": {"NAME": "db2"}, "shard_1": {"NAME": "db1"}, "shard_2": {"NAME": "db2"}},
            raising=False,
        )

        assert sharding.ShardedResource.get_current_shard() == "shard_2"

    def test_raises_when_no_shard_matches_default(self, monkeypatch):
        monkeypatch.setattr(sharding.ShardedResource, "all_shard_connection_names", lambda: ["shard_1"])
        monkeypatch.setattr(
            sharding.settings,
            "DATABASES",
            {"default": {"NAME": "unmatched"}, "shard_1": {"NAME": "db1"}},
            raising=False,
        )

        with pytest.raises(Exception, match="No active shard was found"):
            sharding.ShardedResource.get_current_shard()


class TestShardNameToId:
    @pytest.mark.parametrize(
        "shard,expected",
        [
            ("shard_0", 0),
            ("shard_1", 1),
            ("shard_123", 123),
            ("foo_bar_9", 9),
        ],
    )
    def test_extracts_the_trailing_integer(self, shard, expected):
        assert sharding.ShardedResource.shard_name_to_id(shard) == expected


class _Widget:
    """Minimal stand-in model class -- only ``__name__`` is used."""


class TestShardCacheKeyHelpers:
    def test_shard_cache_prefix(self):
        assert sharding.ShardedResource.shard_cache_prefix(_Widget, "external_id") == "shard:_Widget-external_id:"

    def test_shard_cache_key(self):
        assert sharding.ShardedResource.shard_cache_key(_Widget, "external_id", 42) == "shard:_Widget-external_id:42"


# ---------------------------------------------------------------------------
# ShardedResource._cache_set / _cache_get
# ---------------------------------------------------------------------------


class TestCacheSetGet:
    def test_cache_set_success(self, monkeypatch):
        cli = mock.MagicMock()
        monkeypatch.setattr(sharding, "get_memcache_client", mock.Mock(return_value=cli))

        sharding.ShardedResource._cache_set("k", "v")

        cli.set.assert_called_once_with("k", "v")

    def test_cache_set_swallows_memcache_error(self, monkeypatch):
        monkeypatch.setattr(sharding, "get_memcache_client", mock.Mock(side_effect=pylibmc.Error("boom")))
        sharding.ShardedResource._cache_set("k", "v")  # must not raise

    def test_cache_get_success(self, monkeypatch):
        cli = mock.MagicMock()
        cli.get.return_value = "v"
        monkeypatch.setattr(sharding, "get_memcache_client", mock.Mock(return_value=cli))

        assert sharding.ShardedResource._cache_get("k") == "v"

    def test_cache_get_swallows_memcache_error_and_returns_none(self, monkeypatch):
        monkeypatch.setattr(sharding, "get_memcache_client", mock.Mock(side_effect=pylibmc.Error("boom")))
        assert sharding.ShardedResource._cache_get("k") is None


# ---------------------------------------------------------------------------
# ShardedResource.warm_shard_id_cache
# ---------------------------------------------------------------------------


class TestWarmShardIdCache:
    def test_builds_and_sets_a_mapping_per_non_empty_shard(self, monkeypatch):
        monkeypatch.setattr(sharding, "_get_shards", lambda: ["shard_1", "shard_2"])
        cli = mock.MagicMock()
        monkeypatch.setattr(sharding, "get_memcache_client", mock.Mock(return_value=cli))

        model = mock.MagicMock()
        model.__name__ = "Widget"
        values_by_shard = {"shard_1": ["1", "2", "2"], "shard_2": []}

        def using_side_effect(shard):
            qs = mock.MagicMock()
            qs.only.return_value.values_list.return_value = values_by_shard[shard]
            return qs

        model.objects.using.side_effect = using_side_effect

        sharding.ShardedResource.warm_shard_id_cache(model, "external_id")

        # shard_2 contributed an empty mapping (`len(mapping) > 0` guard), so
        # only shard_1's non-empty, de-duplicated mapping is ever set.
        cli.set_multi.assert_called_once_with({"1": "1", "2": "1"}, key_prefix="shard:Widget-external_id:")

    def test_swallows_memcache_error(self, monkeypatch):
        monkeypatch.setattr(sharding, "_get_shards", lambda: ["shard_1"])
        monkeypatch.setattr(sharding, "get_memcache_client", mock.Mock(side_effect=pylibmc.Error("boom")))

        model = mock.MagicMock()
        model.__name__ = "Widget"

        sharding.ShardedResource.warm_shard_id_cache(model, "external_id")  # must not raise


# ---------------------------------------------------------------------------
# ShardedResource.set_shard_id (retry-wrapped staticmethod)
# ---------------------------------------------------------------------------


class TestSetShardId:
    def test_success_returns_true_on_first_attempt(self, monkeypatch):
        cli = mock.MagicMock()
        monkeypatch.setattr(sharding, "get_memcache_client", mock.Mock(return_value=cli))

        result = sharding.ShardedResource.set_shard_id(_Widget, "external_id", 42, 3)

        assert result is True
        cli.set.assert_called_once_with("shard:_Widget-external_id:42", "3")

    def test_memcache_error_retries_then_returns_false(self, monkeypatch):
        # `@retry(tries=3)` defaults to real `time.sleep(delay)` between
        # attempts; stub it out so this test doesn't take ~21s.
        monkeypatch.setattr(retry_mod._time, "sleep", lambda *_args: None)

        cli = mock.MagicMock()
        cli.set.side_effect = pylibmc.Error("boom")
        monkeypatch.setattr(sharding, "get_memcache_client", mock.Mock(return_value=cli))

        result = sharding.ShardedResource.set_shard_id(_Widget, "external_id", 42, 3)

        assert result is False
        assert cli.set.call_count == 4  # 1 initial attempt + 3 retries


# ---------------------------------------------------------------------------
# ShardedResource.find_shard_id
# ---------------------------------------------------------------------------


class TestFindShardId:
    def test_cache_hit_returns_int_without_scanning_shards(self, monkeypatch):
        monkeypatch.setattr(sharding.ShardedResource, "_cache_get", lambda key: "4")
        get_shards_mock = mock.Mock()
        monkeypatch.setattr(sharding, "_get_shards", get_shards_mock)

        result = sharding.ShardedResource.find_shard_id(_Widget, "external_id", 42)

        assert result == 4
        get_shards_mock.assert_not_called()

    def test_cache_miss_scans_shards_and_populates_cache(self, monkeypatch):
        monkeypatch.setattr(sharding.ShardedResource, "_cache_get", lambda key: None)
        cache_set_mock = mock.Mock()
        monkeypatch.setattr(sharding.ShardedResource, "_cache_set", cache_set_mock)
        monkeypatch.setattr(sharding, "_get_shards", lambda: ["shard_1", "shard_2"])

        model = mock.MagicMock()
        model.__name__ = "Widget"
        counts = {"shard_1": 0, "shard_2": 1}

        def using_side_effect(shard):
            qs = mock.MagicMock()
            qs.filter.return_value.count.return_value = counts[shard]
            return qs

        model.objects.using.side_effect = using_side_effect

        result = sharding.ShardedResource.find_shard_id(model, "external_id", 42)

        assert result == 2
        cache_set_mock.assert_called_once_with("shard:Widget-external_id:42", "2")

    def test_use_cache_false_skips_cache_entirely(self, monkeypatch):
        cache_get_mock = mock.Mock()
        cache_set_mock = mock.Mock()
        monkeypatch.setattr(sharding.ShardedResource, "_cache_get", cache_get_mock)
        monkeypatch.setattr(sharding.ShardedResource, "_cache_set", cache_set_mock)
        monkeypatch.setattr(sharding, "_get_shards", lambda: ["shard_1"])

        model = mock.MagicMock()
        model.__name__ = "Widget"
        model.objects.using.return_value.filter.return_value.count.return_value = 1

        result = sharding.ShardedResource.find_shard_id(model, "external_id", 42, use_cache=False)

        assert result == 1
        cache_get_mock.assert_not_called()
        cache_set_mock.assert_not_called()

    def test_not_found_on_any_shard_raises_lookup_failure(self, monkeypatch):
        monkeypatch.setattr(sharding.ShardedResource, "_cache_get", lambda key: None)
        monkeypatch.setattr(sharding, "_get_shards", lambda: ["shard_1"])

        model = mock.MagicMock()
        model.__name__ = "Widget"
        model.objects.using.return_value.filter.return_value.count.return_value = 0

        with pytest.raises(sharding.ShardLookupFailure):
            sharding.ShardedResource.find_shard_id(model, "external_id", 42)


# ---------------------------------------------------------------------------
# ShardedResource._real_user_id_to_physical_shard_id
# ---------------------------------------------------------------------------


class TestRealUserIdToPhysicalShardId:
    def test_returns_the_physical_shard_id(self, monkeypatch):
        db_query_mock = mock.Mock(return_value=[[7]])
        monkeypatch.setattr(db_pkg, "db_query", db_query_mock)

        result = sharding.ShardedResource._real_user_id_to_physical_shard_id(123)

        assert result == 7
        called_sql = db_query_mock.call_args.args[0]
        assert '"id" = 123' in called_sql
        assert db_query_mock.call_args.kwargs == {"using": "shard_1"}

    def test_empty_result_raises_lookup_failure(self, monkeypatch):
        monkeypatch.setattr(db_pkg, "db_query", mock.Mock(return_value=[]))

        with pytest.raises(sharding.ShardLookupFailure):
            sharding.ShardedResource._real_user_id_to_physical_shard_id(123)

    def test_empty_row_raises_lookup_failure(self, monkeypatch):
        monkeypatch.setattr(db_pkg, "db_query", mock.Mock(return_value=[[]]))

        with pytest.raises(sharding.ShardLookupFailure):
            sharding.ShardedResource._real_user_id_to_physical_shard_id(123)


# ---------------------------------------------------------------------------
# ShardedResource._caching_user_id_to_physical_shard_id
# ---------------------------------------------------------------------------


class TestCachingUserIdToPhysicalShardId:
    def test_cache_hit_returns_cached_value_without_a_real_lookup(self, monkeypatch):
        cli = mock.MagicMock()
        cli.get.return_value = 9
        monkeypatch.setattr(sharding, "get_memcache_client", mock.Mock(return_value=cli))
        real_mock = mock.Mock()
        monkeypatch.setattr(sharding.ShardedResource, "_real_user_id_to_physical_shard_id", real_mock)

        result = sharding.ShardedResource._caching_user_id_to_physical_shard_id(123)

        assert result == 9
        real_mock.assert_not_called()

    def test_cache_miss_computes_and_populates_the_cache(self, monkeypatch):
        cli = mock.MagicMock()
        cli.get.return_value = None
        monkeypatch.setattr(sharding, "get_memcache_client", mock.Mock(return_value=cli))
        monkeypatch.setattr(sharding.ShardedResource, "_real_user_id_to_physical_shard_id", mock.Mock(return_value=5))

        result = sharding.ShardedResource._caching_user_id_to_physical_shard_id(123)

        assert result == 5
        cli.set.assert_called_once_with("logicalShard:123", 5, time=180)

    def test_memcache_get_error_falls_back_to_the_real_lookup(self, monkeypatch):
        cli = mock.MagicMock()
        cli.get.side_effect = pylibmc.Error("boom")
        monkeypatch.setattr(sharding, "get_memcache_client", mock.Mock(return_value=cli))
        monkeypatch.setattr(sharding.ShardedResource, "_real_user_id_to_physical_shard_id", mock.Mock(return_value=6))

        result = sharding.ShardedResource._caching_user_id_to_physical_shard_id(123)

        assert result == 6

    def test_memcache_set_error_after_a_real_lookup_still_returns_the_shard_id(self, monkeypatch):
        # `shard_id` is already bound (from the real lookup) by the time
        # `.set()` raises, so the except-block's `"shard_id" not in vars()`
        # check is False and it does *not* recompute -- it just returns the
        # value it already had.
        cli = mock.MagicMock()
        cli.get.return_value = None
        cli.set.side_effect = pylibmc.Error("boom")
        monkeypatch.setattr(sharding, "get_memcache_client", mock.Mock(return_value=cli))
        real_mock = mock.Mock(return_value=8)
        monkeypatch.setattr(sharding.ShardedResource, "_real_user_id_to_physical_shard_id", real_mock)

        result = sharding.ShardedResource._caching_user_id_to_physical_shard_id(123)

        assert result == 8
        real_mock.assert_called_once()


# ---------------------------------------------------------------------------
# ShardedResource.user_id_to_physical_shard_id / all_shard_connection_names
# ---------------------------------------------------------------------------


class TestUserIdToPhysicalShardId:
    def test_use_cache_false_calls_the_real_lookup(self, monkeypatch):
        real_mock = mock.Mock(return_value=1)
        monkeypatch.setattr(sharding.ShardedResource, "_real_user_id_to_physical_shard_id", real_mock)
        caching_mock = mock.Mock()
        monkeypatch.setattr(sharding.ShardedResource, "_caching_user_id_to_physical_shard_id", caching_mock)

        result = sharding.ShardedResource.user_id_to_physical_shard_id(5, use_cache=False)

        assert result == 1
        caching_mock.assert_not_called()

    def test_use_cache_true_calls_the_caching_lookup(self, monkeypatch):
        real_mock = mock.Mock()
        monkeypatch.setattr(sharding.ShardedResource, "_real_user_id_to_physical_shard_id", real_mock)
        monkeypatch.setattr(sharding.ShardedResource, "_caching_user_id_to_physical_shard_id", mock.Mock(return_value=2))

        result = sharding.ShardedResource.user_id_to_physical_shard_id(5)

        assert result == 2
        real_mock.assert_not_called()


def test_all_shard_connection_names_delegates_to_get_shards(monkeypatch):
    monkeypatch.setattr(sharding, "_get_shards", lambda: ["shard_9"])
    assert sharding.ShardedResource.all_shard_connection_names() == ["shard_9"]


def test_user_id_to_shard_name_lambda(monkeypatch):
    monkeypatch.setattr(sharding.ShardedResource, "user_id_to_physical_shard_id", lambda user_id: 3)
    assert sharding.userIdToShardName(999) == "shard_3"


# ---------------------------------------------------------------------------
# ShardedResource._subscribe_to_shard_events / the "movedUser" handler
# ---------------------------------------------------------------------------


class TestSubscribeToShardEvents:
    def test_moved_user_updates_shard_ids_for_user_extended_user_and_phone(self, monkeypatch):
        fake_auth_models = types.ModuleType("django.contrib.auth.models")
        fake_auth_models.User = object()

        fake_main_models = types.ModuleType("main.models")
        fake_extended_user = mock.MagicMock(name="ExtendedUser")
        fake_extended_user.objects.get.return_value.twilio_phone_number.number = "+15551234567"
        fake_main_models.ExtendedUser = fake_extended_user
        fake_main_models.PhoneNumber = object()

        fake_conn_cm = mock.MagicMock()
        fake_conn_cm.__enter__ = mock.Mock(return_value="shard_7")
        fake_conn_cm.__exit__ = mock.Mock(return_value=False)
        fake_sharded_connection_cls = mock.Mock(return_value=fake_conn_cm)
        monkeypatch.setattr(sharding, "ShardedConnection", fake_sharded_connection_cls)

        set_shard_id_mock = mock.Mock()
        monkeypatch.setattr(sharding.ShardedResource, "set_shard_id", set_shard_id_mock)

        with mock.patch.dict(
            sys.modules,
            {"django.contrib.auth.models": fake_auth_models, "main.models": fake_main_models},
        ):
            # Hold a single `ShardEvent()` reference and never re-instantiate:
            # `_subscribe_to_shard_events()` itself calls `ShardEvent()`
            # internally (which resets `.subscribers` -- see module
            # docstring), but that reset lands on this same shared object, so
            # reading `.subscribers` back through `event` afterwards (instead
            # of calling `sharding.ShardEvent()` again) sees the update
            # without triggering another reset.
            event = sharding.ShardEvent()
            sharding.ShardedResource._subscribe_to_shard_events()
            moved_user = event.subscribers["movedUser"][-1]
            moved_user({"user_id": 55, "shardId": 7})

        assert set_shard_id_mock.call_args_list == [
            mock.call(fake_auth_models.User, "user_id", 55, "7"),
            mock.call(fake_main_models.ExtendedUser, "user_id", 55, "7"),
            mock.call(fake_main_models.PhoneNumber, "number", "+15551234567", "7"),
        ]
        fake_sharded_connection_cls.assert_called_once_with("7")
        fake_extended_user.objects.get.assert_called_once_with(user=55)


# ---------------------------------------------------------------------------
# ShardedAuthenticationMiddleware
# ---------------------------------------------------------------------------


class _FakeSimpleLazyObject:
    """Stand-in for ``django.utils.functional.SimpleLazyObject`` that just
    remembers the wrapped callable, so tests can invoke it to assert on
    what ``process_request`` would have resolved ``request.user`` to."""

    def __init__(self, fn):
        self.fn = fn


@pytest.fixture
def fake_django_auth_modules():
    fake_auth = types.ModuleType("django.contrib.auth")
    fake_auth.SESSION_KEY = "_auth_user_id"
    fake_auth.get_user = mock.Mock(return_value="RESOLVED_USER")

    fake_functional = types.ModuleType("django.utils.functional")
    fake_functional.SimpleLazyObject = _FakeSimpleLazyObject

    with mock.patch.dict(
        sys.modules,
        {"django.contrib.auth": fake_auth, "django.utils.functional": fake_functional},
    ):
        yield fake_auth


class TestShardedAuthenticationMiddleware:
    def test_missing_session_middleware_raises_assertion_error(self, fake_django_auth_modules):
        request = types.SimpleNamespace()  # deliberately has no `.session`

        with pytest.raises(AssertionError):
            sharding.ShardedAuthenticationMiddleware.process_request(request)

    def test_logged_in_user_switches_to_their_physical_shard(self, monkeypatch, fake_django_auth_modules):
        # `process_request` calls this via an *instance* (`ShardedResource()`),
        # not the class -- a plain lambda would get bound as a method (and
        # receive `self` as an extra positional arg), unlike the real
        # `@staticmethod`, so the replacement must also be a `staticmethod`.
        monkeypatch.setattr(sharding.ShardedResource, "user_id_to_physical_shard_id", staticmethod(lambda user_id: 3))
        switch_mock = mock.Mock()
        monkeypatch.setattr(db_pkg, "switch_default_database", switch_mock)

        request = types.SimpleNamespace(session={fake_django_auth_modules.SESSION_KEY: "42"})

        result = sharding.ShardedAuthenticationMiddleware.process_request(request)

        assert result is None
        switch_mock.assert_called_once_with("shard_3")
        assert isinstance(request.user, _FakeSimpleLazyObject)
        assert request.user.fn() == "RESOLVED_USER"
        fake_django_auth_modules.get_user.assert_called_once_with(request)

    def test_no_session_user_id_skips_shard_switch(self, monkeypatch, fake_django_auth_modules):
        switch_mock = mock.Mock()
        monkeypatch.setattr(db_pkg, "switch_default_database", switch_mock)

        request = types.SimpleNamespace(session={})

        result = sharding.ShardedAuthenticationMiddleware.process_request(request)

        assert result is None
        switch_mock.assert_not_called()
        assert isinstance(request.user, _FakeSimpleLazyObject)
