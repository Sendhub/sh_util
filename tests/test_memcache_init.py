"""Unit tests for ``sh_util.memcache`` (package ``__init__``): the lazily
initialized memcache client and its warm-up/flush helpers.

The pymemcache client is never invoked for real -- ``_Client`` is monkeypatched
with a stand-in factory so no actual memcached connection is
attempted. ``settings.MEMCACHE_CLIENTS`` is a shared module-level dict (see
``src/config.py``); each test gets a fresh one via monkeypatch so client
caching doesn't leak between tests (the cache key is just the current PID,
which is constant for the whole test run, so a shared dict would otherwise
carry a client from one test into the next).
"""

from unittest import mock

import pytest

import sh_util.memcache as memcache_module


@pytest.fixture(autouse=True)
def fresh_settings(monkeypatch):
    monkeypatch.setattr(memcache_module._settings, "MEMCACHE_CLIENTS", {}, raising=False)
    monkeypatch.setattr(memcache_module._settings, "MEMCACHE_SERVERS", "localhost:11211", raising=False)


@pytest.fixture
def fake_pylibmc(monkeypatch):
    fake_client = mock.Mock(name="pylibmc_client")
    fake_module = mock.Mock(name="pymemcache_facade")
    fake_module.Client = mock.Mock(name="Client", return_value=fake_client)
    monkeypatch.setattr(memcache_module, "_Client", fake_module.Client)
    return fake_module, fake_client


class TestGetMemcacheClient:
    def test_creates_a_new_client_when_none_cached(self, fake_pylibmc):
        fake_module, fake_client = fake_pylibmc

        client = memcache_module.get_memcache_client()

        assert client is fake_client
        fake_module.Client.assert_called_once_with("localhost:11211")
        key = f"{memcache_module._os.getpid()}"
        assert memcache_module._settings.MEMCACHE_CLIENTS[key] is fake_client

    def test_reuses_the_cached_client_on_the_fast_path(self, fake_pylibmc):
        fake_module, _first_client = fake_pylibmc
        first = memcache_module.get_memcache_client()
        fake_module.Client.reset_mock()

        second = memcache_module.get_memcache_client()

        assert second is first
        fake_module.Client.assert_not_called()

    def test_new_connection_true_forces_a_fresh_client_even_when_cached(self, fake_pylibmc):
        fake_module, _first_client = fake_pylibmc
        memcache_module.get_memcache_client()
        new_client = mock.Mock(name="new_pylibmc_client")
        fake_module.Client.return_value = new_client

        client = memcache_module.get_memcache_client(new_connection=True)

        assert client is new_client
        assert fake_module.Client.call_count == 2

    def test_client_created_by_another_thread_while_waiting_for_the_lock_is_reused(self, fake_pylibmc, monkeypatch):
        # Simulates the race the module's docstring is about: the fast-path
        # check (outside the lock) sees no client, but by the time this
        # call re-checks inside the lock, another thread has already
        # created and cached one -- that one must be reused, not replaced.
        fake_module, _client = fake_pylibmc
        winner_client = mock.Mock(name="other_threads_client")

        class RaceyDict(dict):
            def __init__(self):
                super().__init__()
                self.get_calls = 0

            def get(self_inner, key, default=None):
                self_inner.get_calls += 1
                if self_inner.get_calls == 1:
                    return None
                self_inner[key] = winner_client
                return winner_client

        monkeypatch.setattr(memcache_module._settings, "MEMCACHE_CLIENTS", RaceyDict(), raising=False)

        client = memcache_module.get_memcache_client()

        assert client is winner_client
        fake_module.Client.assert_not_called()


class TestWarmMemcacheClient:
    def test_successfully_initializes_the_client(self, fake_pylibmc):
        fake_module, _fake_client = fake_pylibmc

        memcache_module.warm_memcache_client()

        fake_module.Client.assert_called_once()

    def test_swallows_and_logs_errors(self, monkeypatch, caplog):
        monkeypatch.setattr(memcache_module, "get_memcache_client", mock.Mock(side_effect=Exception("boom")))

        with caplog.at_level("ERROR"):
            memcache_module.warm_memcache_client()

        assert "Warm-up failed" in caplog.text
        assert "boom" in caplog.text


class TestAttemptMemcacheFlush:
    def test_successfully_flushes(self, fake_pylibmc, caplog):
        _fake_module, fake_client = fake_pylibmc

        with caplog.at_level("INFO"):
            memcache_module.attempt_memcache_flush()

        fake_client.flush_all.assert_called_once()
        assert "Flush completed" in caplog.text

    def test_swallows_and_logs_errors(self, monkeypatch, caplog):
        monkeypatch.setattr(memcache_module, "get_memcache_client", mock.Mock(side_effect=Exception("boom")))

        with caplog.at_level("ERROR"):
            memcache_module.attempt_memcache_flush()

        assert "Flush failed" in caplog.text
        assert "boom" in caplog.text


class TestSafeKey:
    def test_plain_key_is_unchanged(self):
        assert memcache_module._safe_key("customer:cus_123") == "customer:cus_123"

    @pytest.mark.parametrize("key", ["has space", "new\nline", "caf\u00e9", "x" * 251])
    def test_keys_the_text_protocol_rejects_become_a_stable_digest(self, key):
        safe = memcache_module._safe_key(key)

        assert safe.startswith("h:") and len(safe) == 42
        assert safe == memcache_module._safe_key(key)

    def test_bytes_keys_are_decoded(self):
        assert memcache_module._safe_key(b"abc") == "abc"


class TestSerde:
    @pytest.mark.parametrize("value", [{"a": [1, 2]}, ("t", 3), [1, "x"], True, None.__class__])
    def test_pickled_values_round_trip(self, value):
        data, flags = memcache_module._serialize("k", value)

        assert memcache_module._deserialize("k", data, flags) == value

    def test_str_bytes_and_int_round_trip(self):
        assert memcache_module._deserialize("k", *memcache_module._serialize("k", "text")) == "text"
        assert memcache_module._deserialize("k", *memcache_module._serialize("k", 42)) == 42
        assert memcache_module._serialize("k", b"raw") == (b"raw", 0)

    def test_entries_compressed_by_the_old_client_are_still_readable(self):
        import pickle
        import zlib

        payload = zlib.compress(pickle.dumps({"a": 1}))

        assert memcache_module._deserialize("k", payload, memcache_module._FLAG_PICKLE | memcache_module._FLAG_COMPRESSED) == {"a": 1}

    def test_unknown_flags_return_the_raw_value(self):
        assert memcache_module._deserialize("k", b"x", 1 << 6) == b"x"


class TestClientFacade:
    @pytest.fixture
    def facade(self, monkeypatch):
        pooled = mock.Mock(name="pooled")
        factory = mock.Mock(return_value=pooled)
        monkeypatch.setattr(memcache_module, "_PooledClient", factory)
        return memcache_module._Client("cache.internal:11211"), pooled, factory

    def test_builds_a_pooled_client_for_host_and_port(self, facade):
        _client, _pooled, factory = facade

        assert factory.call_args.args == (("cache.internal", 11211),)
        assert factory.call_args.kwargs["default_noreply"] is False

    def test_server_without_a_port_defaults_to_11211(self, monkeypatch):
        factory = mock.Mock()
        monkeypatch.setattr(memcache_module, "_PooledClient", factory)

        memcache_module._Client("cache.internal")

        assert factory.call_args.args == (("cache.internal", 11211),)

    def test_get_returns_default_on_miss(self, facade):
        client, pooled, _ = facade
        pooled.get.return_value = None

        assert client.get("k", "fallback") == "fallback"

    def test_set_maps_time_to_expire_and_ignores_min_compress_len(self, facade):
        client, pooled, _ = facade

        client.set("k", {"a": 1}, time=60, min_compress_len=1024)

        pooled.set.assert_called_once_with("k", {"a": 1}, expire=60, noreply=False)

    def test_set_multi_applies_the_key_prefix(self, facade):
        client, pooled, _ = facade

        client.set_multi({"a": "1"}, key_prefix="shard:")

        pooled.set_many.assert_called_once_with({"shard:a": "1"}, expire=0, noreply=False)

    def test_delete_incr_and_flush_all_delegate(self, facade):
        client, pooled, _ = facade

        client.delete("k")
        client.incr("n", 2)
        client.flush_all()

        pooled.delete.assert_called_once_with("k", noreply=False)
        pooled.incr.assert_called_once_with("n", 2, noreply=False)
        pooled.flush_all.assert_called_once_with(noreply=False)
