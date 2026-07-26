"""Unit tests for ``sh_util.memcache`` (package ``__init__``): the lazily
initialized memcache client and its warm-up/flush helpers.

``pylibmc.Client`` is never invoked for real -- ``_pylibmc`` is monkeypatched
with a stand-in ``Client`` factory so no actual memcached connection is
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
    fake_module = mock.Mock(name="pylibmc")
    fake_module.Client = mock.Mock(name="Client", return_value=fake_client)
    monkeypatch.setattr(memcache_module, "_pylibmc", fake_module)
    return fake_module, fake_client


class TestGetMemcacheClient:
    def test_creates_a_new_client_when_none_cached(self, fake_pylibmc):
        fake_module, fake_client = fake_pylibmc

        client = memcache_module.get_memcache_client()

        assert client is fake_client
        fake_module.Client.assert_called_once_with(["localhost:11211"], binary=True)
        assert fake_client.behaviors == {"tcp_nodelay": True, "ketama": True}
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
