"""
Memcache lazy initialization.

[00] IMPORTANT NOTE:
----------------------
This module is used in ECS + EC2 environments where:

- Multiple processes
- Multiple threads
- A SINGLE shared Memcached cluster

Losing a cache `add()` race is NORMAL in distributed systems.
Resetting memcache clients on contention can cause connection storms.

We DO NOT call `get_memcache_client(new_connection=True)` unless we are
certain the error is a REAL transport failure (timeout, broken pipe, etc).


[01] Issue summary:
----------------------
Not a load balancer problem.
Not an ECS task-to-task race.
Not a memcache server bug.

This was a **thread-level race inside a single ECS task**, triggered by **unsafe memcache client reinitialization** in response to **normal, expected cache contention**.


[02] The Misunderstanding
----------------------
> “The ALB sends the request to one task. How can there be a race?”

That sentence is true — and **irrelevant**.

The race does **not** happen:

* between ALB → ECS tasks
* between ECS tasks
* between memcache servers

The race happens **inside one ECS task**, between **multiple threads**, sharing **mutable global state**, without locks.


[03] The Architecture
----------------------
Inside **one ECS task** you have:

+--------------------------------------------------+
| ECS TASK                                         |
|                                                  |
|  PID 12345                                       |
|  ├── Thread A  ── handles Request #1             |
|  ├── Thread B  ── handles Request #2             |
|  ├── Thread C  ── handles Request #3             |
|                                                  |
|  settings.MEMCACHE_CLIENTS  (GLOBAL DICT)        |
+--------------------------------------------------+

Each thread:

* handles a separate HTTP request
* runs concurrently
* calls caching code independently


[04] Timeline inside ONE ECS task
----------------------

Thread A                          Thread B
--------                          --------
_ns = None                        _ns = None
add(...) → False                  add(...) → True
raise Error
catch Error
_Cli(new_connection=True)
(overwrites client in dict)

                                  continues using old client
                                  client state now inconsistent
                                  retries namespace logic


Now both threads:

* re-run namespace logic
* re-log the same messages
* re-hit memcache
* appear as “duplicate execution”


[05] Why did this happen?
----------------------

Not because:

* ALB duplicated requests
* ECS duplicated traffic
* Memcache duplicated data

But because:

* thread A invalidated its own client
* thread B retried with a half-torn client state

Which **looks** like multiple tasks — but isn't.


[06] ASCII diagram: the failure mode
----------------------

                ECS TASK (ONE CONTAINER)
        -------------------------------------------------
        |                                               |
        |  Thread A            Thread B                 |
        |  --------            --------                 |
        |  get(ns) → None      get(ns) → None           |
        |  add(ns) → False     add(ns) → True           |
        |  raise Error                                  |
        |  _Cli(new_connection=True)                    |
        |      |                                        |
        |      |  (global dict mutated)                 |
        |      v                                        |
        |  MEMCACHE_CLIENTS[pid-tid] replaced           |
        |                                               |
        |                      old client still in use  |
        |                      undefined behavior       |
        |                                               |
        -------------------------------------------------

"""

__author__ = "Jay Taylor [@jtaylor]"
__contributors__ = [
    "Dipayan Ray",
]

import hashlib as _hashlib
import logging
import os as _os
import pickle as _pickle
import re as _re
import threading as _threading
import time as _time
import zlib as _zlib

import settings as _settings
from pymemcache import exceptions as _mc_exceptions
from pymemcache.client.base import PooledClient as _PooledClient

# Exceptions a cache call can raise (pymemcache protocol errors, plus the socket
# errors it lets through). Call sites catch this instead of ``pylibmc.Error``.
MEMCACHE_ERRORS = (_mc_exceptions.MemcacheError, OSError)

# Value flags, kept identical to pylibmc's so entries written by the old client
# stay readable (pickle=1, int=2, long=4, compressed=8) and nothing is lost on cutover.
_FLAG_PICKLE = 1 << 0
_FLAG_INTEGER = 1 << 1
_FLAG_LONG = 1 << 2
_FLAG_COMPRESSED = 1 << 3

_POOL_SIZE = 10
_TIMEOUT_SECONDS = 5


_SAFE_KEY = _re.compile(r"^[\x21-\x7e]{1,250}$")


def _safe_key(key):
    """Return ``key`` if memcached's text protocol accepts it, else a stable digest.

    pylibmc's binary protocol took any bytes; the text protocol rejects whitespace,
    control/non-ASCII characters and keys over 250 chars (e.g. the repr of pickled
    args that ``functional.py`` embeds in its memoize keys).
    """
    if isinstance(key, bytes):
        key = key.decode("utf-8", errors="surrogateescape")
    if _SAFE_KEY.match(key):
        return key
    return "h:" + _hashlib.sha1(key.encode("utf-8", errors="surrogatepass")).hexdigest()


def _serialize(key, value):
    if isinstance(value, bytes):
        return value, 0
    if isinstance(value, str):
        return value.encode("utf-8"), 0
    if isinstance(value, bool):
        return _pickle.dumps(value, protocol=_pickle.HIGHEST_PROTOCOL), _FLAG_PICKLE
    if isinstance(value, int):
        return str(value).encode("ascii"), _FLAG_INTEGER
    return _pickle.dumps(value, protocol=_pickle.HIGHEST_PROTOCOL), _FLAG_PICKLE


def _deserialize(key, value, flags):
    if flags & _FLAG_COMPRESSED:
        value = _zlib.decompress(value)
        flags &= ~_FLAG_COMPRESSED
    if flags == 0:
        # pylibmc returned str for text; callers here only store str/bytes via set().
        return value.decode("utf-8", errors="surrogateescape") if isinstance(value, bytes) else value
    if flags & (_FLAG_INTEGER | _FLAG_LONG):
        return int(value)
    if flags & _FLAG_PICKLE:
        return _pickle.loads(value)  # noqa: S301 - cache is internal, same trust boundary as before
    return value


class _Serde:
    serialize = staticmethod(_serialize)
    deserialize = staticmethod(_deserialize)


class _Client:
    """pylibmc-shaped facade over a thread-safe ``pymemcache`` pooled client.

    Keeps the call signatures the services already use (``set(..., time=,
    min_compress_len=)``, ``set_multi(mapping, key_prefix=)``) so only this module
    changes. ``min_compress_len`` is accepted and ignored: values are stored
    uncompressed (reads still decompress entries the old client compressed).
    """

    def __init__(self, server):
        host, _, port = str(server).rpartition(":")
        self._client = _PooledClient(
            (host, int(port)) if host else (str(server), 11211),
            serde=_Serde,
            default_noreply=False,
            no_delay=True,
            connect_timeout=_TIMEOUT_SECONDS,
            timeout=_TIMEOUT_SECONDS,
            max_pool_size=_POOL_SIZE,
        )

    def get(self, key, default=None):
        value = self._client.get(_safe_key(key))
        return default if value is None else value

    def set(self, key, value, time=0, min_compress_len=0):
        return self._client.set(_safe_key(key), value, expire=time, noreply=False)

    def set_multi(self, mapping, time=0, key_prefix=""):
        return self._client.set_many({_safe_key(f"{key_prefix}{k}"): v for k, v in mapping.items()}, expire=time, noreply=False)

    def delete(self, key):
        return self._client.delete(_safe_key(key), noreply=False)

    def incr(self, key, delta=1):
        return self._client.incr(_safe_key(key), delta, noreply=False)

    def flush_all(self):
        return self._client.flush_all(noreply=False)


# ------------------------------------------------------------------------------
# Internal lock to protect client creation ONLY.
# We do NOT lock normal usage.
# ------------------------------------------------------------------------------
_MEMCACHE_CLIENT_LOCK = _threading.Lock()


def get_memcache_client(new_connection=False):
    """
    Lazily access a memcache client instance.

    Args:
        new_connection (bool):
            Defaults to False.
            When True, forces a new client to be created for the current
            process.

    WARNING:
        Forcing a new connection should ONLY be done for REAL transport
        failures (network issues, broken pipe, timeouts).

        We DO NOT reset clients for:
            - Cache contention
            - add() races
            - Key not found
            - Semantic cache errors
    """

    pid = _os.getpid()
    tid = _threading.current_thread().ident
    key = f"{pid}"

    start_ts = _time.monotonic()

    # Fast path: existing client and no forced reset
    client = _settings.MEMCACHE_CLIENTS.get(key)
    if client is not None and not new_connection:
        logging.debug(
            "[MEMCACHE] pid:%d threadid:%d Reusing existing memcache client (fast path)",
            pid,
            tid,
        )
        return client

    # Slow path: creation or forced reset
    lock_wait_start = _time.monotonic()
    with _MEMCACHE_CLIENT_LOCK:
        lock_wait_ms = (_time.monotonic() - lock_wait_start) * 1000.0
        # Re-checking inside lock to avoid duplicate creation
        client = _settings.MEMCACHE_CLIENTS.get(key)
        if client is None or new_connection:
            logging.info("[MEMCACHE] pid:%d threadid:%d Creating new memcache client%s", pid, tid, " (forced reset)" if new_connection else "")

            # pymemcache's pooled client opens connections lazily and reconnects
            # on failure, so there is nothing to tune beyond the pool size.
            client = _Client(_settings.MEMCACHE_SERVERS)

            _settings.MEMCACHE_CLIENTS[key] = client

            total_ms = (_time.monotonic() - start_ts) * 1000.0
            logging.debug(
                "[MEMCACHE] pid:%d threadid:%d client created (lock_wait_ms=%.2f, total_ms=%.2f)",
                pid,
                tid,
                lock_wait_ms,
                total_ms,
            )

        return client


def warm_memcache_client():
    """
    Initialize a memcache client early to avoid cold-start contention.
    """
    try:
        get_memcache_client()
    except Exception as err:
        logging.error("[MEMCACHE] Warm-up failed: %s", str(err))


def attempt_memcache_flush():
    """
    Attempt to flush the memcache server.

    WARNING:
        This is a destructive operation and should only be used
        in controlled environments (maintenance, admin tools).
    """
    try:
        logging.info("[MEMCACHE] Attempting to flush all")
        get_memcache_client().flush_all()
        logging.info("[MEMCACHE] Flush completed")
    except Exception as err:
        logging.error("[MEMCACHE] Flush failed: %s", str(err))


__all__ = [
    "MEMCACHE_ERRORS",
    "get_memcache_client",
    "attempt_memcache_flush",
    "warm_memcache_client",
]
