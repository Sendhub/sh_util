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

__author__ = 'Jay Taylor [@jtaylor]'
__contributors__ = ['Dipayan Ray',]

import logging
import os as _os
import threading as _threading
import time as _time

import pylibmc as _pylibmc

import settings as _settings

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
            logging.info(
                "[MEMCACHE] pid:%d threadid:%d Creating new memcache client%s",
                pid,
                tid,
                " (forced reset)" if new_connection else ""
            )

            # NOTE:
            # We intentionally keep the server list minimal here.
            # pylibmc internally handles reconnection and failover.
            client = _pylibmc.Client([_settings.MEMCACHE_SERVERS], binary=True,)

            client.behaviors = {"tcp_nodelay": True, "ketama": True,}

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
    "get_memcache_client",
    "attempt_memcache_flush",
    "warm_memcache_client",
]
