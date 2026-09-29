"""
This module provides functionality for acquiring and releasing Redis-based locks.
"""

__author__ = "brock"

import uuid

# Note: settings import moved to function level to avoid circular import issues

# Lua script for atomic lock release: only deletes the key if the stored value
# matches the caller's token, preventing accidental release by a different holder.
_RELEASE_SCRIPT = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
"""


def acquire_lock(lock_id, timeout=60):
    """
    Acquire a Redis lock atomically with a specified timeout.

    Uses a single SET NX EX command so that the key and its TTL are set
    in one atomic operation, eliminating the race condition between a
    SETNX and a subsequent EXPIRE.

    Args:
        lock_id (str): The unique identifier for the lock.
        timeout (int): The timeout duration in seconds. Defaults to 60.

    Returns:
        str: A unique lock token if the lock was acquired, empty string otherwise.
             The token must be passed to release_lock() to release the lock safely.
    """
    # Import moved here to avoid circular import issues
    import settings

    _redis = settings.REDIS

    # Ensuring the timeout value is always valid
    assert timeout > 0

    lock_value = str(uuid.uuid4())
    acquired = bool(_redis.set(lock_id, lock_value, nx=True, ex=timeout))
    return lock_value if acquired else ""


def release_lock(lock_id, lock_value=""):
    """
    Release a Redis lock, but only if this caller still holds it.

    Uses a Lua script so the check-and-delete is atomic: the lock is only
    deleted when the stored value matches lock_value, preventing a process
    from accidentally releasing a lock it no longer owns (e.g. after a
    timeout expiry and re-acquisition by another holder).

    Args:
        lock_id (str): The unique identifier for the lock to release.
        lock_value (str): The token returned by acquire_lock(). When empty,
                          falls back to a plain delete for backwards compatibility.
    """
    # Import moved here to avoid circular import issues
    import settings

    _redis = settings.REDIS

    if lock_value:
        _redis.eval(_RELEASE_SCRIPT, 1, lock_id, lock_value)
    else:
        _redis.delete(lock_id)
