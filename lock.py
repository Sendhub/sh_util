"""
This module provides functionality for acquiring and releasing Redis-based locks.
"""

__author__ = 'brock'

# Note: settings import moved to function level to avoid circular import issues


def acquire_lock(lock_id, timeout=60):
    """
    Acquiring a Redis lock with a specified timeout.

    Args:
        lock_id (str): The unique identifier for the lock.
        timeout (int): The timeout duration in seconds. Defaults to 60.

    Returns:
        bool: True if the lock is successfully acquired, False otherwise.
    """
    # Import moved here to avoid circular import issues
    import settings
    _redis = settings.REDIS

    # Ensuring the timeout value is always valid
    assert timeout > 0

    acquired = _redis.setnx(lock_id, "true")
    if acquired:
        _redis.expire(lock_id, timeout)
    else:
        # Checking if there is no timeout set and ensuring a timeout is applied
        if not _redis.ttl(lock_id):
            _redis.expire(lock_id, timeout)

    return acquired


def release_lock(lock_id):
    """
    Releasing a Redis lock by its identifier.

    Args:
        lock_id (str): The unique identifier for the lock to release.
    """
    # Import moved here to avoid circular import issues
    import settings
    _redis = settings.REDIS

    _redis.delete(lock_id)
