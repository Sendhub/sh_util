"""
redis cache locks
"""

__author__ = 'brock'

import settings
_redis = settings.REDIS


def acquire_lock(lock_id, timeout=60):
    """
    redis locks
    """
    # make sure these redis locks always have a valid timeout
    assert timeout > 0

    acquired = _redis.setnx(lock_id, "true")
    if acquired:
        _redis.expire(lock_id, timeout)
    else:
        # if there is no timeout set and we couldn't acquire the lock
        # then make sure that we set a timeout on the lock so we
        # cant have a deadlock
        if not _redis.ttl(lock_id):
            _redis.expire(lock_id, timeout)

    return acquired


def release_lock(lock_id):
    """
    release lock_id
    """
    _redis.delete(lock_id)
