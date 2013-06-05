__author__ = 'brock'

import settings

_redis = settings.REDIS

def acquireLock(lockId, timeout=60):

    # make sure these redis locks always have a valid timeout
    assert timeout > 0

    acquired = _redis.setnx(lockId, "true")
    if acquired:
        _redis.expire(lockId, timeout)
    else:
        # if there is no timeout set and we couldn't acquire the lock
        # then make sure that we set a timeout on the lock so we
        # cant have a deadlock
        if not _redis.ttl(lockId):
            _redis.expire(lockId, timeout)

    return acquired

def releaseLock(lockId):
    _redis.delete(lockId)
