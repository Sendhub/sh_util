"""
Circuit breaker for phone number release attempts.

Guards against hammering a carrier (Twilio/Bandwidth) with release attempts
during an outage: after a run of consecutive failures for a gateway, further
release attempts for that gateway are skipped for 24 hours rather than
retried one-by-one across a batch.

The breaker state lives in Redis (``settings.REDIS``). A service that does not
configure ``settings.REDIS`` simply runs without a breaker: it is never open and
every call below is a no-op, so the carrier calls behave as they did before.
"""

import functools
import logging
import sys

try:
    import settings
except ImportError:
    # when unit testing is invoked
    sys.path.append("/opt/sendhub/inforeach/app")
    import settings

CONSECUTIVE_FAILURE_THRESHOLD = 3
BREAKER_OPEN_SECONDS = 60 * 60 * 24


def _fail_open(default):
    """
    Decorator: a Redis outage must never block a phone release or purchase.
    Any Redis error is logged and the breaker behaves as if it were not there.
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as err:
                logging.warning(f"Phone-release circuit breaker unavailable ({func.__name__}): {err}")
                return default

        return wrapper

    return decorator


def _redis():
    """The service's Redis client, or None when the service has none configured."""
    return getattr(settings, "REDIS", None)


def _failures_key(gateway):
    return f"phone_release:failures:{gateway}"


def _breaker_key(gateway):
    return f"phone_release:breaker_open:{gateway}"


@_fail_open(False)
def is_breaker_open(gateway):
    """True if release attempts for this gateway are currently paused."""
    client = _redis()
    if client is None:
        return False
    return bool(client.exists(_breaker_key(gateway)))


@_fail_open(None)
def record_success(gateway):
    """Clear the consecutive-failure count for this gateway."""
    client = _redis()
    if client is None:
        return
    client.delete(_failures_key(gateway))


@_fail_open(None)
def breaker_ttl(gateway):
    """Seconds remaining on an open breaker, or None if it is not open.

    Redis returns -2 when the key is absent and -1 when it has no expiry.
    """
    client = _redis()
    if client is None:
        return None
    ttl = client.ttl(_breaker_key(gateway))
    return ttl if ttl and ttl > 0 else None


@_fail_open(0)
def failure_count(gateway):
    """Current consecutive-failure count for this gateway."""
    client = _redis()
    if client is None:
        return 0
    raw = client.get(_failures_key(gateway))
    if raw is None:
        return 0
    try:
        return int(raw)
    except (TypeError, ValueError):
        return 0


@_fail_open(False)
def reset_breaker(gateway):
    """
    Manually clear an open breaker and its failure counter for this gateway.

    Intended for the case where the breaker was tripped by a bug rather than a
    real carrier outage (e.g. a success status code misclassified as a failure):
    once the misclassification is fixed there is no reason to sit out the
    remaining 24h. Returns True if a breaker was actually open.

    Deliberately separate from record_success(), which only clears the failure
    counter — a success cannot occur while the breaker is open, since callers
    check is_breaker_open() before attempting a release.
    """
    client = _redis()
    if client is None:
        return False
    was_open = bool(client.exists(_breaker_key(gateway)))
    client.delete(_breaker_key(gateway))
    client.delete(_failures_key(gateway))
    logging.info(f"Phone-release circuit breaker reset for {gateway} (was_open={was_open})")
    return was_open


@_fail_open(None)
def record_failure(gateway):
    """
    Record a release failure for this gateway. After
    CONSECUTIVE_FAILURE_THRESHOLD consecutive failures, opens the breaker for
    BREAKER_OPEN_SECONDS so further attempts are skipped without hitting the
    carrier API.
    """
    client = _redis()
    if client is None:
        return
    count = client.incr(_failures_key(gateway))
    if count >= CONSECUTIVE_FAILURE_THRESHOLD:
        client.set(_breaker_key(gateway), 1, ex=BREAKER_OPEN_SECONDS)
        client.delete(_failures_key(gateway))
        logging.error(f"Circuit breaker tripped for {gateway} phone release after {count} consecutive failures — pausing release attempts for 24h")
