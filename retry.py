# encoding: utf-8

"""
Retry decorator.

Pulled from
"""

__author__ = 'Jay Taylor [@jtaylor]'


import math as _math, time as _time


def retry(tries, delay=3, backoff=2, desiredOutcome=True, failValue=None):
    """
    Retry decorator with exponential backoff
    Retries a function or method until it produces a desired outcome.

    @param delay int Sets the initial delay in seconds, and backoff sets the
        factor by which the delay should lengthen after each failure.
    @param backoff int Must be greater than 1, or else it isn't really a
        backoff.  Tries must be at least 0, and delay greater than 0.
    @param desiredOutcome Can be a value or a callable.  If it is a callable the
        produced value will be passed and success is presumed if the invocation
        returns True.
    @param failValue Value to return in the case of failure.
    """

    if backoff <= 1:
        raise ValueError('backoff must be greater than 1')

    tries = _math.floor(tries)
    if tries < 0:
        raise ValueError('tries must be 0 or greater')

    if delay <= 0:
        raise ValueError('delay must be greater than 0')

    def wrappedRetry(fn):
        """Decorative wrapper."""
        def retryFn(*args, **kwargs):
            """The function which does the actual retrying."""
            # Make mutable:
            mtries, mdelay = tries, delay

            # First attempt.
            rv = fn(*args, **kwargs)

            while mtries > 0:
                if rv == desiredOutcome or \
                    (callable(desiredOutcome) and desiredOutcome(rv) is True):
                    # Success.
                    return rv

                # Consume an attempt.
                mtries -= 1

                # Wait...
                _time.sleep(mdelay)

                # Make future wait longer.
                mdelay *= backoff

                # Try again.
                rv = fn(*args, **kwargs)

            # Ran out of tries :-(
            return False

        # True decorator -> decorated function.
        return retryFn

    # @retry(arg[, ...]) -> decorator.
    return wrappedRetry

