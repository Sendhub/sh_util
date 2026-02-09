"""
Retry decorator module.

This module provides a retry decorator with exponential backoff functionality. It allows retrying a function or method until it produces a desired outcome or the maximum number of attempts is reached.


"""

__author__ = 'Jay Taylor [@jtaylor]'

import math as _math
import time as _time


def retry(tries, delay=3, backoff=2, desired_outcome=True, fail_value=None):
    """
    Applying a retry decorator with exponential backoff.

    Retries a function or method until it produces a desired outcome.

    Args:
        tries (int): Number of attempts to retry. Must be at least 0.
        delay (int): Sets the initial delay in seconds. Must be greater than 0.
        backoff (int): Factor by which the delay lengthens after each failure. Must be greater than 1.
        desired_outcome: Value or callable to determine success. If callable, the produced value is passed, and success is presumed if it returns True.
        fail_value: Value to return in case of failure.

    Returns:
        The result of the function if successful, or False if all retries fail.

    Raises:
        ValueError: If `backoff` is less than or equal to 1, `tries` is less than 0, or `delay` is less than or equal to 0.
    """

    if backoff <= 1:
        raise ValueError('Backoff must be greater than 1.')

    tries = _math.floor(tries)
    if tries < 0:
        raise ValueError('Tries must be 0 or greater.')

    if delay <= 0:
        raise ValueError('Delay must be greater than 0.')

    def wrapped_retry(_fn):
        """
        Wrapping the function with retry logic.

        Args:
            _fn (callable): The function to be retried.

        Returns:
            callable: The wrapped function with retry logic.
        """

        def retry_fn(*args, **kwargs):
            """
            Executing the retry logic.

            Args:
                *args: Positional arguments for the function.
                **kwargs: Keyword arguments for the function.

            Returns:
                The result of the function if successful, or False if all retries fail.
            """
            # Making variables mutable:
            mtries, mdelay = tries, delay

            # First attempt.
            _rv = _fn(*args, **kwargs)

            while mtries > 0:
                if _rv == desired_outcome or (callable(desired_outcome) and desired_outcome(_rv) is True):
                    # Returning success result.
                    return _rv

                # Consuming an attempt.
                mtries -= 1

                # Waiting before the next attempt.
                _time.sleep(mdelay)

                # Increasing the delay for the next attempt.
                mdelay *= backoff

                # Retrying the function.
                _rv = _fn(*args, **kwargs)

            # Returning failure result after exhausting retries.
            return False

        # Returning the decorated function.
        return retry_fn

    # Returning the decorator.
    return wrapped_retry
