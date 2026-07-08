"""
This module provides utility functions for functional programming, including memoization,
flattening nested sequences, and generating stable hashes for complex objects.
"""

__author__ = "Jay Taylor [@jtaylor]"


import logging
from collections.abc import Iterable
from copy import deepcopy
from inspect import getfullargspec
from time import time

from .memcache import get_memcache_client as cli

try:
    import pickle as _pickle
except ImportError:
    import pickle as _pickle


def flatten(array):
    """
    Flattens an arbitrarily nested sequence of iterables into a single sequence.

    Args:
        array (Iterable): The nested sequence to flatten.

    Yields:
        Elements of the flattened sequence.
    """

    for arr in array:
        if isinstance(arr, Iterable) and not isinstance(arr, str):
            yield from flatten(arr)
        else:
            yield arr


def distinct(seq):
    """
    Retrieves all unique items from an iterable while preserving order.

    Args:
        seq (Iterable): The sequence to process.

    Returns:
        list: A list of unique items in the order they first appear.
    """

    seen = set()
    return [x for x in seq if x not in seen and not seen.add(x)]


# Filtering an iterable to elements of a particular class.
filterByClass = lambda clazz, iterable: [x for x in iterable if isinstance(x, clazz)]  # noqa


def curry(arg1, argc=None):
    """
    Creates a curried version of a function.

    Args:
        arg1 (function): The function to curry.
        argc (int, optional): The number of arguments the function takes. Defaults to the function's argument count.

    Returns:
        function: The curried function.
    """

    if argc is None:
        argc = arg1.__code__.co_argcount

    def wrapper1(*array):
        """
        Returns the curried function or the result if all arguments are provided.
        """

        if len(array) == argc:
            return arg1(*array)

        def wrapper2(*arr_list):
            """
            Combines arguments and calls the original function.
            """

            return arg1(*(array + arr_list))

        return curry(wrapper2, argc - len(array))

    return wrapper1


def memoize(function):
    """
    Decorates a function to cache its results for faster future calls.

    Args:
        function (function): The function to memoize.

    Returns:
        Memoize: The memoized function.
    """

    class Memoize:
        """
        Abstracts the details for method memoization.
        """

        def __init__(self, func):
            """
            Initializes the memoization class.

            Args:
                func (function): The function to memoize.
            """

            self.func = func
            self._cached = {}
            from inspect import getfullargspec, signature

            # Determining whether the function accepts keyword arguments.
            try:
                sig = signature(self.func)
                self._accepts_kw = any(param.kind == param.VAR_KEYWORD for param in sig.parameters.values())
            except (ValueError, TypeError):
                self._accepts_kw = getfullargspec(self.func).varkw is not None

        def __call__(self, *args, **kw):
            """
            Generates the unique key and retrieves the memoized result.

            Args:
                *args: Positional arguments for the function.
                **kw: Keyword arguments for the function.

            Returns:
                The memoized result.
            """

            key = _pickle.dumps((args, kw))
            if key not in self._cached:
                self._cached[key] = self.func(*args, **kw) if self._accepts_kw is True else self.func(*args)

            return deepcopy(self._cached[key])

    return Memoize(function)


class Memoizewithexpiry:
    """
    Decorates a function to cache its results with an expiration time.
    """

    def __init__(self, ttl_seconds):
        """
        Initializes the memoization class with expiry.

        Args:
            ttl_seconds (int): The number of seconds to cache results for.
        """

        self.ttl_seconds = ttl_seconds
        self._cached = {}

    def _clean_cache(self):
        """
        Cleans expired items from the cache.
        """

        now = time()
        expired = [tup[0] for tup in [tup for tup in list(self._cached.items()) if tup[1][0] - now > self.ttl_seconds]]
        logging.info("Cleaning expired items: %s", expired)
        for key in expired:
            del self._cached[key]

    def __call__(self, func):
        """
        Wraps the function with memoization and expiry logic.

        Args:
            func (function): The function to memoize.

        Returns:
            function: The wrapped function.
        """

        self._clean_cache()

        accepts_kw = getfullargspec(func)[2] is not None

        def wrapped(*args, **kw):
            """
            Caches the result or retrieves it if already cached.

            Args:
                *args: Positional arguments for the function.
                **kw: Keyword arguments for the function.

            Returns:
                The cached or computed result.
            """

            key = _pickle.dumps((args, kw))

            if key not in self._cached or time() - self._cached[key][0] > self.ttl_seconds:
                result = func(*args, **kw) if accepts_kw is True else func(*args)
                self._cached[key] = (time(), result)

            return deepcopy(self._cached[key][1])

        return wrapped


class Distmemoizewithexpiry(Memoizewithexpiry):
    """
    Decorates a function to cache its results with an expiration time, using distributed caching.
    """

    def __init__(self, ttl_seconds):
        """
        Initializes the distributed memoization class with expiry.

        Args:
            ttl_seconds (int): The number of seconds to cache results for.
        """

        super().__init__(ttl_seconds)

    def __call__(self, func):
        """
        Wraps the function with distributed memoization and expiry logic.

        Args:
            func (function): The function to memoize.

        Returns:
            function: The wrapped function.
        """

        self._clean_cache()

        accepts_kw = getfullargspec(func)[2] is not None

        def wrapped(*args, **kw):
            """
            Caches the result locally and in distributed cache, or retrieves it if already cached.

            Args:
                *args: Positional arguments for the function.
                **kw: Keyword arguments for the function.

            Returns:
                The cached or computed result.
            """
            # Import moved here to avoid missing package dependency
            import pylibmc

            key = _pickle.dumps((args, kw))
            now = time()

            if key not in self._cached or now - self._cached[key][0] > self.ttl_seconds:
                mc_key = f"memoize.{func.__name__}:{key}"

                result = None
                try:
                    test = cli().get(mc_key)
                    if test is not None:
                        set_ts, result = test
                        if now - set_ts > self.ttl_seconds:
                            result = None

                except pylibmc.Error as err:
                    logging.error("Distmemoizewithexpiry caught %s", str(err))

                if result is None:
                    result = func(*args, **kw) if accepts_kw is True else func(*args)

                self._cached[key] = (time(), result)

                try:
                    cli().set(mc_key, self._cached[key], time=self.ttl_seconds)

                except pylibmc.Error as err:
                    logging.error("Distmemoizewithexpiry caught %s", str(err))

            return deepcopy(self._cached[key][1])

        return wrapped


def saferHash(obj):
    """
    Generates a stable hash for nested structures, safe for recursive/self-referential objects.

    Args:
        obj: The object to hash.

    Returns:
        int: The stable hash value.
    """

    from collections.abc import Mapping, Sequence, Set

    seen = set()
    CYCLE = ("<CYCLIC>",)

    def _tuplify(x):
        oid = id(x)

        if oid in seen:
            return CYCLE

        if x is None or isinstance(x, (bool, int, float)):
            return ("_val", x)
        if isinstance(x, (str, bytes, bytearray)):
            return ("_str", bytes(x) if isinstance(x, (bytes, bytearray)) else x)

        if isinstance(x, Mapping):
            seen.add(oid)
            try:
                items = tuple((repr(k), _tuplify(v)) for k, v in sorted(x.items(), key=lambda kv: repr(kv[0])))
            finally:
                seen.remove(oid)
            return ("_dict",) + items

        if isinstance(x, Set) and not isinstance(x, (str, bytes, bytearray)):
            seen.add(oid)
            try:
                members = tuple(sorted((_tuplify(v) for v in x)))
            finally:
                seen.remove(oid)
            return ("_set",) + members

        if isinstance(x, Sequence) and not isinstance(x, (str, bytes, bytearray)):
            seen.add(oid)
            try:
                seq = tuple(_tuplify(v) for v in x)
            finally:
                seen.remove(oid)
            return ("_seq",) + seq

        try:
            h = hash(x)
            return ("_hashable", h)
        except Exception:
            try:
                return ("_repr", repr(x))
            except Exception:
                return ("_id", oid)

    return hash(_tuplify(obj))
