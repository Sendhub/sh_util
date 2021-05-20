# encoding: utf-8

"""Extracts any nested lists."""

__author__ = 'Jay Taylor [@jtaylor]'
# pylint: disable=R0903,W0235,E1101,C0123

import collections as _collections
import logging
from inspect import getfullargspec
from copy import deepcopy
from time import time
import pylibmc
from sh_util.memcache import get_memcache_client as cli
try:
    import pickle as _pickle
except ImportError:
    import pickle as _pickle


def flatten(array):
    """Flatten an arbitrarily nested sequence of iterables."""
    for arr in array:
        if isinstance(arr, _collections.Iterable) and \
           not isinstance(arr, str):
            for sub in flatten(arr):
                yield sub
        else:
            yield arr


def distinct(seq):
    """
    Get all unique items from an iterable.  Order preserving.

    Originally found here: http://www.peterbe.com/plog/uniqifiers-benchmark
    """
    seen = set()
    return [x for x in seq if x not in seen and not seen.add(x)]


# Filter an interable to elements of a particular class.
filterByClass = lambda clazz, iterable: [x for x in iterable if isinstance(x, clazz)]  # noqa


def curry(arg1, argc=None):
    """Curry decorator."""
    if argc is None:
        argc = arg1.__code__.co_argcount

    def wrapper1(*array):
        """@return curried function."""
        if len(array) == argc:
            return arg1(*array)

        def wrapper2(*arr_list):
            """."""
            return arg1(*(array + arr_list))

        return curry(wrapper2, argc - len(array))

    return wrapper1


def memoize(function):
    """Memoization decorator wraps Memoize class."""
    class Memoize():
        """Class to abstract away the details for method memoization."""
        def __init__(self, func):
            """@param func Function to memoize."""
            self.func = func
            self._cached = {}

            # Determine whether or not the function accepts keyword arguments.
            # NB: getargspec return format is: args, varargs, varkw, defaults
            self._accepts_kw = getfullargspec(self.func)[2] is not None

        def __call__(self, *args, **kw):
            """
            Generate the unique key and rtrieve the memoized result.
            That was too messy and anything w/o serialization which does
            conversion/coercion can produce potentially incorrect results,
            just use pickle instead:
            """
            key = _pickle.dumps((args, kw))
            if key not in self._cached:
                self._cached[key] = self.func(*args, **kw) \
                    if self._accepts_kw is True else self.func(*args)

            # Return a copy because we don't want the invoker to
            # then modify the result that will be returned forever.
            return deepcopy(self._cached[key])

    return Memoize(function)


class Memoizewithexpiry():
    """Memoization decorator wraps Memoize class."""

    def __init__(self, ttl_seconds):
        """@param ttl_seconds Number of seconds to cache results for."""
        self.ttl_seconds = ttl_seconds
        self._cached = {}

    def _clean_cache(self):
        """Clean expired items from the cache."""
        now = time()
        expired = [tup[0] for tup in [tup for tup in list(self._cached.items()) if  # noqa
                                      tup[1][0] - now > self.ttl_seconds]]
        logging.info('Cleaning expired items: %s', expired)
        for key in expired:
            del self._cached[key]

    def __call__(self, func):
        """Call override."""
        self._clean_cache()

        # Determine whether or not the function accepts keyword arguments.
        # NB: getargspec return format is: args, varargs, varkw, defaults
        accepts_kw = getfullargspec(func)[2] is not None

        def wrapped(*args, **kw):
            """Inner function"""
            key = _pickle.dumps((args, kw))

            if key not in self._cached or \
                    time() - self._cached[key][0] > self.ttl_seconds:
                result = func(*args, **kw) \
                    if accepts_kw is True else func(*args)
                self._cached[key] = (time(), result)

            # Return a copy because we don't want the invoker to then modify
            # the result that will be returned forever.
            return deepcopy(self._cached[key][1])

        return wrapped


class Distmemoizewithexpiry(Memoizewithexpiry):
    """Memoization decorator wraps Memoize class."""

    def __init__(self, ttl_seconds):
        """@param ttl_seconds Number of seconds to cache results for."""
        super().__init__(ttl_seconds)

    def __call__(self, func):
        """Call override."""
        self._clean_cache()

        # Determine whether or not the function accepts keyword arguments.
        # NB: getargspec return format is: args, varargs, varkw, defaults
        accepts_kw = getfullargspec(func)[2] is not None

        def wrapped(*args, **kw):
            """Inner function"""
            key = _pickle.dumps((args, kw))
            now = time()

            if key not in self._cached or \
                    now - self._cached[key][0] > self.ttl_seconds:
                # Memcache key.
                mc_key = 'memoize.{0}:{1}'.format(func.__name__, key)

                result = None
                try:
                    test = cli().get(mc_key)
                    if test is not None:
                        set_ts, result = test
                        if now - set_ts > self.ttl_seconds:
                            result = None

                except pylibmc.Error as err:
                    logging.error('Distmemoizewithexpiry caught %s', str(err))

                if result is None:
                    # Calculate result.
                    result = func(*args, **kw) \
                        if accepts_kw is True else func(*args)

                # Store result locally.
                self._cached[key] = (time(), result)

                try:
                    # Store result in memcache.
                    cli().set(mc_key, self._cached[key], time=self.ttl_seconds)

                except pylibmc.Error as err:
                    logging.error('Distmemoizewithexpiry caught %s', str(err))

            # Return a copy because we don't want the invoker to then modify
            # the result that will be returned forever.
            return deepcopy(self._cached[key][1])

        return wrapped


def safer_hash(obj):
    """
    Get a consistent hash for objects, even when they are
    a dictionary or contain dictionaries.
    """
    def tuplify_dicts(obj):
        """Recursively turn dicts into sorted tuples."""
        if not type(obj) == str and type(obj) == tuple:
            return obj
        if isinstance(obj, dict):
            return tuple(sorted(map(tuplify_dicts, list(obj.items()))))
        return tuple(sorted(map(tuplify_dicts, obj)))
    return hash(tuplify_dicts(obj))
