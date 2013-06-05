# encoding: utf-8

"""Extracts any nested lists."""

__author__ = 'Jay Taylor [@jtaylor]'

import collections as _collections
import logging

try:
    import cPickle as _pickle
except ImportError:
    import pickle as _pickle


def flatten(l, generator=True):
    """Flatten an arbitrarily nested sequence of iterables."""
    for el in l:
        if isinstance(el, _collections.Iterable) and \
           not isinstance(el, basestring):
            for sub in flatten(el):
                yield sub
        else:
            yield el


def distinct(seq):
    """
    Get all unique items from an iterable.  Order preserving.

    Originally found here: http://www.peterbe.com/plog/uniqifiers-benchmark
    """
    seen = set()
    return [x for x in seq if x not in seen and not seen.add(x)]


# Filter an interable to elements of a particular class.
filterByClass = \
    lambda clazz, iterable: filter(lambda x: isinstance(x, clazz), iterable)


def curry(x, argc=None):
    """Curry decorator."""
    if argc is None:
        argc = x.__code__.co_argcount

    def p(*a):
        """@return curried function."""
        if len(a) == argc:
            return x(*a)

        def q(*b):
            """."""
            return x(*(a + b))

        return curry(q, argc - len(a))

    return p


def memoize(fn):
    """Memoization decorator wraps Memoize class."""
    class Memoize(object):
        """Class to abstract away the details for method memoization."""
        def __init__(self, f):
            """@param f Function to memoize."""
            from inspect import getargspec

            self.f = f
            self._cached = {}

            # Determine whether or not the function accepts keyword arguments.
            # NB: getargspec return format is: args, varargs, varkw, defaults
            self._acceptsKw = getargspec(self.f)[2] is not None

        def __call__(self, *args, **kw):
            """Generate the unique key and rtrieve the memoized result."""
            from copy import deepcopy

            #import collections
            #
            ## The key can't contain anything mutable (no lists! convert to
            ## tuples).
            #key = (
            #    tuple(map(
            #        lambda x: tuple(x) if x is not None and \
            #            hasattr(x, '__iter__') \
            #            else (x.keys(), x.values()) if isinstance(x, dict) \
            #            else x,
            #        args
            #    )),
            #    (tuple(kw.keys()), tuple(kw.values()))
            #)
            ##print 'f=%s key=%s' % (self.f, key)

            # That was too messy and anything w/o serialization which does
            # conversion/coercion can produce potentially incorrect results,
            # just use pickle instead:

            key = _pickle.dumps((args, kw))
            #print 'key=%s' % key

            if key not in self._cached:
                self._cached[key] = self.f(*args, **kw) \
                    if self._acceptsKw is True else self.f(*args)

            # Return a copy because we don't want the invoker to then modify the
            # result that will be returned forever.
            return deepcopy(self._cached[key])

    return Memoize(fn)


class memoizeWithExpiry(object):
    """Memoization decorator wraps Memoize class."""

    def __init__(self, ttlSeconds):
        """@param ttlSeconds Number of seconds to cache results for."""
        self.ttlSeconds = ttlSeconds
        self._cached = {}

    def _cleanCache(self):
        """Clean expired items from the cache."""
        from time import time

        #maxTime = time() - self.ttlSeconds
        now = time()

        expired = map(
            lambda tup: tup[0],
            filter(
                lambda tup: tup[1][0] - now > self.ttlSeconds,
                self._cached.items()
            )
        )

        logging.info('Cleaning expired items: {0}'.format(expired))

        for k in expired:
            del self._cached[k]

    def __call__(self, fn):
        """Call override."""
        from inspect import getargspec

        self._cleanCache()

        # Determine whether or not the function accepts keyword arguments.
        # NB: getargspec return format is: args, varargs, varkw, defaults
        acceptsKw = getargspec(fn)[2] is not None

        def wrapped(*args, **kw):
            """Inner function"""
            from copy import deepcopy
            from time import time

            key = _pickle.dumps((args, kw))

            if key not in self._cached or \
                time() - self._cached[key][0] > self.ttlSeconds:
                result = fn(*args, **kw) \
                    if acceptsKw is True else fn(*args)
                self._cached[key] = (time(), result)

            # Return a copy because we don't want the invoker to then modify the
            # result that will be returned forever.
            return deepcopy(self._cached[key][1])

        return wrapped


class distMemoizeWithExpiry(memoizeWithExpiry):
    """Memoization decorator wraps Memoize class."""

    def __init__(self, ttlSeconds):
        """@param ttlSeconds Number of seconds to cache results for."""
        super(distMemoizeWithExpiry, self).__init__(ttlSeconds)

    def __call__(self, fn):
        """Call override."""
        from inspect import getargspec

        self._cleanCache()

        # Determine whether or not the function accepts keyword arguments.
        # NB: getargspec return format is: args, varargs, varkw, defaults
        acceptsKw = getargspec(fn)[2] is not None

        def wrapped(*args, **kw):
            """Inner function"""
            from copy import deepcopy
            from time import time
            from .memcache import getMemcacheClient as cli
            import pylibmc

            key = _pickle.dumps((args, kw))
            now = time()

            if key not in self._cached or \
                now - self._cached[key][0] > self.ttlSeconds:
                # Memcache key.
                mcKey = 'memoize.{0}:{1}'.format(fn.__name__, key)

                result = None
                try:
                    test = cli().get(mcKey)
                    if test is not None:
                        setTs, result = test
                        #logging.debug(
                        #    'result is {0} seconds old'.format(now - setTs)
                        #)
                        if now - setTs > self.ttlSeconds:
                            #logging.debug('result was too old')
                            result = None
                        #else:
                        #    logging.info('found mcKey={0}'.format(mcKey))

                except pylibmc.Error, e:
                    logging.error('distMemoizeWithExpiry caught {0}'.format(e))

                if result is None:
                    # Calculate result.
                    result = fn(*args, **kw) if acceptsKw is True else fn(*args)

                # Store result locally.
                self._cached[key] = (time(), result)

                try:
                    # Store result in memcache.
                    cli().set(mcKey, self._cached[key], time=self.ttlSeconds)

                except pylibmc.Error, e:
                    logging.error('distMemoizeWithExpiry caught {0}'.format(e))


            # Return a copy because we don't want the invoker to then modify the
            # result that will be returned forever.
            return deepcopy(self._cached[key][1])

        return wrapped


def saferHash(o):
    """Get a consistent hash for objects, even when they are a dictionary or contain dictionaries."""
    def tuplifyDicts(o):
        """Recursively turn dicts into sorted tuples."""
        if not hasattr(o, '__iter__'):
            return o
        if isinstance(o, dict):
            return tuple(sorted(map(tuplifyDicts, o.items())))
        return tuple(sorted(map(tuplifyDicts, o)))
    return hash(tuplifyDicts(o))

