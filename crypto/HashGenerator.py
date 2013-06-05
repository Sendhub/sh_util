# encoding: utf-8

"""Hash generator(s)."""

__author__ = 'Jay Taylor [@jtaylor]'


import datetime, random, string, hashlib
import logging


class HashGenerator(object):
    """Class for generating hashes of arbitrary lengths, one at a time."""
    def __init__(self, extraSalt=None):
        """Initialize the salt."""
        self._salt = '{0}{1}'.format(
            '*1337-o(]{`Rand0m1um}[)`:.x6x5x4x2x1x9x000000000::',
            datetime.datetime.utcnow().isoformat()
        )

        if extraSalt is not None:
            self._salt = '{0}{1}'.format(self._salt, extraSalt)

        self._salt = self._nextSalt()

    def _randomStr(self, minLength=256, maxLength=513):
        """
        Produce a random string of a semi-random length.

        @param minLength must be greater than 0.
        @param maxLength must be greater than 0 and gte than minLength.
        """
        random.seed(self._salt)
        rLen = random.randrange(minLength, maxLength)
        symbols = string.printable
        return ''.join([random.choice(symbols) for _ in xrange(rLen)])

    def _nextSalt(self):
        """Produces the next salt value."""
        return '{0}:{1}'.format(
            self._randomStr(),
            datetime.datetime.utcnow().isoformat()
        )

    def _nextHash(self):
        """
        This is a DRY way of creating hashes in the application and makes it
        easier to implement a hashing change if one is needed in the future.
        """
        self._salt = self._nextSalt()
        digest = hashlib.sha256(self._salt).hexdigest()
        return digest

    def generate(self, length=64):
        """Generate a hash of arbitrary length."""
        lengthSoFar = 0
        hashes = []

        while lengthSoFar < length:
            hashes.append(self._nextHash())
            lengthSoFar += len(hashes[-1])

        return ''.join(hashes)[0:length]


_generator = HashGenerator()

def generateHashSet(quantity, length=64):
    """Generate a set of hashes, each of a certain length."""
    assert int(quantity) >= 0

    digests = list(set([_generator.generate(length) for _ in xrange(quantity)]))

    # If there were dupes, generate and append the requisite number of
    # additional items.
    while len(digests) < quantity:
        logging.info('NOTICE :: generateHashSet :: ' \
            'initial generated digests contained duplicates (should not ' \
            'happen (in theory))')
        digest = _generator.generate(length)
        if digest not in digests:
            digests.append(digest)

    return digests


#_generator = HashGenerator()
#
#def test1():
##    from HashGenerator import HashGenerator
#    z = [_generator.generate(64) for _ in xrange(1000)]
#
#def test2():
##    from HashGenerator import generateHashSet
#    generateHashSet(1000, length=64)
#
#
#if __name__ == '__main__':
#    from timeit import Timer
#    t1 = Timer("test1()", "from __main__ import test1, HashGenerator")
#    #t2 = Timer("test2()", "from __main__ import test2, generateHashSet")
#    print 'strating'
#    print 'T1', t1.timeit()
#    #print 'T2', t2.timeit()

