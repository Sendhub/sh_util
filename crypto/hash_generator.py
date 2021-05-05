# encoding: utf-8

"""Hash generator(s)."""

__author__ = 'Jay Taylor [@jtaylor]'

# pylint: disable=R0903,R1718
import datetime
import random
import string
import hashlib
import logging


class HashGenerator():
    """Class for generating hashes of arbitrary lengths, one at a time."""
    def __init__(self, extra_salt=None):
        """Initialize the salt."""
        self._salt = '{0}{1}'.format(
            '*1337-o(]{`Rand0m1um}[)`:.x6x5x4x2x1x9x000000000::',
            datetime.datetime.utcnow().isoformat()
        )

        if extra_salt is not None:
            self._salt = '{0}{1}'.format(self._salt, extra_salt)

        self._salt = self._next_salt()

    def _random_str(self, min_length=256, max_length=513):
        """
        Produce a random string of a semi-random length.

        @param min_length must be greater than 0.
        @param max_length must be greater than 0 and gte than minLength.
        """
        random.seed(self._salt)
        r_len = random.randrange(min_length, max_length)
        symbols = string.printable
        return ''.join([random.choice(symbols) for _ in range(r_len)])

    def _next_salt(self):
        """Produces the next salt value."""
        return '{0}:{1}'.format(
            self._random_str(),
            datetime.datetime.utcnow().isoformat()
        )

    def _next_hash(self):
        """
        This is a DRY way of creating hashes in the application and makes it
        easier to implement a hashing change if one is needed in the future.
        """
        self._salt = self._next_salt()
        digest = hashlib.sha256(self._salt).hexdigest()
        return digest

    def generate(self, length=64):
        """Generate a hash of arbitrary length."""
        length_so_far = 0
        hashes = []

        while length_so_far < length:
            hashes.append(self._next_hash())
            length_so_far += len(hashes[-1])

        return ''.join(hashes)[0:length]


_generator = HashGenerator()


def generate_hash_set(quantity, length=64):
    """Generate a set of hashes, each of a certain length."""
    assert int(quantity) >= 0

    digests = list(set([_generator.generate(length) for _ in range(quantity)]))

    # If there were dupes, generate and append the requisite number of
    # additional items.
    while len(digests) < quantity:
        logging.info('NOTICE :: generateHashSet :: initial generated digests'
                     ' contained duplicates (should not happen (in theory))')
        digest = _generator.generate(length)
        if digest not in digests:
            digests.append(digest)

    return digests
