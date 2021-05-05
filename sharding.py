# -*- coding: utf-8 -*-

"""Sharding resources."""

__author__ = 'Jay Taylor [@jtaylor]'
# pylint: disable=C0415,W0212,W1202,R0903,R1711,E0402,C0301,C0103,E1101
import logging
import pylibmc
import settings
from sh_util.db import connections
from .singleton import Singleton
from .memcache import get_memcache_client
from .retry import retry
#from .functional import Memoizewithexpiry


def coerceIdToShardName(shardOrShardId):
    """Coerce a physical shard id to be the name of the shards connection."""
    if str(shardOrShardId).isdigit():
        return 'shard_{0}'.format(shardOrShardId)
    return shardOrShardId


class ShardException(Exception):
    """Base shard exception."""

class ShardLookupFailure(ShardException):
    """Raised when a shard lookup fails."""


class ShardedConnection(object):
    """Shard connection resoure for `with` statements."""
    def __init__(self, connectionNameOrId):
        """
        @param connection_name str or int Name or id of database connection
            to use.
        """
        self.connectionName = 'shard_{0}'.format(connectionNameOrId) if \
            isinstance(connectionNameOrId, int) or \
            isinstance(connectionNameOrId, int) or \
            connectionNameOrId.isdigit() is True else connectionNameOrId

        # Keey a reference to the original connection so it can be restored
        # afterwards.
        self.originalShard = None

    def __enter__(self):
        """
        Start of `with` statement.  Change the default connection to the
        specified shard for the duration of the `with` block.
        """
        from .db import switchDefaultDatabase
        self.originalShard = ShardedResource.getCurrentShard()
        switchDefaultDatabase(self.connectionName)
        return self.connectionName

    def __exit__(self, _type, value, traceback):
        """
        End of `with` statement.  Restore the original connection as the
        default.
        """
        from .db import switchDefaultDatabase
        switchDefaultDatabase(self.originalShard)


def userIdToLogicalShardId(userId):
    """Takes a user id and returns the logical shard id."""
    return int(userId) % settings.NUM_LOGICAL_SHARDS


class ShardEvent(Singleton):
    """Singleton for shard event notification publishing and subscriptions."""

    def __init__(self):
        """Initialize subscribers."""
        super(ShardEvent, self).__init__()
        self.subscribers = {}

    def publish(self, event, data):
        """Publish a new event."""
        # Notify event subscribers.
        for subscriber in self.subscribers.get(event, []):
            subscriber(data)

        # Notify global subscribers.
        for subscriber in self.subscribers.get('*', []):
            subscriber(event, data)

    def subscribe(self, event, fn):
        """
        Subscribe a function to an event.  To subscribe to all events, use
        the event name '*'.

        @param event str Event name.
        @param fn Function to be subscribed.
        """
        if event not in self.subscribers:
            self.subscribers[event] = []

        self.subscribers[event].append(fn)

    def unsubscribe(self, event, fn):
        """Unsubscribe a function from an event."""
        if event not in self.subscribers:
            return

        try:
            self.subscribers[event].remove(self.subscribers[event].index(fn))
        except ValueError:
            pass


# Prune connection keys down to only shard connections.
_shards = [c for c in connections() if c.startswith('shard_')]

class ShardedResource(object):
    """Determine which shard a given resource lives on."""

    @staticmethod
    def getCurrentShard():
        """Determine and return the name of the current shard."""
        for c in ShardedResource.allShardConnectionNames():
            if settings.DATABASES[c] == settings.DATABASES['default']:
                return c

        raise Exception('No active shard was found')

    @staticmethod
    def shardNameToId(shard):
        """
        Extract the shard id from the shard connection name.

        @return int containing the shard id
        """
        return int(shard[shard.rindex('_') + 1:])

    @staticmethod
    def shardCachePrefix(model, column):
        """
        Generate a shard cache prefix for a given model and column.

        @param model Model class object.
        @param column String containing the relvent column/field name.
        """
        return 'shard:{0}-{1}:'.format(model.__name__, column)

    @staticmethod
    def shardCacheKey(model, column, value):
        """
        Generate a shard cache prefix for a given model and column.

        @param model Model Class object.
        @param column str String containing the relvent column/field name.
        @param value Any unique part of key.

        @return str Key.
        """
        return 'shard:{0}-{1}:{2}'.format(model.__name__, column, value)

    @staticmethod
    def _cacheSet(key, value):
        """Set a value in the cache."""
        try:
            cli = get_memcache_client()
            value = cli.set(key, value)

        except pylibmc.Error as e:
            logging.info('ShardedResource._cacheSet error: {0}'.format(e))

    @staticmethod
    def _cacheGet(key):
        """Get a value from the cache."""
        try:
            cli = get_memcache_client()
            return cli.get(key)

        except pylibmc.Error as e:
            logging.info('ShardedResource._cacheSet error: {0}'.format(e))
            return None

    @staticmethod
    def warmShardIdCache(model, column):
        """Warm the cache mapping of a model's column to a shard id."""
        prefix = ShardedResource.shardCachePrefix(model, column)

        try:
            cli = get_memcache_client()

            for shard in _shards:
                shardId = str(ShardedResource.shardNameToId(shard))

                values = model.objects.using(shard) \
                    .only(column).values_list(column, flat=True)

                mapping = dict([(str(v), shardId) for v in values])

                if len(mapping) > 0:
                    cli.set_multi(mapping, key_prefix=prefix)

        except pylibmc.Error as e:
            logging.info(
                'ShardedResource.warmShardCache :: memcache error: {0}' \
                    .format(e)
            )

    @retry(tries=3)
    @staticmethod
    def setShardId(model, column, value, shardId):
        """
        Explicitly set the location for a model/column/value in the cache.
        """
        try:
            cli = get_memcache_client()
            key = ShardedResource.shardCacheKey(model, column, value)
            cli.set(key, str(shardId))

            return True

        except pylibmc.Error as e:
            logging.error('memcache error: {0}'.format(e))
            return False

    @staticmethod
    def findShardId(model, column, value, useCache=True):
        """
        Determine which shard a record lives on.

        @param model Model class to filter with.
        @param column String name of column to filter on.
        @param value Any Value of column to match.
        @param useCache Boolean defaults to True. Whether or not to use the
            cache.

        @return int Shard id, or None if record not located.
        """
        key = ShardedResource.shardCacheKey(model, column, value)

        if useCache is True:
            cached = ShardedResource._cacheGet(key)
            if cached is not None:
                return int(cached)

        for shard in _shards:
            n = model.objects.using(shard).filter(**{column: value}).count()
            if n > 0:
                shardId = ShardedResource.shardNameToId(shard)
                if useCache is True:
                    ShardedResource._cacheSet(key, str(shardId))
                logging.info(
                    'FOUND {0}.{1}={2} on shardId={3}'.format(
                        model.__name__,
                        column,
                        value,
                        shardId
                    )
                )
                return shardId

        # Not found
        raise ShardLookupFailure(
            'No shard containing {0}.{1}="{2}" found'.format(
                model.__name__,
                column,
                value
            )
        )

    @staticmethod
    def _realUserIdToPhysicalShardId(userId):
        """
        Query the db to determine which physical shard a user-id exists on.
        """
        from sh_util.db import db_query

        res = db_query(
            'SELECT "physical_shard_id" FROM "LogicalShard" WHERE "id" = {0}' \
                .format(userIdToLogicalShardId(userId)),
            using='shard_1'
        )

        if len(res) == 0 or len(res[0]) == 0:
            raise ShardLookupFailure(
                'Unable to find shard for user_id={0}'.format(userId)
            )

        physicalShardId = res[0][0]

        return physicalShardId

    @staticmethod
    # @TODO MEMOIZATION TEMPORARILY DISABLED
    #@Memoizewithexpiry(180)
    def _cachingUserIdToPhysicalShardId(userId):
        """Memoizing function to algorythmically resolve user-id to shard-id."""
        logicalShardId = userIdToLogicalShardId(userId)

        try:
            key = 'logicalShard:{0}'.format(logicalShardId)
            value = get_memcache_client().get(key)
            if value is not None:
                return value

            shardId = ShardedResource._realUserIdToPhysicalShardId(userId)
            get_memcache_client().set(key, shardId, time=180)

        except pylibmc.Error:
            if 'shardId' not in vars():
                shardId = ShardedResource._realUserIdToPhysicalShardId(userId)

        return shardId

    @staticmethod
    def userIdToPhysicalShardId(userId, useCache=True):
        """
        Wrapper around userIdToPhysicalShardId to allow cache and memoization
        to be disabled.
        """
        if useCache is not True:
            return ShardedResource._realUserIdToPhysicalShardId(userId)

        return ShardedResource._cachingUserIdToPhysicalShardId(userId)

    @staticmethod
    def allShardConnectionNames():
        """Get all shard connection names."""
        return _shards

    @staticmethod
    def _subscribeToShardEvents():
        """Subscribe to "movedUser" shard event."""

        def movedUser(data):
            """Trigger memcache updates when a user is moved."""
            from django.contrib.auth.models import User
            from main.models import ExtendedUser, PhoneNumber

            userId = data['user_id']
            shardId = str(data['shardId'])

            ShardedResource.setShardId(User, 'user_id', userId, shardId)

            ShardedResource.setShardId(
                ExtendedUser,
                'user_id',
                userId,
                shardId
            )

            with ShardedConnection(shardId) as _:
                number = ExtendedUser.objects.get(user=userId) \
                    .twilio_phone_number.number
                ShardedResource.setShardId(
                    PhoneNumber,
                    'number',
                    number,
                    shardId
                )

        ShardEvent().subscribe('movedUser', movedUser)


ShardedResource()._subscribeToShardEvents()


userIdToShardName = lambda userId: 'shard_{0}'.format(ShardedResource.userIdToPhysicalShardId(userId))


class ShardedAuthenticationMiddleware(object):
    """Shard selection django middleware."""
    @staticmethod
    def process_request(request):
        """Override for the stock django authentication middleware."""
        from django.contrib.auth import get_user, SESSION_KEY
        #from django.contrib import auth
        from django.utils.functional import SimpleLazyObject

        assert hasattr(request, 'session'), 'The Django authentication ' \
            'middleware requires session middleware to be installed. Edit ' \
            'your MIDDLEWARE_CLASSES setting to insert ' \
            '"django.contrib.sessions.middleware.SessionMiddleware".'

        userId = int(request.session.get(SESSION_KEY, -1))

        if userId != -1:
            shardId = ShardedResource().userIdToPhysicalShardId(userId)

            logging.info(
                '[SHARD-SELECTOR] Selecting shard #{0} for user_id={1}' \
                .format(shardId, userId)
            )

            from .db import switchDefaultDatabase
            switchDefaultDatabase('shard_{0}'.format(shardId))

        else:
            logging.info(
                '[SHARD-SELECTOR] USER DOES NOT LOOK LOGGED IN RIGHT NOW'
            )

        request.user = SimpleLazyObject(lambda: get_user(request))
        #print 'request.user = %s' % auth.get_user(request)

        return None
