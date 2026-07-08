"""
Sharding resources module.

This module provides utilities for managing sharded resources, including
shard connections, shard event handling, and shard-based caching.
"""

__author__ = "Jay Taylor [@jtaylor]"

import logging

# pylibmc and settings imports kept at module level but note: pylibmc may cause issues if not installed
# Consider moving to function level if package availability is a concern
import pylibmc
import settings

from .db import connections
from .memcache import get_memcache_client
from .retry import retry
from .singleton import Singleton


def coerceIdToShardName(shardOrShardId):
    """
    Coercing a physical shard ID to the name of the shard's connection.

    Args:
        shardOrShardId (str or int): The shard ID or name.

    Returns:
        str: The name of the shard connection.
    """

    if str(shardOrShardId).isdigit():
        return f"shard_{shardOrShardId}"
    return shardOrShardId


class ShardException(Exception):
    """
    Base exception for shard-related errors.
    """


class ShardLookupFailure(ShardException):
    """
    Exception raised when a shard lookup fails.
    """


class ShardedConnection:
    """
    Managing shard connections for `with` statements.

    This class allows switching the default database connection to a specific
    shard for the duration of a `with` block.
    """

    def __init__(self, connectionNameOrId):
        """
        Initializing the shard connection.

        Args:
            connectionNameOrId (str or int): The name or ID of the database
                connection to use.
        """

        self.connectionName = f"shard_{connectionNameOrId}" if isinstance(connectionNameOrId, int) or connectionNameOrId.isdigit() else connectionNameOrId

        # Keeping a reference to the original connection to restore it later.
        self.originalShard = None

    def __enter__(self):
        """
        Starting the `with` statement by switching to the specified shard.

        Returns:
            str: The name of the shard connection.
        """

        from .db import switchDefaultDatabase

        self.originalShard = ShardedResource.getCurrentShard()
        switchDefaultDatabase(self.connectionName)
        return self.connectionName

    def __exit__(self, _type, value, traceback):
        """
        Ending the `with` statement by restoring the original shard connection.
        """

        from .db import switchDefaultDatabase

        switchDefaultDatabase(self.originalShard)


def userIdToLogicalShardId(userId):
    """
    Calculating the logical shard ID for a given user ID.

    Args:
        userId (int): The user ID.

    Returns:
        int: The logical shard ID.
    """

    return int(userId) % settings.NUM_LOGICAL_SHARDS


class ShardEvent(Singleton):
    """
    Singleton for managing shard event notifications.

    This class allows publishing and subscribing to shard events.
    """

    def __init__(self):
        """
        Initializing the shard event manager with an empty subscriber list.
        """
        super().__init__()
        self.subscribers = {}

    def publish(self, event, data):
        """
        Publishing a new event to notify subscribers.

        Args:
            event (str): The name of the event.
            data: The data associated with the event.
        """

        # Notifying event-specific subscribers.
        for subscriber in self.subscribers.get(event, []):
            subscriber(data)

        # Notifying global subscribers.
        for subscriber in self.subscribers.get("*", []):
            subscriber(event, data)

    def subscribe(self, event, fn):
        """
        Subscribing a function to an event.

        Args:
            event (str): The name of the event.
            fn (callable): The function to subscribe.
        """

        if event not in self.subscribers:
            self.subscribers[event] = []

        self.subscribers[event].append(fn)

    def unsubscribe(self, event, fn):
        """
        Unsubscribing a function from an event.

        Args:
            event (str): The name of the event.
            fn (callable): The function to unsubscribe.
        """

        if event not in self.subscribers:
            return

        try:
            self.subscribers[event].remove(self.subscribers[event].index(fn))
        except ValueError:
            pass


# Prune connection keys down to only shard connections.
# Lazy initialization to avoid import errors when parent app is not yet configured
_shards = None


def _get_shards():
    """Lazy initialization of shards list."""
    global _shards
    if _shards is None:
        try:
            _shards = [c for c in connections() if c.startswith("shard_")]
        except (ImportError, AttributeError):
            # Parent app not configured yet, return empty list
            _shards = []
    return _shards


class ShardedResource:
    """
    Managing shard resources and determining shard locations.
    """

    @staticmethod
    def getCurrentShard():
        """
        Determining and returning the name of the current shard.

        Returns:
            str: The name of the current shard.

        Raises:
            Exception: If no active shard is found.
        """

        for c in ShardedResource.allShardConnectionNames():
            if settings.DATABASES[c] == settings.DATABASES["default"]:
                return c

        raise Exception("No active shard was found")

    @staticmethod
    def shardNameToId(shard):
        """
        Extracting the shard ID from the shard connection name.

        Args:
            shard (str): The name of the shard connection.

        Returns:
            int: The shard ID.
        """

        return int(shard[shard.rindex("_") + 1 :])

    @staticmethod
    def shardCachePrefix(model, column):
        """
        Generating a shard cache prefix for a given model and column.

        Args:
            model: The model class object.
            column (str): The relevant column/field name.

        Returns:
            str: The shard cache prefix.
        """

        return f"shard:{model.__name__}-{column}:"

    @staticmethod
    def shardCacheKey(model, column, value):
        """
        Generating a shard cache key for a given model, column, and value.

        Args:
            model: The model class object.
            column (str): The relevant column/field name.
            value: The unique part of the key.

        Returns:
            str: The shard cache key.
        """

        return f"shard:{model.__name__}-{column}:{value}"

    @staticmethod
    def _cacheSet(key, value):
        """Set a value in the cache."""
        try:
            cli = get_memcache_client()
            value = cli.set(key, value)

        except pylibmc.Error as e:
            logging.info(f"ShardedResource._cacheSet error: {e}")

    @staticmethod
    def _cacheGet(key):
        """Get a value from the cache."""
        try:
            cli = get_memcache_client()
            return cli.get(key)

        except pylibmc.Error as e:
            logging.info(f"ShardedResource._cacheSet error: {e}")
            return None

    @staticmethod
    def warmShardIdCache(model, column):
        """Warm the cache mapping of a model's column to a shard id."""
        prefix = ShardedResource.shardCachePrefix(model, column)

        try:
            cli = get_memcache_client()

            for shard in _get_shards():
                shardId = str(ShardedResource.shardNameToId(shard))

                values = model.objects.using(shard).only(column).values_list(column, flat=True)

                mapping = {str(v): shardId for v in values}

                if len(mapping) > 0:
                    cli.set_multi(mapping, key_prefix=prefix)

        except pylibmc.Error as e:
            logging.info(f"ShardedResource.warmShardCache :: memcache error: {e}")

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
            logging.error(f"memcache error: {e}")
            return False

    @staticmethod
    def findShardId(model, column, value, useCache=True):
        """
        Determining which shard a record resides on.

        Args:
            model: The model class to filter with.
            column (str): The name of the column to filter on.
            value: The value of the column to match.
            useCache (bool, optional): Whether to use the cache. Defaults to True.

        Returns:
            int: The shard ID where the record is located.

        Raises:
            ShardLookupFailure: If the record is not found on any shard.
        """

        key = ShardedResource.shardCacheKey(model, column, value)

        if useCache is True:
            # Attempting to retrieve the shard ID from the cache.
            cached = ShardedResource._cacheGet(key)
            if cached is not None:
                return int(cached)

        # Iterating through all shards to locate the record.
        for shard in _get_shards():
            n = model.objects.using(shard).filter(**{column: value}).count()
            if n > 0:
                shardId = ShardedResource.shardNameToId(shard)
                if useCache is True:
                    # Caching the shard ID for future lookups.
                    ShardedResource._cacheSet(key, str(shardId))
                logging.info(f"FOUND {model.__name__}.{column}={value} on shardId={shardId}")
                return shardId

        # Raising an exception if the record is not found on any shard.
        raise ShardLookupFailure(f'No shard containing {model.__name__}.{column}="{value}" found')

    @staticmethod
    def _realUserIdToPhysicalShardId(userId):
        """
        Query the db to determine which physical shard a user-id exists on.
        """
        from sh_util.db import db_query

        res = db_query('SELECT "physical_shard_id" FROM "LogicalShard" WHERE "id" = {}'.format(userIdToLogicalShardId(userId)), using="shard_1")

        if len(res) == 0 or len(res[0]) == 0:
            raise ShardLookupFailure(f"Unable to find shard for user_id={userId}")

        physicalShardId = res[0][0]

        return physicalShardId

    @staticmethod
    # @TODO MEMOIZATION TEMPORARILY DISABLED
    # @Memoizewithexpiry(180)
    def _cachingUserIdToPhysicalShardId(userId):
        """Memoizing function to algorythmically resolve
        user-id to shard-id."""
        logicalShardId = userIdToLogicalShardId(userId)

        try:
            key = f"logicalShard:{logicalShardId}"
            value = get_memcache_client().get(key)
            if value is not None:
                return value

            shardId = ShardedResource._realUserIdToPhysicalShardId(userId)
            get_memcache_client().set(key, shardId, time=180)

        except pylibmc.Error:
            if "shardId" not in vars():
                shardId = ShardedResource._realUserIdToPhysicalShardId(userId)

        return shardId

    @staticmethod
    def userIdToPhysicalShardId(userId, useCache=True):
        """
        Wrapper around userIdToPhysicalShardId to allow cache and memoization to be disabled.
        """
        if useCache is not True:
            return ShardedResource._realUserIdToPhysicalShardId(userId)

        return ShardedResource._cachingUserIdToPhysicalShardId(userId)

    @staticmethod
    def allShardConnectionNames():
        """Get all shard connection names."""
        return _get_shards()

    @staticmethod
    def _subscribeToShardEvents():
        """Subscribe to "movedUser" shard event."""

        def movedUser(data):
            """Trigger memcache updates when a user is moved."""
            from django.contrib.auth.models import User
            from main.models import ExtendedUser, PhoneNumber

            userId = data["user_id"]
            shardId = str(data["shardId"])

            ShardedResource.setShardId(User, "user_id", userId, shardId)

            ShardedResource.setShardId(ExtendedUser, "user_id", userId, shardId)

            with ShardedConnection(shardId) as _:
                number = ExtendedUser.objects.get(user=userId).twilio_phone_number.number
                ShardedResource.setShardId(PhoneNumber, "number", number, shardId)

        ShardEvent().subscribe("movedUser", movedUser)


ShardedResource()._subscribeToShardEvents()

userIdToShardName = lambda userId: f"shard_{ShardedResource.userIdToPhysicalShardId(userId)}"  # noqa


class ShardedAuthenticationMiddleware:
    """
    Middleware for shard selection in Django.

    This middleware overrides the stock Django authentication middleware to
    select the appropriate shard based on the user ID.
    """

    @staticmethod
    def process_request(request):
        """
        Processing the incoming request to select the appropriate shard.

        Args:
            request: The incoming HTTP request.

        Returns:
            None
        """

        from django.contrib.auth import SESSION_KEY, get_user
        from django.utils.functional import SimpleLazyObject

        assert hasattr(request, "session"), (
            'The Django authentication middleware requires session middleware to be installed. Edit your MIDDLEWARE_CLASSES setting to insert "django.contrib.sessions.middleware.SessionMiddleware".'
        )

        userId = int(request.session.get(SESSION_KEY, -1))

        if userId != -1:
            shardId = ShardedResource().userIdToPhysicalShardId(userId)

            logging.info("[SHARD-SELECTOR] Selecting shard #%s for user_id=%s", str(shardId), str(userId))

            from .db import switchDefaultDatabase

            switchDefaultDatabase(f"shard_{shardId}")

        else:
            logging.info("[SHARD-SELECTOR] USER DOES NOT LOOK LOGGED IN RIGHT NOW")

        request.user = SimpleLazyObject(lambda: get_user(request))

        return None
