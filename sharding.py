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


def coerce_id_to_shard_name(shard_or_shard_id):
    """
    Coercing a physical shard ID to the name of the shard's connection.

    Args:
        shardOrShardId (str or int): The shard ID or name.

    Returns:
        str: The name of the shard connection.
    """

    if str(shard_or_shard_id).isdigit():
        return f"shard_{shard_or_shard_id}"
    return shard_or_shard_id


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

    def __init__(self, connection_name_or_id):
        """
        Initializing the shard connection.

        Args:
            connectionNameOrId (str or int): The name or ID of the database
                connection to use.
        """

        self.connection_name = f"shard_{connection_name_or_id}" if isinstance(connection_name_or_id, int) or connection_name_or_id.isdigit() else connection_name_or_id

        # Keeping a reference to the original connection to restore it later.
        self.original_shard = None

    def __enter__(self):
        """
        Starting the `with` statement by switching to the specified shard.

        Returns:
            str: The name of the shard connection.
        """

        from .db import switch_default_database

        self.original_shard = ShardedResource.get_current_shard()
        switch_default_database(self.connection_name)
        return self.connection_name

    def __exit__(self, _type, value, traceback):
        """
        Ending the `with` statement by restoring the original shard connection.
        """

        from .db import switch_default_database

        switch_default_database(self.original_shard)


def user_id_to_logical_shard_id(user_id):
    """
    Calculating the logical shard ID for a given user ID.

    Args:
        userId (int): The user ID.

    Returns:
        int: The logical shard ID.
    """

    return int(user_id) % settings.NUM_LOGICAL_SHARDS


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
    def get_current_shard():
        """
        Determining and returning the name of the current shard.

        Returns:
            str: The name of the current shard.

        Raises:
            Exception: If no active shard is found.
        """

        for c in ShardedResource.all_shard_connection_names():
            if settings.DATABASES[c] == settings.DATABASES["default"]:
                return c

        raise LookupError("No active shard was found")

    @staticmethod
    def shard_name_to_id(shard):
        """
        Extracting the shard ID from the shard connection name.

        Args:
            shard (str): The name of the shard connection.

        Returns:
            int: The shard ID.
        """

        return int(shard[shard.rindex("_") + 1 :])

    @staticmethod
    def shard_cache_prefix(model, column):
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
    def shard_cache_key(model, column, value):
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
    def _cache_set(key, value):
        """Set a value in the cache."""
        try:
            cli = get_memcache_client()
            value = cli.set(key, value)

        except pylibmc.Error as e:
            logging.info(f"ShardedResource._cacheSet error: {e}")

    @staticmethod
    def _cache_get(key):
        """Get a value from the cache."""
        try:
            cli = get_memcache_client()
            return cli.get(key)

        except pylibmc.Error as e:
            logging.info(f"ShardedResource._cacheSet error: {e}")
            return None

    @staticmethod
    def warm_shard_id_cache(model, column):
        """Warm the cache mapping of a model's column to a shard id."""
        prefix = ShardedResource.shard_cache_prefix(model, column)

        try:
            cli = get_memcache_client()

            for shard in _get_shards():
                shard_id = str(ShardedResource.shard_name_to_id(shard))

                values = model.objects.using(shard).only(column).values_list(column, flat=True)

                mapping = {str(v): shard_id for v in values}

                if len(mapping) > 0:
                    cli.set_multi(mapping, key_prefix=prefix)

        except pylibmc.Error as e:
            logging.info(f"ShardedResource.warmShardCache :: memcache error: {e}")

    @retry(tries=3)
    @staticmethod
    def set_shard_id(model, column, value, shard_id):
        """
        Explicitly set the location for a model/column/value in the cache.
        """
        try:
            cli = get_memcache_client()
            key = ShardedResource.shard_cache_key(model, column, value)
            cli.set(key, str(shard_id))

            return True

        except pylibmc.Error as e:
            logging.error(f"memcache error: {e}")
            return False

    @staticmethod
    def find_shard_id(model, column, value, use_cache=True):
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

        key = ShardedResource.shard_cache_key(model, column, value)

        if use_cache is True:
            # Attempting to retrieve the shard ID from the cache.
            cached = ShardedResource._cache_get(key)
            if cached is not None:
                return int(cached)

        # Iterating through all shards to locate the record.
        for shard in _get_shards():
            n = model.objects.using(shard).filter(**{column: value}).count()
            if n > 0:
                shard_id = ShardedResource.shard_name_to_id(shard)
                if use_cache is True:
                    # Caching the shard ID for future lookups.
                    ShardedResource._cache_set(key, str(shard_id))
                logging.info(f"FOUND {model.__name__}.{column}={value} on shardId={shard_id}")
                return shard_id

        # Raising an exception if the record is not found on any shard.
        raise ShardLookupFailure(f'No shard containing {model.__name__}.{column}="{value}" found')

    @staticmethod
    def _real_user_id_to_physical_shard_id(user_id):
        """
        Query the db to determine which physical shard a user-id exists on.
        """
        from sh_util.db import db_query

        res = db_query('SELECT "physical_shard_id" FROM "LogicalShard" WHERE "id" = {}'.format(user_id_to_logical_shard_id(user_id)), using="shard_1")

        if len(res) == 0 or len(res[0]) == 0:
            raise ShardLookupFailure(f"Unable to find shard for user_id={user_id}")

        physical_shard_id = res[0][0]

        return physical_shard_id

    @staticmethod
    # @TODO MEMOIZATION TEMPORARILY DISABLED
    # @Memoizewithexpiry(180)
    def _caching_user_id_to_physical_shard_id(user_id):
        """Memoizing function to algorythmically resolve
        user-id to shard-id."""
        logical_shard_id = user_id_to_logical_shard_id(user_id)

        try:
            key = f"logicalShard:{logical_shard_id}"
            value = get_memcache_client().get(key)
            if value is not None:
                return value

            shard_id = ShardedResource._real_user_id_to_physical_shard_id(user_id)
            get_memcache_client().set(key, shard_id, time=180)

        except pylibmc.Error:
            if "shard_id" not in vars():
                shard_id = ShardedResource._real_user_id_to_physical_shard_id(user_id)

        return shard_id

    @staticmethod
    def user_id_to_physical_shard_id(user_id, use_cache=True):
        """
        Wrapper around userIdToPhysicalShardId to allow cache and memoization to be disabled.
        """
        if use_cache is not True:
            return ShardedResource._real_user_id_to_physical_shard_id(user_id)

        return ShardedResource._caching_user_id_to_physical_shard_id(user_id)

    @staticmethod
    def all_shard_connection_names():
        """Get all shard connection names."""
        return _get_shards()

    @staticmethod
    def _subscribe_to_shard_events():
        """Subscribe to "movedUser" shard event."""

        def moved_user(data):
            """Trigger memcache updates when a user is moved."""
            from django.contrib.auth.models import User
            from main.models import ExtendedUser, PhoneNumber

            user_id = data["user_id"]
            shard_id = str(data["shardId"])

            ShardedResource.set_shard_id(User, "user_id", user_id, shard_id)

            ShardedResource.set_shard_id(ExtendedUser, "user_id", user_id, shard_id)

            with ShardedConnection(shard_id) as _:
                number = ExtendedUser.objects.get(user=user_id).twilio_phone_number.number
                ShardedResource.set_shard_id(PhoneNumber, "number", number, shard_id)

        ShardEvent().subscribe("movedUser", moved_user)


ShardedResource()._subscribe_to_shard_events()

userIdToShardName = lambda user_id: f"shard_{ShardedResource.user_id_to_physical_shard_id(user_id)}"  # noqa


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

        user_id = int(request.session.get(SESSION_KEY, -1))

        if user_id != -1:
            shard_id = ShardedResource().user_id_to_physical_shard_id(user_id)

            logging.info("[SHARD-SELECTOR] Selecting shard #%s for user_id=%s", str(shard_id), str(user_id))

            from .db import switch_default_database

            switch_default_database(f"shard_{shard_id}")

        else:
            logging.info("[SHARD-SELECTOR] USER DOES NOT LOOK LOGGED IN RIGHT NOW")

        request.user = SimpleLazyObject(lambda: get_user(request))

        return None
