"""The redis cache for Django ORM."""

import logging
import pickle
import time
from contextlib import suppress
from datetime import datetime
from enum import Enum
from functools import partial
from itertools import islice
from os import getpid
from typing import Any, Iterable, Optional, Sequence, Type, Union

import redis
from django.conf import settings
from django.db import models

from resolwe.flow.managers.listener.plugin_interface import Plugin, PluginManager
from resolwe.flow.models import Data
from resolwe.utils import BraceMessage as __

redis_server = redis.from_url(
    getattr(settings, "REDIS_CONNECTION_STRING", "redis://localhost")
)
logger = logging.getLogger(__name__)


class RedisLockStatus(Enum):
    """Redis lock status."""

    PROCESSING = "PR"
    OK = "OK"
    ERROR = "ER"


def chunked(iterable: Iterable, size: int):
    """Iterate the given iterable in chunks of the given size."""
    iterator = iter(iterable)
    while True:
        chunk = tuple(islice(iterator, size))
        if not chunk:
            break
        yield chunk


FieldNames = Sequence[str]
FieldValues = dict[str, Any]
Identifier = Sequence
Cache = dict[Identifier, FieldValues]


class RedisCache:
    """Stores a dictionaries with striLahkongs for keys.

    They are stored as hashes in Redis with pickle dumps as values.
    """

    # Max number of keys to retrieve from Redis in a single batch.
    KEY_BATCH_SIZE = 10000

    def __init__(self, *args, **kwargs):
        """Set the cached field set."""
        # The set of fields on the data object that can be safely cached inside
        # Redis to avoid hitting the database (we are assuming Redis is faster).
        # The list contains a set of commonly used fields that do not change
        # from the outside (or do no harm if they do).

        self._redis = redis_server
        super().__init__(*args, **kwargs)

    def _model_str(self, Model: Optional[Type[models.Model]]) -> str:
        """Get string representation for the given content type."""
        return Model._meta.label_lower if Model else ""

    def _identifiers_str(self, identifiers: Optional[Sequence]) -> str:
        """Get string representation for the given identifier."""
        if not identifiers:
            return ""
        else:
            return "-".join(map(str, identifiers))

    def _cached_field_name(self, Model: Type[models.Model], field_name: str) -> str:
        """Get the cached field name."""
        return f"{self._model_str(Model)}:{field_name}"

    def get_redis_key(self, Model: type[models.Model], identifiers: Identifier) -> str:
        """Get the redis key from the field name.

        When testing every listener process must have its own redis shared
        storage since data ids will repeat when tests are run in parallel.
        """
        return self._get_redis_key_prefix(Model, identifiers)

    def _get_redis_key_prefix(
        self,
        Model: Optional[Type[models.Model]] = None,
        identifiers: Optional[Identifier] = None,
        field_name: Optional[str] = None,
    ) -> str:
        """Get redis key prefix based on input parameters.

        :raises RuntimeError: when input parameters are not valid.
        """
        if not Model and (identifiers or field_name):
            raise RuntimeError(
                "When model is None then identifiers and field name must be provided."
            )
        if field_name and not identifiers:
            raise RuntimeError(
                "When field name is not None then identifiers must be provided."
            )

        from resolwe.test import is_testing  # Remove circular import.

        parts = [
            "listener" if not is_testing() else f"listener-{getpid()}",
            self._model_str(Model),
            self._identifiers_str(identifiers),
            field_name,
        ]
        return ":".join([part for part in parts if part])

    def _get_redis_data(self, redis_keys: Sequence[str]) -> list[Optional[Any]]:
        """Retrieve the data from Redis for the given keys.

        The data is also unpickled, None is returned when data is not cached.
        """

        def unpickle(data: Optional[bytes]) -> Optional[FieldValues]:
            """Unpickle the data.

            When data is None return None.
            """
            return pickle.loads(data) if data is not None else None

        cached_data: list[Optional[FieldValues]] = list()
        for batch_keys in chunked(redis_keys, self.KEY_BATCH_SIZE):
            try:
                batch_data = self._redis.mget(*batch_keys)
            except redis.exceptions.RedisError:
                logger.exception(
                    __(
                        "Could not retrieve data from Redis for keys: '{}'.",
                        ", ".join(batch_keys),
                    )
                )
                raise

            try:
                cached_data.extend(map(unpickle, batch_data))
            except pickle.PickleError:
                logger.exception(
                    __(
                        "Could not deserialize data from Redis for keys: '{}'.",
                        batch_keys,
                    )
                )
                raise
        return cached_data  # type: ignore

    def mget(
        self, Model: Type[models.Model], identifiers_list: Sequence[Identifier]
    ) -> list[Optional[FieldValues]]:
        """Obtain the set of fields from the redis cache.

        All the cached field names are always returned.

        The query does not run in a transaction.

        :raises pickle.PickleError: when data cannot be unpickled.
        :raises redis.exceptions.RedisError: when data cannot be retrieved from Redis.
        """
        get_redis_key = partial(self.get_redis_key, Model)
        redis_keys = list(map(get_redis_key, identifiers_list))
        return self._get_redis_data(redis_keys)

    def _lock_key(self, Model: Type[models.Model], identifiers: Sequence) -> str:
        """Get the key for the lock for the given entry."""
        return self._get_redis_key_prefix(Model, identifiers, "__lock__")

    def lock(
        self,
        Model: Type[models.Model],
        identifiers_list: Sequence[Identifier],
        valid_for: int = 300,
    ) -> list[tuple[bool, RedisLockStatus]]:
        """Set the lock for the given entry.

        Create the lock indicating the given entry is processing. The lock will
        auto-expire after valid_for seconds.

        :returns: the tuple (success, status). The first value indicates if obtaining
        a lock was a success and the other the status of the lock.
        """
        data = pickle.dumps(RedisLockStatus.PROCESSING)
        pipe = self._redis.pipeline()
        to_return: list[tuple[bool, RedisLockStatus]] = []
        for identifier in identifiers_list:
            key = self._lock_key(Model, identifier)
            pipe = pipe.set(key, data, ex=valid_for, nx=True).get(key)
        results = pipe.execute()
        for index in range(0, len(results), 2):
            status, value = results[index : index + 2]
            to_return.append((status == True, pickle.loads(value)))
        return to_return

    def unlock(
        self,
        Model: Type[models.Model],
        identifiers_list: Sequence[Identifier],
        status: RedisLockStatus = RedisLockStatus.OK,
    ):
        """Release the lock for the given entry with status.

        :raise AssertionError: when status in not OK or ERROR.
        """
        # The status should persist for longer period, such as a day.
        assert status in (RedisLockStatus.OK, RedisLockStatus.ERROR)
        valid_for = 24 * 60 * 60  # One day.
        status_pickle = pickle.dumps(status)
        redis_map = {
            self._lock_key(Model, identifier): status_pickle
            for identifier in identifiers_list
        }
        for redis_key in redis_map:
            self._redis.set(redis_key, status_pickle, ex=valid_for)

    def _get_redis_locks(
        self, Model: Type[models.Model], identifiers_list: Sequence[Identifier]
    ) -> list[Optional[RedisLockStatus]]:
        """Get the locks for the given entries.

        When no lock is given None is returned as its value.
        """
        redis_keys = [
            self._lock_key(Model, identifiers) for identifiers in identifiers_list
        ]
        # When there is no lock return error: ok could abort the processing.
        return self._get_redis_data(redis_keys)

    def wait(
        self,
        Model: Type[models.Model],
        identifiers_list: Sequence[Sequence],
        timeout: int = 60,
        refresh_interval: int = 1,
    ) -> set[Optional[RedisLockStatus]]:
        """Wait for the locks for the given entries to be released.

        If the locks are not released within the given timeout proceed anyway.
        """
        redis_lock_keys = [
            self._lock_key(Model, identifiers) for identifiers in identifiers_list
        ]
        start_time = time.time()
        while time.time() - start_time < timeout:
            statuses = set(self._get_redis_data(redis_lock_keys))
            if RedisLockStatus.PROCESSING not in statuses:
                break
            time.sleep(refresh_interval)
        return statuses

    def get(
        self,
        Model: Type[models.Model],
        identifiers: Identifier,
        field_names: Iterable[str],
    ) -> FieldValues:
        """Obtain the set of fields from the redis cache.

        If field_name is requested that is not cached the rusult for that key is None.

        The query is run in a transaction.
        """

        def get_redis_data(
            pipeline: redis.client.Pipeline,
        ) -> dict[str, Optional[Union[str, datetime]]]:
            """Get the redis data from the specific key."""

            cached_data = dict()
            try:
                unpickled_data = pipeline.get(redis_key)
            except redis.exceptions.RedisError:
                logger.exception(
                    __(
                        "Could not retrieve data from Redis for key: '{}'.",
                        ", ".join(redis_key),
                    )
                )
                raise

            try:
                cached_data = pickle.loads(unpickled_data)  # type: ignore
            except pickle.PickleError:
                logger.exception(
                    __(
                        "Could not deserialize data from Redis for key: '{}'.",
                        redis_key,
                    )
                )
                raise

            return {
                field_name: cached_data.get(field_name) for field_name in field_names
            }

        redis_key = self.get_redis_key(Model, identifiers)
        return self._redis.transaction(
            get_redis_data, redis_key, value_from_callable=True
        )

    def clear(
        self,
        Model: Optional[Type[models.Model]] = None,
        identifiers: Optional[Identifier] = None,
    ):
        """Clear the entire Redis cache for the given data object.

        When data_id is not given the entire cache is cleared.

        :raises RuntimeError: if identifiers are given without the content type.
        """
        key_prefix = f"{self._get_redis_key_prefix(Model, identifiers)}"
        if identifiers is None:
            key_prefix += "*"
        redis_keys = self._redis.keys(key_prefix)
        if identifiers is not None:
            redis_keys += self._redis.keys(f"{key_prefix}-*")
        if redis_keys:
            self._redis.unlink(*redis_keys)

    def mset(
        self,
        Model: Type[models.Model],
        to_cache: Cache,
        expiration_time: Optional[int] = None,
    ):
        """Set multiple keys in the redis cache.

        The method does not run in a transaction.

        To speed up the method it is not checked if the field is cached.

        When error occurs it is logged but not raised. Only the part of the given keys
        is cached in such case.

        Note: expiration time is set key by key so it can be slow for large number of
        keys.
        """
        redis_data = {
            self.get_redis_key(Model, identifiers): pickle.dumps(item)
            for identifiers, item in to_cache.items()
        }
        # Write data in chunks. Do not abort if single chunk fails.
        for chunk in chunked(redis_data.items(), self.KEY_BATCH_SIZE):
            try:
                self._redis.mset(dict(chunk))  # type: ignore
                if expiration_time:
                    for key, _ in chunk:
                        self._redis.expire(key, expiration_time)
            except redis.exceptions.RedisError:
                logger.exception(
                    __(
                        "Could not set data in Redis for keys: {}",
                        ", ".join(redis_data.keys()),
                    )
                )

    def set(
        self,
        Model: Type[models.Model],
        identifiers: Identifier,
        field_values: FieldValues,
        expiration_time: Optional[int] = None,
    ):
        """Set the redis cache in the transaction.

        When field is not in the set of cached fields it is silentry ignored.
        When the given content type/identifiers pair is already in the cache it is updated.
        """

        def update_cache(pipeline: redis.client.Pipeline):
            """Update the cache using pipeline."""
            cached_fields = {key: value for key, value in field_values.items()}
            redis_key = self.get_redis_key(Model, identifiers)
            existing_cache = {}
            with suppress(Exception):
                existing_cache = pickle.loads(pipeline.get(redis_key))  # type: ignore
            cache_data = pickle.dumps({**existing_cache, **cached_fields})
            pipeline.set(redis_key, cache_data, ex=expiration_time)

        self._redis.transaction(update_cache)

    def extend_lock(
        self,
        Model: Type[models.Model],
        identifier: Identifier,
        valid_for: int = 300,
    ) -> bool:
        """Extend the lock for the given entry.

        Make sure the lock exists and is set to PROCESSING before extending status.
        """
        key = self._lock_key(Model, identifier)
        result = self._redis.get(key)
        # If the lock does not exist return False.
        if result is None:
            return False
        status = pickle.loads(result)
        if status != RedisLockStatus.PROCESSING:
            return False
        # Extend the lock.
        self._redis.set(key, pickle.dumps(RedisLockStatus.PROCESSING), ex=valid_for)
        return True


redis_cache = RedisCache()


class CachedObjectManager(PluginManager["CachedObjectPlugin"]):
    """Redis cache plugin manager."""

    def _get_plugin_identifier(self, Model: Type[models.Model]) -> str:
        """Get the plugin identifier."""
        return Model._meta.label_lower

    def get_plugin_for_model(self, Model: Type[models.Model]) -> "CachedObjectPlugin":
        """Get the plugin based on the content type.

        :raises KeyError: when no plugin is registered for the given content type.
        """
        return super().get_plugin(self._get_plugin_identifier(Model))

    def mcache(self, instances: models.QuerySet) -> None:
        """Cache the given queryset.

        Items with the same identifiers are overwritten.
        """
        plugin = self.get_plugin_for_model(instances.model)
        to_cache = plugin.serialize(instances)
        redis_cache.mset(plugin.model, to_cache, plugin.expiration_time)

    def cache(self, instance: models.Model) -> None:
        """Cache the given instance."""
        plugin = self.get_plugin_for_model(type(instance))
        to_cache = plugin.serialize(instance)
        redis_cache.mset(plugin.model, to_cache, plugin.expiration_time)

    def mget(
        self, Model: Type[models.Model], identifiers_list: Sequence[Identifier]
    ) -> list[Optional[FieldValues]]:
        """Get the cache values for the given identifiers."""
        plugin = self.get_plugin_for_model(Model)
        return redis_cache.mget(plugin.model, identifiers_list)

    def get(
        self, Model: Type[models.Model], identifiers: Identifier
    ) -> Optional[FieldValues]:
        """Get the cache values for the given instance."""
        return self.mget(Model, [identifiers])[0]

    def update_cache(
        self, Model: Type[models.Model], identifiers: Identifier, values: FieldValues
    ):
        """Update the given cache with values."""
        plugin = self.get_plugin_for_model(Model)
        current_cache = self.get(Model, identifiers) or {}
        for field in plugin.cached_fields:
            if cached_field_value := values.get(field):
                current_cache[field] = cached_field_value
        to_cache = {identifiers: current_cache}
        redis_cache.mset(plugin.model, to_cache, plugin.expiration_time)

    def is_cached(self, Model: Type[models.Model], field_name: str) -> bool:
        """Check if the given field is cached."""
        return field_name in self.get_plugin_for_model(Model).cached_fields

    def lock(
        self, Model: Type[models.Model], identifiers_list: Sequence[Identifier]
    ) -> list[tuple[bool, RedisLockStatus]]:
        """Create lock for the given entry."""
        return redis_cache.lock(Model, identifiers_list)

    def extend_lock(
        self, Model: Type[models.Model], identifier: Identifier, valid_for: int = 300
    ):
        """Extend the lock for the given entry."""
        return redis_cache.extend_lock(Model, identifier, valid_for=valid_for)

    def unlock(
        self,
        Model: Type[models.Model],
        identifiers_list: Sequence[Identifier],
        status: RedisLockStatus = RedisLockStatus.OK,
    ):
        """Unlock locks for the given entries."""
        return redis_cache.unlock(Model, identifiers_list, status)

    def clear(
        self,
        Model: Type[models.Model],
        identifiers_list: Sequence[Identifier],
    ):
        """Clear the cache for the given identifiers."""
        return redis_cache.clear(Model, identifiers_list)

    def wait(
        self,
        Model: Type[models.Model],
        identifiers_list: Sequence[Sequence],
        timeout: int = 60,
    ) -> set[Optional[RedisLockStatus]]:
        """Wait for locks to be released for up to 60 seconds."""
        return redis_cache.wait(Model, identifiers_list, timeout)


cache_manager = CachedObjectManager()


class CachedObjectPlugin(Plugin):
    """Cache a single ORM object in Redis.

    The object content type and identifiers are used to create the key under which the
    object is stored. The object is stored as a pickle dump of the dictionary
    mapping cached keys to their values.

    Assumptions:
    - the data is safe, pickle is used to create its binary representation.
    """

    abstract = True
    plugin_manager = cache_manager
    model: Type[models.Model]
    # The list of cached fields.
    cached_fields: FieldNames
    # The list of identifier fields: must be a subset of cached_fields.
    identifier_fields: FieldNames
    # Default key expiration time is 1 day.
    expiration_time: int = 24 * 3600

    def _get_identifiers(self, values: FieldValues) -> Identifier:
        """Get the identifiers from values."""
        return tuple(values[field] for field in self.identifier_fields)

    def _serialize_instance(self, instance: models.Model) -> Cache:
        """Return the dictionary to store in redis."""
        values = {
            field_name: getattr(instance, field_name, None)
            for field_name in self.cached_fields
        }
        return {self._get_identifiers(values): values}

    def _serialize_queryset(self, instances: models.QuerySet) -> Cache:
        """Return a dictionaryto store in redis.

        The keys are identifiers and values dictionaries.
        """
        return {
            self._get_identifiers(values): values
            for values in instances.values(*self.cached_fields)
        }

    def serialize(self, instances: Union[models.Model, models.QuerySet]) -> Cache:
        """Serialize the given model instance or queryset."""
        serializer = (
            self._serialize_instance
            if isinstance(instances, models.Model)
            else self._serialize_queryset
        )
        return serializer(instances)

    @classmethod
    def get_identifier(cls) -> str:
        """Return the plugin identifier."""
        return cls.model._meta.label_lower


class DataCache(CachedObjectPlugin):
    """Cache the data model in Redis."""

    model = Data
    cached_fields = ("id", "status", "started", "worker__status")
    identifier_fields = ("id",)
