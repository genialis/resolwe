"""Test Redis cache in listener."""

import time

from resolwe.flow.managers.listener.redis_cache import (
    RedisLockStatus,
    cache_manager,
    redis_cache,
)
from resolwe.flow.models import Data, Process
from resolwe.test import TestCase


class RedisCache(TestCase):
    """Check that redis cache plugin is registered."""

    def setUp(self):
        """Clear the cache and prepare the objects."""
        result = super().setUp()
        redis_cache.clear()

        proc = Process.objects.create(
            type="data:test:process",
            slug="test-process",
            contributor=self.contributor,
        )
        self.data1 = Data.objects.create(
            name="Test data 1",
            contributor=self.contributor,
            process=proc,
            status=Data.STATUS_DONE,
        )
        self.data2 = Data.objects.create(
            name="Test data 2",
            contributor=self.contributor,
            process=proc,
            status=Data.STATUS_DONE,
        )
        return result

    def _assertBetween(self, value, min, max):
        """Assert value is between min and max."""
        self.assertLessEqual(min, value)
        self.assertGreaterEqual(max, value)

    def test_register_data(self):
        """Test that Data plugin is registered."""
        self.assertIn("flow.data", cache_manager._plugins)
        cache_manager.get_plugin_for_model(Data)

    def test_cache_single(self):
        """Test that object can be cached."""
        # Test that data is not cached.
        self.assertIsNone(cache_manager.get(Data, (self.data1.id,)))

        # Cache the data object and verify that it is cached.
        cache_manager.cache(self.data1)
        result = cache_manager.get(Data, (self.data1.id,))
        self.assertEqual(result["id"], self.data1.id)

    def test_cache_multi(self):
        """Test that multiple objects can be cached."""
        # Assert no cached data exists in Redis.
        cache = cache_manager.mget(Data, ((self.data1.id,), (self.data2.id,)))
        self.assertEqual(cache, [None, None])

        # Cache the data and verify that it is cached.
        cache_manager.mcache(Data.objects.filter(pk__in=[self.data1.pk, self.data2.pk]))
        cache = cache_manager.mget(Data, ((self.data1.id,), (self.data2.id,)))
        self.assertEqual(
            [entry["id"] for entry in cache], [self.data1.id, self.data2.id]
        )

    def test_clear(self):
        """Test that multiple objects can be cached."""
        # Cache the data and verify that it is cached.
        cache_manager.mcache(Data.objects.filter(pk__in=[self.data1.pk, self.data2.pk]))
        # Remove the first cache
        redis_cache.clear(Data, (self.data1.id,))
        cache = cache_manager.mget(Data, ((self.data1.id,), (self.data2.id,)))
        self.assertEqual(cache[0], None)
        self.assertEqual(cache[1]["id"], self.data2.id)

        redis_cache.clear(Data)
        cache = cache_manager.mget(Data, ((self.data1.id,), (self.data2.id,)))
        self.assertEqual(cache, [None, None])

        cache_manager.mcache(Data.objects.filter(pk__in=[self.data1.pk, self.data2.pk]))
        redis_cache.clear()
        cache = cache_manager.mget(Data, ((self.data1.id,), (self.data2.id,)))
        self.assertEqual(cache, [None, None])

    def test_lock(self):
        """Test locking."""
        identifiers_list = [(self.data1.id,), (self.data2.id,)]
        # Without locking the wait should return immediately.
        start = time.time()
        result = cache_manager.wait(Data, identifiers_list, timeout=0.1)
        elapsed = time.time() - start
        self.assertLess(elapsed, 0.05)
        self.assertEqual(result, {None})

        # Test locking processing for a given message.
        identifier = [(self.data1.id, "uuid_for_test")]
        lock_result, status = cache_manager.lock(Data, identifier)[0]
        lock_key = redis_cache._lock_key(Data, identifier[0])
        self.assertEqual(lock_result, True)
        self.assertEqual(status, RedisLockStatus.PROCESSING)
        ttl = redis_cache._redis.ttl(lock_key)
        self._assertBetween(ttl, 290, 300)

        # Locking again should return False.
        time.sleep(2)  # Make new ttl lower.
        lock_result, status = cache_manager.lock(Data, identifier)[0]
        self.assertEqual(lock_result, False)
        self.assertEqual(status, RedisLockStatus.PROCESSING)
        new_ttl = redis_cache._redis.ttl(lock_key)
        self._assertBetween(new_ttl, 290, 300)
        self.assertLess(new_ttl, ttl)

        # Unlocking should change the status of the lock and set TTL to 1 day.
        cache_manager.unlock(Data, identifier)
        lock_result, status = cache_manager.lock(Data, identifier)[0]
        self.assertEqual(lock_result, False)
        self.assertEqual(status, RedisLockStatus.OK)
        ttl = redis_cache._redis.ttl(lock_key)
        self._assertBetween(ttl, 24 * 60 * 60 - 2, 24 * 60 * 60)

        # Extend the lock - only processing lock can be extended.
        result = cache_manager.extend_lock(Data, identifier[0], valid_for=5)
        self.assertEqual(result, False)
        ttl = redis_cache._redis.ttl(lock_key)
        self._assertBetween(ttl, 24 * 60 * 60 - 2, 24 * 60 * 60)
        redis_cache._redis.unlink(lock_key)

        # Extend the lock.
        lock_result, status = cache_manager.lock(Data, identifier)[0]
        lock_key = redis_cache._lock_key(Data, identifier[0])
        self.assertEqual(lock_result, True)
        self.assertEqual(status, RedisLockStatus.PROCESSING)
        ttl = redis_cache._redis.ttl(lock_key)
        self._assertBetween(ttl, 290, 300)
        result = cache_manager.extend_lock(Data, identifier[0], valid_for=10)
        ttl = redis_cache._redis.ttl(lock_key)
        self._assertBetween(ttl, 9, 10)

        # Lock the data1 and wait for it.
        cache_manager.lock(Data, [(self.data1.id,)])
        start = time.time()
        result = redis_cache.wait(
            Data, identifiers_list, timeout=0.1, refresh_interval=0.05
        )
        elapsed = time.time() - start
        self.assertTrue(0.1 < elapsed < 0.2)
        self.assertEqual(result, {None, RedisLockStatus.PROCESSING})

        # Verify the refresh interval is working.
        start = time.time()
        result = redis_cache.wait(
            Data, identifiers_list, timeout=0.1, refresh_interval=0.5
        )
        elapsed = time.time() - start
        self.assertTrue(0.5 < elapsed < 0.6)
        self.assertEqual(result, {None, RedisLockStatus.PROCESSING})

        # Unlocking data2 should not have any effect.
        cache_manager.unlock(Data, [(self.data2.id,)])
        start = time.time()
        result = redis_cache.wait(
            Data, identifiers_list, timeout=0.1, refresh_interval=0.05
        )
        elapsed = time.time() - start
        self.assertTrue(0.1 < elapsed < 0.2)
        self.assertEqual(result, {RedisLockStatus.OK, RedisLockStatus.PROCESSING})

        # Unlocking data1 should remove lock.
        cache_manager.unlock(Data, [(self.data1.id,)])
        start = time.time()
        result = cache_manager.wait(Data, identifiers_list, timeout=0.1)
        elapsed = time.time() - start
        self.assertLess(elapsed, 0.05)
        self.assertEqual(result, {RedisLockStatus.OK})

        # Try the lock unlocks itself after timeout.
        lock_key = redis_cache._lock_key(Data, (self.data1.id,))
        redis_cache._redis.unlink(lock_key)
        lock_key = redis_cache._lock_key(Data, (self.data2.id,))
        redis_cache._redis.unlink(lock_key)
        redis_cache.lock(Data, identifiers_list, valid_for=1)
        start = time.time()
        result = redis_cache.wait(
            Data, identifiers_list, timeout=2, refresh_interval=0.05
        )
        elapsed = time.time() - start
        self.assertEqual(result, {None})
        self.assertTrue(1 < elapsed < 1.1)
