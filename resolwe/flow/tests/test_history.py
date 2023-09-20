# pylint: disable=missing-docstring,too-many-lines
from datetime import timedelta

from django.utils.timezone import now

from resolwe.flow.models import (
    Collection,
    CollectionHistory,
    Data,
    DataHistory,
    Process,
)
from resolwe.flow.models.history import CollectionChange, SizeChange
from resolwe.flow.models.history_manager import history_manager
from resolwe.test import TestCase


class HistoryModelTest(TestCase):
    """Check that the history of data and collection objects is kept."""

    def test_data_history_created(self):
        process = Process.objects.create(contributor=self.contributor)
        data = Data.objects.create(
            name="data", contributor=self.contributor, process=process, size=0
        )
        current_time = now()
        history = data.history.get()
        self.assertIsInstance(history, DataHistory)
        size_history = history.sizechange.get()
        name_history = history.datanamechange.get()
        slug_history = history.dataslugchange.get()
        # There should be one history object with values as on data object.
        collection_history = history.collectionchange.get()
        self.assertEqual(size_history.value, data.size)
        self.assertEqual(name_history.value, data.name)
        self.assertEqual(slug_history.value, data.slug)
        self.assertEqual(collection_history.value, data.collection)
        # The timestamps of the history objects should be close to the current time.
        self.assertLess(size_history.timestamp - current_time, timedelta(seconds=1))
        self.assertLess(slug_history.timestamp - current_time, timedelta(seconds=1))
        self.assertLess(name_history.timestamp - current_time, timedelta(seconds=1))
        self.assertLess(
            collection_history.timestamp - current_time, timedelta(seconds=0.1)
        )

    def test_processing_history(self):
        process = Process.objects.create(contributor=self.contributor)
        data = Data.objects.create(
            name="data", contributor=self.contributor, process=process, size=0
        )
        history = data.history.get()
        self.assertEqual(history.processinghistory.count(), 0)

        data.started = now() - timedelta(hours=1)
        data.finished = now()
        data.save()
        processing_history = history.processinghistory.get()
        self.assertEqual(processing_history.allocated_cpu, data.process_cores)
        self.assertEqual(processing_history.allocated_memory, data.process_memory)
        self.assertEqual(processing_history.node_cpu, data.process_cores)
        self.assertEqual(processing_history.node_memory, data.process_memory)
        self.assertEqual(processing_history.interval.lower, data.started)
        self.assertEqual(processing_history.interval.upper, data.finished)

        data.process_cores += 1
        data.process_memory += 1
        data.finished = now() + timedelta(hours=1)
        data.save()
        self.assertEqual(history.processinghistory.count(), 2)
        processing_history = history.processinghistory.last()
        self.assertEqual(processing_history.allocated_cpu, data.process_cores)
        self.assertEqual(processing_history.allocated_memory, data.process_memory)
        self.assertEqual(processing_history.node_cpu, data.process_cores)
        self.assertEqual(processing_history.node_memory, data.process_memory)
        self.assertEqual(processing_history.interval.lower, data.started)
        self.assertEqual(processing_history.interval.upper, data.finished)

    def test_collection_history_created(self):
        collection = Collection.objects.create(
            name="collection", contributor=self.contributor
        )
        current_time = now()
        history = collection.history.get()
        self.assertIsInstance(history, CollectionHistory)
        with self.assertRaises(SizeChange.DoesNotExist):
            SizeChange.objects.get()

        with self.assertRaises(CollectionChange.DoesNotExist):
            CollectionChange.objects.get()

        name_history = history.collectionnamechange.get()
        slug_history = history.collectionslugchange.get()
        # There should be one history object with values as on data object.
        self.assertEqual(name_history.value, collection.name)
        self.assertEqual(slug_history.value, collection.slug)
        # The timestamps of the history objects should be close to the current time.
        self.assertLess(slug_history.timestamp - current_time, timedelta(seconds=1))
        self.assertLess(name_history.timestamp - current_time, timedelta(seconds=1))

    def test_data_history_updated(self):
        process = Process.objects.create(contributor=self.contributor)
        data = Data.objects.create(
            name="data", contributor=self.contributor, process=process, size=0
        )

        data.name = "new name"
        data.save()
        history = data.history.get()
        # There should be two namechange objects.
        self.assertEqual(history.datanamechange.count(), 2)
        self.assertEqual(history.datanamechange.first().value, "data")
        self.assertEqual(history.datanamechange.last().value, data.name)
        self.assertEqual(history.dataslugchange.count(), 1)
        self.assertEqual(history.sizechange.count(), 1)
        self.assertEqual(history.collectionchange.count(), 1)

        data.slug = "new-slug"
        data.save()
        history = data.history.get()
        # There should be two slugchange objects.
        self.assertEqual(history.dataslugchange.count(), 2)
        self.assertEqual(history.dataslugchange.first().value, "data")
        self.assertEqual(history.dataslugchange.last().value, data.slug)
        self.assertEqual(history.datanamechange.count(), 2)
        self.assertEqual(history.sizechange.count(), 1)
        self.assertEqual(history.collectionchange.count(), 1)

        data.size = 10
        data.save()
        history = data.history.get()
        # There should be two sizechange objects.
        self.assertEqual(history.sizechange.count(), 2)
        self.assertEqual(history.sizechange.first().value, 0)
        self.assertEqual(history.sizechange.last().value, data.size)
        self.assertEqual(history.datanamechange.count(), 2)
        self.assertEqual(history.dataslugchange.count(), 2)
        self.assertEqual(history.sizechange.count(), 2)
        self.assertEqual(history.collectionchange.count(), 1)

        collection = Collection.objects.create(
            name="collection", contributor=self.contributor
        )
        data.collection = collection
        data.save()
        history = data.history.get()
        # There should be two collectionchange objects.
        self.assertEqual(history.collectionchange.count(), 2)
        self.assertIsNone(history.collectionchange.first().value)
        self.assertEqual(
            history.collectionchange.last().value, collection.history.get()
        )
        self.assertEqual(history.datanamechange.count(), 2)
        self.assertEqual(history.dataslugchange.count(), 2)
        self.assertEqual(history.sizechange.count(), 2)
        self.assertEqual(history.collectionchange.count(), 2)

    def test_collection_history_updated(self):
        collection = Collection.objects.create(
            name="collection", contributor=self.contributor
        )

        collection.name = "new name"
        collection.save()
        history = collection.history.get()
        # There should be two namechange objects.
        self.assertEqual(history.collectionnamechange.count(), 2)
        first_name_change = history.collectionnamechange.first()
        last_name_change = history.collectionnamechange.last()
        self.assertIsNotNone(first_name_change)
        self.assertIsNotNone(last_name_change)
        # Add asserts bellow to keep mypy happy.
        assert first_name_change is not None
        assert last_name_change is not None
        self.assertEqual(first_name_change.value, "collection")
        self.assertEqual(last_name_change.value, collection.name)
        self.assertEqual(history.collectionnamechange.count(), 2)
        self.assertEqual(history.collectionslugchange.count(), 1)

        collection.slug = "new-slug"
        collection.save()
        history = collection.history.get()
        # There should be two slugchange objects.
        first_slug_change = history.collectionslugchange.first()
        last_slug_change = history.collectionslugchange.last()
        self.assertIsNotNone(first_slug_change)
        self.assertIsNotNone(last_slug_change)
        # Add asserts bellow to keep mypy happy.
        assert first_slug_change is not None
        assert last_slug_change is not None
        self.assertEqual(history.collectionslugchange.count(), 2)
        self.assertEqual(first_slug_change.value, "collection")
        self.assertEqual(last_slug_change.value, collection.slug)
        self.assertEqual(history.collectionnamechange.count(), 2)
        self.assertEqual(history.collectionslugchange.count(), 2)

    def test_history_values(self):
        """Test getting values at certain timestamp."""
        collection = Collection.objects.create(
            name="collection", contributor=self.contributor
        )
        collection.name = "new name"
        collection.save()
        history = collection.history.get()
        self.assertEqual(history.collectionnamechange.count(), 2)
        first_change = history.collectionnamechange.first()
        second_change = history.collectionnamechange.last()
        self.assertIsNotNone(first_change)
        self.assertIsNotNone(second_change)
        # Add asserts bellow to keep mypy happy.
        assert first_change is not None
        assert second_change is not None
        second_change.timestamp = second_change.timestamp + timedelta(seconds=2)
        second_change.save(update_fields=["timestamp"])
        before_timestamp = first_change.timestamp - timedelta(seconds=1)
        after_timestamp = second_change.timestamp + timedelta(seconds=1)
        between_timestamp = first_change.timestamp + timedelta(seconds=1)
        # Reading value before the first change should raise RuntimeError.
        with self.assertRaises(RuntimeError):
            history_manager.valid_value(collection, "name", before_timestamp)
        value_between = history_manager.valid_value(
            collection, "name", between_timestamp
        )
        value_after = history_manager.valid_value(collection, "name", after_timestamp)

        self.assertEqual(value_between, "collection")
        self.assertEqual(value_after, "new name")

    def test_delete(self):
        """Test deletion is tracked."""
        collection = Collection.objects.create(
            name="collection", contributor=self.contributor
        )
        history = collection.history.get()
        self.assertIsNotNone(history.datum)
        self.assertIsNone(history.deleted)
        collection.delete()
        history.refresh_from_db()
        current_time = now()
        self.assertIsNone(history.datum)
        self.assertIsNotNone(history.deleted)
        assert history.deleted is not None  # keep mypy happy
        self.assertLess(current_time - history.deleted, timedelta(seconds=1))
