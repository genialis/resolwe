# pylint: disable=missing-docstring
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from unittest.mock import MagicMock, call, patch

from django.db import connection, transaction

from resolwe.storage.cleanup import Cleaner
from resolwe.storage.models import FileStorage, StorageLocation
from resolwe.test import TransactionTestCase


class CleanupTest(TransactionTestCase):
    def setUp(self):
        self.file_storage1: FileStorage = FileStorage.objects.create()
        self.file_storage2: FileStorage = FileStorage.objects.create()
        self.cleaner = Cleaner()
        super().setUp()

    def test_process(self):
        process_filestorage_mock = MagicMock()
        with patch(
            "resolwe.storage.cleanup.Cleaner._process_file_storage",
            process_filestorage_mock,
        ):
            self.cleaner.process()
        self.assertEqual(process_filestorage_mock.call_count, 2)
        self.assertIn(call(self.file_storage1), process_filestorage_mock.call_args_list)
        self.assertIn(call(self.file_storage2), process_filestorage_mock.call_args_list)

    def test_skip_locked(self):
        rows_locked = Event()
        cleanup_finished = Event()

        def task_a(lock_ids=[]):
            with transaction.atomic():
                list(FileStorage.objects.select_for_update().filter(id__in=lock_ids))
                rows_locked.set()
                cleanup_finished.wait()
            connection.close()

        def task_b():
            rows_locked.wait()
            self.cleaner.process()
            cleanup_finished.set()
            connection.close()

        process_filestorage_mock = MagicMock()
        with patch(
            "resolwe.storage.cleanup.Cleaner._process_file_storage",
            process_filestorage_mock,
        ):
            with ThreadPoolExecutor() as executor:
                executor.submit(task_a, [self.file_storage1.id, self.file_storage2.id])
                executor.submit(task_b)
        process_filestorage_mock.assert_not_called()

        rows_locked.clear()
        cleanup_finished.clear()
        process_filestorage_mock = MagicMock()
        with patch(
            "resolwe.storage.cleanup.Cleaner._process_file_storage",
            process_filestorage_mock,
        ):
            with ThreadPoolExecutor() as executor:
                executor.submit(task_a, [self.file_storage1.id])
                executor.submit(task_b)
        process_filestorage_mock.assert_called_once_with(self.file_storage2)

    def test_process_file_storage(self):
        delete_mock = MagicMock()
        filter_mock = MagicMock(return_value=[])
        locations_mock = MagicMock(filter=filter_mock, count=lambda: 0)
        file_storage_mock = MagicMock(
            storage_locations=locations_mock, delete=delete_mock
        )
        self.cleaner._process_file_storage(file_storage_mock)
        filter_mock.assert_called_once_with(status=StorageLocation.STATUS_DELETING)
        delete_mock.assert_called_once_with()

        delete_mock = MagicMock()
        filter_mock = MagicMock(return_value=[])
        locations_mock = MagicMock(filter=filter_mock, count=lambda: 1)
        file_storage_mock = MagicMock(
            storage_locations=locations_mock, delete=delete_mock
        )
        self.cleaner._process_file_storage(file_storage_mock)
        filter_mock.assert_called_once_with(status=StorageLocation.STATUS_DELETING)
        delete_mock.assert_not_called()

        delete_mock = MagicMock()
        filter_mock = MagicMock(return_value=["My location"])
        locations_mock = MagicMock(filter=filter_mock, count=lambda: 1)
        file_storage_mock = MagicMock(
            storage_locations=locations_mock, delete=delete_mock
        )
        cleanup_mock = MagicMock()
        with patch(
            "resolwe.storage.cleanup.Cleaner._cleanup",
            cleanup_mock,
        ):
            self.cleaner._process_file_storage(file_storage_mock)
        cleanup_mock.assert_called_once_with("My location")
        filter_mock.assert_called_once_with(status=StorageLocation.STATUS_DELETING)
        delete_mock.assert_not_called()

    def test_delete(self):
        connector_mock = MagicMock()
        delete_mock = MagicMock()
        storage_location = MagicMock(connector=connector_mock, delete=delete_mock)
        self.cleaner._cleanup(storage_location)
        delete_mock.assert_called_once_with()

        connector_mock = MagicMock()
        delete_mock = MagicMock(side_effect=Exception)
        storage_location = MagicMock(connector=connector_mock, delete=delete_mock)
        self.cleaner._cleanup(storage_location)
        delete_mock.assert_called_once_with()
