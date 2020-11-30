# pylint: disable=missing-docstring
import copy
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from threading import Event
from unittest.mock import MagicMock, call, patch

from django.db import connection, transaction
from django.utils import timezone

from resolwe.flow.models import Data
from resolwe.storage.connectors import (
    AwsS3Connector,
    GoogleConnector,
    LocalFilesystemConnector,
)
from resolwe.storage.connectors.exceptions import DataTransferError
from resolwe.storage.manager import DecisionMaker, Manager
from resolwe.storage.models import (
    AccessLog,
    FileStorage,
    ReferencedPath,
    StorageLocation,
)
from resolwe.test import TestCase, TransactionTestCase

CONNECTORS_SETTINGS = {
    "local": {
        "connector": "resolwe.storage.connectors.localconnector.LocalFilesystemConnector",
        "config": {
            "priority": 1,
            "path": "/",
        },
    },
    "S3": {
        "connector": "resolwe.storage.connectors.s3connector.AwsS3Connector",
        "config": {
            "priority": 100,
            "bucket": "genialis-test-storage",
            "copy": {
                "delay": 2,
            },
            "delete": {
                "delay": 5,
            },
            "credentials": "test.json",
        },
    },
    "GCS": {
        "connector": "resolwe.storage.connectors.googleconnector.GoogleConnector",
        "config": {
            "priority": 200,
            "bucket": "genialis_storage_test",
            "copy": {
                "delay": 3,
            },
            "delete": {"delay": 5, "min_other_copies": 2},
            "credentials": "test.json",
        },
    },
}

CONNECTORS = {
    "local": LocalFilesystemConnector(CONNECTORS_SETTINGS["local"]["config"], "local"),
    "GCS": GoogleConnector(CONNECTORS_SETTINGS["GCS"]["config"], "GCS"),
    "S3": AwsS3Connector(CONNECTORS_SETTINGS["S3"]["config"], "S3"),
}


@patch("resolwe.storage.models.connectors", CONNECTORS)
@patch("resolwe.storage.manager.connectors", CONNECTORS)
@patch("resolwe.storage.manager.STORAGE_CONNECTORS", CONNECTORS_SETTINGS)
class DecisionMakerTest(TestCase):
    fixtures = [
        "storage_processes.yaml",
        "storage_data.yaml",
    ]

    def setUp(self):
        self.file_storage: FileStorage = FileStorage.objects.get(pk=1)
        self.decision_maker = DecisionMaker(self.file_storage)
        super().setUp()

    def test_norule(self):
        storage_location: StorageLocation = StorageLocation.objects.create(
            file_storage=self.file_storage, url="url", connector_name="S3"
        )
        FileStorage.objects.filter(pk=self.file_storage.pk).update(
            created=timezone.now() - timedelta(days=30)
        )
        StorageLocation.objects.filter(pk=storage_location.pk).update(
            last_update=timezone.now() - timedelta(days=30)
        )
        storage_location.refresh_from_db()
        self.file_storage.refresh_from_db()
        with patch(
            "resolwe.storage.manager.STORAGE_CONNECTORS",
            {"local": CONNECTORS_SETTINGS["local"]},
        ):
            self.assertEqual(self.decision_maker.copy(), [])
            self.assertIsNone(self.decision_maker.delete())

    def test_copy(self):
        storage_location: StorageLocation = StorageLocation.objects.create(
            file_storage=self.file_storage,
            url="url",
            connector_name="local",
        )
        FileStorage.objects.filter(pk=self.file_storage.pk).update(
            created=timezone.now() - timedelta(days=2)
        )
        self.file_storage.refresh_from_db()
        self.assertEqual(self.decision_maker.copy(), [])

        storage_location.status = StorageLocation.STATUS_DONE
        storage_location.save()
        self.assertEqual(self.decision_maker.copy(), ["S3"])

        FileStorage.objects.filter(pk=self.file_storage.pk).update(
            created=timezone.now() - timedelta(days=3)
        )
        self.file_storage.refresh_from_db()
        copies = self.decision_maker.copy()
        self.assertEqual(len(copies), 2)
        self.assertIn("S3", copies)
        self.assertIn("GCS", copies)

    def test_copy_negative_delay(self):
        StorageLocation.objects.create(
            file_storage=self.file_storage,
            url="url",
            connector_name="local",
            status=StorageLocation.STATUS_DONE,
        )
        FileStorage.objects.filter(pk=self.file_storage.pk).update(
            created=timezone.now() - timedelta(days=3)
        )
        self.file_storage.refresh_from_db()

        connectors_settings = copy.deepcopy(CONNECTORS_SETTINGS)
        connectors_settings["S3"]["config"]["copy"]["delay"] = -1
        with patch("resolwe.storage.manager.STORAGE_CONNECTORS", connectors_settings):
            copies = self.decision_maker.copy()
        self.assertEqual(len(copies), 1)
        self.assertIn("GCS", copies)

    def test_delete_last(self):
        location_s3 = StorageLocation.objects.create(
            file_storage=self.file_storage,
            url="url",
            connector_name="S3",
            status=StorageLocation.STATUS_DONE,
        )
        StorageLocation.objects.filter(pk=location_s3.pk).update(
            last_update=timezone.now() - timedelta(days=30)
        )
        self.assertIsNone(self.decision_maker.delete())

    def test_delete_early(self):
        location_s3 = StorageLocation.objects.create(
            file_storage=self.file_storage,
            url="url",
            connector_name="S3",
            status=StorageLocation.STATUS_DONE,
        )
        StorageLocation.objects.create(
            file_storage=self.file_storage,
            url="url",
            connector_name="GCS",
            status=StorageLocation.STATUS_DONE,
        )
        StorageLocation.objects.filter(pk=location_s3.pk).update(
            last_update=timezone.now() - timedelta(days=4)
        )
        self.assertIsNone(self.decision_maker.delete())

    def test_delete(self):
        location_s3: StorageLocation = StorageLocation.objects.create(
            file_storage=self.file_storage,
            url="url",
            connector_name="S3",
            status=StorageLocation.STATUS_DONE,
        )
        StorageLocation.objects.create(
            file_storage=self.file_storage,
            url="url",
            connector_name="local",
            status=StorageLocation.STATUS_DONE,
        )
        StorageLocation.objects.filter(pk=location_s3.pk).update(
            last_update=timezone.now() - timedelta(days=5)
        )
        access_log = AccessLog.objects.create(storage_location=location_s3)
        self.assertIsNone(self.decision_maker.delete())

        access_log.delete()
        self.assertEqual(self.decision_maker.delete(), location_s3)

        StorageLocation.objects.filter(pk=location_s3.pk).update(
            status=StorageLocation.STATUS_DELETING
        )
        self.assertIsNone(self.decision_maker.delete())

    def test_delete_negative_delay(self):
        location_s3 = StorageLocation.objects.create(
            file_storage=self.file_storage,
            url="url",
            connector_name="S3",
            status=StorageLocation.STATUS_DONE,
        )
        StorageLocation.objects.create(
            file_storage=self.file_storage,
            url="url",
            connector_name="local",
            status=StorageLocation.STATUS_DONE,
        )
        StorageLocation.objects.filter(pk=location_s3.pk).update(
            last_update=timezone.now() - timedelta(days=5)
        )
        connectors_settings = copy.deepcopy(CONNECTORS_SETTINGS)
        connectors_settings["S3"]["config"]["delete"]["delay"] = -1
        with patch("resolwe.storage.manager.STORAGE_CONNECTORS", connectors_settings):
            self.assertIsNone(self.decision_maker.delete())

    def test_delete_mincopy(self):
        StorageLocation.objects.create(
            file_storage=self.file_storage,
            url="url",
            connector_name="local",
            status=StorageLocation.STATUS_DONE,
        )
        location_gcs = StorageLocation.objects.create(
            file_storage=self.file_storage,
            url="url",
            connector_name="GCS",
            status=StorageLocation.STATUS_DONE,
        )
        StorageLocation.objects.filter(pk=location_gcs.pk).update(
            last_update=timezone.now() - timedelta(days=5)
        )
        self.assertIsNone(self.decision_maker.delete())
        storage_location = StorageLocation.objects.create(
            file_storage=self.file_storage,
            url="url",
            connector_name="GCS1",
            status=StorageLocation.STATUS_DELETING,
        )
        self.assertIsNone(self.decision_maker.delete())
        storage_location.status = StorageLocation.STATUS_DONE
        storage_location.save()
        self.assertEqual(self.decision_maker.delete(), location_gcs)

    def test_delete_priority(self):
        location_gcs = StorageLocation.objects.create(
            file_storage=self.file_storage,
            url="url",
            connector_name="GCS",
            status=StorageLocation.STATUS_DONE,
        )
        location_s3 = StorageLocation.objects.create(
            file_storage=self.file_storage,
            url="url",
            connector_name="S3",
            status=StorageLocation.STATUS_DONE,
        )
        StorageLocation.objects.filter(pk=location_gcs.pk).update(
            last_update=timezone.now() - timedelta(days=5)
        )
        StorageLocation.objects.filter(pk=location_s3.pk).update(
            last_update=timezone.now() - timedelta(days=5)
        )
        # Do not delete location with highest priority.
        self.assertIsNone(self.decision_maker.delete())

        StorageLocation.objects.create(
            file_storage=self.file_storage,
            url="url",
            connector_name="local",
            status=StorageLocation.STATUS_DONE,
        )
        with patch.dict(
            CONNECTORS,
            {
                "GCS": MagicMock(priority=CONNECTORS["GCS"].priority),
                "S3": MagicMock(priority=CONNECTORS["S3"].priority),
            },
        ):
            self.assertEqual(self.decision_maker.delete(), location_gcs)
            location_gcs.delete()
            self.assertEqual(self.decision_maker.delete(), location_s3)
            location_s3.delete()
            self.assertIsNone(self.decision_maker.delete())


@patch("resolwe.storage.manager.connectors", CONNECTORS)
@patch("resolwe.storage.manager.STORAGE_CONNECTORS", CONNECTORS_SETTINGS)
class DecisionMakerOverrideRuleTest(TestCase):
    fixtures = [
        "storage_processes.yaml",
        "storage_data.yaml",
    ]

    def setUp(self):
        self.file_storage1: FileStorage = FileStorage.objects.create()
        self.file_storage2: FileStorage = FileStorage.objects.create()
        self.file_storage1.data.add(Data.objects.get(pk=1))
        self.file_storage2.data.add(Data.objects.get(pk=2))
        super().setUp()

    def test_override_process_type(self):
        decision_maker = DecisionMaker(self.file_storage1)
        settings = copy.deepcopy(CONNECTORS_SETTINGS)
        override = {"data:test": {"delay": 10}}
        override_nonexisting = {"data:nonexisting": {"delay": 10}}
        FileStorage.objects.filter(pk=self.file_storage1.pk).update(
            created=timezone.now() - timedelta(days=6)
        )
        self.file_storage1.refresh_from_db()
        StorageLocation.objects.create(
            file_storage=self.file_storage1,
            url="url",
            connector_name="S3",
            status=StorageLocation.STATUS_DONE,
        )
        self.assertEqual(decision_maker.copy(), ["GCS"])

        settings["GCS"]["config"]["copy"]["process_type"] = override
        with patch(
            "resolwe.storage.manager.STORAGE_CONNECTORS",
            settings,
        ):
            self.assertEqual(decision_maker.copy(), [])

        settings["GCS"]["config"]["copy"]["process_type"] = override_nonexisting
        with patch(
            "resolwe.storage.manager.STORAGE_CONNECTORS",
            settings,
        ):
            self.assertEqual(decision_maker.copy(), ["GCS"])

    def test_override_data_slug(self):
        decision_maker = DecisionMaker(self.file_storage1)
        settings = copy.deepcopy(CONNECTORS_SETTINGS)
        override = {"test_data": {"delay": 10}}
        override_nonexisting = {"data_nonexisting": {"delay": 10}}
        FileStorage.objects.filter(pk=self.file_storage1.pk).update(
            created=timezone.now() - timedelta(days=6)
        )
        self.file_storage1.refresh_from_db()
        StorageLocation.objects.create(
            file_storage=self.file_storage1,
            url="url",
            connector_name="S3",
            status=StorageLocation.STATUS_DONE,
        )
        self.assertEqual(decision_maker.copy(), ["GCS"])

        settings["GCS"]["config"]["copy"]["data_slug"] = override
        with patch(
            "resolwe.storage.manager.STORAGE_CONNECTORS",
            settings,
        ):
            self.assertEqual(decision_maker.copy(), [])

        settings["GCS"]["config"]["copy"]["data_slug"] = override_nonexisting
        with patch(
            "resolwe.storage.manager.STORAGE_CONNECTORS",
            settings,
        ):
            self.assertEqual(decision_maker.copy(), ["GCS"])

    def test_override_priority(self):
        decision_maker = DecisionMaker(self.file_storage1)
        settings = copy.deepcopy(CONNECTORS_SETTINGS)
        override_process_type = {"test:data:": {"delay": 10}}
        override_data_slug = {"test_data": {"delay": 5}}
        FileStorage.objects.filter(pk=self.file_storage1.pk).update(
            created=timezone.now() - timedelta(days=6)
        )
        self.file_storage1.refresh_from_db()
        StorageLocation.objects.create(
            file_storage=self.file_storage1,
            url="url",
            connector_name="S3",
            status=StorageLocation.STATUS_DONE,
        )
        self.assertEqual(decision_maker.copy(), ["GCS"])

        settings["GCS"]["config"]["copy"]["data_slug"] = override_data_slug
        settings["GCS"]["config"]["copy"]["process_type"] = override_process_type

        with patch(
            "resolwe.storage.manager.STORAGE_CONNECTORS",
            settings,
        ):
            self.assertEqual(decision_maker.copy(), ["GCS"])

        override_data_slug["test_data"]["delay"] = 10
        override_process_type["test:data:"]["delay"] = 5
        with patch(
            "resolwe.storage.manager.STORAGE_CONNECTORS",
            settings,
        ):
            self.assertEqual(decision_maker.copy(), [])


@patch("resolwe.storage.manager.connectors", CONNECTORS)
@patch("resolwe.storage.manager.STORAGE_CONNECTORS", CONNECTORS_SETTINGS)
@patch("resolwe.storage.models.connectors", CONNECTORS)
class ManagerTest(TransactionTestCase):
    fixtures = [
        "storage_processes.yaml",
        "storage_data.yaml",
        "storage_users.yaml",
    ]

    def setUp(self):
        self.file_storage1: FileStorage = FileStorage.objects.get(pk=1)
        self.file_storage2: FileStorage = FileStorage.objects.get(pk=2)
        self.manager = Manager()
        super().setUp()

    def test_process(self):
        process_filestorage_mock = MagicMock()
        with patch(
            "resolwe.storage.manager.Manager._process_file_storage",
            process_filestorage_mock,
        ):
            self.manager.process()
        self.assertEqual(process_filestorage_mock.call_count, 2)
        self.assertIn(call(self.file_storage1), process_filestorage_mock.call_args_list)
        self.assertIn(call(self.file_storage2), process_filestorage_mock.call_args_list)

    def test_skip_locked(self):
        rows_locked = Event()
        manager_finished = Event()

        def task_a(lock_ids=[]):
            with transaction.atomic():
                list(FileStorage.objects.select_for_update().filter(id__in=lock_ids))
                rows_locked.set()
                manager_finished.wait()
            connection.close()

        def task_b():
            rows_locked.wait()
            self.manager.process()
            manager_finished.set()
            connection.close()

        process_filestorage_mock = MagicMock()
        with patch(
            "resolwe.storage.manager.Manager._process_file_storage",
            process_filestorage_mock,
        ):
            with ThreadPoolExecutor() as executor:
                executor.submit(task_a, [self.file_storage1.id, self.file_storage2.id])
                executor.submit(task_b)
        process_filestorage_mock.assert_not_called()

        rows_locked.clear()
        manager_finished.clear()
        process_filestorage_mock = MagicMock()
        with patch(
            "resolwe.storage.manager.Manager._process_file_storage",
            process_filestorage_mock,
        ):
            with ThreadPoolExecutor() as executor:
                executor.submit(task_a, [self.file_storage1.id])
                executor.submit(task_b)
        process_filestorage_mock.assert_called_once_with(self.file_storage2)

    def test_transfer(self):
        FileStorage.objects.filter(pk=self.file_storage1.pk).update(
            created=timezone.now() - timedelta(days=2)
        )
        self.file_storage1.refresh_from_db()
        location_local = StorageLocation.objects.create(
            file_storage=self.file_storage1,
            url="url",
            connector_name="local",
            status=StorageLocation.STATUS_DONE,
        )
        path = ReferencedPath.objects.create(
            path="testme.txt",
        )
        path.storage_locations.add(location_local)
        transfer_objects = MagicMock(return_value=None)
        transfer_instance = MagicMock(transfer_objects=transfer_objects)
        transfer_module = MagicMock(return_value=transfer_instance)
        with patch("resolwe.storage.models.Transfer", transfer_module):
            self.manager._process_file_storage(self.file_storage1)
        transfer_objects.assert_called_once()
        self.assertEqual(len(transfer_objects.call_args[0]), 2)
        arg1, arg2 = transfer_objects.call_args[0]
        self.assertEqual(arg1, "url")
        self.assertEqual(len(arg2), 1)
        self.assertEqual(arg2[0]["path"], "testme.txt")
        self.assertEqual(AccessLog.objects.all().count(), 1)
        self.assertEqual(StorageLocation.objects.all().count(), 2)
        created_location = StorageLocation.objects.exclude(pk=location_local.pk).get()
        self.assertEqual(created_location.connector_name, "S3")
        self.assertEqual(created_location.url, "url")
        access_log = AccessLog.objects.all().first()
        self.assertEqual(access_log.storage_location, location_local)
        self.assertIsNotNone(access_log.finished)

    def test_transfer_failed(self):
        def raise_datatransfererror(*args, **kwargs):
            raise DataTransferError()

        FileStorage.objects.filter(pk=self.file_storage1.pk).update(
            created=timezone.now() - timedelta(days=2)
        )
        self.file_storage1.refresh_from_db()
        location_local = StorageLocation.objects.create(
            file_storage=self.file_storage1,
            url="url",
            connector_name="local",
            status=StorageLocation.STATUS_DONE,
        )
        path = ReferencedPath.objects.create(
            path="testme.txt",
        )
        path.storage_locations.add(location_local)
        transfer_objects = MagicMock(side_effect=raise_datatransfererror)
        transfer_instance = MagicMock(transfer_objects=transfer_objects)
        transfer_module = MagicMock(return_value=transfer_instance)
        with patch.dict(
            CONNECTORS,
            {
                "GCS": MagicMock(priority=CONNECTORS["GCS"].priority),
                "S3": MagicMock(priority=CONNECTORS["S3"].priority),
            },
        ):
            with patch("resolwe.storage.models.Transfer", transfer_module):
                self.manager._process_file_storage(self.file_storage1)
        transfer_objects.assert_called_once()
        self.assertEqual(len(transfer_objects.call_args[0]), 2)
        arg1, arg2 = transfer_objects.call_args[0]
        self.assertEqual(arg1, "url")
        self.assertEqual(len(arg2), 1)
        self.assertEqual(arg2[0]["path"], "testme.txt")
        self.assertEqual(AccessLog.objects.all().count(), 1)
        self.assertEqual(StorageLocation.objects.all().count(), 1)
        self.assertEqual(location_local, StorageLocation.objects.all().first())
        access_log = AccessLog.objects.all().first()
        self.assertEqual(access_log.storage_location, location_local)
        self.assertIsNotNone(access_log.finished)

    def test_delete(self):
        location_local: StorageLocation = StorageLocation.objects.create(
            file_storage=self.file_storage1,
            url="url",
            connector_name="local",
            status=StorageLocation.STATUS_DONE,
        )
        # Do not delete.
        copy = MagicMock(return_value=[])
        delete = MagicMock(return_value=None)
        decision_maker = MagicMock(copy=copy, delete=delete)
        DecisionMaker = MagicMock(return_value=decision_maker)
        with patch("resolwe.storage.manager.DecisionMaker", DecisionMaker):
            self.manager._process_file_storage(self.file_storage1)
        copy.assert_called_once_with()
        delete.assert_called_once_with()

        # Delete location_local.
        delete_data = MagicMock()
        location_local.delete_data = delete_data()
        copy = MagicMock(return_value=[])
        delete = MagicMock(side_effect=[location_local, None])
        decision_maker = MagicMock(copy=copy, delete=delete)
        DecisionMaker = MagicMock(return_value=decision_maker)
        with patch("resolwe.storage.manager.DecisionMaker", DecisionMaker):
            self.manager._process_file_storage(self.file_storage1)
        copy.assert_called_once_with()
        self.assertEqual(delete.call_count, 2)
        delete_data.assert_called_once_with()

    def test_delete_failed(self):
        # Error while deleting location, do not fall into endless loop.
        delete_location = MagicMock()
        location_local_mock = MagicMock(
            spec=StorageLocation, delete=delete_location, connector_name="local"
        )
        file_storage_mock = MagicMock(
            spec=FileStorage, default_storage_location=location_local_mock
        )
        copy = MagicMock(return_value=[])
        delete = MagicMock(return_value=location_local_mock)
        decision_maker = MagicMock(copy=copy, delete=delete)
        DecisionMaker = MagicMock(return_value=decision_maker)
        with patch("resolwe.storage.manager.DecisionMaker", DecisionMaker):
            self.manager._process_file_storage(file_storage_mock)
        copy.assert_called_once_with()
        self.assertEqual(delete.call_count, 2)
        delete_location.assert_called_once_with()
