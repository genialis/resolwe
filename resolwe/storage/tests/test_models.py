# pylint: disable=missing-docstring
from datetime import timedelta
from unittest.mock import MagicMock, patch

from django.db.utils import IntegrityError
from django.utils import timezone

from resolwe.storage.connectors import (
    AwsS3Connector,
    GoogleConnector,
    LocalFilesystemConnector,
)
from resolwe.storage.models import (
    AccessLog,
    FileStorage,
    ReferencedPath,
    StorageLocation,
)
from resolwe.test import TransactionTestCase

CONNECTORS_SETTINGS = {
    "local": {
        "connector": "resolwe.storage.connectors.localconnector.LocalFilesystemConnector",
        "config": {
            "priority": 1,
            "path": "/",
            "delete": {
                "delay": 2,
                "min_other_copies": 2,
            },
        },
    },
    "S3": {
        "connector": "resolwe.storage.connectors.s3connector.AwsS3Connector",
        "config": {
            "priority": 200,
            "bucket": "genialis-test-storage",
            "credentials": "test.json",
        },
    },
    "GCS": {
        "connector": "resolwe.storage.connectors.googleconnector.GoogleConnector",
        "config": {
            "bucket": "genialis-test-storage",
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
class FileStorageTest(TransactionTestCase):
    def setUp(self):
        self.file_storage: FileStorage = FileStorage.objects.create()
        super().setUp()

    def test_default_values(self):
        self.assertLessEqual(
            timezone.now() - self.file_storage.created, timedelta(seconds=1)
        )

    def test_has_storage_location(self):
        self.assertFalse(self.file_storage.has_storage_location("local"))
        storage_local = StorageLocation.objects.create(
            file_storage=self.file_storage, url="url", connector_name="local"
        )
        self.assertFalse(self.file_storage.has_storage_location("local"))
        storage_local.status = StorageLocation.STATUS_DONE
        storage_local.save()
        self.assertTrue(self.file_storage.has_storage_location("local"))

    def test_default_storage_location(self):
        self.assertIsNone(self.file_storage.default_storage_location)
        storage_s3: StorageLocation = StorageLocation.objects.create(
            file_storage=self.file_storage, url="url", connector_name="S3"
        )
        self.assertEqual(self.file_storage.default_storage_location, storage_s3)
        storage_s3.status = StorageLocation.STATUS_DONE
        storage_s3.save()
        self.assertEqual(self.file_storage.default_storage_location, storage_s3)
        storage_gcs: StorageLocation = StorageLocation.objects.create(
            file_storage=self.file_storage, url="url", connector_name="GCS"
        )
        self.assertEqual(self.file_storage.default_storage_location, storage_s3)
        storage_gcs.status = StorageLocation.STATUS_DONE
        storage_gcs.save()
        # GCS should be considered before S3 (default priority 100)
        self.assertEqual(self.file_storage.default_storage_location, storage_gcs)
        storage_local: StorageLocation = StorageLocation.objects.create(
            file_storage=self.file_storage, url="url", connector_name="local"
        )
        self.assertEqual(self.file_storage.default_storage_location, storage_gcs)
        storage_local.status = StorageLocation.STATUS_DONE
        storage_local.save()
        # Local location should be default (lowest priority)
        self.assertEqual(self.file_storage.default_storage_location, storage_local)

    def test_default_storage_location_nonexisting(self):
        StorageLocation.objects.create(
            file_storage=self.file_storage,
            url="url",
            connector_name="S3",
            status=StorageLocation.STATUS_DONE,
        )
        # This one should have default priority of 100.
        nonexisting = StorageLocation.objects.create(
            file_storage=self.file_storage,
            url="url",
            connector_name="nonexisting",
            status=StorageLocation.STATUS_DONE,
        )
        self.assertEqual(self.file_storage.default_storage_location, nonexisting)

    def test_get_path(self):
        with self.assertRaises(AttributeError):
            self.file_storage.get_path()
        StorageLocation.objects.create(
            file_storage=self.file_storage,
            url="url",
            connector_name="local",
            status=StorageLocation.STATUS_DONE,
        )
        StorageLocation.objects.create(
            file_storage=self.file_storage,
            url="url",
            connector_name="S3",
            status=StorageLocation.STATUS_DONE,
        )
        self.assertEqual(self.file_storage.get_path(), "/url")
        self.assertEqual(self.file_storage.get_path(filename="test"), "/url/test")
        self.assertEqual(self.file_storage.get_path(prefix="/prefix"), "/prefix/url")
        self.assertEqual(
            self.file_storage.get_path(prefix="/prefix", filename="test"),
            "/prefix/url/test",
        )

    def test_subpath(self):
        with self.assertRaises(AttributeError):
            self.assertEqual(self.file_storage.subpath, "url")
        StorageLocation.objects.create(
            file_storage=self.file_storage,
            url="s3url",
            connector_name="S3",
            status=StorageLocation.STATUS_DONE,
        )
        self.assertEqual(self.file_storage.subpath, "s3url")
        StorageLocation.objects.create(
            file_storage=self.file_storage,
            url="localurl",
            connector_name="local",
            status=StorageLocation.STATUS_DONE,
        )
        self.assertEqual(self.file_storage.subpath, "localurl")


class StorageLocationTest(TransactionTestCase):
    def setUp(self):
        self.file_storage: FileStorage = FileStorage.objects.create()
        super().setUp()

    def test_default_values(self):
        location: StorageLocation = StorageLocation.objects.create(
            file_storage=self.file_storage, url="url"
        )
        self.assertEqual(location.status, StorageLocation.STATUS_PREPARING)
        self.assertLessEqual(
            timezone.now() - location.last_update, timedelta(seconds=1)
        )

    def test_last_update(self):
        location: StorageLocation = StorageLocation.objects.create(
            file_storage=self.file_storage, url="url"
        )
        last_update = location.last_update
        location.status = StorageLocation.STATUS_DONE
        location.save()
        self.assertNotEqual(location.last_update, last_update)

    def test_default_object_manager(self):
        location_preparing: StorageLocation = StorageLocation.objects.create(
            file_storage=self.file_storage, url="preparing"
        )
        location_done: StorageLocation = StorageLocation.objects.create(
            file_storage=self.file_storage,
            url="done",
            status=StorageLocation.STATUS_DONE,
        )
        done_objects = StorageLocation.objects.all()
        all_objects = StorageLocation.all_objects.all()
        self.assertEqual(done_objects.count(), 1)
        self.assertIn(location_done, done_objects)
        self.assertEqual(all_objects.count(), 2)
        self.assertIn(location_preparing, all_objects)
        self.assertIn(location_done, all_objects)

    def test_unique_together(self):
        StorageLocation.objects.create(file_storage=self.file_storage, url="url")
        StorageLocation.objects.create(file_storage=self.file_storage, url="url1")
        StorageLocation.objects.create(
            file_storage=self.file_storage, url="url", connector_name="S3"
        )
        with self.assertRaises(IntegrityError):
            StorageLocation.objects.create(file_storage=self.file_storage, url="url")
        with self.assertRaises(IntegrityError):
            StorageLocation.objects.create(
                file_storage=self.file_storage, url="url", connector_name="S3"
            )

    def test_transfer_data(self):
        file = ReferencedPath.objects.create(path="existing.path")
        source = StorageLocation.objects.create(
            file_storage=self.file_storage, url="url", connector_name="local"
        )
        source.files.add(file)

        destination = StorageLocation.objects.create(
            file_storage=self.file_storage, url="url", connector_name="S3"
        )
        transfer_objects_mock = MagicMock(
            side_effect=lambda url, files: files
            + [
                {
                    "path": "new.path",
                    "size": 12,
                    "md5": "1",
                    "crc32c": "2",
                    "awss3etag": "3",
                }
            ]
        )
        transfer_mock = MagicMock(
            return_value=MagicMock(transfer_objects=transfer_objects_mock)
        )
        with patch("resolwe.storage.models.Transfer", transfer_mock):
            source.transfer_data(destination)

        self.assertCountEqual(source.files.all(), [file])
        new_file = destination.files.get(path="new.path")
        self.assertEqual(new_file.size, 12)
        self.assertEqual(new_file.md5, "1")
        self.assertEqual(new_file.crc32c, "2")
        self.assertEqual(new_file.awss3etag, "3")
        self.assertCountEqual(destination.files.all(), [file, new_file])

    def test_delete_data(self):
        path1 = ReferencedPath.objects.create(path="remove_me.txt")
        path2 = ReferencedPath.objects.create(path="dir/remove_me.txt")
        storage_location = StorageLocation.objects.create(
            file_storage=self.file_storage, url="url", connector_name="local"
        )
        storage_location.files.add(path1, path2)
        delete = MagicMock()
        connector_mock = MagicMock(delete=delete)
        with patch("resolwe.storage.models.connectors", {"local": connector_mock}):
            storage_location.delete_data()
        self.assertEqual(delete.call_count, 1)
        self.assertCountEqual(
            delete.call_args_list[0][0][1],
            ["remove_me.txt", "dir/remove_me.txt"],
        )

    def test_verify_data_ok(self):
        path1 = ReferencedPath.objects.create(
            path="1", md5="1", crc32c="1", awss3etag="1"
        )
        storage_location: StorageLocation = StorageLocation.objects.create(
            file_storage=self.file_storage, url="url", connector_name="local"
        )
        storage_location.files.add(path1)
        with patch(
            "resolwe.storage.models.StorageLocation.connector"
        ) as connector_mock:
            connector_mock.duplicate = MagicMock(
                return_value=MagicMock(check_url=MagicMock(return_value=True))
            )
            connector_mock.get_hashes = MagicMock(
                side_effect=[
                    {"md5": "1", "crc32c": "1", "awss3etag": "1"},
                ]
            )
            verified = storage_location.verify_data()
            self.assertTrue(verified)

    def test_verify_data_connector_failed(self):
        path1 = ReferencedPath.objects.create(
            path="1", md5="1", crc32c="1", awss3etag="1"
        )
        storage_location: StorageLocation = StorageLocation.objects.create(
            file_storage=self.file_storage, url="url", connector_name="local"
        )
        storage_location.files.add(path1)
        with patch(
            "resolwe.storage.models.StorageLocation.connector"
        ) as connector_mock:
            connector_mock.duplicate = MagicMock(
                return_value=MagicMock(check_url=MagicMock(return_value=False))
            )
            connector_mock.get_hashes = MagicMock(
                side_effect=[
                    {"md5": "1", "crc32c": "1", "awss3etag": "1"},
                ]
            )
            verified = storage_location.verify_data()
            self.assertFalse(verified)

    def test_verify_data_hash_failed(self):
        path1 = ReferencedPath.objects.create(
            path="1", md5="1", crc32c="1", awss3etag="1"
        )
        storage_location: StorageLocation = StorageLocation.objects.create(
            file_storage=self.file_storage, url="url", connector_name="local"
        )
        storage_location.files.add(path1)
        with patch(
            "resolwe.storage.models.StorageLocation.connector"
        ) as connector_mock:
            connector_mock.duplicate = MagicMock(
                return_value=MagicMock(check_url=MagicMock(return_value=False))
            )
            connector_mock.get_hashes = MagicMock(
                side_effect=[
                    {"md5": "1", "crc32c": "invalid", "awss3etag": "1"},
                ]
            )
            verified = storage_location.verify_data()
            self.assertFalse(verified)


class AccessLogTest(TransactionTestCase):
    def setUp(self):
        self.file_storage: FileStorage = FileStorage.objects.create()
        self.storage_location: StorageLocation = StorageLocation.objects.create(
            file_storage=self.file_storage, url="url"
        )
        super().setUp()

    def test_default_values(self):
        log: AccessLog = AccessLog.objects.create(
            storage_location=self.storage_location, reason="reason"
        )
        self.assertIsNone(log.finished)
        self.assertLessEqual(timezone.now() - log.started, timedelta(seconds=1))
