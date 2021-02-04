# pylint: disable=missing-docstring
from pathlib import Path
from unittest.mock import patch

from resolwe.flow.executors.socket_utils import Message, Response, ResponseStatus
from resolwe.flow.managers.listener.basic_commands_plugin import BasicCommands
from resolwe.flow.managers.listener.listener import Processor
from resolwe.flow.managers.protocol import ExecutorProtocol
from resolwe.flow.models import Data, DataDependency
from resolwe.storage.connectors.baseconnector import BaseStorageConnector
from resolwe.storage.models import FileStorage, ReferencedPath, StorageLocation
from resolwe.test import TestCase


class ListenerTest(TestCase):
    fixtures = ["storage_data.yaml", "storage_processes.yaml", "storage_users.yaml"]

    @classmethod
    def setUpTestData(cls):
        cls.manager = Processor(b"1", 1, None)
        cls.processor = BasicCommands()
        cls.file_storage = FileStorage.objects.get(id=1)
        cls.storage_location = StorageLocation.objects.create(
            file_storage=cls.file_storage, connector_name="GCS", status="OK"
        )
        cls.path = ReferencedPath.objects.create(
            path="test.me", md5="md5", crc32c="crc", awss3etag="aws"
        )
        cls.storage_location.files.add(cls.path)

    def test_handle_download_finished_missing_storage_location(self):
        obj = Message.command(ExecutorProtocol.DOWNLOAD_FINISHED, -2)
        with self.assertRaises(StorageLocation.DoesNotExist):
            self.processor.handle_download_finished(obj, self.manager)

    def test_handle_download_finished(self):
        storage_location = StorageLocation.objects.create(
            file_storage=self.file_storage, connector_name="local"
        )
        obj = Message.command(ExecutorProtocol.DOWNLOAD_FINISHED, storage_location.id)

        with patch(
            "resolwe.storage.models.FileStorage.default_storage_location",
            self.storage_location,
        ):
            response = self.processor.handle_download_finished(obj, self.manager)

        self.assertEqual(response.response_status, ResponseStatus.OK)
        storage_location.refresh_from_db()
        self.assertEqual(storage_location.status, StorageLocation.STATUS_DONE)
        self.assertEqual(storage_location.files.count(), 1)
        file = storage_location.files.get()
        self.assertEqual(file.path, "test.me")
        self.assertEqual(file.md5, "md5")
        self.assertEqual(file.crc32c, "crc")
        self.assertEqual(file.awss3etag, "aws")

    def test_handle_download_aborted_missing_storage_location(self):
        obj = Message.command(ExecutorProtocol.DOWNLOAD_ABORTED, -2)
        response = self.processor.handle_download_aborted(obj, self.manager)
        self.assertEqual(response.response_status, ResponseStatus.OK)

    def test_handle_download_aborted(self):
        storage_location = StorageLocation.objects.create(
            file_storage=self.file_storage,
            connector_name="local",
            status=StorageLocation.STATUS_UPLOADING,
        )
        obj = Message.command(ExecutorProtocol.DOWNLOAD_ABORTED, storage_location.id)
        self.processor.handle_download_aborted(obj, self.manager)

        storage_location.refresh_from_db()
        self.assertEqual(storage_location.status, StorageLocation.STATUS_PREPARING)

    def test_handle_download_started_no_location(self):
        obj = Message.command(
            ExecutorProtocol.DOWNLOAD_STARTED,
            {
                "storage_location_id": -2,
                "download_started_lock": True,
            },
        )
        with self.assertRaises(StorageLocation.DoesNotExist):
            self.processor.handle_download_started(obj, self.manager)

    def test_handle_download_started_ok_no_lock_preparing(self):
        storage_location = StorageLocation.objects.create(
            file_storage=self.file_storage, connector_name="local"
        )

        obj = Message.command(
            ExecutorProtocol.DOWNLOAD_STARTED,
            {
                "storage_location_id": storage_location.id,
                "download_started_lock": False,
            },
        )
        response = self.processor.handle_download_started(obj, self.manager)
        self.assertEqual(
            response, Response(ResponseStatus.OK.value, "download_started")
        )
        storage_location.refresh_from_db()
        self.assertEqual(storage_location.status, StorageLocation.STATUS_PREPARING)

    def test_handle_download_started_ok_no_lock_uploading(self):
        storage_location = StorageLocation.objects.create(
            file_storage=self.file_storage,
            connector_name="local",
            status=StorageLocation.STATUS_UPLOADING,
        )
        obj = Message.command(
            ExecutorProtocol.DOWNLOAD_STARTED,
            {
                "storage_location_id": storage_location.id,
                "download_started_lock": False,
            },
        )

        response = self.processor.handle_download_started(obj, self.manager)
        self.assertEqual(
            response, Response(ResponseStatus.OK.value, "download_in_progress")
        )
        storage_location.refresh_from_db()
        self.assertEqual(storage_location.status, StorageLocation.STATUS_UPLOADING)

    def test_handle_download_started_ok_no_lock_done(self):
        storage_location = StorageLocation.objects.create(
            file_storage=self.file_storage,
            connector_name="local",
            status=StorageLocation.STATUS_DONE,
        )
        obj = Message.command(
            ExecutorProtocol.DOWNLOAD_STARTED,
            {
                "storage_location_id": storage_location.id,
                "download_started_lock": False,
            },
        )
        response = self.processor.handle_download_started(obj, self.manager)
        self.assertEqual(
            response, Response(ResponseStatus.OK.value, "download_finished")
        )
        storage_location.refresh_from_db()
        self.assertEqual(storage_location.status, StorageLocation.STATUS_DONE)

    def test_handle_download_started_ok_lock(self):
        storage_location = StorageLocation.objects.create(
            file_storage=self.file_storage, connector_name="local"
        )

        obj = Message.command(
            ExecutorProtocol.DOWNLOAD_STARTED,
            {
                "storage_location_id": storage_location.id,
                "download_started_lock": True,
            },
        )
        response = self.processor.handle_download_started(obj, self.manager)
        self.assertEqual(
            response, Response(ResponseStatus.OK.value, "download_started")
        )
        storage_location.refresh_from_db()
        self.assertEqual(storage_location.status, StorageLocation.STATUS_UPLOADING)

    def test_handle_get_files_to_download_missing_storage_location(self):
        obj = Message.command(ExecutorProtocol.GET_FILES_TO_DOWNLOAD, -2)
        response = self.processor.handle_get_files_to_download(obj, self.manager)
        self.assertEqual(response, Response(ResponseStatus.OK.value, []))

    def test_handle_get_files_to_download(self):
        obj = Message.command(
            ExecutorProtocol.GET_FILES_TO_DOWNLOAD, self.storage_location.id
        )
        response = self.processor.handle_get_files_to_download(obj, self.manager)
        expected = Response(
            ResponseStatus.OK.value,
            [
                {
                    "id": self.path.id,
                    "path": "test.me",
                    "size": -1,
                    "md5": "md5",
                    "crc32c": "crc",
                    "awss3etag": "aws",
                    "chunk_size": BaseStorageConnector.CHUNK_SIZE,
                }
            ],
        )
        self.assertEqual(response, expected)

    def test_handle_get_referenced_files(self):
        obj = Message.command(ExecutorProtocol.GET_REFERENCED_FILES, "")
        storage_location = StorageLocation.objects.create(
            file_storage=self.file_storage,
            connector_name="local",
            status=StorageLocation.STATUS_DONE,
            url=str(self.file_storage.id),
        )
        path = Path(storage_location.get_path(filename="output.txt"))
        path.parent.mkdir(exist_ok=True, parents=True)
        path.touch()
        data = Data.objects.get(id=1)
        data.process.output_schema = [{"name": "output_file", "type": "basic:file:"}]
        data.process.save()
        data.output = {"output_file": {"file": "output.txt"}}
        data.save()

        response = self.processor.handle_get_referenced_files(obj, self.manager)
        expected = Response(
            ResponseStatus.OK.value,
            [
                "jsonout.txt",
                "stdout.txt",
                "output.txt",
            ],
        )
        self.assertEqual(response, expected)

    def test_handle_missing_data_locations_missing_data(self):
        obj = Message.command(ExecutorProtocol.MISSING_DATA_LOCATIONS, "")
        response = self.processor.handle_missing_data_locations(obj, self.manager)
        self.assertEqual(response, Response(ResponseStatus.OK.value, []))

    def test_handle_missing_data_locations_missing_storage_location(self):
        obj = Message.command(ExecutorProtocol.MISSING_DATA_LOCATIONS, "")
        parent = Data.objects.get(id=2)
        child = Data.objects.get(id=1)
        DataDependency.objects.create(
            parent=parent, child=child, kind=DataDependency.KIND_IO
        )
        response = self.processor.handle_missing_data_locations(obj, self.manager)
        expected = Response(ResponseStatus.ERROR.value, "No storage location exists")
        self.assertEqual(response, expected)
        self.assertEqual(StorageLocation.all_objects.count(), 1)

    def test_handle_missing_data_locations_none(self):
        obj = Message.command(ExecutorProtocol.MISSING_DATA_LOCATIONS, "")
        parent = Data.objects.get(id=2)
        child = Data.objects.get(id=1)
        DataDependency.objects.create(
            parent=parent, child=child, kind=DataDependency.KIND_IO
        )
        StorageLocation.objects.create(
            file_storage=parent.location,
            connector_name="local",
            status=StorageLocation.STATUS_DONE,
            url="url",
        )
        response = self.processor.handle_missing_data_locations(obj, self.manager)
        expected = Response(ResponseStatus.OK.value, [])
        self.assertEqual(response, expected)
        self.assertEqual(StorageLocation.all_objects.count(), 2)

    def test_handle_missing_data_locations(self):
        obj = Message.command(ExecutorProtocol.MISSING_DATA_LOCATIONS, "")
        parent = Data.objects.get(id=2)
        child = Data.objects.get(id=1)
        DataDependency.objects.create(
            parent=parent, child=child, kind=DataDependency.KIND_IO
        )
        storage_location = StorageLocation.objects.create(
            file_storage=parent.location,
            connector_name="not_local",
            status=StorageLocation.STATUS_DONE,
            url="url",
        )
        response = self.processor.handle_missing_data_locations(obj, self.manager)
        self.assertEqual(StorageLocation.all_objects.count(), 3)
        created = StorageLocation.all_objects.last()
        expected = Response(
            ResponseStatus.OK.value,
            [
                {
                    "connector_name": "not_local",
                    "url": "url",
                    "data_id": child.id,
                    "to_storage_location_id": created.id,
                    "from_storage_location_id": storage_location.id,
                }
            ],
        )
        self.assertEqual(response, expected)
