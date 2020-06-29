# pylint: disable=missing-docstring
from pathlib import Path
from unittest.mock import MagicMock, call, patch

from resolwe.flow.managers.listener import ExecutorListener
from resolwe.flow.managers.protocol import ExecutorProtocol
from resolwe.flow.models import Data, DataDependency
from resolwe.storage.models import (
    AccessLog,
    FileStorage,
    ReferencedPath,
    StorageLocation,
)
from resolwe.test import TestCase


class ListenerTest(TestCase):
    fixtures = ["storage_data.yaml", "storage_processes.yaml", "storage_users.yaml"]

    @classmethod
    def setUpTestData(cls):
        cls.listener = ExecutorListener()
        cls.file_storage = FileStorage.objects.get(id=1)
        cls.storage_location = StorageLocation.objects.create(
            file_storage=cls.file_storage, connector_name="GCS", status="OK"
        )
        cls.path = ReferencedPath.objects.create(
            path="test.me", md5="md5", crc32c="crc", awss3etag="aws"
        )
        cls.storage_location.files.add(cls.path)

    @patch("resolwe.flow.managers.listener.ExecutorListener._send_reply")
    @patch("resolwe.flow.managers.listener.async_to_sync")
    def test_handle_download_finished_missing_storage_location(
        self, async_to_sync_mock, send_reply_mock
    ):
        obj = {
            "command": ExecutorProtocol.DOWNLOAD_FINISHED,
            "data_id": -1,
            "storage_location_id": -2,
        }
        send_wrapper = MagicMock()
        async_to_sync_mock.return_value = send_wrapper
        with patch(
            "resolwe.storage.models.FileStorage.default_storage_location",
            self.storage_location,
        ):
            self.listener.handle_download_finished(obj)

        async_to_sync_mock.assert_called_once_with(send_reply_mock)
        send_wrapper.assert_called_once_with(
            {"command": "download_finished", "data_id": -1, "storage_location_id": -2,},
            {"result": "ER"},
        )

    @patch("resolwe.flow.managers.listener.ExecutorListener._send_reply")
    @patch("resolwe.flow.managers.listener.async_to_sync")
    def test_handle_download_finished(self, async_to_sync_mock, send_reply_mock):
        storage_location = StorageLocation.objects.create(
            file_storage=self.file_storage, connector_name="local"
        )
        obj = {
            "command": ExecutorProtocol.DOWNLOAD_FINISHED,
            "data_id": -1,
            "storage_location_id": storage_location.id,
        }
        send_wrapper = MagicMock()
        async_to_sync_mock.return_value = send_wrapper

        with patch(
            "resolwe.storage.models.FileStorage.default_storage_location",
            self.storage_location,
        ):
            self.listener.handle_download_finished(obj)

        send_wrapper.assert_called_once_with(
            {
                "command": "download_finished",
                "data_id": -1,
                "storage_location_id": storage_location.id,
            },
            {"result": "OK"},
        )
        storage_location.refresh_from_db()
        self.assertEqual(storage_location.status, StorageLocation.STATUS_DONE)
        self.assertEqual(storage_location.files.count(), 1)
        file = storage_location.files.get()
        self.assertEqual(file.path, "test.me")
        self.assertEqual(file.md5, "md5")
        self.assertEqual(file.crc32c, "crc")
        self.assertEqual(file.awss3etag, "aws")

    @patch("resolwe.flow.managers.listener.logger.error")
    def test_handle_download_aborted_missing_storage_location(self, error_logger_mock):
        obj = {
            "command": ExecutorProtocol.DOWNLOAD_ABORTED,
            "data_id": -1,
            "storage_location_id": -2,
        }

        self.listener.handle_download_aborted(obj)
        error_logger_mock.assert_called_once_with(
            "StorageLocation for data does not exist",
            extra={"storage_location_id": -2, "data_id": -1},
        )

    @patch("resolwe.flow.managers.listener.logger.error")
    def test_handle_download_aborted(self, error_logger_mock):
        storage_location = StorageLocation.objects.create(
            file_storage=self.file_storage,
            connector_name="local",
            status=StorageLocation.STATUS_UPLOADING,
        )
        obj = {
            "command": ExecutorProtocol.DOWNLOAD_ABORTED,
            "data_id": -1,
            "storage_location_id": storage_location.id,
        }
        self.listener.handle_download_aborted(obj)
        error_logger_mock.assert_not_called()
        storage_location.refresh_from_db()
        self.assertEqual(storage_location.status, StorageLocation.STATUS_PREPARING)

    @patch("resolwe.flow.managers.listener.consumer.send_event")
    @patch("resolwe.flow.managers.listener.logger.error")
    @patch("resolwe.flow.managers.listener.ExecutorListener._send_reply")
    @patch("resolwe.flow.managers.listener.async_to_sync")
    def test_handle_download_started_no_location(
        self, async_to_sync_mock, send_reply_mock, error_logger_mock, send_event_mock
    ):
        obj = {
            "command": ExecutorProtocol.DOWNLOAD_STARTED,
            "data_id": -1,
            "storage_location_id": -2,
            "download_started_lock": True,
        }
        self.listener.handle_download_started(obj)

        async_to_sync_mock.assert_has_calls(
            [
                call(send_reply_mock),
                call()(
                    {
                        "command": "download_started",
                        "data_id": -1,
                        "storage_location_id": -2,
                        "download_started_lock": True,
                    },
                    {"result": "ER"},
                ),
                call(send_event_mock),
                call()(
                    {
                        "command": "abort_data",
                        "data_id": -1,
                        "communicate_kwargs": {
                            "executor": "resolwe.flow.executors.local"
                        },
                    }
                ),
            ]
        )
        error_logger_mock.assert_called_once_with(
            "StorageLocation for downloaded data does not exist",
            extra={"storage_location_id": -2},
        )

    @patch("resolwe.flow.managers.listener.consumer.send_event")
    @patch("resolwe.flow.managers.listener.logger.error")
    @patch("resolwe.flow.managers.listener.ExecutorListener._send_reply")
    @patch("resolwe.flow.managers.listener.async_to_sync")
    def test_handle_download_started_ok_no_lock_preparing(
        self, async_to_sync_mock, send_reply_mock, error_logger_mock, send_event_mock
    ):
        storage_location = StorageLocation.objects.create(
            file_storage=self.file_storage, connector_name="local"
        )

        obj = {
            "command": ExecutorProtocol.DOWNLOAD_STARTED,
            "data_id": -1,
            "storage_location_id": storage_location.id,
            "download_started_lock": False,
        }
        self.listener.handle_download_started(obj)

        async_to_sync_mock.assert_has_calls(
            [
                call(send_reply_mock),
                call()(
                    {
                        "command": "download_started",
                        "data_id": -1,
                        "storage_location_id": storage_location.id,
                        "download_started_lock": False,
                    },
                    {"result": "OK", "download_result": "download_started"},
                ),
            ]
        )
        error_logger_mock.assert_not_called()
        storage_location.refresh_from_db()
        self.assertEqual(storage_location.status, StorageLocation.STATUS_PREPARING)

    @patch("resolwe.flow.managers.listener.consumer.send_event")
    @patch("resolwe.flow.managers.listener.logger.error")
    @patch("resolwe.flow.managers.listener.ExecutorListener._send_reply")
    @patch("resolwe.flow.managers.listener.async_to_sync")
    def test_handle_download_started_ok_no_lock_uploading(
        self, async_to_sync_mock, send_reply_mock, error_logger_mock, send_event_mock
    ):
        storage_location = StorageLocation.objects.create(
            file_storage=self.file_storage,
            connector_name="local",
            status=StorageLocation.STATUS_UPLOADING,
        )
        obj = {
            "command": ExecutorProtocol.DOWNLOAD_STARTED,
            "data_id": -1,
            "storage_location_id": storage_location.id,
            "download_started_lock": False,
        }

        self.listener.handle_download_started(obj)

        async_to_sync_mock.assert_has_calls(
            [
                call(send_reply_mock),
                call()(
                    {
                        "command": "download_started",
                        "data_id": -1,
                        "storage_location_id": storage_location.id,
                        "download_started_lock": False,
                    },
                    {"result": "OK", "download_result": "download_in_progress"},
                ),
            ]
        )
        error_logger_mock.assert_not_called()
        storage_location.refresh_from_db()
        self.assertEqual(storage_location.status, StorageLocation.STATUS_UPLOADING)

    @patch("resolwe.flow.managers.listener.consumer.send_event")
    @patch("resolwe.flow.managers.listener.logger.error")
    @patch("resolwe.flow.managers.listener.ExecutorListener._send_reply")
    @patch("resolwe.flow.managers.listener.async_to_sync")
    def test_handle_download_started_ok_no_lock_done(
        self, async_to_sync_mock, send_reply_mock, error_logger_mock, send_event_mock
    ):
        storage_location = StorageLocation.objects.create(
            file_storage=self.file_storage,
            connector_name="local",
            status=StorageLocation.STATUS_DONE,
        )
        obj = {
            "command": ExecutorProtocol.DOWNLOAD_STARTED,
            "data_id": -1,
            "storage_location_id": storage_location.id,
            "download_started_lock": False,
        }
        self.listener.handle_download_started(obj)
        async_to_sync_mock.assert_has_calls(
            [
                call(send_reply_mock),
                call()(
                    {
                        "command": "download_started",
                        "data_id": -1,
                        "storage_location_id": storage_location.id,
                        "download_started_lock": False,
                    },
                    {"result": "OK", "download_result": "download_finished"},
                ),
            ]
        )
        error_logger_mock.assert_not_called()
        storage_location.refresh_from_db()
        self.assertEqual(storage_location.status, StorageLocation.STATUS_DONE)

    @patch("resolwe.flow.managers.listener.consumer.send_event")
    @patch("resolwe.flow.managers.listener.logger.error")
    @patch("resolwe.flow.managers.listener.ExecutorListener._send_reply")
    @patch("resolwe.flow.managers.listener.async_to_sync")
    def test_handle_download_started_ok_lock(
        self, async_to_sync_mock, send_reply_mock, error_logger_mock, send_event_mock
    ):
        storage_location = StorageLocation.objects.create(
            file_storage=self.file_storage, connector_name="local"
        )

        obj = {
            "command": ExecutorProtocol.DOWNLOAD_STARTED,
            "data_id": -1,
            "storage_location_id": storage_location.id,
            "download_started_lock": True,
        }
        self.listener.handle_download_started(obj)

        async_to_sync_mock.assert_has_calls(
            [
                call(send_reply_mock),
                call()(
                    {
                        "command": "download_started",
                        "data_id": -1,
                        "storage_location_id": storage_location.id,
                        "download_started_lock": True,
                    },
                    {"result": "OK", "download_result": "download_started"},
                ),
            ]
        )
        error_logger_mock.assert_not_called()
        storage_location.refresh_from_db()
        self.assertEqual(storage_location.status, StorageLocation.STATUS_UPLOADING)

    @patch("resolwe.flow.managers.listener.consumer.send_event")
    @patch("resolwe.flow.managers.listener.logger.error")
    @patch("resolwe.flow.managers.listener.ExecutorListener._send_reply")
    @patch("resolwe.flow.managers.listener.async_to_sync")
    def test_handle_get_files_to_download_missing_storage_location(
        self, async_to_sync_mock, send_reply_mock, error_logger_mock, send_event_mock
    ):
        obj = {
            "command": ExecutorProtocol.GET_FILES_TO_DOWNLOAD,
            "data_id": -1,
            "storage_location_id": -2,
        }
        self.listener.handle_get_files_to_download(obj)
        async_to_sync_mock.assert_has_calls(
            [
                call(send_reply_mock),
                call()(
                    {
                        "command": "get_files_to_download",
                        "data_id": -1,
                        "storage_location_id": -2,
                    },
                    {"result": "ER"},
                ),
                call(send_event_mock),
                call()(
                    {
                        "command": "abort_data",
                        "data_id": -1,
                        "communicate_kwargs": {
                            "executor": "resolwe.flow.executors.local"
                        },
                    }
                ),
            ]
        )
        error_logger_mock.assert_called_once_with(
            "StorageLocation object does not exist (handle_get_files_to_download).",
            extra={"storage_location_id": -2},
        )

    @patch("resolwe.flow.managers.listener.consumer.send_event")
    @patch("resolwe.flow.managers.listener.logger.error")
    @patch("resolwe.flow.managers.listener.ExecutorListener._send_reply")
    @patch("resolwe.flow.managers.listener.async_to_sync")
    def test_handle_get_files_to_download(
        self, async_to_sync_mock, send_reply_mock, error_logger_mock, send_event_mock
    ):
        obj = {
            "command": ExecutorProtocol.GET_FILES_TO_DOWNLOAD,
            "data_id": -1,
            "storage_location_id": self.storage_location.id,
        }
        self.listener.handle_get_files_to_download(obj)
        async_to_sync_mock.assert_has_calls(
            [
                call(send_reply_mock),
                call()(
                    {
                        "command": "get_files_to_download",
                        "data_id": -1,
                        "storage_location_id": self.storage_location.id,
                    },
                    {
                        "result": "OK",
                        "referenced_files": [
                            {
                                "id": self.path.id,
                                "path": "test.me",
                                "size": -1,
                                "md5": "md5",
                                "crc32c": "crc",
                                "awss3etag": "aws",
                            }
                        ],
                    },
                ),
            ]
        )

    @patch("resolwe.flow.managers.listener.consumer.send_event")
    @patch("resolwe.flow.managers.listener.logger.error")
    @patch("resolwe.flow.managers.listener.ExecutorListener._send_reply")
    @patch("resolwe.flow.managers.listener.async_to_sync")
    def test_handle_get_referenced_files_missing_data(
        self, async_to_sync_mock, send_reply_mock, error_logger_mock, send_event_mock
    ):
        obj = {
            "command": ExecutorProtocol.GET_REFERENCED_FILES,
            "data_id": -1,
        }
        self.listener.handle_get_referenced_files(obj)
        async_to_sync_mock.assert_has_calls(
            [
                call(send_reply_mock),
                call()(
                    {"command": "get_referenced_files", "data_id": -1}, {"result": "ER"}
                ),
                call(send_event_mock),
                call()(
                    {
                        "command": "abort_data",
                        "data_id": -1,
                        "communicate_kwargs": {
                            "executor": "resolwe.flow.executors.local"
                        },
                    }
                ),
            ]
        )
        error_logger_mock.assert_called_once_with(
            "Data object does not exist (handle_get_referenced_files).",
            extra={"data_id": -1},
        )

    @patch("resolwe.flow.managers.listener.consumer.send_event")
    @patch("resolwe.flow.managers.listener.logger.error")
    @patch("resolwe.flow.managers.listener.ExecutorListener._send_reply")
    @patch("resolwe.flow.managers.listener.async_to_sync")
    def test_handle_get_referenced_files(
        self, async_to_sync_mock, send_reply_mock, error_logger_mock, send_event_mock
    ):
        obj = {
            "command": ExecutorProtocol.GET_REFERENCED_FILES,
            "data_id": 1,
        }
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

        self.listener.handle_get_referenced_files(obj)
        async_to_sync_mock.assert_has_calls(
            [
                call(send_reply_mock),
                call()(
                    {"command": "get_referenced_files", "data_id": 1},
                    {
                        "result": "OK",
                        "referenced_files": [
                            "jsonout.txt",
                            "stderr.txt",
                            "stdout.txt",
                            "output.txt",
                        ],
                    },
                ),
            ]
        )
        error_logger_mock.assert_not_called()

    @patch("resolwe.flow.managers.listener.consumer.send_event")
    @patch("resolwe.flow.managers.listener.logger.error")
    @patch("resolwe.flow.managers.listener.ExecutorListener._send_reply")
    @patch("resolwe.flow.managers.listener.async_to_sync")
    def test_handle_missing_data_locations_missing_data(
        self, async_to_sync_mock, send_reply_mock, error_logger_mock, send_event_mock
    ):
        obj = {
            "command": ExecutorProtocol.MISSING_DATA_LOCATIONS,
            "data_id": -1,
        }
        self.listener.handle_missing_data_locations(obj)
        async_to_sync_mock.assert_has_calls(
            [
                call(send_reply_mock),
                call()(
                    {"command": "missing_data_locations", "data_id": -1},
                    {"result": "ER"},
                ),
                call(send_event_mock),
                call()(
                    {
                        "command": "abort_data",
                        "data_id": -1,
                        "communicate_kwargs": {
                            "executor": "resolwe.flow.executors.local"
                        },
                    }
                ),
            ]
        )
        error_logger_mock.assert_called_once_with(
            "Data object does not exist (handle_missing_data_locations).",
            extra={"data_id": -1},
        )

    @patch("resolwe.flow.managers.listener.consumer.send_event")
    @patch("resolwe.flow.managers.listener.logger.error")
    @patch("resolwe.flow.managers.listener.ExecutorListener._send_reply")
    @patch("resolwe.flow.managers.listener.async_to_sync")
    def test_handle_missing_data_locations_missing_storage_location(
        self, async_to_sync_mock, send_reply_mock, error_logger_mock, send_event_mock
    ):
        obj = {
            "command": ExecutorProtocol.MISSING_DATA_LOCATIONS,
            "data_id": 1,
        }
        parent = Data.objects.get(id=2)
        child = Data.objects.get(id=1)
        DataDependency.objects.create(
            parent=parent, child=child, kind=DataDependency.KIND_IO
        )
        self.listener.handle_missing_data_locations(obj)
        async_to_sync_mock.assert_has_calls(
            [
                call(send_reply_mock),
                call()(
                    {"command": "missing_data_locations", "data_id": 1},
                    {"result": "ER"},
                ),
                call(send_event_mock),
                call()(
                    {
                        "command": "abort_data",
                        "data_id": 1,
                        "communicate_kwargs": {
                            "executor": "resolwe.flow.executors.local"
                        },
                    }
                ),
            ]
        )
        error_logger_mock.assert_called_once_with(
            "No storage location exists (handle_get_missing_data_locations).",
            extra={"data_id": 1, "file_storage_id": 2},
        )
        self.assertEqual(StorageLocation.all_objects.count(), 1)

    @patch("resolwe.flow.managers.listener.consumer.send_event")
    @patch("resolwe.flow.managers.listener.logger.error")
    @patch("resolwe.flow.managers.listener.ExecutorListener._send_reply")
    @patch("resolwe.flow.managers.listener.async_to_sync")
    def test_handle_missing_data_locations_none(
        self, async_to_sync_mock, send_reply_mock, error_logger_mock, send_event_mock
    ):
        obj = {
            "command": ExecutorProtocol.MISSING_DATA_LOCATIONS,
            "data_id": 1,
        }
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
        self.listener.handle_missing_data_locations(obj)
        async_to_sync_mock.assert_has_calls(
            [
                call(send_reply_mock),
                call()(
                    {"command": "missing_data_locations", "data_id": 1},
                    {"result": "OK", "storage_data_locations": []},
                ),
            ]
        )
        error_logger_mock.assert_not_called()
        self.assertEqual(StorageLocation.all_objects.count(), 2)

    @patch("resolwe.flow.managers.listener.consumer.send_event")
    @patch("resolwe.flow.managers.listener.logger.error")
    @patch("resolwe.flow.managers.listener.ExecutorListener._send_reply")
    @patch("resolwe.flow.managers.listener.async_to_sync")
    def test_handle_missing_data_locations(
        self, async_to_sync_mock, send_reply_mock, error_logger_mock, send_event_mock
    ):
        obj = {
            "command": ExecutorProtocol.MISSING_DATA_LOCATIONS,
            "data_id": 1,
        }
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
        self.listener.handle_missing_data_locations(obj)
        self.assertEqual(StorageLocation.all_objects.count(), 3)
        created = StorageLocation.all_objects.last()
        async_to_sync_mock.assert_has_calls(
            [
                call(send_reply_mock),
                call()(
                    {"command": "missing_data_locations", "data_id": child.id},
                    {
                        "result": "OK",
                        "storage_data_locations": [
                            {
                                "connector_name": "not_local",
                                "url": "url",
                                "data_id": child.id,
                                "to_storage_location_id": created.id,
                                "from_storage_location_id": storage_location.id,
                            }
                        ],
                    },
                ),
            ]
        )
        error_logger_mock.assert_not_called()

    @patch("resolwe.flow.managers.listener.logger.error")
    @patch("resolwe.flow.managers.listener.ExecutorListener._send_reply")
    @patch("resolwe.flow.managers.listener.async_to_sync")
    def test_handle_storage_location_lock_missing_storage_location(
        self, async_to_sync_mock, send_reply_mock, error_logger_mock
    ):
        obj = {
            "command": ExecutorProtocol.STORAGE_LOCATION_LOCK,
            ExecutorProtocol.STORAGE_LOCATION_LOCK_REASON: "test_reason",
            "data_id": -1,
            "storage_location_id": -2,
        }
        self.listener.handle_storage_location_lock(obj)
        async_to_sync_mock.assert_has_calls(
            [
                call(send_reply_mock),
                call()(
                    {
                        "command": "storage_location_lock",
                        "storage_location_lock_reason": "test_reason",
                        "data_id": -1,
                        "storage_location_id": -2,
                    },
                    {"result": "ER"},
                ),
            ]
        )
        error_logger_mock.assert_called_once_with(
            "StorageLocation does not exist", extra={"storage_location_id": -2},
        )

    @patch("resolwe.flow.managers.listener.logger.error")
    @patch("resolwe.flow.managers.listener.ExecutorListener._send_reply")
    @patch("resolwe.flow.managers.listener.async_to_sync")
    def test_handle_storage_location_lock(
        self, async_to_sync_mock, send_reply_mock, error_logger_mock
    ):
        obj = {
            "command": ExecutorProtocol.STORAGE_LOCATION_LOCK,
            ExecutorProtocol.STORAGE_LOCATION_LOCK_REASON: "test_reason",
            "data_id": -1,
            "storage_location_id": self.storage_location.id,
        }
        self.listener.handle_storage_location_lock(obj)
        async_to_sync_mock.assert_has_calls(
            [
                call(send_reply_mock),
                call()(
                    {
                        "command": "storage_location_lock",
                        "storage_location_lock_reason": "test_reason",
                        "data_id": -1,
                        "storage_location_id": self.storage_location.id,
                    },
                    {"result": "OK", "storage_access_log_id": 1},
                ),
            ]
        )
        self.assertTrue(AccessLog.objects.filter(id=1).exists())
        access_log = AccessLog.objects.get(id=1)
        self.assertEqual(access_log.reason, "test_reason")
        self.assertEqual(access_log.storage_location_id, self.storage_location.id)
        self.assertIsNone(access_log.finished)
        self.assertTrue(self.storage_location.locked)

    @patch("resolwe.flow.managers.listener.logger.error")
    @patch("resolwe.flow.managers.listener.ExecutorListener._send_reply")
    @patch("resolwe.flow.managers.listener.async_to_sync")
    def test_handle_storage_location_unlock(
        self, async_to_sync_mock, send_reply_mock, error_logger_mock
    ):
        obj = {
            "command": ExecutorProtocol.STORAGE_LOCATION_UNLOCK,
            "data_id": -1,
            "storage_access_log_id": -2,
        }
        self.listener.handle_storage_location_unlock(obj)
        async_to_sync_mock.assert_not_called()
        error_logger_mock.assert_called_once_with(
            "AccessLog does not exist", extra={"access_log_id": -2},
        )

        access_log = AccessLog.objects.create(
            storage_location=self.storage_location, reason="test"
        )

        async_to_sync_mock.reset_mock()
        send_reply_mock.reset_mock()
        error_logger_mock.reset_mock()
        obj["storage_access_log_id"] = access_log.id
        self.listener.handle_storage_location_unlock(obj)
        async_to_sync_mock.assert_not_called()
        error_logger_mock.assert_not_called()
        access_log.refresh_from_db()
        self.assertIsNotNone(access_log.finished)
        self.assertFalse(self.storage_location.locked)
