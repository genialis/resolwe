# pylint: disable=missing-docstring
from unittest.mock import patch

from django.contrib.auth import get_user_model

from resolwe.flow.executors import constants
from resolwe.flow.executors.socket_utils import Message, Response, ResponseStatus
from resolwe.flow.managers.listener.authenticator import ZMQAuthenticator
from resolwe.flow.managers.listener.basic_commands_plugin import BasicCommands
from resolwe.flow.managers.listener.listener import Processor
from resolwe.flow.managers.protocol import ExecutorProtocol
from resolwe.flow.models import Data, DataDependency, Entity, Worker
from resolwe.flow.models.annotations import (
    AnnotationField,
    AnnotationGroup,
    AnnotationType,
    AnnotationValue,
)
from resolwe.flow.models.process import Process
from resolwe.permissions.models import Permission
from resolwe.storage.connectors.baseconnector import BaseStorageConnector
from resolwe.storage.connectors.s3connector import AwsS3Connector
from resolwe.storage.models import FileStorage, ReferencedPath, StorageLocation
from resolwe.test import TestCase


class ListenerTest(TestCase):
    fixtures = ["storage_data.yaml", "storage_processes.yaml", "storage_users.yaml"]

    @classmethod
    def setUpTestData(cls):
        super().setUpTestData()
        cls.manager = Processor(None)
        cls.processor = BasicCommands()
        cls.file_storage = FileStorage.objects.get(id=1)
        cls.storage_location = StorageLocation.objects.create(
            file_storage=cls.file_storage, connector_name="GCS", status="OK"
        )
        cls.path = ReferencedPath.objects.create(
            path="test.me", md5="md5", crc32c="crc", awss3etag="aws"
        )
        cls.storage_location.files.add(cls.path)

    def setUp(self):
        """Set the security provider before tests."""
        super().setUp()
        self.security_provider = ZMQAuthenticator.instance()

    def test_handle_download_finished_missing_storage_location(self):
        obj = Message.command(ExecutorProtocol.DOWNLOAD_FINISHED, -2)
        with self.assertRaises(StorageLocation.DoesNotExist):
            self.processor.handle_download_finished(b"1", obj, self.manager)

    def test_handle_download_finished(self):
        storage_location = StorageLocation.objects.create(
            file_storage=self.file_storage, connector_name="local"
        )
        obj = Message.command(ExecutorProtocol.DOWNLOAD_FINISHED, storage_location.id)

        with patch(
            "resolwe.storage.models.FileStorage.default_storage_location",
            self.storage_location,
        ):
            response = self.processor.handle_download_finished(b"1", obj, self.manager)

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
        response = self.processor.handle_download_aborted(b"1", obj, self.manager)
        self.assertEqual(response.response_status, ResponseStatus.OK)

    def test_handle_download_aborted(self):
        storage_location = StorageLocation.objects.create(
            file_storage=self.file_storage,
            connector_name="local",
            status=StorageLocation.STATUS_UPLOADING,
        )
        obj = Message.command(ExecutorProtocol.DOWNLOAD_ABORTED, storage_location.id)
        self.processor.handle_download_aborted(b"1", obj, self.manager)

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
            self.processor.handle_download_started(b"1", obj, self.manager)

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
        response = self.processor.handle_download_started(b"1", obj, self.manager)
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

        response = self.processor.handle_download_started(b"1", obj, self.manager)
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
        response = self.processor.handle_download_started(b"1", obj, self.manager)
        self.assertEqual(
            response, Response(ResponseStatus.OK.value, "download_finished")
        )
        storage_location.refresh_from_db()
        self.assertEqual(storage_location.status, StorageLocation.STATUS_DONE)

    def test_handle_resolve_url(self):
        """Test resolve URL."""

        def resolve(url: str) -> str:
            """Resolve the given URL."""
            request = Message.command("resolve_url", url)
            return self.processor.handle_resolve_url(
                b"1", request, self.manager
            ).message_data

        def presigned_url_mock(key: str, expiration: int):
            """Patch for presigned URL method.

            Just return the given key prefixed by "presigned_".
            """
            return f"presigned_{key}"

        # When URL is not handled by the plugin it should not change.
        url = "http://test_me"
        self.assertEqual(resolve(url), url)
        url = "unsupported urls should not change."
        self.assertEqual(resolve(url), url)

        # Test URL resolving for S3 plugin.
        # When bucket is not known, the URL should not change.
        # When bucket is known, is must be resolved to the presigned url.
        s3connector = AwsS3Connector({"bucket": "test"}, "S3")
        s3connector.presigned_url = presigned_url_mock
        with patch(
            "resolwe.flow.managers.basic_commands_plugin.connectors",
            {"S3": s3connector},
        ):
            # Unknown bucket.
            url = "s3://unknown-bucket/resolve"
            self.assertEqual(resolve(url), url)
            # Known bucket.
            url = "s3://test/key"
            self.assertEqual(resolve(url), "presigned_key")

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
        response = self.processor.handle_download_started(b"1", obj, self.manager)
        self.assertEqual(
            response, Response(ResponseStatus.OK.value, "download_started")
        )
        storage_location.refresh_from_db()
        self.assertEqual(storage_location.status, StorageLocation.STATUS_UPLOADING)

    def test_handle_get_files_to_download_missing_storage_location(self):
        obj = Message.command(ExecutorProtocol.GET_FILES_TO_DOWNLOAD, -2)
        response = self.processor.handle_get_files_to_download(b"1", obj, self.manager)
        self.assertEqual(response, Response(ResponseStatus.OK.value, []))

    def test_handle_get_files_to_download(self):
        obj = Message.command(
            ExecutorProtocol.GET_FILES_TO_DOWNLOAD, self.storage_location.id
        )
        response = self.processor.handle_get_files_to_download(b"1", obj, self.manager)
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

    def test_handle_missing_data_locations_missing_data(self):
        obj = Message.command(ExecutorProtocol.MISSING_DATA_LOCATIONS, "")
        response = self.processor.handle_missing_data_locations(b"1", obj, self.manager)
        self.assertEqual(response, Response(ResponseStatus.OK.value, {}))

    def test_handle_missing_data_locations_missing_storage_location(self):
        obj = Message.command(ExecutorProtocol.MISSING_DATA_LOCATIONS, "")
        parent = Data.objects.get(id=2)
        child = Data.objects.get(id=1)
        DataDependency.objects.create(
            parent=parent, child=child, kind=DataDependency.KIND_IO
        )
        response = self.processor.handle_missing_data_locations(b"1", obj, self.manager)
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
        response = self.processor.handle_missing_data_locations(b"1", obj, self.manager)
        expected = Response(ResponseStatus.OK.value, {})
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
        response = self.processor.handle_missing_data_locations(b"1", obj, self.manager)
        self.assertEqual(StorageLocation.all_objects.count(), 3)
        created = StorageLocation.all_objects.last()
        expected = Response(
            ResponseStatus.OK.value,
            {
                "url": {
                    "data_id": parent.id,
                    "from_connector": "not_local",
                    "from_storage_location_id": storage_location.id,
                    "to_storage_location_id": created.id,
                    "to_connector": "local",
                }
            },
        )
        self.assertEqual(response, expected)

    def test_handle_resolve_data_path(self):
        """Test data path resolving."""
        file_storage = FileStorage.objects.create()
        process = Process.objects.create(contributor=self.contributor)
        data = Data.objects.create(
            name="Test min", process=process, contributor=self.contributor
        )
        data.status = Data.STATUS_PROCESSING
        data.location_id = file_storage.id
        data.save()
        Worker.objects.get_or_create(data_id=data.pk, status=Worker.STATUS_PROCESSING)

        peer_identity = str(data.pk).encode()
        self.security_provider.authorize_client(b"0", data.pk)
        message = Message.command("resolve_data_path", data.pk, client_id=b"0")
        response = self.manager.process_command(peer_identity, message)
        self.assertEqual(response.message_data, str(constants.INPUTS_VOLUME))
        connector_name = "local"
        self.storage_location = StorageLocation.objects.create(
            file_storage=file_storage, connector_name="local", status="OK"
        )
        # Generate new command uuid or the processing will be skipped.
        message = Message.command("resolve_data_path", data.pk, client_id=b"0")
        response = self.manager.process_command(peer_identity, message)
        self.assertEqual(response.message_data, f"/data_{connector_name}")

    def test_get_entity_annotations(self):
        """Test entity annotations retrieval."""
        user_model = get_user_model()
        user_model.objects.create_user(
            username="public",
            email="public@test.com",
            password="public",
            first_name="James",
            last_name="Smith",
        )
        entity = Entity.objects.create(name="Entity", contributor=self.contributor)

        group = AnnotationGroup.objects.create(name="group", sort_order=1)
        field = AnnotationField.objects.create(
            name="field",
            type=AnnotationType.STRING.value,
            group=group,
            required=False,
            sort_order=1,
        )
        field2 = AnnotationField.objects.create(
            name="another field",
            type=AnnotationType.STRING.value,
            group=group,
            required=False,
            sort_order=2,
        )
        AnnotationValue.objects.create(
            entity=entity, field=field, value="value", contributor=self.contributor
        )
        AnnotationValue.objects.create(
            entity=entity, field=field2, value="value2", contributor=self.contributor
        )

        process = Process.objects.create(contributor=self.contributor)
        data = Data.objects.create(
            name="Test min", process=process, contributor=self.contributor
        )
        data.status = Data.STATUS_PROCESSING
        data.save()
        peer_identity = str(data.pk).encode()
        Worker.objects.update_or_create(
            data_id=data.pk,
            status=Worker.STATUS_PROCESSING,
            private_key=b"0",
            public_key=b"0",
        )
        self.security_provider.authorize_client(b"0", data.pk)

        # Request without permisions should fail.
        message = Message.command(
            "get_entity_annotations",
            [entity.pk, ["group.field", "group.another field", "nonexisting.field"]],
            client_id=b"0",
        )
        expected = Response(
            ResponseStatus.ERROR.value,
            "Error in command handler 'handle_get_entity_annotations'.",
        )
        response = self.manager.process_command(peer_identity, message)
        self.assertEqual(response, expected)

        data = Data.objects.create(
            name="Test min", process=process, contributor=self.contributor
        )

        # Request with permissions should succeed and return only existing annotations.
        data.status = Data.STATUS_PROCESSING
        data.save()
        peer_identity = str(data.pk).encode()
        Worker.objects.update_or_create(
            data_id=data.pk, status=Worker.STATUS_PROCESSING
        )
        entity.set_permission(Permission.VIEW, self.contributor)
        self.security_provider.authorize_client(b"0", data.pk)

        response = self.manager.process_command(peer_identity, message)
        expected = Response(
            ResponseStatus.OK.value,
            {"group.field": "value", "group.another field": "value2"},
        )
        self.assertEqual(response, expected)

        # Request without annotation names should return all annotations.
        message = Message.command(
            "get_entity_annotations", [entity.pk, None], client_id=b"0"
        )
        response = self.manager.process_command(peer_identity, message)
        expected = Response(
            ResponseStatus.OK.value,
            {"group.field": "value", "group.another field": "value2"},
        )
        self.assertEqual(response, expected)

    def test_set_entity_annotations(self):
        """Test entity annotations creation / update."""

        user_model = get_user_model()
        user_model.objects.create_user(
            username="public",
            email="public@test.com",
            password="public",
            first_name="James",
            last_name="Smith",
        )
        entity = Entity.objects.create(name="Entity", contributor=self.contributor)

        group = AnnotationGroup.objects.create(name="group", sort_order=1)
        AnnotationField.objects.create(
            name="field",
            type=AnnotationType.STRING.value,
            group=group,
            required=False,
            sort_order=1,
        )
        AnnotationField.objects.create(
            name="another field",
            type=AnnotationType.STRING.value,
            group=group,
            required=False,
            sort_order=2,
        )

        process = Process.objects.create(contributor=self.contributor)
        data = Data.objects.create(
            name="Test min", process=process, contributor=self.contributor
        )
        data.status = Data.STATUS_PROCESSING
        data.save()
        peer_identity = str(data.pk).encode()
        Worker.objects.update_or_create(
            data_id=data.pk, status=Worker.STATUS_PROCESSING
        )

        # Request without permisions should fail.
        message = Message.command(
            "set_entity_annotations",
            [entity.pk, {"group.field": "value"}, True],
            client_id=b"0",
        )
        expected = Response(
            ResponseStatus.ERROR.value,
            "Error in command handler 'handle_set_entity_annotations'.",
        )
        self.security_provider.authorize_client(b"0", data.pk)
        response = self.manager.process_command(peer_identity, message)
        self.assertEqual(response, expected)

        # Request with permissions should succeed.
        data = Data.objects.create(
            name="Test min", process=process, contributor=self.contributor
        )
        data.status = Data.STATUS_PROCESSING
        data.save()
        peer_identity = str(data.pk).encode()
        Worker.objects.update_or_create(
            data_id=data.pk, status=Worker.STATUS_PROCESSING
        )
        entity.set_permission(Permission.EDIT, self.contributor)
        self.assertEqual(entity.annotations.count(), 0)

        self.security_provider.authorize_client(b"0", data.pk)
        response = self.manager.process_command(peer_identity, message)
        expected = Response(ResponseStatus.OK.value, "OK")
        self.assertEqual(entity.annotations.count(), 1)
        self.assertAnnotation(entity, "group.field", "value")
        self.assertEqual(response.response_status, ResponseStatus.OK)

        # Annotations should be updated or created.
        message = Message.command(
            "set_entity_annotations",
            [
                entity.pk,
                {"group.field": "updated value", "group.another field": "value2"},
                True,
            ],
            client_id=b"0",
        )
        response = self.manager.process_command(peer_identity, message)
        expected = Response(ResponseStatus.OK.value, "OK")
        self.assertEqual(entity.annotations.count(), 2)
        self.assertAnnotation(entity, "group.field", "updated value")
        self.assertAnnotation(entity, "group.another field", "value2")
        self.assertEqual(response.response_status, ResponseStatus.OK)

        # Error should be raised if annotations are not found.
        AnnotationValue.all_objects.filter(entity=entity).delete()
        message = Message.command(
            "set_entity_annotations",
            [
                entity.pk,
                {"nonexisting.field": "updated value", "group.field": "updated value"},
                True,
            ],
            client_id=b"0",
        )
        response = self.manager.process_command(peer_identity, message)
        self.assertEqual(response.response_status, ResponseStatus.ERROR)
        self.assertEqual(response.message_data, "Validation error")
        self.assertEqual(entity.annotations.all().count(), 0)
