# pylint: disable=missing-docstring
import asyncio
import logging
import os
import subprocess
import sys
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from time import sleep, time
from unittest import mock
from unittest.mock import patch

import zmq
import zmq.asyncio
from django.conf import settings
from django.db import IntegrityError, connection
from django.test import override_settings

from resolwe.flow.executors.prepare import BaseFlowExecutorPreparer
from resolwe.flow.executors.socket_utils import Message, Response, ResponseStatus
from resolwe.flow.executors.startup_processing_container import (
    connect_to_communication_container,
    initialize_connections,
)
from resolwe.flow.executors.zeromq_utils import ZMQCommunicator
from resolwe.flow.managers.dispatcher import Manager
from resolwe.flow.managers.listener.listener import LISTENER_PUBLIC_KEY
from resolwe.flow.managers.listener.redis_cache import redis_cache
from resolwe.flow.models import Data, DataDependency, Process, Worker
from resolwe.flow.models.annotations import (
    AnnotationField,
    AnnotationGroup,
    AnnotationType,
)
from resolwe.flow.models.fields import ResolweSlugField
from resolwe.storage import settings as storage_settings
from resolwe.test import (
    ProcessTestCase,
    TestCase,
    tag_process,
    with_docker_executor,
    with_null_executor,
)

PROCESSES_DIR = os.path.join(os.path.dirname(__file__), "processes")
DESCRIPTORS_DIR = os.path.join(os.path.dirname(__file__), "descriptors")

logger = logging.getLogger(__name__)


class GetToolsTestCase(TestCase):
    @mock.patch("resolwe.flow.utils.apps")
    @mock.patch("resolwe.flow.utils.Path")
    @mock.patch("resolwe.flow.utils.settings")
    def test_get_tools_paths(self, settings_mock, path_mock, apps_mock):
        def path_mock_side_effect(*args):
            if args[0] == "/custom_tools":
                return mock.MagicMock(path=args[0])
            else:
                return app_config_path_mock

        path_mock.side_effect = path_mock_side_effect

        app_config1_mock = mock.MagicMock()
        app_config1_mock.name = "/resolwe/test_app1"
        app_config2_mock = mock.MagicMock()
        app_config2_mock.name = "/resolwe/test_app1"

        apps_mock.get_app_configs.return_value = [app_config1_mock, app_config2_mock]
        first_mock = mock.MagicMock(is_dir=lambda: False, path="/resolwe/test_app1")
        second_mock = mock.MagicMock(is_dir=lambda: True, path="/resolwe/test_app2")

        app_config_path_mock = mock.MagicMock()
        app_config_path_mock.__truediv__.side_effect = [first_mock, second_mock]

        settings_mock.RESOLWE_CUSTOM_TOOLS_PATHS = ["/custom_tools"]
        base_executor = BaseFlowExecutorPreparer()
        tools_list = [mock.path for mock in base_executor.get_tools_paths()]

        self.assertEqual(len(tools_list), 2)
        self.assertIn("/resolwe/test_app2", tools_list)
        self.assertIn("/custom_tools", tools_list)

    @mock.patch("resolwe.flow.utils.apps")
    @mock.patch("resolwe.flow.utils.settings")
    def test_not_list(self, settings_mock, apps_mock):
        apps_mock.get_app_configs.return_value = []
        settings_mock.RESOLWE_CUSTOM_TOOLS_PATHS = "/custom_tools"

        base_executor = BaseFlowExecutorPreparer()
        with self.assertRaisesRegex(KeyError, "setting must be a list"):
            base_executor.get_tools_paths()


class ManagerRunProcessTest(ProcessTestCase):
    def setUp(self):
        super().setUp()
        self._register_schemas(
            processes_paths=[PROCESSES_DIR], descriptors_paths=[DESCRIPTORS_DIR]
        )
        group = AnnotationGroup.objects.create(name="general", sort_order=1)
        self.species_field = AnnotationField.objects.create(
            name="species",
            group=group,
            sort_order=1,
            type=AnnotationType.STRING.value,
            vocabulary={"Valid": "Valid"},
        )
        AnnotationField.objects.create(
            name="description",
            group=group,
            sort_order=2,
            type=AnnotationType.STRING.value,
        )

    @tag_process("test-min")
    def test_minimal_process(self):
        self.run_process("test-min")

    @tag_process("test-min")
    @tag_process("test-workflow-simple")
    def test_restart(self):
        def restarted_object_sanity_check(
            original_values, restarted_values, additional_skip_keys=[]
        ):
            self.assertEqual(restarted_values["status"], Data.STATUS_DONE)
            self.assertEqual(original_values.keys(), restarted_values.keys())

            # These values should be both None or modified greater than original.
            for key in {"started", "finished", "modified", "scheduled"}:
                if restarted_values[key] is not None:
                    compare_value = original_values["finished"] or original_values[key]
                    self.assertGreater(restarted_values[key], compare_value)
                else:
                    self.assertEqual(restarted_values[key], original_values[key])

            skip_keys = {"started", "finished", "modified", "scheduled", "location_id"}
            skip_keys.update(additional_skip_keys)
            for key in original_values.keys():
                if key not in skip_keys:
                    self.assertEqual(
                        original_values[key],
                        restarted_values[key],
                        f"Key '{key}' does not match.",
                    )

        original = self.run_process("test-min", contributor=self.contributor)
        with self.assertRaises(
            RuntimeError, msg="Only data in status ERROR can be restarted, not OK."
        ):
            original.restart()

        original_values = Data.objects.filter(pk=original.pk).values()[0]

        # No need to actually save the status, it is checked on the instance.
        original.status = Data.STATUS_ERROR
        original.restart()

        restarted_values = Data.objects.filter(pk=original.pk).values()[0]
        restarted_object_sanity_check(original_values, restarted_values)

        # Now test the workflow duplication. The workflow creates two data objects
        # and the second one depends on the first one.
        workflow_data = self.run_process(
            "test-workflow-simple",
            {"param1": "world"},
            tags=["test-tag"],
            contributor=self.contributor,
        )
        workflow_data.refresh_from_db()
        step1_data = Data.objects.get(process__slug="test-example-1-simple")
        step2_data = Data.objects.get(process__slug="test-example-2-simple")

        workflow_dict = Data.objects.filter(pk=workflow_data.pk).values()[0]
        step1_dict = Data.objects.filter(pk=step1_data.pk).values()[0]
        step2_dict = Data.objects.filter(pk=step2_data.pk).values()[0]

        # First try to duplicate the entire workflow.
        workflow_data.status = Data.STATUS_ERROR
        workflow_data.restart()

        # Subprocess dependencies must be deleted.
        with self.assertRaises(Data.DoesNotExist):
            step1_data.refresh_from_db()
        with self.assertRaises(Data.DoesNotExist):
            step2_data.refresh_from_db()

        restarted_workflow_dict = Data.objects.filter(pk=workflow_data.pk).values()[0]
        restarted_step1_dict = Data.objects.filter(
            process__slug="test-example-1-simple"
        ).values()[0]
        restarted_step2_dict = Data.objects.filter(
            process__slug="test-example-2-simple"
        ).values()[0]
        # Output also differs (different step ids).
        restarted_object_sanity_check(
            workflow_dict, restarted_workflow_dict, ["output"]
        )
        # Ids will not match, since object is restarted.
        self.assertNotEqual(restarted_step1_dict["id"], step1_dict["id"])
        self.assertNotEqual(restarted_step2_dict["id"], step2_dict["id"])
        self.assertGreater(restarted_step1_dict["created"], step1_dict["created"])
        self.assertGreater(restarted_step2_dict["created"], step2_dict["created"])
        restarted_object_sanity_check(
            step1_dict, restarted_step1_dict, ["id", "permission_group_id", "created"]
        )
        restarted_object_sanity_check(
            step2_dict, restarted_step2_dict, ["id", "permission_group_id", "created"]
        )

        # Override resources on step1 and step2. Be careful: when restarting the entire
        # workflow the workflow will spawn new objects and its children will be restarted.
        Data.objects.all().delete()
        workflow_data = self.run_process(
            "test-workflow-simple",
            {"param1": "world"},
            tags=["test-tag"],
            contributor=self.contributor,
        )
        workflow_data.refresh_from_db()

        step1_data = Data.objects.get(process__slug="test-example-1-simple")
        step2_data = Data.objects.get(process__slug="test-example-2-simple")

        workflow_dict = Data.objects.filter(pk=workflow_data.pk).values()[0]
        step1_dict = Data.objects.filter(pk=step1_data.pk).values()[0]
        step2_dict = Data.objects.filter(pk=step2_data.pk).values()[0]

        # Duplicate the entire workflow, override resources.
        workflow_data.status = Data.STATUS_ERROR

        resource_overrides = {}
        resource_overrides[workflow_data.pk] = {"cores": 1}
        workflow_data.restart(resource_overrides=resource_overrides)

        # Subprocess dependencies must be deleted.
        with self.assertRaises(Data.DoesNotExist):
            step1_data.refresh_from_db()
        with self.assertRaises(Data.DoesNotExist):
            step2_data.refresh_from_db()

        restarted_workflow_dict = Data.objects.filter(pk=workflow_data.pk).values()[0]
        restarted_step1_dict = Data.objects.filter(
            process__slug="test-example-1-simple"
        ).values()[0]
        restarted_step2_dict = Data.objects.filter(
            process__slug="test-example-2-simple"
        ).values()[0]
        # Output also differs (different step ids).
        restarted_object_sanity_check(
            workflow_dict, restarted_workflow_dict, ["output", "process_resources"]
        )
        # Ids will not match, since object is restarted.
        self.assertNotEqual(restarted_step1_dict["id"], step1_dict["id"])
        self.assertNotEqual(restarted_step2_dict["id"], step2_dict["id"])
        self.assertGreater(restarted_step1_dict["created"], step1_dict["created"])
        self.assertGreater(restarted_step2_dict["created"], step2_dict["created"])
        restarted_object_sanity_check(
            step1_dict, restarted_step1_dict, ["id", "permission_group_id", "created"]
        )
        restarted_object_sanity_check(
            step2_dict, restarted_step2_dict, ["id", "permission_group_id", "created"]
        )
        self.assertEqual(restarted_workflow_dict["process_resources"], {"cores": 1})
        self.assertEqual(workflow_dict["process_resources"], {})

    @tag_process("test-annotate")
    def test_annotate(self):
        data = self.run_process("test-annotate")
        self.assertIsNotNone(data.entity)
        self.assertEqual(data.entity.annotations.count(), 1)
        self.assertAnnotation(data.entity, "general.species", "Valid")

    def test_router_handover(self):
        """Test router socket handover.

        When multiple clients with the same identity connect to the socket, the
        last one must replace the previous one.
        """
        listener_settings = getattr(settings, "FLOW_EXECUTOR", {}).get(
            "LISTENER_CONNECTION", {}
        )
        port = listener_settings.get("port", 53893)
        host = listener_settings.get("hosts", {}).get("local", "127.0.0.1")
        protocol = settings.FLOW_EXECUTOR.get("LISTENER_CONNECTION", {}).get(
            "protocol", "tcp"
        )
        logger = logging.getLogger(__name__)
        logger.handlers = []
        process = Process.objects.create(contributor=self.contributor)
        data = Data.objects.create(process=process, contributor=self.contributor)
        public_key, private_key = zmq.curve_keypair()
        Worker.objects.create(
            data=data,
            status=Worker.STATUS_PROCESSING,
            private_key=private_key,
            public_key=public_key,
        )

        async def send_single_message():
            """Open connection to listener and send single message."""
            connection_string = f"{protocol}://{host}:{port}"
            zmq_context = zmq.asyncio.Context.instance()
            zmq_socket = zmq_context.socket(zmq.DEALER)
            zmq_socket.curve_secretkey = private_key
            zmq_socket.curve_publickey = public_key
            zmq_socket.curve_serverkey = LISTENER_PUBLIC_KEY
            zmq_socket.setsockopt(zmq.IDENTITY, b"1")
            zmq_socket.connect(connection_string)
            communicator = ZMQCommunicator(
                zmq_socket, "init_container <-> listener", logger
            )
            async with communicator:
                await asyncio.ensure_future(
                    communicator.send_command(Message.command("update_status", "PP"))
                )

        async def send_two_messages():
            """Send two messages with the same identity.

            When handover is not working the second connection will be rejected
            and no answer received, triggering timeout and raising exception.
            """
            for _ in range(2):
                await send_single_message()

        return asyncio.new_event_loop().run_until_complete(send_two_messages())

    # @mock.patch("resolwe.flow.managers.workload_connectors.local.LISTENER_PUBLIC_KEY", b"0")
    def test_encryption(self):
        """Test encryption between the listener and the process.

        Verify that:
        - invalid listener key in the process results in the failure to communicate.
        - invalid process key results in the error in the listener.
        - the process with valid key can only process its data object.
        """
        listener_settings = getattr(settings, "FLOW_EXECUTOR", {}).get(
            "LISTENER_CONNECTION", {}
        )
        port = listener_settings.get("port", 53893)
        host = listener_settings.get("hosts", {}).get("local", "127.0.0.1")
        protocol = settings.FLOW_EXECUTOR.get("LISTENER_CONNECTION", {}).get(
            "protocol", "tcp"
        )
        process = Process.objects.create(contributor=self.contributor)
        data = Data.objects.create(process=process, contributor=self.contributor)
        another_data = Data.objects.create(
            process=process, contributor=self.contributor
        )
        public_key, private_key = zmq.curve_keypair()
        another_public_key, another_private_key = zmq.curve_keypair()
        fake_public_key, fake_private_key = zmq.curve_keypair()
        Worker.objects.create(
            data=data,
            status=Worker.STATUS_PROCESSING,
            private_key=private_key,
            public_key=public_key,
        )
        Worker.objects.create(
            data=another_data,
            status=Worker.STATUS_PROCESSING,
            private_key=another_private_key,
            public_key=another_public_key,
        )

        async def send_single_message(
            identity,
            public_key,
            private_key,
            listener_public_key,
            command=("update_status", "PP"),
        ) -> Response:
            """Open connection to listener and send single message."""
            connection_string = f"{protocol}://{host}:{port}"
            zmq_context = zmq.asyncio.Context.instance()
            zmq_socket = zmq_context.socket(zmq.DEALER)
            zmq_socket.curve_secretkey = private_key
            zmq_socket.curve_publickey = public_key
            zmq_socket.curve_serverkey = listener_public_key
            zmq_socket.setsockopt(zmq.IDENTITY, identity)
            zmq_socket.connect(connection_string)
            communicator = ZMQCommunicator(
                zmq_socket, "init_container <-> listener", logger
            )
            async with communicator:
                future = asyncio.ensure_future(
                    communicator.send_command(Message.command(*command))
                )
                return await asyncio.wait_for(future, 1)

        # Correct identity and keys.
        response = asyncio.new_event_loop().run_until_complete(
            send_single_message(
                f"{data.pk}".encode(), public_key, private_key, LISTENER_PUBLIC_KEY
            )
        )
        self.assertEqual(response.status, ResponseStatus.OK)

        # Fake listener identity.
        with self.assertRaises(asyncio.exceptions.TimeoutError):
            asyncio.new_event_loop().run_until_complete(
                send_single_message(
                    f"{data.pk}".encode(), public_key, private_key, fake_public_key
                )
            )

        # Non-matching key pair should not be able to establish a connection.
        with self.assertRaises(asyncio.exceptions.TimeoutError):
            asyncio.new_event_loop().run_until_complete(
                send_single_message(
                    f"{data.pk}".encode(),
                    fake_public_key,
                    private_key,
                    LISTENER_PUBLIC_KEY,
                )
            )
        with self.assertRaises(asyncio.exceptions.TimeoutError):
            asyncio.new_event_loop().run_until_complete(
                send_single_message(
                    f"{data.pk}".encode(),
                    public_key,
                    fake_private_key,
                    LISTENER_PUBLIC_KEY,
                )
            )

        # Fake client keys should be rejected.
        response = asyncio.new_event_loop().run_until_complete(
            send_single_message(
                f"{data.pk}".encode(),
                fake_public_key,
                fake_private_key,
                LISTENER_PUBLIC_KEY,
            )
        )
        not_authorized_error = (
            f"Client with key {fake_public_key!r} is not allowed to process the data "
            f"object with id {data.pk}."
        )
        self.assertEqual(response.status, ResponseStatus.ERROR)
        self.assertEqual(response.message_data, not_authorized_error)

        # The command 'liveness_probe' must be able to get a response even with a set
        # of keys with access to no data object.
        response = asyncio.new_event_loop().run_until_complete(
            send_single_message(
                b"liveness_probe",
                fake_public_key,
                fake_private_key,
                LISTENER_PUBLIC_KEY,
                command=("liveness_probe", ""),
            )
        )
        self.assertEqual(response.status, ResponseStatus.OK)

        # Non-matching keys and data id message must be rejected.
        response = asyncio.new_event_loop().run_until_complete(
            send_single_message(
                f"{another_data.pk}".encode(),
                public_key,
                private_key,
                LISTENER_PUBLIC_KEY,
            )
        )
        not_authorized_error = (
            f"Client with key {public_key!r} is not allowed to process the data "
            f"object with id {another_data.pk}."
        )
        self.assertEqual(response.status, ResponseStatus.ERROR)
        self.assertEqual(response.message_data, not_authorized_error)

        response = asyncio.new_event_loop().run_until_complete(
            send_single_message(
                f"{data.pk}".encode(),
                another_public_key,
                another_private_key,
                LISTENER_PUBLIC_KEY,
            )
        )
        not_authorized_error = (
            f"Client with key {another_public_key!r} is not allowed to process the "
            f"data object with id {data.pk}."
        )
        self.assertEqual(response.status, ResponseStatus.ERROR)
        self.assertEqual(response.message_data, not_authorized_error)

        # Correct identity and keys.
        response = asyncio.new_event_loop().run_until_complete(
            send_single_message(
                f"{data.pk}".encode(), public_key, private_key, LISTENER_PUBLIC_KEY
            )
        )
        self.assertEqual(response.status, ResponseStatus.OK)

        response = asyncio.new_event_loop().run_until_complete(
            send_single_message(
                f"{another_data.pk}".encode(),
                another_public_key,
                another_private_key,
                LISTENER_PUBLIC_KEY,
            )
        )
        self.assertEqual(response.status, ResponseStatus.OK)

    @tag_process("test-annotate-wrong-option")
    def test_annotate_wrong_option(self):
        data = self.run_process(
            "test-annotate-wrong-option", assert_status=Data.STATUS_ERROR
        )
        self.assertEqual(len(data.process_error), 1)
        self.assertIn(
            "The value 'Invalid' is not valid for the field general.species.",
            data.process_error[0],
        )

    @tag_process("test-annotate-wrong-type")
    def test_annotate_wrong_type(self):
        data = self.run_process(
            "test-annotate-wrong-type", assert_status=Data.STATUS_ERROR
        )
        self.assertEqual(len(data.process_error), 1)
        self.assertIn(
            "The value '15' is not of the expected type 'str'.", data.process_error[0]
        )

    @tag_process("test-annotate-missing-field")
    def test_annotate_missing_field(self):
        data = self.run_process(
            "test-annotate-missing-field", assert_status=Data.STATUS_ERROR
        )
        self.assertEqual(len(data.process_error), 1)
        self.assertIn("Field 'general.invalid' does not exist", data.process_error[0])

    @tag_process("test-annotate-no-entity")
    def test_annotate_no_entity(self):
        data = self.run_process(
            "test-annotate-no-entity", assert_status=Data.STATUS_ERROR
        )
        self.assertEqual(len(data.process_error), 1)
        self.assertIn("No entity to annotate", data.process_error[0])

    @tag_process("test-missing-file")
    def test_missing_file(self):
        self.run_process("test-missing-file", assert_status=Data.STATUS_ERROR)

        data = Data.objects.last()
        self.assertEqual(data.status, Data.STATUS_ERROR)

        self.assertEqual(len(data.process_error), 1)
        self.assertIn(
            "Output 'output' set to a missing file: 'i-dont-exist.zip'.",
            data.process_error[0],
        )

    @tag_process("test-spawn-new")
    def test_spawn(self):
        self.run_process("test-spawn-new", tags=["test-tag"])

        data = Data.objects.last()
        file_path = data.location.get_path(filename="foo.bar")
        self.assertEqual(data.output["saved_file"]["file"], "foo.bar")
        self.assertTrue(os.path.isfile(file_path))
        with open(file_path, "rt") as stream:
            self.assertEqual(stream.read(), "foo.bar\n")
        self.assertEqual(data.tags, ["test-tag"])
        parent_data = Data.objects.first()
        self.assertEqual(data.parents.count(), 1)
        self.assertEqual(data.parents.first(), parent_data)

        # Check correct dependency type is created.
        self.assertEqual(
            {d.kind for d in data.parents_dependency.all()},
            {DataDependency.KIND_SUBPROCESS},
        )

    @tag_process("test-spawn-missing-file")
    def test_spawn_missing_export(self):
        data = self.run_process(
            "test-spawn-missing-file", assert_status=Data.STATUS_ERROR
        )
        self.assertIn(
            "Error while preparing spawned Data objects", data.process_error[0]
        )

    @tag_process(
        "test-broken",
        "test-broken-invalid-execution-engine",
        "test-broken-invalid-expression-engine",
        "test-broken-data-name",
    )
    def test_broken(self):
        Process.objects.create(
            slug="test-broken-invalid-execution-engine",
            name="Test Process",
            contributor=self.contributor,
            type="data:test",
            version=1,
            run={
                "language": "invalid",
            },
        )

        self.run_process("test-broken", assert_status=Data.STATUS_ERROR)
        self.run_process(
            "test-broken-invalid-expression-engine", assert_status=Data.STATUS_ERROR
        )
        self.run_process(
            "test-broken-invalid-execution-engine", assert_status=Data.STATUS_ERROR
        )

        # If evaluation of data_name template fails, the process should not abort as the
        # template may be evaluatable later when the process completes.
        self.run_process("test-broken-data-name")

    @tag_process("test-broken-invalide-storage")
    def test_invalid_storage_file(self):
        data = self.run_process(
            "test-broken-invalide-storage", assert_status=Data.STATUS_ERROR
        )

        self.assertEqual(data.status, Data.STATUS_ERROR)
        self.assertIn("must be a valid JSON, current: 1a", data.process_error[0])

    @tag_process("test-workflow-1")
    def test_workflow(self):
        workflow_data = self.run_process(
            "test-workflow-1", {"param1": "world"}, tags=["test-tag"]
        )
        workflow_data.refresh_from_db()
        step1_data = Data.objects.get(process__slug="test-example-1")
        step2_data = Data.objects.get(process__slug="test-example-2")

        # Workflow should output indices of all data objects, in order.
        self.assertEqual(workflow_data.output["steps"], [step1_data.pk, step2_data.pk])

        # Steps should execute with the correct variables.

        self.assertEqual(step1_data.input["param1"], "world")
        self.assertEqual(step1_data.input["param2"], True)
        self.assertEqual(step1_data.output["out1"], "hello world")
        self.assertEqual(step1_data.tags, ["test-tag"])
        self.assertEqual(step2_data.input["param1"], step1_data.pk)
        self.assertEqual(step2_data.input["param2"]["a"], step1_data.pk)
        self.assertEqual(step2_data.input["param2"]["b"], "hello")
        self.assertEqual(step2_data.output["out1"], "simon says: hello world")
        self.assertEqual(step2_data.tags, ["test-tag"])

        self.assertEqual(step1_data.parents.count(), 1)
        self.assertEqual(step1_data.parents.first(), workflow_data)
        self.assertEqual(step2_data.parents.count(), 2)
        self.assertCountEqual(step2_data.parents.all(), [workflow_data, step1_data])

        # Check correct dependency type is created.
        self.assertEqual(
            {d.kind for d in step1_data.parents_dependency.all()},
            {DataDependency.KIND_SUBPROCESS},
        )
        self.assertEqual(
            {d.kind for d in step2_data.parents_dependency.all()},
            {DataDependency.KIND_SUBPROCESS, DataDependency.KIND_IO},
        )

    @tag_process("test-workflow-2")
    def test_workflow_entity(self):
        with self.preparation_stage():
            # `self.run_process` adds input data in self.collection
            input_data = self.run_process("test-example-3")

        workflow = self.run_process("test-workflow-2", {"data1": input_data.pk})

        # Check that workflow results are created.
        self.assertEqual(
            Data.objects.filter(pk__in=workflow.output["steps"]).count(), 1
        )
        # There is a workflow and two "normal" data objects
        self.assertEqual(Data.objects.all().count(), 3)

    @with_docker_executor
    @tag_process("test-docker")
    def test_run_in_docker(self):
        data = self.run_process("test-docker")
        self.assertEqual(data.output["result"], "OK")

    @patch.object(Manager, "communicate")
    def test_slug_colision(self, communicate_mock):
        """Test slug colision resolution.

        Use mocking to force slug colision.

        This checks for a bug in permission architecture that caused exception
        due to missing permission_group when slug colision occured.
        """

        async def dummy_coroutine(*args, **kwargs):
            """Dummy coroutine."""
            pass

        def pre_save(instance, add):
            """Set the current slug or raise exception."""
            current_slug = slugs.pop(0)

            if isinstance(current_slug, Exception):
                raise current_slug

            instance.slug = current_slug
            return current_slug

        expected_slug = "my_data_slug"
        slugs = [IntegrityError("flow_data_slug"), expected_slug]
        process = Process.objects.create(contributor=self.contributor)

        # INFO: Remove this line and the coroutine above after we stop
        # supporting Python <= 3.8. Starting with Python 3.8 Mock objects can
        # be awaited.
        communicate_mock.side_effect = dummy_coroutine

        with patch.object(ResolweSlugField, "pre_save", side_effect=pre_save):
            data = Data.objects.create(process=process, contributor=self.contributor)
        data.refresh_from_db()
        self.assertEqual(data.slug, expected_slug)
        self.assertTrue(data.pk is not None)
        self.assertTrue(data.permission_group is not None)

    @with_docker_executor
    def test_terminate_worker(self):
        process = Process.objects.get(slug="test-terminate")

        def get_status():
            return (
                Data.objects.filter(name="Test terminate")
                .values_list("status", flat=True)
                .first()
            )

        def wait_status_change(expected_statuses, timeout=20):
            """Wait for the data object to change status.

            :attr timeout: how many seconds to wait for the status change.

            :attr expected_statuses: the set of expected statuses to wait for.

            :returns: the last known status.
            """
            start_time = time()
            status = None
            while time() - start_time < timeout and status not in expected_statuses:
                sleep(0.1)
                status = get_status()
            return status

        def check_processing():
            """Start processing data object by saving it."""
            # Wait up to 20s for process to start.
            wait_status_change([Data.STATUS_PROCESSING])
            data = Data.objects.get(name="Test terminate")
            # Terminate the worker and wait for the process to complete. In the
            # worst case it will take 60 seconds for the process to complete.
            logger.debug("test_terminate_worker terminating data '%s'.", data.pk)
            data.worker.terminate()
            wait_status_change([Data.STATUS_ERROR, Data.STATUS_DONE])
            connection.close()

        start_time = time()
        with ThreadPoolExecutor(max_workers=2) as executor:
            # Start the check_processing thread.
            executor.submit(check_processing)
            # Start processing data object.
            data = Data.objects.create(
                name="Test terminate", contributor=self.contributor, process=process
            )

        # To mark termination as a success it should take less than 50 seconds.
        # The uninterrupted process would run for 60 seconds.
        data.refresh_from_db()
        self.assertEqual(data.worker.status, Worker.STATUS_COMPLETED)
        self.assertEqual(data.status, Data.STATUS_ERROR)
        self.assertEqual(data.process_error[0], "Processing was cancelled.")
        self.assertLess(time() - start_time, 50)

    @with_docker_executor
    @tag_process("test-requirements-docker")
    def test_executor_requirements(self):
        data = self.run_process("test-requirements-docker")
        self.assertEqual(data.output["result"], "OK")

    @with_docker_executor
    @tag_process("test-docker-uid-gid")
    def test_docker_uid_gid(self):
        data = self.run_process("test-docker-uid-gid")
        self.assertEqual(data.output["result"], "OK")

    @unittest.skip("Null executor test currently not working.")
    @with_null_executor
    @tag_process("test-save-number")
    def test_null_executor(self):
        data = self.run_process(
            "test-save-number", {"number": 19}, assert_status=Data.STATUS_WAITING
        )
        self.assertEqual(data.input["number"], 19)
        self.assertEqual(data.output, {})

    @unittest.skipIf(
        os.environ.get("GITHUB_ACTIONS", "") == "true", "Fails on Github Actions"
    )
    @with_docker_executor
    @tag_process("test-memory-resource-alloc", "test-memory-resource-noalloc")
    def test_memory_resource(self):
        # This process should be terminated due to too much memory usage.
        self.run_process("test-memory-resource-alloc", assert_status=Data.STATUS_ERROR)
        # This process should run normally (honors the limits).
        self.run_process("test-memory-resource-noalloc")

    @with_docker_executor
    @tag_process("test-cpu-resource-1core", "test-cpu-resource-2core")
    def test_cpu_resource(self):
        # Currently there is no good way to test this reliably, so we just check if the
        # resource limit specification still makes the process run.
        self.run_process("test-cpu-resource-1core")
        self.run_process("test-cpu-resource-2core")

    @with_docker_executor
    @tag_process(
        "test-network-resource-enabled",
        "test-network-resource-disabled",
        "test-network-resource-policy",
    )
    def test_network_resource(self):
        self.run_process("test-network-resource-enabled")
        self.run_process(
            "test-network-resource-disabled", assert_status=Data.STATUS_ERROR
        )
        self.run_process(
            "test-network-resource-policy", assert_status=Data.STATUS_ERROR
        )

    @with_docker_executor
    @override_settings(FLOW_PROCESS_RESOURCE_DEFAULTS={"cpu_time_interactive": 1})
    @tag_process(
        "test-scheduling-class-interactive-ok",
        "test-scheduling-class-interactive-fail",
        "test-scheduling-class-batch",
    )
    def test_scheduling_class(self):
        self.run_process("test-scheduling-class-interactive-ok")
        self.run_process(
            "test-scheduling-class-interactive-fail", assert_status=Data.STATUS_ERROR
        )
        self.run_process("test-scheduling-class-batch")

    @with_docker_executor
    @tag_process("test-save-number")
    def test_executor_fs_lock(self):
        # First, run the process normaly. Do not autoremove container to check
        # if name clash with init contaner is avoided.
        with self.settings(FLOW_DOCKER_AUTOREMOVE=False):
            data = self.run_process("test-save-number", {"number": 42})
        # Make sure that process was successfully ran first time.
        self.assertEqual(data.output["number"], 42)
        data.output = {}
        data.save()

        file = Path(data.location.get_path(filename="temporary_file_do_not_purge.txt"))
        self.assertFalse(file.exists())
        file.touch()

        listener_settings = getattr(settings, "FLOW_EXECUTOR", {}).get(
            "LISTENER_CONNECTION", {}
        )
        port = listener_settings.get("port", 53893)
        hosts = settings.COMMUNICATION_CONTAINER_LISTENER_CONNECTION
        host = hosts.get("local", next(iter(hosts.values())))
        protocol = settings.FLOW_EXECUTOR.get("LISTENER_CONNECTION", {}).get(
            "protocol", "tcp"
        )

        # Set the status or listener will imediatelly respond with error
        # status. Make sure to also clear Redis cache for the data object.
        data.worker.status = Worker.STATUS_PROCESSING
        data.worker.save()
        data.status = Data.STATUS_PROCESSING
        data.save()
        redis_cache.clear(Data, (data.pk,))

        process_environment = os.environ.copy()
        process_environment["LISTENER_PUBLIC_KEY"] = LISTENER_PUBLIC_KEY
        process_environment["CURVE_PRIVATE_KEY"] = data.worker.private_key
        process_environment["CURVE_PUBLIC_KEY"] = data.worker.public_key

        process = subprocess.run(
            [
                "python",
                "-m",
                "executors",
                ".docker",
                str(data.id),
                host,
                str(port),
                protocol,
            ],
            cwd=storage_settings.FLOW_VOLUMES["runtime"]["config"]["path"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=30,
            env=process_environment,
        )
        self.assertEqual(process.returncode, 0, f"The stderr was: '{process.stderr}'.")
        data.refresh_from_db()
        self.assertEqual(data.output, {})
        self.assertEqual(data.status, Data.STATUS_PROCESSING)
        self.assertEqual(data.process_error, [])
        self.assertEqual(data.process_info, [])
        # Check that temporary file was not deleted.
        self.assertTrue(file.exists())

    @mock.patch(
        "resolwe.flow.executors.startup_processing_container.asyncio.open_unix_connection"
    )
    def test_processing_communication_connection_ok(self, open_mock):
        """Test connection under normal conditions."""

        async def main_test():
            """Main test coroutine."""
            await asyncio.wait_for(
                connect_to_communication_container("anypath"), timeout=1
            )

        async def readline():
            """Return PING."""
            return b"PING"

        async def reader_writer():
            """Return reader-writer tuple mock."""
            return (mock.Mock(readline=readline), None)

        reader_mock = mock.MagicMock()
        reader_mock.readline = readline
        if sys.version_info >= (3, 8):
            open_mock.return_value = (mock.Mock(readline=readline), None)
        else:
            open_mock.return_value = reader_writer()
        loop = asyncio.new_event_loop()
        loop.run_until_complete(main_test())

    @mock.patch(
        "resolwe.flow.executors.startup_processing_container.asyncio.open_unix_connection"
    )
    def test_processing_communication_connection_ok_retry(self, open_mock):
        """Test connection if first attempt fails."""

        async def main_test():
            """Main test coroutine."""
            await asyncio.wait_for(
                connect_to_communication_container("anypath"), timeout=3
            )

        async def readline_ok():
            """Return PING."""
            return b"PING"

        async def readline_error():
            """Return ERROR."""
            return b"ERROR"

        async def reader_writer(coroutine):
            """Return reader-writer tuple mock."""
            return (mock.Mock(readline=coroutine), None)

        reader_mock = mock.MagicMock()
        reader_mock.readline.side_effect = [readline_error(), readline_ok()]
        if sys.version_info >= (3, 8):
            open_mock.return_value = (reader_mock, None)
        else:
            open_mock.side_effect = [
                reader_writer(readline_error),
                reader_writer(readline_ok),
            ]
        loop = asyncio.new_event_loop()
        loop.run_until_complete(main_test())

    @mock.patch(
        "resolwe.flow.executors.startup_processing_container.asyncio.open_unix_connection"
    )
    @mock.patch(
        "resolwe.flow.executors.startup_processing_container.constants.CONTAINER_TIMEOUT",
        2,
    )
    def test_processing_communication_connection_fail(self, open_mock):
        """Test function behaviour when connection fails."""

        async def main_test():
            """Main test coroutine."""
            current_loop = asyncio.get_event_loop()
            await initialize_connections(current_loop)

        async def readline_error():
            """Return ERROR."""
            return b"ERROR"

        async def reader_writer():
            """Return reader-writer tuple mock."""
            return (mock.Mock(readline=readline_error), None)

        reader_mock = mock.MagicMock()
        reader_mock.readline.side_effect = [readline_error(), readline_error()]
        if sys.version_info >= (3, 8):
            open_mock.return_value = (reader_mock, None)
        else:
            open_mock.side_effect = [reader_writer(), reader_writer()]
        loop = asyncio.new_event_loop()
        with self.assertRaisesRegex(
            ConnectionError, "Timeout connecting to communication container."
        ):
            loop.run_until_complete(main_test())

    @mock.patch(
        "resolwe.flow.executors.startup_processing_container.asyncio.open_unix_connection"
    )
    @mock.patch("resolwe.flow.executors.startup_processing_container.socket")
    @mock.patch(
        "resolwe.flow.executors.startup_processing_container.asyncio.start_unix_server"
    )
    def test_processing_communicate(self, server_mock, socket_mock, open_mock):
        """Test that communicate is called."""

        async def main_test():
            """Main test coroutine."""
            await asyncio.wait_for(
                connect_to_communication_container("anypath"), timeout=1
            )

        async def readline():
            """Return PING."""
            return b"PING"

        async def reader_writer():
            """Return reader-writer tuple mock."""
            return (mock.Mock(readline=readline), None)

        socket_mock.socket.return_value.connect.side_effect = None
        if sys.version_info >= (3, 8):
            open_mock.return_value = (mock.Mock(readline=readline), None)
        else:
            open_mock.return_value = reader_writer()
        # protocol_mock.return_value.communicate.side_effect = ValueError("test")
        loop = asyncio.new_event_loop()
        loop.run_until_complete(main_test())
