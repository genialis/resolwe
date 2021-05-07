# pylint: disable=missing-docstring
import asyncio
import logging
import os
import subprocess
import sys
import threading
import unittest
from contextlib import suppress
from pathlib import Path
from time import sleep
from unittest import mock

import zmq
import zmq.asyncio
from asgiref.sync import async_to_sync

from django.conf import settings
from django.test import override_settings

from guardian.shortcuts import assign_perm

from resolwe.flow.executors.prepare import BaseFlowExecutorPreparer
from resolwe.flow.executors.socket_utils import Message
from resolwe.flow.executors.startup_processing_container import ProcessingManager
from resolwe.flow.executors.zeromq_utils import ZMQCommunicator
from resolwe.flow.managers import manager
from resolwe.flow.managers.utils import disable_auto_calls
from resolwe.flow.models import Data, DataDependency, Process, Worker
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

    @with_docker_executor
    @tag_process("test-min-python34")
    def test_python_34(self):
        """Test that processing container starts on Python 3.4.

        This test can be removed when we stop using
        broadinstitute/genomes-in-the-cloud:2.3.1-1504795437 docker image in
        the GATK3 pipeline.
        """
        data = self.run_process("test-min-python34")
        self.assertEqual(data.output["out"], "OK")

    @tag_process("test-min")
    def test_minimal_process(self):
        self.run_process("test-min")

    @tag_process("test-annotate")
    def test_annotate(self):
        data = self.run_process("test-annotate")
        self.assertIsNotNone(data.entity)
        dsc = data.entity.descriptor
        self.assertIn("general", dsc)
        self.assertIn("species", dsc["general"])
        self.assertEqual(dsc["general"]["species"], "Valid")

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

        async def send_single_message():
            """Open connection to listener and send single message."""
            connection_string = f"{protocol}://{host}:{port}"
            zmq_context = zmq.asyncio.Context.instance()
            zmq_socket = zmq_context.socket(zmq.DEALER)
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

    @tag_process("test-annotate-wrong-option")
    def test_annotate_wrong_option(self):
        data = self.run_process(
            "test-annotate-wrong-option", assert_status=Data.STATUS_ERROR
        )
        self.assertEqual(len(data.process_error), 1)
        self.assertIn("must match one of predefined choices", data.process_error[0])

    @tag_process("test-annotate-wrong-type")
    def test_annotate_wrong_type(self):
        data = self.run_process(
            "test-annotate-wrong-type", assert_status=Data.STATUS_ERROR
        )
        self.assertEqual(len(data.process_error), 1)
        self.assertIn("is not of type", data.process_error[0])

    @tag_process("test-annotate-missing-field")
    def test_annotate_missing_field(self):
        data = self.run_process(
            "test-annotate-missing-field", assert_status=Data.STATUS_ERROR
        )
        self.assertEqual(len(data.process_error), 1)
        self.assertIn("definition (invalid) missing in schema", data.process_error[0])

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
        print("ERROR", data.process_error[0])
        self.assertIn("must be a valid JSON, current: 1a", data.process_error[0])

    @tag_process("test-workflow-1")
    def test_workflow(self):
        assign_perm("view_collection", self.contributor, self.collection)
        assign_perm("view_collection", self.group, self.collection)

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

        self.assertTrue(self.contributor.has_perm("flow.view_data", step1_data))
        # User inherites permission from group
        self.assertTrue(self.user.has_perm("flow.view_data", step1_data))

    @tag_process("test-workflow-2")
    def test_workflow_entity(self):
        with self.preparation_stage():
            # `self.run_process` adds input data in self.collection
            input_data = self.run_process("test-example-3")

        workflow = self.run_process("test-workflow-2", {"data1": input_data.pk})

        # Check that workflow results are added to the collection.
        self.assertEqual(
            self.collection.data.filter(pk__in=workflow.output["steps"]).count(), 1
        )
        # self.collection now contains workflow and two "normal" data objects
        self.assertEqual(self.collection.data.all().count(), 3)

    @with_docker_executor
    @tag_process("test-docker")
    def test_run_in_docker(self):
        data = self.run_process("test-docker")
        self.assertEqual(data.output["result"], "OK")

    @with_docker_executor
    @disable_auto_calls()
    def test_terminate_worker(self):
        process = Process.objects.get(slug="test-terminate")
        data = Data.objects.create(
            name="Test data",
            contributor=self.contributor,
            process=process,
        )

        def start_processing(data):
            async_to_sync(manager.communicate)(data_id=data.pk, run_sync=True)

        processing_thread = threading.Thread(target=start_processing, args=(data,))
        processing_thread.start()

        # Wait for up to 5s for process to start.
        for _ in range(50):
            sleep(0.1)
            data.refresh_from_db()
            with suppress(Data.worker.RelatedObjectDoesNotExist):
                if data.worker.status == Worker.STATUS_PROCESSING:
                    break

        self.assertEqual(data.worker.status, Worker.STATUS_PROCESSING)
        data.worker.terminate()

        # Give it max 10 seconds to terminate.
        for _ in range(100):
            sleep(0.1)
            data.refresh_from_db()
            if data.worker.status == Worker.STATUS_COMPLETED:
                break

        self.assertEqual(data.worker.status, Worker.STATUS_COMPLETED)
        self.assertEqual(data.status, Data.STATUS_ERROR)
        self.assertEqual(data.process_error[0], "Processing was cancelled.")
        processing_thread.join(timeout=10)
        self.assertFalse(processing_thread.is_alive())

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

    @unittest.skipIf(
        settings.FLOW_DOCKER_DISABLE_SECCOMP, "Docker seccomp is disabled."
    )
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
        hosts = listener_settings.get("hosts", {"local": "127.0.0.1"})
        host = hosts.get("local", next(iter(hosts.values())))
        protocol = settings.FLOW_EXECUTOR.get("LISTENER_CONNECTION", {}).get(
            "protocol", "tcp"
        )
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
        )
        self.assertEqual(process.returncode, 0)
        data.refresh_from_db()
        self.assertEqual(data.output, {})
        self.assertEqual(data.status, Data.STATUS_DONE)
        self.assertEqual(data.process_error, [])
        self.assertEqual(data.process_info, [])
        # Check that temporary file was not deleted.
        self.assertTrue(file.exists())

    @mock.patch(
        "resolwe.flow.executors.startup_processing_container.asyncio.open_unix_connection"
    )
    @mock.patch(
        "resolwe.flow.executors.startup_processing_container.constants.CONTAINER_TIMEOUT",
        2,
    )
    def test_processing_communication_connection_ok(self, open_mock):
        """Test connection under normal conditions."""

        async def main_test():
            """Main test coroutine."""
            current_loop = asyncio.get_event_loop()
            manager = ProcessingManager(current_loop)
            await manager._wait_for_communication_container(None)

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
    @mock.patch(
        "resolwe.flow.executors.startup_processing_container.constants.CONTAINER_TIMEOUT",
        2,
    )
    def test_processing_communication_connection_ok_script(self, open_mock):
        """Test connection if first attempt fails."""

        async def main_test():
            """Main test coroutine."""
            current_loop = asyncio.get_event_loop()
            manager = ProcessingManager(current_loop)
            await manager._wait_for_communication_container(None)

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
            manager = ProcessingManager(current_loop)
            await manager._wait_for_communication_container(None)

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
        with self.assertRaisesRegex(RuntimeError, "Communication .* unreacheable"):
            loop.run_until_complete(main_test())

    @mock.patch(
        "resolwe.flow.executors.startup_processing_container.asyncio.open_unix_connection"
    )
    @mock.patch(
        "resolwe.flow.executors.startup_processing_container.constants.CONTAINER_TIMEOUT",
        2,
    )
    def test_processing_communication_connection_fail_script(self, open_mock):
        """Test script behaviour if connection fails."""

        async def main_test():
            """Main test coroutine."""
            protocol_mock = mock.Mock(terminate_script=terminate_script)
            current_loop = asyncio.get_event_loop()
            manager = ProcessingManager(current_loop)
            manager.protocol_handler = protocol_mock
            await manager.run()

        async def terminate_script():
            """Terminate script mock."""

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
        with self.assertRaisesRegex(RuntimeError, "Communication .* unreacheable"):
            loop.run_until_complete(main_test())

    @mock.patch(
        "resolwe.flow.executors.startup_processing_container.asyncio.open_unix_connection"
    )
    @mock.patch("resolwe.flow.executors.startup_processing_container.socket")
    @mock.patch(
        "resolwe.flow.executors.startup_processing_container.constants.CONTAINER_TIMEOUT",
        2,
    )
    def test_processing_upload_fail(self, socket_mock, open_mock):
        """Test startup script if opening upload socket fails."""

        async def main_test():
            """Main test coroutine."""
            current_loop = asyncio.get_event_loop()
            manager = ProcessingManager(current_loop)
            await manager.run()

        async def readline():
            """Return PING."""
            return b"PING"

        async def reader_writer():
            """Return reader-writer tuple mock."""
            return (mock.Mock(readline=readline), None)

        socket_mock.socket.return_value.connect.side_effect = RuntimeError
        if sys.version_info >= (3, 8):
            open_mock.return_value = (mock.Mock(readline=readline), None)
        else:
            open_mock.return_value = reader_writer()
        loop = asyncio.new_event_loop()
        with self.assertRaises(RuntimeError):
            loop.run_until_complete(main_test())

    @mock.patch(
        "resolwe.flow.executors.startup_processing_container.asyncio.open_unix_connection"
    )
    @mock.patch("resolwe.flow.executors.startup_processing_container.socket")
    @mock.patch(
        "resolwe.flow.executors.startup_processing_container.asyncio.start_unix_server"
    )
    @mock.patch(
        "resolwe.flow.executors.startup_processing_container.constants.CONTAINER_TIMEOUT",
        2,
    )
    def test_processing_script_connection_fail(
        self, server_mock, socket_mock, open_mock
    ):
        """Test startup script if opening processing script socket fails."""

        async def main_test():
            """Main test coroutine."""
            current_loop = asyncio.get_event_loop()
            manager = ProcessingManager(current_loop)
            await manager.run()

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
        loop = asyncio.new_event_loop()
        server_mock.side_effect = ValueError("test")
        with self.assertRaisesRegex(ValueError, "test"):
            loop.run_until_complete(main_test())

    @mock.patch(
        "resolwe.flow.executors.startup_processing_container.asyncio.open_unix_connection"
    )
    @mock.patch("resolwe.flow.executors.startup_processing_container.socket")
    @mock.patch("resolwe.flow.executors.startup_processing_container.create_task")
    @mock.patch(
        "resolwe.flow.executors.startup_processing_container.asyncio.start_unix_server"
    )
    @mock.patch(
        "resolwe.flow.executors.startup_processing_container.constants.CONTAINER_TIMEOUT",
        2,
    )
    def test_processing_create_task_called(
        self, server_mock, create_mock, socket_mock, open_mock
    ):
        """Test that upload timer task is created."""

        async def main_test():
            """Main test coroutine."""
            current_loop = asyncio.get_event_loop()
            manager = ProcessingManager(current_loop)
            await manager.run()

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
        loop = asyncio.new_event_loop()
        create_mock.side_effect = ValueError("test")
        with self.assertRaisesRegex(ValueError, "test"):
            loop.run_until_complete(main_test())

    @mock.patch(
        "resolwe.flow.executors.startup_processing_container.asyncio.open_unix_connection"
    )
    @mock.patch("resolwe.flow.executors.startup_processing_container.socket")
    @mock.patch("resolwe.flow.executors.startup_processing_container.create_task")
    @mock.patch("resolwe.flow.executors.startup_processing_container.ProtocolHandler")
    @mock.patch(
        "resolwe.flow.executors.startup_processing_container.asyncio.start_unix_server"
    )
    @mock.patch(
        "resolwe.flow.executors.startup_processing_container.constants.CONTAINER_TIMEOUT",
        2,
    )
    def test_processing_communicate(
        self, server_mock, protocol_mock, create_mock, socket_mock, open_mock
    ):
        """Test that communicate is called."""

        async def main_test():
            """Main test coroutine."""
            current_loop = asyncio.get_event_loop()
            manager = ProcessingManager(current_loop)
            await manager.run()

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
        protocol_mock.return_value.communicate.side_effect = ValueError("test")
        loop = asyncio.new_event_loop()
        with self.assertRaisesRegex(ValueError, "test"):
            loop.run_until_complete(main_test())
