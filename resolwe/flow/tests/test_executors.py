# pylint: disable=missing-docstring
import os
import subprocess
import threading
import unittest
from contextlib import suppress
from pathlib import Path
from time import sleep
from unittest import mock

from asgiref.sync import async_to_sync

from django.conf import settings
from django.test import override_settings

from guardian.shortcuts import assign_perm

from resolwe.flow.executors.prepare import BaseFlowExecutorPreparer
from resolwe.flow.managers import manager
from resolwe.flow.managers.utils import disable_auto_calls
from resolwe.flow.models import Data, DataDependency, Process, Worker
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
    @mock.patch("resolwe.flow.utils.os")
    @mock.patch("resolwe.flow.utils.settings")
    def test_get_tools_paths(self, settings_mock, os_mock, apps_mock):
        apps_mock.get_app_configs.return_value = [
            mock.MagicMock(path="/resolwe/test_app1"),
            mock.MagicMock(path="/resolwe/test_app2"),
        ]
        os_mock.path.join = os.path.join
        os_mock.path.isdir.side_effect = [False, True]
        settings_mock.RESOLWE_CUSTOM_TOOLS_PATHS = ["/custom_tools"]

        base_executor = BaseFlowExecutorPreparer()
        tools_list = base_executor.get_tools_paths()

        self.assertEqual(len(tools_list), 2)
        self.assertIn("/resolwe/test_app2/tools", tools_list)
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
        self.assertIn("not valid under any of the given schemas", data.process_error[0])

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

    @unittest.skipIf(
        os.environ.get("GITHUB_ACTIONS", "") == "true", "Fails on Github Actions"
    )
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
        # First, run the process normaly.
        data = self.run_process("test-save-number", {"number": 42})
        # Make sure that process was successfully ran first time.
        self.assertEqual(data.output["number"], 42)
        data.output = {}
        data.save()

        file = Path(data.location.get_path(filename="temporary_file_do_not_purge.txt"))
        self.assertFalse(file.exists())
        file.touch()

        process = subprocess.run(
            ["python", "-m", "executors", ".docker"],
            cwd=data.get_runtime_path(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=10,
        )
        self.assertEqual(process.returncode, 0)
        data.refresh_from_db()
        self.assertEqual(data.output, {})
        self.assertEqual(data.status, Data.STATUS_DONE)
        self.assertEqual(data.process_error, [])
        # Check that temporary file was not deleted.
        self.assertTrue(file.exists())
