# pylint: disable=missing-docstring
import os
import subprocess
import unittest

import mock

from django.conf import settings
from django.db import transaction
from django.test import override_settings

from guardian.shortcuts import assign_perm

from resolwe.flow.executors.prepare import BaseFlowExecutorPreparer
from resolwe.flow.managers import manager
from resolwe.flow.models import Collection, Data, DataDependency, DescriptorSchema, Process
from resolwe.test import ProcessTestCase, TestCase, tag_process, with_docker_executor, with_null_executor

# Workaround for false positive warnings in pylint.
# TODO: Can be removed with Django 2.0 or when pylint issue is fixed:
#       https://github.com/PyCQA/pylint/issues/1653
# pylint: disable=deprecated-method

PROCESSES_DIR = os.path.join(os.path.dirname(__file__), 'processes')


class GetToolsTestCase(TestCase):

    @mock.patch('resolwe.flow.utils.apps')
    @mock.patch('resolwe.flow.utils.os')
    @mock.patch('resolwe.flow.utils.settings')
    def test_get_tools_paths(self, settings_mock, os_mock, apps_mock):
        apps_mock.get_app_configs.return_value = [
            mock.MagicMock(path='/resolwe/test_app1'),
            mock.MagicMock(path='/resolwe/test_app2'),
        ]
        os_mock.path.join = os.path.join
        os_mock.path.isdir.side_effect = [False, True]
        settings_mock.RESOLWE_CUSTOM_TOOLS_PATHS = ['/custom_tools']

        base_executor = BaseFlowExecutorPreparer()
        tools_list = base_executor.get_tools_paths()

        self.assertEqual(len(tools_list), 2)
        self.assertIn('/resolwe/test_app2/tools', tools_list)
        self.assertIn('/custom_tools', tools_list)

    @mock.patch('resolwe.flow.utils.apps')
    @mock.patch('resolwe.flow.utils.settings')
    def test_not_list(self, settings_mock, apps_mock):
        apps_mock.get_app_configs.return_value = []
        settings_mock.RESOLWE_CUSTOM_TOOLS_PATHS = '/custom_tools'

        base_executor = BaseFlowExecutorPreparer()
        with self.assertRaisesRegex(KeyError, 'setting must be a list'):
            base_executor.get_tools_paths()


class ManagerRunProcessTest(ProcessTestCase):
    def setUp(self):
        super().setUp()

        self._register_schemas(path=[PROCESSES_DIR])

        DescriptorSchema.objects.create(contributor=self.contributor, slug='sample')

    @tag_process('test-min')
    def test_minimal_process(self):
        self.run_process('test-min')

    @tag_process('test-missing-file')
    def test_missing_file(self):
        self.run_process('test-missing-file', assert_status=Data.STATUS_ERROR)

        data = Data.objects.last()
        self.assertEqual(data.status, Data.STATUS_ERROR)
        self.assertEqual(len(data.process_error), 1)
        self.assertIn('Referenced file does not exist', data.process_error[0])

    @tag_process('test-spawn-new')
    def test_spawn(self):
        self.run_process('test-spawn-new')

        data = Data.objects.last()
        data_dir = settings.FLOW_EXECUTOR['DATA_DIR']
        file_path = os.path.join(data_dir, str(data.pk), 'foo.bar')
        self.assertEqual(data.output['saved_file']['file'], 'foo.bar')
        self.assertTrue(os.path.isfile(file_path))

        parent_data = Data.objects.first()
        self.assertEqual(data.parents.count(), 1)
        self.assertEqual(data.parents.first(), parent_data)

        # Check correct dependency type is created.
        self.assertEqual({d.kind for d in data.parents_dependency.all()}, {DataDependency.KIND_SUBPROCESS})

    @unittest.skipIf(True, "since PR308: the exception happens in other processes, currently impossible to propagate")
    @tag_process('test-spawn-missing-file')
    def test_spawn_missing_export(self):
        with self.assertRaisesRegex(KeyError, 'Use `re-export`'):
            self.run_process('test-spawn-missing-file')

    @tag_process('test-broken', 'test-broken-invalid-execution-engine', 'test-broken-invalid-expression-engine',
                 'test-broken-data-name')
    def test_broken(self):
        Process.objects.create(
            slug='test-broken-invalid-execution-engine',
            name='Test Process',
            contributor=self.contributor,
            type='data:test',
            version=1,
            run={
                'language': 'invalid',
            }
        )

        self.run_process('test-broken', assert_status=Data.STATUS_ERROR)
        self.run_process('test-broken-invalid-expression-engine', assert_status=Data.STATUS_ERROR)
        self.run_process('test-broken-invalid-execution-engine', assert_status=Data.STATUS_ERROR)

        # If evaluation of data_name template fails, the process should not abort as the
        # template may be evaluatable later when the process completes.
        self.run_process('test-broken-data-name')

    @tag_process('test-broken-invalide-storage')
    def test_invalid_storage_file(self):
        data = self.run_process('test-broken-invalide-storage', assert_status=Data.STATUS_ERROR)

        self.assertEqual(data.status, Data.STATUS_ERROR)
        self.assertIn("Value of 'storage' must be a valid JSON, current: 1a", data.process_error)

    @tag_process('test-workflow-1')
    def test_workflow(self):
        with transaction.atomic():
            # We need this transaction to delay calling the manager until we've
            # assigned permissions
            self.run_process('test-workflow-1', {'param1': 'world'})
            workflow_data = Data.objects.get(process__slug='test-workflow-1')

            assign_perm('view_data', self.contributor, workflow_data)
            assign_perm('view_data', self.group, workflow_data)

        workflow_data.refresh_from_db()
        step1_data = Data.objects.get(process__slug='test-example-1')
        step2_data = Data.objects.get(process__slug='test-example-2')

        # Workflow should output indices of all data objects, in order.
        self.assertEqual(workflow_data.output['steps'], [step1_data.pk, step2_data.pk])

        # Steps should execute with the correct variables.
        self.assertEqual(step1_data.input['param1'], 'world')
        self.assertEqual(step1_data.input['param2'], True)
        self.assertEqual(step1_data.output['out1'], 'hello world')
        self.assertEqual(step2_data.input['param1'], step1_data.pk)
        self.assertEqual(step2_data.input['param2']['a'], step1_data.pk)
        self.assertEqual(step2_data.input['param2']['b'], 'hello')
        self.assertEqual(step2_data.output['out1'], 'simon says: hello world')

        self.assertEqual(step1_data.parents.count(), 1)
        self.assertEqual(step1_data.parents.first(), workflow_data)
        self.assertEqual(step2_data.parents.count(), 2)
        self.assertCountEqual(step2_data.parents.all(), [workflow_data, step1_data])

        # Check correct dependency type is created.
        self.assertEqual({d.kind for d in step1_data.parents_dependency.all()}, {DataDependency.KIND_SUBPROCESS})
        self.assertEqual({d.kind for d in step2_data.parents_dependency.all()},
                         {DataDependency.KIND_SUBPROCESS, DataDependency.KIND_IO})

        self.assertTrue(self.contributor.has_perm('flow.view_data', step1_data))
        # User inherites permission from group
        self.assertTrue(self.user.has_perm('flow.view_data', step1_data))

    @tag_process('test-workflow-2')
    def test_workflow_entity(self):
        collection = Collection.objects.create(contributor=self.contributor)

        with transaction.atomic():
            # We need this transaction to delay calling the manager until we've
            # add data to the collection.
            with self.preparation_stage():
                input_data = self.run_process('test-example-3')

            collection.data.add(input_data)
            input_data.entity_set.first().collections.add(collection)

        workflow_data = self.run_process('test-workflow-2', {'data1': input_data.pk})

        # Check that workflow results are added to the collection.
        self.assertEqual(collection.data.filter(pk__in=workflow_data.output['steps']).count(), 1)
        self.assertEqual(collection.data.all().count(), 2)

    @with_docker_executor
    @tag_process('test-docker')
    def test_run_in_docker(self):
        data = self.run_process('test-docker')
        self.assertEqual(data.output['result'], 'OK')

    @with_docker_executor
    @tag_process('test-requirements-docker')
    def test_executor_requirements(self):
        data = self.run_process('test-requirements-docker')
        self.assertEqual(data.output['result'], 'OK')

    @with_docker_executor
    @tag_process('test-docker-uid-gid')
    def test_docker_uid_gid(self):
        data = self.run_process('test-docker-uid-gid')
        self.assertEqual(data.output['result'], 'OK')

    @with_null_executor
    @tag_process('test-save-number')
    def test_null_executor(self):
        data = self.run_process('test-save-number', {'number': 19}, assert_status=Data.STATUS_WAITING)
        self.assertEqual(data.input['number'], 19)
        self.assertEqual(data.output, {})

    # TODO: Debug why the 'test-memory-resource-alloc' process doesn't end with and error on Travis
    @unittest.skipIf(os.environ.get('TRAVIS', '') == 'true', "Fails on Travis CI")
    @with_docker_executor
    @tag_process('test-memory-resource-alloc', 'test-memory-resource-noalloc')
    def test_memory_resource(self):
        # This process should be terminated due to too much memory usage.
        self.run_process('test-memory-resource-alloc', assert_status=Data.STATUS_ERROR)
        # This process should run normally (honors the limits).
        self.run_process('test-memory-resource-noalloc')

    @with_docker_executor
    @tag_process('test-cpu-resource-1core', 'test-cpu-resource-2core')
    def test_cpu_resource(self):
        # Currently there is no good way to test this reliably, so we just check if the
        # resource limit specification still makes the process run.
        self.run_process('test-cpu-resource-1core')
        self.run_process('test-cpu-resource-2core')

    @with_docker_executor
    @tag_process('test-network-resource-enabled', 'test-network-resource-disabled', 'test-network-resource-policy')
    def test_network_resource(self):
        self.run_process('test-network-resource-enabled')
        self.run_process('test-network-resource-disabled', assert_status=Data.STATUS_ERROR)
        self.run_process('test-network-resource-policy', assert_status=Data.STATUS_ERROR)

    @with_docker_executor
    @override_settings(FLOW_PROCESS_RESOURCE_DEFAULTS={'cpu_time_interactive': 1})
    @tag_process('test-scheduling-class-interactive-ok', 'test-scheduling-class-interactive-fail',
                 'test-scheduling-class-batch')
    def test_scheduling_class(self):
        self.run_process('test-scheduling-class-interactive-ok')
        self.run_process('test-scheduling-class-interactive-fail', assert_status=Data.STATUS_ERROR)
        self.run_process('test-scheduling-class-batch')

    @with_docker_executor
    @tag_process('test-save-number')
    def test_executor_fs_lock(self):
        # First, run the process normaly.
        data = self.run_process('test-save-number', {'number': 42})

        # Make sure that process was successfully ran first time.
        self.assertEqual(data.output['number'], 42)
        data.output = {}
        data.save()

        process = subprocess.run(
            ['python', '-m', 'executors', '.docker'],
            cwd=os.path.join(settings.FLOW_EXECUTOR['RUNTIME_DIR'], str(data.pk)),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=5,
        )

        # Drain control events generated by this executor.
        manager.reset(keep_state=True)

        self.assertEqual(process.returncode, 0)

        # Check the status of the data object.
        data.refresh_from_db()
        # Check that output is empty and thus process didn't ran.
        self.assertEqual(data.output, {})
        self.assertEqual(data.status, Data.STATUS_DONE)
        self.assertEqual(data.process_error, [])
