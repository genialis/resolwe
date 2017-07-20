# pylint: disable=missing-docstring
from __future__ import absolute_import, division, print_function, unicode_literals

import os
import unittest

import mock
import six

from django.conf import settings
from django.core.exceptions import ValidationError
from django.test import override_settings

from guardian.shortcuts import assign_perm

from resolwe.flow.executors import BaseFlowExecutor
from resolwe.flow.managers import manager
from resolwe.flow.models import Data, Process
from resolwe.test import ProcessTestCase, TestCase, with_docker_executor, with_null_executor

PROCESSES_DIR = os.path.join(os.path.dirname(__file__), 'processes')


class GetToolsTestCase(TestCase):

    @mock.patch('resolwe.flow.executors.apps')
    @mock.patch('resolwe.flow.executors.os')
    @mock.patch('resolwe.flow.executors.settings')
    def test_get_tools(self, settings_mock, os_mock, apps_mock):
        apps_mock.get_app_configs.return_value = [
            mock.MagicMock(path='/resolwe/test_app1'),
            mock.MagicMock(path='/resolwe/test_app2'),
        ]
        os_mock.path.join = os.path.join
        os_mock.path.isdir.side_effect = [False, True]
        settings_mock.RESOLWE_CUSTOM_TOOLS_PATHS = ['/custom_tools']

        base_executor = BaseFlowExecutor(manager=None)
        tools_list = base_executor.get_tools()

        self.assertEqual(len(tools_list), 2)
        self.assertIn('/resolwe/test_app2/tools', tools_list)
        self.assertIn('/custom_tools', tools_list)

    @mock.patch('resolwe.flow.executors.apps')
    @mock.patch('resolwe.flow.executors.settings')
    def test_not_list(self, settings_mock, apps_mock):
        apps_mock.get_app_configs.return_value = []
        settings_mock.RESOLWE_CUSTOM_TOOLS_PATHS = '/custom_tools'

        base_executor = BaseFlowExecutor(manager=None)
        with six.assertRaisesRegex(self, KeyError, 'setting must be a list'):
            base_executor.get_tools()


class ManagerRunProcessTest(ProcessTestCase):
    def setUp(self):
        super(ManagerRunProcessTest, self).setUp()

        self._register_schemas(path=[PROCESSES_DIR])

    def test_minimal_process(self):
        self.run_process('test-min')

    def test_missing_file(self):
        with self.assertRaises(ValidationError):
            self.run_process('test-missing-file', assert_status=Data.STATUS_ERROR)

        data = Data.objects.last()
        self.assertEqual(data.status, Data.STATUS_ERROR)
        self.assertEqual(len(data.process_error), 1)
        self.assertIn('Referenced file does not exist', data.process_error[0])

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

    def test_spawn_missing_export(self):
        with six.assertRaisesRegex(self, KeyError, 'Use `re-export`'):
            self.run_process('test-spawn-missing-file')

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

    def test_workflow(self):
        self.run_process('test-workflow-1', {'param1': 'world'}, run_manager=False)
        workflow_data = Data.objects.get(process__slug='test-workflow-1')

        # Assign permissions before manager is called
        assign_perm('view_data', self.contributor, workflow_data)
        assign_perm('view_data', self.group, workflow_data)

        # One manager call for each created object
        manager.communicate(run_sync=True, verbosity=0)
        manager.communicate(run_sync=True, verbosity=0)
        manager.communicate(run_sync=True, verbosity=0)

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
        six.assertCountEqual(self, step2_data.parents.all(), [workflow_data, step1_data])

        self.assertTrue(self.contributor.has_perm('flow.view_data', step1_data))
        # User inherites permission from group
        self.assertTrue(self.user.has_perm('flow.view_data', step1_data))

    @with_docker_executor
    def test_run_in_docker(self):
        data = self.run_process('test-docker')
        self.assertEqual(data.output['result'], 'OK')

    @with_docker_executor
    def test_executor_requirements(self):
        data = self.run_process('test-requirements-docker')
        self.assertEqual(data.output['result'], 'OK')

    @with_null_executor
    def test_null_executor(self):
        data = self.run_process('test-save-number', {'number': 19}, assert_status=Data.STATUS_WAITING)
        self.assertEqual(data.input['number'], 19)
        self.assertEqual(data.output, {})

    # TODO: Debug why the 'test-memory-resource-alloc' process doesn't end with and error on Travis
    @unittest.skipIf(os.environ.get('TRAVIS', '') == 'true', "Fails on Travis CI")
    @with_docker_executor
    def test_memory_resource(self):
        # This process should be terminated due to too much memory usage.
        self.run_process('test-memory-resource-alloc', assert_status=Data.STATUS_ERROR)
        # This process should run normally (honors the limits).
        self.run_process('test-memory-resource-noalloc')

    @with_docker_executor
    def test_cpu_resource(self):
        # Currently there is no good way to test this reliably, so we just check if the
        # resource limit specification still makes the process run.
        self.run_process('test-cpu-resource-1core')
        self.run_process('test-cpu-resource-2core')

    @with_docker_executor
    def test_network_resource(self):
        self.run_process('test-network-resource-enabled')
        self.run_process('test-network-resource-disabled', assert_status=Data.STATUS_ERROR)
        self.run_process('test-network-resource-policy', assert_status=Data.STATUS_ERROR)

    @with_docker_executor
    @override_settings(FLOW_DOCKER_LIMIT_DEFAULTS={'cpu_time_interactive': 1})
    def test_scheduling_class(self):
        self.run_process('test-scheduling-class-interactive-ok')
        self.run_process('test-scheduling-class-interactive-fail', assert_status=Data.STATUS_ERROR)
        self.run_process('test-scheduling-class-batch')
