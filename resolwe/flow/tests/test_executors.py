# pylint: disable=missing-docstring
from __future__ import absolute_import, division, print_function, unicode_literals

import os

import mock
import six

from django.conf import settings

from guardian.shortcuts import assign_perm

from resolwe.flow.executors import BaseFlowExecutor
from resolwe.flow.managers import manager
from resolwe.flow.models import Data, Process
from resolwe.test import ProcessTestCase, TestCase, with_docker_executor

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

    def test_spawn(self):
        self.run_process('test-spawn-new')

        data = Data.objects.last()
        data_dir = settings.FLOW_EXECUTOR['DATA_DIR']
        file_path = os.path.join(data_dir, str(data.pk), 'foo.bar')
        self.assertEqual(data.output['saved_file']['file'], 'foo.bar')
        self.assertTrue(os.path.isfile(file_path))

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

        self.contributor.has_perm('flow.view_data', step1_data)
        # User inherites permission from group
        self.user.has_perm('flow.view_data', step1_data)

    @with_docker_executor
    def test_run_in_docker(self):
        data = self.run_process('test-docker')
        self.assertEqual(data.output['result'], 'OK')

    @with_docker_executor
    def test_executor_requirements(self):
        data = self.run_process('test-requirements-docker')
        self.assertEqual(data.output['result'], 'OK')
