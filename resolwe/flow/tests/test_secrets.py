# pylint: disable=missing-docstring
import os
import uuid

from resolwe.flow.models import Data, Secret
from resolwe.test import ProcessTestCase, TransactionTestCase, tag_process, with_docker_executor

PROCESSES_DIR = os.path.join(os.path.dirname(__file__), 'processes')


class SecretsModelTest(TransactionTestCase):
    def test_secret_manager(self):
        handle = Secret.objects.create_secret('foo bar', self.user)
        value = Secret.objects.get_secret(handle, self.user)
        self.assertEqual(value, 'foo bar')

        with self.assertRaises(Secret.DoesNotExist):
            value = Secret.objects.get_secret(handle, self.admin)

        with self.assertRaises(Secret.DoesNotExist):
            value = Secret.objects.get_secret(uuid.uuid4(), self.user)


class SecretsProcessTest(ProcessTestCase):
    def setUp(self):
        super().setUp()

        self._register_schemas(path=[PROCESSES_DIR])

        # Create some secrets.
        self.secret_value = 'hello secret world'
        self.secret_handle = Secret.objects.create_secret(self.secret_value, self.admin)

    @with_docker_executor
    @tag_process('test-secrets-permission-denied')
    def test_permission_denied(self):
        data = self.run_process(
            'test-secrets-permission-denied',
            input_={'token': {'handle': self.secret_handle}},
            assert_status=Data.STATUS_ERROR
        )
        self.assertIn(
            "Permission denied for process: Process 'test-secrets-permission-denied' has secret "
            "inputs, but no permission to see secrets",
            data.process_error
        )

    @with_docker_executor
    @tag_process('test-secrets-echo')
    def test_secret_echo(self):
        data = self.run_process('test-secrets-echo', input_={'token': {'handle': self.secret_handle}})
        self.assertEqual(data.output['secret'], self.secret_value)

    @tag_process('test-secrets-echo')
    def test_secret_local_executor(self):
        data = self.run_process(
            'test-secrets-echo',
            input_={'token': {'handle': self.secret_handle}},
            assert_status=Data.STATUS_ERROR
        )
        self.assertIn(
            "Permission denied for process: Process which requires access to secrets cannot be run "
            "using the local executor",
            data.process_error
        )
