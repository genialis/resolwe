# pylint: disable=missing-docstring
from __future__ import absolute_import, division, print_function, unicode_literals

import mock
import shlex
import subprocess
import unittest

from django.conf import settings
from django.test import override_settings

from ..executors.docker import FlowExecutor

try:
    import builtins  # py3
except ImportError:
    import __builtin__ as builtins  # py2


def check_docker():
    return subprocess.call(shlex.split('docker info'), stdout=subprocess.PIPE) == 1


class DockerExecutorTestCase(unittest.TestCase):
    @unittest.skipIf(check_docker(), 'Docker is not installed')
    @mock.patch('os.mkdir')
    @mock.patch('os.chdir')
    @mock.patch('resolwe.flow.executors.Data.objects.filter')
    def test_run_in_docker(self, data_filter_mock, chdir_mock, mkdir_mock):
        executor_settings = settings.FLOW_EXECUTOR
        executor_settings['CONTAINER_IMAGE'] = 'centos'

        with override_settings(FLOW_EXECUTOR=executor_settings):
            executor = FlowExecutor()

            script = 'if [[ -f /.dockerinit ]]; then echo "Running inside Docker"; else echo "Running locally"; fi'

            def assert_output(line):
                self.assertEqual(line.strip(), 'Running inside Docker')

            write_mock = mock.MagicMock(side_effect=assert_output)
            stdout_mock = mock.MagicMock(write=write_mock)
            open_mock = mock.MagicMock(side_effect=[stdout_mock, mock.MagicMock()])
            with mock.patch.object(builtins, 'open', open_mock):
                executor.run('no_data_id', script, verbosity=0)

            self.assertEqual(write_mock.call_count, 1)
