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
    """Check if Docker is installed and working.

    :return: tuple (indicator of the availability of Docker, reason for
             unavailability)
    :rtype: (bool, str)

    """
    command = settings.FLOW_EXECUTOR.get('COMMAND', 'docker')
    info_command = '{} info'.format(command)
    available, reason = True, ""
    try:
        subprocess.check_call(shlex.split(info_command), stdout=subprocess.PIPE)
    except OSError:
        available, reason = False, "Docker command '{}' not found".format(command)
    except subprocess.CalledProcessError:
        available, reason = (False, "Docker command '{}' returned non-zero "
                                    "exit status".format(info_command))
    return available, reason


class DockerExecutorTestCase(unittest.TestCase):
    @unittest.skipUnless(*check_docker())
    @mock.patch('os.mkdir')
    @mock.patch('os.chmod')
    @mock.patch('os.chdir')
    @mock.patch('resolwe.flow.executors.Data.objects.filter')
    @mock.patch('resolwe.flow.executors.Data.objects.get')
    def test_run_in_docker(self, data_get_mock, data_filter_mock, chdir_mock, chmod_mock, mkdir_mock):
        executor_settings = settings.FLOW_EXECUTOR
        executor_settings['CONTAINER_IMAGE'] = 'centos'

        with override_settings(FLOW_EXECUTOR=executor_settings):
            executor = FlowExecutor()

            script = 'if [[ -f /.dockerinit ]]; then echo "Running inside Docker"; else echo "Running locally"; fi'

            count = {'running': 0}

            def assert_output(line):
                if line.strip() == 'Running inside Docker':
                    count['running'] += 1

            write_mock = mock.MagicMock(side_effect=assert_output)
            stdout_mock = mock.MagicMock(write=write_mock)
            open_mock = mock.MagicMock(side_effect=[stdout_mock, mock.MagicMock()])
            with mock.patch.object(builtins, 'open', open_mock):
                executor.run('no_data_id', script, verbosity=0)

            self.assertEqual(count['running'], 1)
