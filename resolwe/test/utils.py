""".. Ignore pydocstyle D400.

===================================
Resolwe Test Helpers and Decorators
===================================

"""
from __future__ import absolute_import, division, print_function, unicode_literals

import os
import shlex
import shutil
import subprocess
import unittest

import six

from django.conf import settings
from django.test import override_settings

from resolwe.flow.managers import manager

if six.PY2:
    # Monkey-patch shutil package with which function (available in Python 3.3+)
    import shutilwhich  # pylint: disable=import-error,unused-import

__all__ = ('check_installed', 'check_docker', 'with_docker_executor')


def check_installed(command):
    """Check if the given command is installed.

    :param str command: name of the command

    :return: (indicator of the availability of the command, message saying
              command is not available)
    :rtype: tuple(bool, str)

    """
    if shutil.which(command):
        return True, ""
    else:
        return False, "Command '{}' is not found.".format(command)


def check_docker():
    """Check if Docker is installed and working.

    :return: (indicator of the availability of Docker, reason for
              unavailability)
    :rtype: tuple(bool, str)

    """
    command = getattr(settings, 'FLOW_EXECUTOR', {}).get('COMMAND', 'docker')
    info_command = '{} info'.format(command)
    available, reason = True, ""
    # TODO: Use subprocess.DEVNULL after dropping support for Python 2
    with open(os.devnull, 'wb') as DEVNULL:  # pylint: disable=invalid-name
        try:
            subprocess.check_call(shlex.split(info_command), stdout=DEVNULL, stderr=subprocess.STDOUT)
        except OSError:
            available, reason = False, "Docker command '{}' not found".format(command)
        except subprocess.CalledProcessError:
            available, reason = (False, "Docker command '{}' returned non-zero "
                                        "exit status".format(info_command))
    return available, reason


def with_docker_executor(method):
    """Decorate unit test to run processes with Docker executor."""
    # pylint: disable=missing-docstring
    @unittest.skipUnless(*check_docker())
    def wrapper(*args, **kwargs):
        executor_settings = settings.FLOW_EXECUTOR.copy()
        executor_settings.update({
            'NAME': 'resolwe.flow.executors.docker',
            'CONTAINER_IMAGE': 'resolwe/test:base'
        })

        try:
            with override_settings(FLOW_EXECUTOR=executor_settings):
                # Re-run engine discovery as the settings have changed.
                manager.discover_engines()

                # Run the actual unit test method.
                method(*args, **kwargs)
        finally:
            # Re-run engine discovery as the settings have changed.
            manager.discover_engines()

    return wrapper
