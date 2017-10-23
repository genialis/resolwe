""".. Ignore pydocstyle D400.

==================
Resolwe Test Cases
==================

.. autoclass:: resolwe.test.TestCaseHelpers
    :members:

.. autoclass:: resolwe.test.TransactionTestCase
    :members:

.. autoclass:: resolwe.test.TestCase
    :members:

.. automodule:: resolwe.test.testcases.process

.. automodule:: resolwe.test.testcases.api

.. automodule:: resolwe.test.testcases.elastic

"""

from __future__ import absolute_import, division, print_function, unicode_literals

import copy
import os
import shutil

import mock
import six

from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.contrib.contenttypes.models import ContentType
from django.test import SimpleTestCase as DjangoSimpleTestCase
from django.test import TestCase as DjangoTestCase
from django.test import TransactionTestCase as DjangoTransactionTestCase
from django.test import override_settings
from django.utils.crypto import get_random_string

from .setting_overrides import FLOW_DOCKER_MAPPINGS, FLOW_EXECUTOR_SETTINGS, _get_updated_docker_mappings


def _test_dirs(*base_dirs):
    """Create unique test directory paths.

    Look at existing directories that start with the 'test_' prefix in
    the given base directories. Use the value of largest test directory
    counter found in all given base directories and increase it by 1.
    Then use it to create unique test directories with the same name in
    all base directories.

    :param *base_dirs: paths of base test directories

    :return: paths of the created test directories

    """
    while True:
        counter = 1
        created_dirs = []
        for base_dir_ in base_dirs:
            existing_test_dirs = [
                int(name.replace('test_', '', 1)) for name in os.listdir(base_dir_) if
                os.path.isdir(os.path.join(base_dir_, name)) and name.startswith('test_') and
                name.replace('test_', '', 1).isdecimal()
            ]
            largest = max(existing_test_dirs) if existing_test_dirs else 0
            if largest >= counter:
                counter = largest + 1
        try:
            for dir_ in base_dirs:
                test_dir = os.path.join(dir_, 'test_{}'.format(counter))
                created_dirs.append(test_dir)
                os.makedirs(test_dir)
            break
        except OSError:
            # A directory with the same name was already created in the mean time (e.g. by another
            # test on another thread).
            # Remove already created directories and try again.
            for dir_ in created_dirs:
                os.rmdir(dir_)
            continue

    return created_dirs


@override_settings(FLOW_EXECUTOR=FLOW_EXECUTOR_SETTINGS)
@override_settings(FLOW_DOCKER_MAPPINGS=FLOW_DOCKER_MAPPINGS)
@override_settings(CELERY_ALWAYS_EAGER=True)
class TestCaseHelpers(DjangoSimpleTestCase):
    """Mixin for test case helpers."""

    def _pre_setup(self, *args, **kwargs):
        # NOTE: This is a work-around for Django issue #10827
        # (https://code.djangoproject.com/ticket/10827) that clears the
        # ContentType cache before permissions are setup.
        ContentType.objects.clear_cache()
        super(TestCaseHelpers, self)._pre_setup(*args, **kwargs)

    def _override_executor_settings(self):
        """Override Resolwe Flow Executor's settings to avoid clashes.

        When Resolwe's tests are run in parallel or from different
        Resolwe's code bases on the same system (e.g. on a CI server),
        we must ensure executor's test directories and containers have
        unique names to avoid clashes.

        """
        flow_executor_settings = copy.copy(getattr(settings, 'FLOW_EXECUTOR', {}))

        # Create unique executor's test directories.
        executor_dirs = ('DATA_DIR', 'UPLOAD_DIR')
        for exec_dir, new_dir_path in zip(
                executor_dirs,
                _test_dirs(*(flow_executor_settings[d] for d in executor_dirs))):
            flow_executor_settings[exec_dir] = new_dir_path

        # Update FLOW_DOCKER_MAPPINGS setting.
        flow_docker_mappings = _get_updated_docker_mappings(flow_executor_settings)

        # Create unique container name prefix.
        flow_executor_settings['CONTAINER_NAME_PREFIX'] = '{}_{}_{}'.format(
            flow_executor_settings.get('CONTAINER_NAME_PREFIX', 'resolwe'),
            get_random_string(length=6),
            os.path.basename(flow_executor_settings['DATA_DIR'])
        )

        self.settings = override_settings(FLOW_EXECUTOR=flow_executor_settings,
                                          FLOW_DOCKER_MAPPINGS=flow_docker_mappings)

    def _clean_up(self):
        """Clean up after test."""
        if not self._keep_data:
            shutil.rmtree(settings.FLOW_EXECUTOR['DATA_DIR'], ignore_errors=True)
            shutil.rmtree(settings.FLOW_EXECUTOR['UPLOAD_DIR'], ignore_errors=True)

        self.settings.disable()

    def setUp(self):
        """Prepare environment for test."""
        super(TestCaseHelpers, self).setUp()

        self._override_executor_settings()
        self.settings.enable()

        self._keep_data = False

        self.addCleanup(self._clean_up)

    def keep_data(self, mock_purge=True):
        """Do not delete output files after test."""
        self._keep_data = True

        if mock_purge:
            purge_mock_os = mock.patch('resolwe.flow.utils.purge.os', wraps=os).start()
            purge_mock_os.remove = mock.MagicMock()

            purge_mock_shutil = mock.patch('resolwe.flow.utils.purge.shutil', wraps=shutil).start()
            purge_mock_shutil.rmtree = mock.MagicMock()

    def assertAlmostEqualGeneric(self, actual, expected, msg=None):  # pylint: disable=invalid-name
        """Assert almost equality for common types of objects.

        This is the same as :meth:`~unittest.TestCase.assertEqual`, but using
        :meth:`~unittest.TestCase.assertAlmostEqual` when floats are encountered
        inside common containers (currently this includes :class:`dict`,
        :class:`list` and :class:`tuple` types).

        :param actual: object to compare
        :param expected: object to compare against
        :param msg: optional message printed on failures
        """
        # pylint: disable=no-member
        self.assertEqual(type(actual), type(expected), msg=msg)

        if isinstance(actual, dict):
            six.assertCountEqual(self, actual.keys(), expected.keys(), msg=msg)
            for key in actual.keys():
                self.assertAlmostEqualGeneric(actual[key], expected[key], msg=msg)
        elif isinstance(actual, (list, tuple)):
            for actual_item, expected_item in zip(actual, expected):
                self.assertAlmostEqualGeneric(actual_item, expected_item, msg=msg)
        elif isinstance(actual, float):
            self.assertAlmostEqual(actual, expected, msg=msg)
        else:
            self.assertEqual(actual, expected, msg=msg)


class TransactionTestCase(TestCaseHelpers, DjangoTransactionTestCase):
    """Base class for writing Resolwe tests not enclosed in a transaction.

    It is based on Django's :class:`~django.test.TransactionTestCase`.
    Use it if you need to access the test's database from another
    thread/process.

    """

    def setUp(self):
        """Initialize test data."""
        super(TransactionTestCase, self).setUp()

        user_model = get_user_model()
        self.admin = user_model.objects.create_superuser(username='admin', email='admin@test.com', password='admin')
        self.contributor = user_model.objects.create_user(username='contributor', email='contributor@test.com')
        self.user = user_model.objects.create_user(username='normal_user', email='user@test.com')

        self.group = Group.objects.create(name='Users')
        self.group.user_set.add(self.user)


class TestCase(TransactionTestCase, DjangoTestCase):
    """Base class for writing Resolwe tests.

    It is based on :class:`~resolwe.test.TransactionTestCase` and
    Django's :class:`~django.test.TestCase`.
    The latter encloses the test code in a database transaction that is
    rolled back at the end of the test.

    """

    pass
