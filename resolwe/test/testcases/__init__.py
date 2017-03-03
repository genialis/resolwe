""".. Ignore pydocstyle D400.

==================
Resolwe Test Cases
==================

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

from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.contrib.contenttypes.models import ContentType
from django.test import TestCase as DjangoTestCase
from django.test import TransactionTestCase as DjangoTransactionTestCase
from django.test import override_settings
from django.utils.crypto import get_random_string

from .setting_overrides import FLOW_EXECUTOR_SETTINGS


class TransactionTestCase(DjangoTransactionTestCase):
    """Base class for writing Resolwe tests not enclosed in a transaction.

    It is based on Django's :class:`~django.test.TransactionTestCase`.
    Use it if you need to access the test's database from another
    thread/process.

    """

    def _test_data_dir(self, path):
        """Return test data directory path.

        Increase counter in the path name by 1.

        """
        while True:
            try:
                counter = 1
                for name in os.listdir(path):
                    if os.path.isdir(os.path.join(path, name)) and name.startswith('test'):
                        try:
                            current = int(name.split('_')[-1])
                            if current >= counter:
                                counter = current + 1
                        except ValueError:
                            pass

                test_data_dir = os.path.join(path, 'test_{}'.format(counter))
                os.makedirs(test_data_dir)
                break
            except OSError:
                # Try again if a folder with the same name was created
                # by another test on another thread
                continue

        return test_data_dir

    def _pre_setup(self, *args, **kwargs):
        # NOTE: This is a work-around for Django issue #10827
        # (https://code.djangoproject.com/ticket/10827) that clears the
        # ContentType cache before permissions are setup.
        ContentType.objects.clear_cache()
        super(TransactionTestCase, self)._pre_setup(*args, **kwargs)

    def setUp(self):
        """Initialize test data."""
        super(TransactionTestCase, self).setUp()

        # Override flow executor settings
        flow_executor_settings = copy.copy(getattr(settings, 'FLOW_EXECUTOR', {}))

        # Override data directory settings
        data_dir = self._test_data_dir(FLOW_EXECUTOR_SETTINGS['DATA_DIR'])
        flow_executor_settings['DATA_DIR'] = data_dir

        # Override container name prefix setting
        flow_executor_settings['CONTAINER_NAME_PREFIX'] = '{}_{}_{}'.format(
            getattr(settings, 'FLOW_EXECUTOR', {}).get('CONTAINER_NAME_PREFIX', 'resolwe'),
            # NOTE: This is necessary to avoid container name clashes when tests are run from
            # different Resolwe code bases on the same system (e.g. on a CI server).
            get_random_string(length=6),
            os.path.basename(data_dir)
        )

        # Override Docker data directory mappings
        flow_docker_mappings = copy.copy(getattr(settings, 'FLOW_DOCKER_MAPPINGS', []))
        for mapping in flow_docker_mappings:
            if mapping['dest'] == '/data':
                mapping['src'] = os.path.join(data_dir, '{data_id}')
            elif mapping['dest'] == '/data_all':
                mapping['src'] = data_dir

        self.settings = override_settings(FLOW_EXECUTOR=flow_executor_settings,
                                          FLOW_DOCKER_MAPPINGS=flow_docker_mappings)
        self.settings.enable()

        self._keep_data = False

        user_model = get_user_model()
        self.admin = user_model.objects.create_superuser(username='admin', email='admin@test.com', password='admin')
        self.contributor = user_model.objects.create_user(username='contributor')
        self.user = user_model.objects.create_user(username='normal_user')

        self.group = Group.objects.create(name='Users')
        self.group.user_set.add(self.user)

    def tearDown(self):
        """Clean up after the test."""
        if not self._keep_data:
            shutil.rmtree(settings.FLOW_EXECUTOR['DATA_DIR'], ignore_errors=True)

        self.settings.disable()

        super(TransactionTestCase, self).tearDown()

    def keep_data(self, mock_purge=True):
        """Do not delete output files after tests."""
        self._keep_data = True

        if mock_purge:
            purge_mock_os = mock.patch('resolwe.flow.utils.purge.os', wraps=os).start()
            purge_mock_os.remove = mock.MagicMock()

            purge_mock_shutil = mock.patch('resolwe.flow.utils.purge.shutil', wraps=shutil).start()
            purge_mock_shutil.rmtree = mock.MagicMock()


class TestCase(TransactionTestCase, DjangoTestCase):
    """Base class for writing Resolwe tests.

    It is based on :class:`~resolwe.test.TransactionTestCase` and
    Django's :class:`~django.test.TestCase`.
    The latter encloses the test code in a database transaction that is
    rolled back at the end of the test.

    """

    pass
