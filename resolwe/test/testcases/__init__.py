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

"""

import os
import shutil

from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.contrib.contenttypes.models import ContentType
from django.test import SimpleTestCase as DjangoSimpleTestCase
from django.test import TestCase as DjangoTestCase
from django.test import TransactionTestCase as DjangoTransactionTestCase

from .setting_overrides import FLOW_EXECUTOR_SETTINGS


class TestCaseHelpers(DjangoSimpleTestCase):
    """Mixin for test case helpers."""

    def _pre_setup(self, *args, **kwargs):
        # NOTE: This is a work-around for Django issue #10827
        # (https://code.djangoproject.com/ticket/10827) that clears the
        # ContentType cache before permissions are setup.
        ContentType.objects.clear_cache()
        super()._pre_setup(*args, **kwargs)

    def _clean_up(self):
        """Clean up after test."""
        if not self._keep_data:
            # Do delete this here. See comment below near the makedirs
            # in setUp.
            shutil.rmtree(settings.FLOW_EXECUTOR["DATA_DIR"], ignore_errors=True)
            shutil.rmtree(settings.FLOW_EXECUTOR["UPLOAD_DIR"], ignore_errors=True)
            shutil.rmtree(settings.FLOW_EXECUTOR["RUNTIME_DIR"], ignore_errors=True)

    def setUp(self):
        """Prepare environment for test."""
        super().setUp()

        # Directories need to be recreated here in case a previous
        # TestCase deleted them. Moving this logic into the test runner
        # and manager infrastructure would not work, because the manager
        # and listener can't know where the testcase boundaries are,
        # they just see a series of data objects; deleting too soon
        # might cause problems for some tests. The runner does not have
        # any code between tests, so could only remove data at the very
        # end, by which time it's already too late, since some tests may
        # deal specifically with the purging functionality and should
        # start in a clean environment, without the sediment
        # (e.g. jsonout.txt, stdout.txt) from previous tests.
        os.makedirs(FLOW_EXECUTOR_SETTINGS["DATA_DIR"], exist_ok=True)
        os.makedirs(FLOW_EXECUTOR_SETTINGS["UPLOAD_DIR"], exist_ok=True)
        os.makedirs(FLOW_EXECUTOR_SETTINGS["RUNTIME_DIR"], exist_ok=True)

        self._keep_data = settings.FLOW_MANAGER_KEEP_DATA

        self.addCleanup(self._clean_up)

    def keep_data(self, mock_purge=True):
        """Do not delete output files after tests."""
        self.fail(
            "*ERROR* TestCaseHelpers.keep_data() is deprecated and does not work anymore.\n"
            "Using it will result in attribute errors in the future.\n"
            "Please use the command line options --keep-data and --no-mock-purge instead.\n"
        )

    def assertAlmostEqualGeneric(self, actual, expected, msg=None):
        """Assert almost equality for common types of objects.

        This is the same as :meth:`~unittest.TestCase.assertEqual`, but using
        :meth:`~unittest.TestCase.assertAlmostEqual` when floats are encountered
        inside common containers (currently this includes :class:`dict`,
        :class:`list` and :class:`tuple` types).

        :param actual: object to compare
        :param expected: object to compare against
        :param msg: optional message printed on failures
        """
        self.assertEqual(type(actual), type(expected), msg=msg)

        if isinstance(actual, dict):
            self.assertCountEqual(actual.keys(), expected.keys(), msg=msg)
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
        super().setUp()

        user_model = get_user_model()
        self.admin = user_model.objects.create_superuser(
            username="admin",
            email="admin@test.com",
            password="admin",
            first_name="James",
            last_name="Smith",
        )
        self.contributor = user_model.objects.create_user(
            username="contributor",
            email="contributor@test.com",
            first_name="Joe",
            last_name="Miller",
        )
        self.user = user_model.objects.create_user(
            username="normal_user",
            email="user@test.com",
            first_name="John",
            last_name="Williams",
        )

        self.group = Group.objects.create(name="Users")
        self.group.user_set.add(self.user)


class TestCase(TransactionTestCase, DjangoTestCase):
    """Base class for writing Resolwe tests.

    It is based on :class:`~resolwe.test.TransactionTestCase` and
    Django's :class:`~django.test.TestCase`.
    The latter encloses the test code in a database transaction that is
    rolled back at the end of the test.

    """
