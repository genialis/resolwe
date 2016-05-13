# pylint: disable=missing-docstring
from __future__ import absolute_import, division, print_function, unicode_literals

import os

from django.contrib.auth import get_user_model
from django.core.management import call_command
from django.test import TestCase
from django.utils.six import StringIO


PROCESSES_DIR = os.path.join(os.path.dirname(__file__), 'processes')


class ProcessRegisterTest(TestCase):

    def setUp(self):
        get_user_model().objects.create_superuser('test', 'test@genialis.com', 'test')

    def test_process_register_all(self):
        out, err = StringIO(), StringIO()
        call_command('register', path=[PROCESSES_DIR], stdout=out, stderr=err)
        self.assertTrue('Inserted test-min' in out.getvalue())
        self.assertTrue('Skip processor test-min: newer version installed' in err.getvalue())

        out, err = StringIO(), StringIO()
        call_command('register', path=[PROCESSES_DIR], stdout=out, stderr=err)
        self.assertTrue('Skip processor test-min: same version installed' in out.getvalue())
        self.assertTrue('Skip processor test-bloated: same version installed' in out.getvalue())
        self.assertTrue('Skip processor test-min: newer version installed' in err.getvalue())

        out, err = StringIO(), StringIO()
        call_command('register', path=[PROCESSES_DIR], force=True, stdout=out, stderr=err)
        self.assertTrue('Updated test-min' in out.getvalue())
        self.assertTrue('Updated test-bloated' in out.getvalue())
        self.assertTrue('Skip processor test-min: newer version installed' in err.getvalue())

    def test_process_register_filter(self):
        out, err = StringIO(), StringIO()
        call_command('register', path=[PROCESSES_DIR], schemas=['test-bloated'], stdout=out, stderr=err)
        self.assertTrue('Inserted test-bloated' in out.getvalue())
        self.assertTrue('Inserted test-min' not in out.getvalue())
        self.assertEqual('', err.getvalue())

        out, err = StringIO(), StringIO()
        call_command('register', path=[PROCESSES_DIR], schemas=['test-bloated'], stdout=out, stderr=err)
        self.assertTrue('Skip processor test-bloated: same version installed' in out.getvalue())
        self.assertEqual('', err.getvalue())

        out, err = StringIO(), StringIO()
        call_command(
            'register', path=[PROCESSES_DIR], schemas=['test-bloated'], force=True, stdout=out, stderr=err)
        self.assertTrue('Updated test-bloated' in out.getvalue())
        self.assertEqual('', err.getvalue())


class ProcessRegisterTestNoAdmin(TestCase):

    def test_process_register_no_admin(self):
        err = StringIO()
        self.assertRaises(SystemExit, call_command, 'register', path=[PROCESSES_DIR], stderr=err)
        self.assertEqual('Admin does not exist: create a superuser\n', err.getvalue())
