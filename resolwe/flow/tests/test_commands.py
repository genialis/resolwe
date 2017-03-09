# pylint: disable=missing-docstring
from __future__ import absolute_import, division, print_function, unicode_literals

import os

from django.core.management import call_command
from django.test import TestCase as DjangoTestCase
from django.utils.six import StringIO

from guardian.models import UserObjectPermission
from guardian.shortcuts import assign_perm

from resolwe.flow.models import Process
from resolwe.test import TestCase

PROCESSES_DIR = os.path.join(os.path.dirname(__file__), 'processes')


class ProcessRegisterTest(TestCase):

    def test_process_register_all(self):
        out, err = StringIO(), StringIO()
        call_command('register', path=[PROCESSES_DIR], stdout=out, stderr=err)
        self.assertTrue('Inserted test-min' in out.getvalue())
        self.assertTrue('Skip processor test-min: newer version installed' in err.getvalue())
        self.assertTrue(
            'Skip processor test-broken-invalid-execution-engine: '
            'execution engine \'invalid\' not supported' in err.getvalue()
        )

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

    def test_inherit_perms(self):
        out, err = StringIO(), StringIO()
        first_version_path = os.path.join(PROCESSES_DIR, 'first_version')
        call_command('register', path=[first_version_path], stdout=out, stderr=err)

        process = Process.objects.latest()
        assign_perm('view_process', self.user, process)

        out, err = StringIO(), StringIO()
        second_version_path = os.path.join(PROCESSES_DIR, 'second_version')
        call_command('register', path=[second_version_path], stdout=out, stderr=err)

        process = Process.objects.latest()

        self.assertEqual(UserObjectPermission.objects.count(), 2)
        self.assertTrue(self.user.has_perm('flow.view_process', process))


class ProcessRegisterTestNoAdmin(DjangoTestCase):

    def test_process_register_no_admin(self):
        err = StringIO()
        self.assertRaises(SystemExit, call_command, 'register', path=[PROCESSES_DIR], stderr=err)
        self.assertEqual('Admin does not exist: create a superuser\n', err.getvalue())
