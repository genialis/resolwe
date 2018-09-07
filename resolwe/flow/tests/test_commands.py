# pylint: disable=missing-docstring
import os
from io import StringIO

import yaml

from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TestCase as DjangoTestCase
from django.test import override_settings

from guardian.models import UserObjectPermission
from guardian.shortcuts import assign_perm, get_perms

from resolwe.flow.models import Data, Process
from resolwe.test import ProcessTestCase, TestCase

PROCESSES_DIR = os.path.join(os.path.dirname(__file__), 'processes')


@override_settings(FLOW_PROCESSES_FINDERS=['resolwe.flow.finders.FileSystemProcessesFinder'])
@override_settings(FLOW_PROCESSES_DIRS=[PROCESSES_DIR])
@override_settings(FLOW_DESCRIPTORS_DIRS=[PROCESSES_DIR])
class ProcessRegisterTest(TestCase):

    def test_process_register_all(self):
        out, err = StringIO(), StringIO()
        call_command('register', stdout=out, stderr=err)
        self.assertTrue('Inserted test-min' in out.getvalue())
        self.assertTrue('Skip processor test-min: newer version installed' in err.getvalue())
        self.assertTrue(
            'Skip processor test-broken-invalid-execution-engine: '
            'execution engine \'invalid\' not supported' in err.getvalue()
        )

        # Check that contributor gets all permissions.
        process = Process.objects.first()
        self.assertEqual(len(get_perms(self.admin, process)), 3)

        out, err = StringIO(), StringIO()
        call_command('register', stdout=out, stderr=err)
        self.assertTrue('Skip processor test-min: same version installed' in out.getvalue())
        self.assertTrue('Skip processor test-bloated: same version installed' in out.getvalue())
        self.assertTrue('Skip processor test-min: newer version installed' in err.getvalue())

        out, err = StringIO(), StringIO()
        call_command('register', force=True, stdout=out, stderr=err)
        self.assertTrue('Updated test-min' in out.getvalue())
        self.assertTrue('Updated test-bloated' in out.getvalue())
        self.assertTrue('Skip processor test-min: newer version installed' in err.getvalue())

    def test_validation_of_defaults(self):
        out, err = StringIO(), StringIO()

        with self.settings(FLOW_PROCESSES_DIRS=[os.path.join(PROCESSES_DIR, 'wrong_defaults')]):
            call_command('register', stdout=out, stderr=err)

        self.assertIn('VALIDATION ERROR: Test Process Wrong Type', err.getvalue())
        self.assertIn('VALIDATION ERROR: Test Process Out of Range', err.getvalue())

    def test_inherit_perms(self):
        out, err = StringIO(), StringIO()

        with self.settings(FLOW_PROCESSES_DIRS=[os.path.join(PROCESSES_DIR, 'first_version')]):
            call_command('register', stdout=out, stderr=err)

        process = Process.objects.latest()
        assign_perm('view_process', self.user, process)

        out, err = StringIO(), StringIO()

        with self.settings(FLOW_PROCESSES_DIRS=[os.path.join(PROCESSES_DIR, 'second_version')]):
            call_command('register', stdout=out, stderr=err)

        process = Process.objects.latest()

        self.assertEqual(UserObjectPermission.objects.filter(user=self.user).count(), 2)
        self.assertTrue(self.user.has_perm('flow.view_process', process))

    def test_retire(self):
        # No process should be in data base initially
        self.assertEqual(Process.objects.count(), 0)

        out, err = StringIO(), StringIO()
        # Register process test-proc, version 1.0.0 (first version of the process)
        # There is also version 2.0.0 of the same process
        with self.settings(FLOW_PROCESSES_DIRS=[os.path.join(PROCESSES_DIR, 'first_version')]):
            call_command('register', stdout=out, stderr=err)

        self.assertEqual(Process.objects.get(slug='test-proc').version, '1.0.0')

        call_command('register', stdout=out, stderr=err)

        # Check that all registered processes are active
        initial_processes = Process.objects.count()
        active_processes = Process.objects.filter(is_active=True).count()
        self.assertGreater(initial_processes, 0)
        self.assertEqual(initial_processes, active_processes)

        # Create data with one of the processes
        process = Process.objects.filter(slug='test-min').latest()
        Data.objects.create(name='Test min', process=process, contributor=self.contributor)

        # Nothing changes if register is called again in subfolder without --retire
        with self.settings(FLOW_PROCESSES_DIRS=[os.path.join(PROCESSES_DIR, 'second_version')]):
            call_command('register', stdout=out, stderr=err)

        self.assertEqual(initial_processes, Process.objects.count())
        self.assertEqual(initial_processes, Process.objects.filter(is_active=True).count())

        # Check that retired processes without data are removed and
        # that retired processes with data are deactivated
        with self.settings(FLOW_PROCESSES_DIRS=[os.path.join(PROCESSES_DIR, 'first_version'),
                                                os.path.join(PROCESSES_DIR, 'second_version')]):
            call_command('register', '--retire', stdout=out, stderr=err)

        # One process had data and is kept but inactive
        # Of the two process of the same slug only the latest remains
        self.assertEqual(Process.objects.count(), 2)
        # The process that is left in the schema path remains active and is
        # of the latest version
        self.assertEqual(Process.objects.filter(is_active=True).count(), 1)
        self.assertEqual(Process.objects.filter(is_active=True)[0].slug, 'test-proc')
        self.assertEqual(Process.objects.filter(is_active=True)[0].version, '2.0.0')
        # The process with associated data is inactive
        self.assertEqual(Process.objects.filter(is_active=False).count(), 1)


@override_settings(FLOW_PROCESSES_FINDERS=['resolwe.flow.finders.FileSystemProcessesFinder'])
@override_settings(FLOW_PROCESSES_DIRS=[PROCESSES_DIR])
@override_settings(FLOW_DESCRIPTORS_DIRS=[PROCESSES_DIR])
class ProcessRegisterTestNoAdmin(DjangoTestCase):

    def test_process_register_no_admin(self):
        err = StringIO()
        self.assertRaises(SystemExit, call_command, 'register', stderr=err)
        self.assertEqual('Admin does not exist: create a superuser\n', err.getvalue())


class ListDockerImagesTest(ProcessTestCase):

    def setUp(self):
        super().setUp()

        # Make sure the test processes are in the database
        self._register_schemas(path=[PROCESSES_DIR])

    def test_basic_list(self):
        # List Docker images and check if there's at least one
        out, err = StringIO(), StringIO()
        call_command('list_docker_images', stdout=out, stderr=err)
        self.assertNotEqual('', out.getvalue())
        self.assertEqual('', err.getvalue())

    def test_basic_list_yaml(self):
        # List Docker images in YAML format and see if the output is valid YAML
        out, err = StringIO(), StringIO()
        call_command('list_docker_images', format='yaml', stdout=out, stderr=err)
        self.assertNotEqual('', out.getvalue())
        self.assertEqual('', err.getvalue())
        imgs = yaml.safe_load(out.getvalue())
        self.assertTrue(isinstance(imgs, list))
        self.assertTrue(len(imgs) != 0)
        self.assertTrue(isinstance(imgs[0], dict))

    def test_invalid_format(self):
        # An unsupported format option should return an error
        self.assertRaises(CommandError, call_command, 'list_docker_images', format='invalid')

    def test_list_has_test_image(self):
        # The returned list must contain the image 'resolwe/test:base'
        out, err = StringIO(), StringIO()
        call_command('list_docker_images', stdout=out, stderr=err)
        self.assertEqual('', err.getvalue())
        self.assertTrue('resolwe/test:base' in out.getvalue())

    def test_list_has_test_image_yaml(self):
        # The returned list must contain the image 'resolwe/test:base',
        # in the YAML output as well
        out, err = StringIO(), StringIO()
        call_command('list_docker_images', format='yaml', stdout=out, stderr=err)
        self.assertEqual('', err.getvalue())
        imgs = yaml.safe_load(out.getvalue())
        self.assertTrue(isinstance(imgs, list))
        self.assertTrue(len(imgs) != 0)
        self.assertTrue(isinstance(imgs[0], dict))
        self.assertTrue(dict(name='resolwe/test', tag='base') in imgs)

    def test_list_is_sorted(self):
        # The returned list must be sorted alphabetically
        out, err = StringIO(), StringIO()
        call_command('list_docker_images', stdout=out, stderr=err)
        self.assertEqual('', err.getvalue())
        self.assertNotEqual('', out.getvalue())
        lines = out.getvalue().split('\n')[:-1]
        self.assertEqual(lines, sorted(lines))

    def test_list_versions(self):
        # The returned list must contain only the Docker image from
        # the latest version of a given process
        out, err = StringIO(), StringIO()
        call_command('list_docker_images', stdout=out, stderr=err)
        self.assertEqual('', err.getvalue())
        self.assertIn('resolwe/test:versioning-2', out.getvalue())
        self.assertNotIn('resolwe/test:versioning-1', out.getvalue())
