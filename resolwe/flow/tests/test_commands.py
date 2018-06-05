# pylint: disable=missing-docstring
import os
from io import StringIO

import yaml

from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TestCase as DjangoTestCase

from guardian.models import UserObjectPermission
from guardian.shortcuts import assign_perm, get_perms

from resolwe.flow.models import Process
from resolwe.test import ProcessTestCase, TestCase

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

        # Check that contributor gets all permissions.
        process = Process.objects.first()
        self.assertEqual(len(get_perms(self.admin, process)), 3)

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

    def test_validation_of_defaults(self):
        process_path = os.path.join(PROCESSES_DIR, 'wrong_defaults')

        out, err = StringIO(), StringIO()
        call_command('register', path=[process_path], schemas=['test-wrong-type'], stdout=out, stderr=err)
        self.assertIn('VALIDATION ERROR:', err.getvalue())

        out, err = StringIO(), StringIO()
        call_command('register', path=[process_path], schemas=['test-out-of-range'], stdout=out, stderr=err)
        self.assertIn('VALIDATION ERROR:', err.getvalue())

    def test_process_register_filter(self):
        out, err = StringIO(), StringIO()
        # Fields types are also tested here, as process won't register
        # if any of them fail.
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

        self.assertEqual(UserObjectPermission.objects.filter(user=self.user).count(), 2)
        self.assertTrue(self.user.has_perm('flow.view_process', process))


class ProcessRegisterTestNoAdmin(DjangoTestCase):

    def test_process_register_no_admin(self):
        err = StringIO()
        self.assertRaises(SystemExit, call_command, 'register', path=[PROCESSES_DIR], stderr=err)
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
