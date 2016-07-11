# pylint: disable=missing-docstring
from __future__ import absolute_import, division, print_function, unicode_literals

from mock import patch
import os
import shutil
import unittest

from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.core.exceptions import ValidationError
from django.template import Context
from django.test import TestCase, override_settings

from guardian.models import GroupObjectPermission, UserObjectPermission
from guardian.shortcuts import assign_perm, remove_perm

from resolwe.flow.managers import manager
from resolwe.flow.models import Data, hydrate_size, Process, render_template, Storage


class DataModelTest(TestCase):

    def setUp(self):
        self.user = get_user_model().objects.create_superuser('test', 'test@genialis.com', 'test')

    def tearDown(self):
        for data in Data.objects.all():
            data_dir = os.path.join(settings.FLOW_EXECUTOR['DATA_DIR'], str(data.id))
            shutil.rmtree(data_dir, ignore_errors=True)

    def test_name(self):
        process = Process.objects.create(slug='test-first',
                                         type='data:test:first:',
                                         contributor=self.user,
                                         data_name='Process based data name',
                                         output_schema=[{
                                             'name': 'stat',
                                             'type': 'basic:string:',
                                             'required': False,
                                         }],
                                         run={'bash': 'echo {"stat": "42"}'})

        data = Data.objects.create(contributor=self.user,
                                   process=process)

        self.assertEqual(data.name, 'Process based data name')
        self.assertFalse(data.named_by_user)

        data.name = 'Name changed by user'
        data.save()

        self.assertEqual(data.name, 'Name changed by user')
        self.assertTrue(data.named_by_user)

        data = Data.objects.create(name='Explicit data name',
                                   contributor=self.user,
                                   process=process)

        self.assertEqual(data.name, 'Explicit data name')
        self.assertTrue(data.named_by_user)

        process = Process.objects.create(slug='test-second',
                                         type='test:second',
                                         contributor=self.user,
                                         data_name='Process based data name, value: {{src.stat}}',
                                         input_schema=[{
                                             'name': 'src',
                                             'type': 'data:test:first:',
                                             'required': False,
                                         }])

        second = Data.objects.create(contributor=self.user,
                                     process=process,
                                     input={'src': data.id})

        data.output = {'stat': '42'}
        data.status = 'OK'
        data.save()

        self.assertEqual(second.name, 'Process based data name, value: ')
        self.assertFalse(second.named_by_user)

        manager.communicate(verbosity=0)

        second = Data.objects.get(id=second.id)

        self.assertEqual(second.name, 'Process based data name, value: 42')
        self.assertFalse(second.named_by_user)

        data.output = {}
        data.status = 'RE'
        data.save()

        second = Data.objects.create(contributor=self.user,
                                     process=process,
                                     input={'src': data.id})

        second.name = 'User\' data name'
        second.save()

        data.output = {'stat': '42'}
        data.status = 'OK'
        data.save()

        self.assertEqual(second.name, 'User\' data name')
        self.assertTrue(second.named_by_user)

        manager.communicate(verbosity=0)

        second = Data.objects.get(id=second.id)

        self.assertEqual(second.name, 'User\' data name')
        self.assertTrue(second.named_by_user)

    def test_hydrate_file_size(self):
        proc = Process.objects.create(
            name='Test process',
            contributor=self.user,
            output_schema=[
                {'name': 'output_file', 'type': 'basic:file:'}
            ]
        )

        data = Data.objects.create(
            name='Test data',
            contributor=self.user,
            process=proc,
        )

        data.output = {
            'output_file': {'file': 'output.txt'}
        }

        with self.assertRaises(ValidationError):
            data.save()

        dir_path = os.path.join(settings.FLOW_EXECUTOR['DATA_DIR'], str(data.pk))
        os.makedirs(dir_path)
        file_path = os.path.join(dir_path, 'output.txt')
        with open(file_path, 'w') as fn:
            fn.write('foo bar')

        data.save()

        self.assertEqual(data.output['output_file']['size'], 7)


@patch('resolwe.flow.models.os')
class HydrateFileSizeUnitTest(unittest.TestCase):
    def setUp(self):
        self.process = Process(
            output_schema=[
                {'name': 'test_file', 'type': 'basic:file:', 'required': False},
                {'name': 'file_list', 'type': 'list:basic:file:', 'required': False}
            ]
        )
        self.data = Data(
            pk=13,
            process=self.process,
            output={'test_file': {'file': 'test_file.tmp'}}
        )

    def test_done_data(self, os_mock):
        os_mock.path.isfile.return_value = True
        os_mock.path.getsize.return_value = 42000
        hydrate_size(self.data)
        self.assertEqual(self.data.output['test_file']['size'], 42000)

    def test_list(self, os_mock):
        os_mock.path.isfile.return_value = True
        os_mock.path.getsize.side_effect = [34, 42000]
        self.data.output = {
            'file_list': [
                {'file': 'test_01.tmp'},
                {'file': 'test_02.tmp'}
            ]
        }
        hydrate_size(self.data)
        self.assertEqual(self.data.output['file_list'][0]['size'], 34)
        self.assertEqual(self.data.output['file_list'][1]['size'], 42000)

    def test_change_size(self, os_mock):
        """Size is not changed after object is done."""
        os_mock.path.isfile.return_value = True
        os_mock.path.getsize.return_value = 42000
        hydrate_size(self.data)
        self.assertEqual(self.data.output['test_file']['size'], 42000)

        os_mock.path.getsize.return_value = 43000
        hydrate_size(self.data)
        self.assertEqual(self.data.output['test_file']['size'], 43000)

        self.data.status = Data.STATUS_DONE
        os_mock.path.getsize.return_value = 44000
        hydrate_size(self.data)
        self.assertEqual(self.data.output['test_file']['size'], 43000)

    def test_missing_file(self, os_mock):
        os_mock.path.isfile.return_value = False
        with self.assertRaises(ValidationError):
            hydrate_size(self.data)


class StorageModelTestcase(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create(username="test_user")
        self.proc = Process.objects.create(
            name='Test process',
            contributor=self.user,
            output_schema=[
                {'name': 'json_field', 'type': 'basic:json:'},
            ],
        )

    def test_save_storage(self):
        """`basic:json:` fields are stored in Storage"""
        data = Data.objects.create(
            name='Test data',
            contributor=self.user,
            process=self.proc,
        )

        data.output = {'json_field': {'foo': 'bar'}}
        data.status = Data.STATUS_DONE
        data.save()

        self.assertEqual(Storage.objects.count(), 1)
        storage = Storage.objects.first()
        self.assertEqual(data.output['json_field'], storage.pk)

    def test_delete_data(self):
        """`Storage` is deleted when `Data` object is deleted"""
        data = Data.objects.create(
            name='Test data',
            contributor=self.user,
            process=self.proc,
        )

        data.output = {'json_field': {'foo': 'bar'}}
        data.status = Data.STATUS_DONE
        data.save()

        self.assertEqual(Storage.objects.count(), 1)

        data.delete()
        self.assertEqual(Storage.objects.count(), 0)


class UtilsTestCase(unittest.TestCase):
    @override_settings(RESOLWE_CUSTOM_TEMPLATE_TAGS=['test_tags'])
    def test_render_template(self):
        template = render_template('{{ 1 | increase }}', Context())
        self.assertEqual(template, '2')

    @override_settings(RESOLWE_CUSTOM_TEMPLATE_TAGS='test_tags')
    def test_render_template_error(self):
        with self.assertRaises(KeyError):
            render_template('{{ 1 | increase }}', Context())


class StoragePermsTestCase(TestCase):
    def setUp(self):
        contributor = get_user_model().objects.create(username="contributor")
        proc = Process.objects.create(name='Test process', contributor=contributor)
        self.data = Data.objects.create(name='Test data', contributor=contributor, process=proc)

        self.storage = Storage.objects.create(
            name='Test storage',
            json={},
            data=self.data,
            contributor=contributor,
        )

        self.user = get_user_model().objects.create(username="test_user")
        self.group = Group.objects.create(name="test_group")

    def test_sync_user_perms(self):
        assign_perm("view_data", self.user, self.data)
        self.assertEqual(UserObjectPermission.objects.count(), 2)
        self.assertTrue(UserObjectPermission.objects.filter(
            object_pk=self.storage.pk, user=self.user, permission__codename="view_storage"
        ).exists())

        remove_perm("view_data", self.user, self.data)
        self.assertEqual(UserObjectPermission.objects.count(), 0)

    def test_sync_group_perms(self):
        assign_perm("view_data", self.group, self.data)
        self.assertEqual(GroupObjectPermission.objects.count(), 2)
        self.assertTrue(GroupObjectPermission.objects.filter(
            object_pk=self.storage.pk, group=self.group, permission__codename="view_storage"
        ).exists())

        remove_perm("view_data", self.group, self.data)
        self.assertEqual(GroupObjectPermission.objects.count(), 0)

    def test_download_perms(self):
        assign_perm("download_data", self.user, self.data)
        self.assertEqual(UserObjectPermission.objects.count(), 1)
