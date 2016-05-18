# pylint: disable=missing-docstring
from __future__ import absolute_import, division, print_function, unicode_literals

import os
import shutil

from django.conf import settings
from django.contrib.auth import get_user_model
from django.test import TestCase

from resolwe.flow.managers import manager
from resolwe.flow.models import Data, Process, Storage


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
