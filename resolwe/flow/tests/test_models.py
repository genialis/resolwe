# pylint: disable=missing-docstring
from __future__ import absolute_import, division, print_function, unicode_literals

import io
import json
import os
import shutil

import six
from mock import MagicMock, patch

from django.conf import settings
from django.contrib.auth import get_user_model
from django.core.exceptions import ValidationError
from django.db import transaction

from guardian.shortcuts import assign_perm, remove_perm
from rest_framework.test import APIRequestFactory, APITestCase, force_authenticate

from resolwe.flow.expression_engines import EvaluationError
from resolwe.flow.models import Data, DataDependency, DescriptorSchema, Entity, Process, Storage
from resolwe.flow.models.data import hydrate_size, render_template
from resolwe.flow.views import DataViewSet
from resolwe.test import TestCase, TransactionTestCase

try:
    import builtins  # py3
except ImportError:
    import __builtin__ as builtins  # py2


class DataModelNameTest(TransactionTestCase):

    def test_name(self):
        process = Process.objects.create(slug='test-first',
                                         type='data:test:first:',
                                         contributor=self.contributor,
                                         data_name='Process based data name',
                                         output_schema=[{
                                             'name': 'stat',
                                             'type': 'basic:string:',
                                             'required': False,
                                         }],
                                         run={
                                             'language': 'bash',
                                             'program': 'echo {"stat": "42"}'
                                         })

        data = Data.objects.create(contributor=self.contributor,
                                   process=process)
        data.refresh_from_db()

        self.assertEqual(data.name, 'Process based data name')
        self.assertFalse(data.named_by_user)

        data.name = 'Name changed by user'
        data.save()
        data.refresh_from_db()

        self.assertEqual(data.name, 'Name changed by user')
        self.assertTrue(data.named_by_user)

        data = Data.objects.create(name='Explicit data name',
                                   contributor=self.contributor,
                                   process=process)
        data.refresh_from_db()

        self.assertEqual(data.name, 'Explicit data name')
        self.assertTrue(data.named_by_user)

        process = Process.objects.create(slug='test-second',
                                         type='test:second',
                                         contributor=self.contributor,
                                         requirements={'expression-engine': 'jinja'},
                                         data_name='Process based data name, value: {{src.stat}}',
                                         input_schema=[{
                                             'name': 'src',
                                             'type': 'data:test:first:',
                                             'required': False,
                                         }])

        with transaction.atomic():
            second = Data.objects.create(contributor=self.contributor,
                                         process=process,
                                         input={'src': data.id})

            data.output = {'stat': '42'}
            data.status = 'OK'
            data.save()

            self.assertEqual(second.name, 'Process based data name, value: ')
            self.assertFalse(second.named_by_user)

        second = Data.objects.get(id=second.id)

        self.assertEqual(second.name, 'Process based data name, value: 42')
        self.assertFalse(second.named_by_user)

        with transaction.atomic():
            data.output = {}
            data.status = 'RE'
            data.save()

            second = Data.objects.create(contributor=self.contributor,
                                         process=process,
                                         input={'src': data.id})

            second.name = 'User\' data name'
            second.save()

            data.output = {'stat': '42'}
            data.status = 'OK'
            data.save()

            self.assertEqual(second.name, 'User\' data name')
            self.assertTrue(second.named_by_user)

        second = Data.objects.get(id=second.id)

        self.assertEqual(second.name, 'User\' data name')
        self.assertTrue(second.named_by_user)


class DataModelTest(TestCase):

    def test_trim_name(self):
        process = Process.objects.create(contributor=self.contributor, data_name='test' * 50)
        data = Data.objects.create(contributor=self.contributor, process=process)

        self.assertEqual(len(data.name), 100)
        self.assertEqual(data.name[-3:], '...')

    def test_hydrate_file_size(self):
        proc = Process.objects.create(
            name='Test process',
            contributor=self.contributor,
            output_schema=[
                {'name': 'output_file', 'type': 'basic:file:'}
            ]
        )

        data = Data.objects.create(
            name='Test data',
            contributor=self.contributor,
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

    def test_dependencies_single(self):
        process = Process.objects.create(slug='test-dependencies',
                                         type='data:test:dependencies:',
                                         contributor=self.contributor,
                                         input_schema=[{
                                             'name': 'src',
                                             'type': 'data:test:dependencies:',
                                             'required': False,
                                         }])

        first = Data.objects.create(name='First data',
                                    contributor=self.contributor,
                                    process=process)

        second = Data.objects.create(name='Second data',
                                     contributor=self.contributor,
                                     process=process,
                                     input={'src': first.id})

        third = Data.objects.create(name='Third data',
                                    contributor=self.contributor,
                                    process=process,
                                    input={'src': first.id})

        self.assertEqual(first.parents.all().count(), 0)
        self.assertEqual(first.children.all().count(), 2)
        self.assertEqual(second.children.all().count(), 0)
        self.assertEqual(second.parents.all().count(), 1)
        self.assertEqual(third.children.all().count(), 0)
        self.assertEqual(third.parents.all().count(), 1)
        self.assertIn(first, second.parents.all())
        self.assertIn(first, third.parents.all())
        self.assertIn(second, first.children.all())
        self.assertIn(third, first.children.all())

        # Check correct dependency type is created.
        self.assertEqual({d.kind for d in first.children_dependency.all()}, {DataDependency.KIND_IO})
        self.assertEqual({d.kind for d in second.parents_dependency.all()}, {DataDependency.KIND_IO})
        self.assertEqual({d.kind for d in third.parents_dependency.all()}, {DataDependency.KIND_IO})

    def test_dependencies_list(self):
        process = Process.objects.create(slug='test-dependencies-list',
                                         type='data:test:dependencies:list:',
                                         contributor=self.contributor,
                                         input_schema=[{
                                             'name': 'src',
                                             'type': 'list:data:test:dependencies:list:',
                                             'required': False,
                                         }])

        first = Data.objects.create(name='First data',
                                    contributor=self.contributor,
                                    process=process)

        second = Data.objects.create(name='Second data',
                                     contributor=self.contributor,
                                     process=process,
                                     input={'src': [first.id]})

        third = Data.objects.create(name='Third data',
                                    contributor=self.contributor,
                                    process=process,
                                    input={'src': [first.id, second.id]})

        self.assertEqual(first.parents.all().count(), 0)
        self.assertEqual(first.children.all().count(), 2)
        self.assertEqual(second.children.all().count(), 1)
        self.assertEqual(second.parents.all().count(), 1)
        self.assertEqual(third.children.all().count(), 0)
        self.assertEqual(third.parents.all().count(), 2)
        self.assertIn(first, second.parents.all())
        self.assertIn(first, third.parents.all())
        self.assertIn(second, first.children.all())
        self.assertIn(third, first.children.all())
        self.assertIn(third, second.children.all())

        # Check correct dependency type is created.
        self.assertEqual({d.kind for d in first.children_dependency.all()}, {DataDependency.KIND_IO})
        self.assertEqual({d.kind for d in second.children_dependency.all()}, {DataDependency.KIND_IO})
        self.assertEqual({d.kind for d in second.parents_dependency.all()}, {DataDependency.KIND_IO})
        self.assertEqual({d.kind for d in third.parents_dependency.all()}, {DataDependency.KIND_IO})


class EntityModelTest(TestCase):

    def setUp(self):
        super(EntityModelTest, self).setUp()

        DescriptorSchema.objects.create(name='Sample', slug='sample', contributor=self.contributor)
        self.process = Process.objects.create(name='Test process',
                                              contributor=self.contributor,
                                              flow_collection='sample')
        # `Sample`is created automatically when `Data` object is created
        self.data = Data.objects.create(name='Test data', contributor=self.contributor, process=self.process)

    def test_delete_last_data(self):
        self.data.delete()
        self.assertEqual(Entity.objects.count(), 0)

    def test_new_sample(self):
        data = Data.objects.create(name='Test data', contributor=self.contributor, process=self.process,
                                   tags=['foo', 'bar'])
        entity = Entity.objects.last()
        self.assertTrue(entity.data.filter(pk=data.pk).exists())

        # Make sure tags are copied.
        self.assertEqual(entity.tags, data.tags)


class GetOrCreateTestCase(APITestCase):

    def setUp(self):
        super(GetOrCreateTestCase, self).setUp()

        user_model = get_user_model()
        self.user = user_model.objects.create(username='test_user', password='test_pwd')

        self.process = Process.objects.create(
            name='Temporary process',
            contributor=self.user,
            slug='tmp-process',
            persistence=Process.PERSISTENCE_TEMP,
            input_schema=[
                {'name': 'some_value', 'type': 'basic:integer:', 'default': 42}
            ],
        )
        assign_perm('view_process', self.user, self.process)

        process_2 = Process.objects.create(
            name='Another process',
            contributor=self.user,
            slug='another-process',
            persistence=Process.PERSISTENCE_TEMP,
            input_schema=[
                {'name': 'some_value', 'type': 'basic:integer:'}
            ],
        )
        assign_perm('view_process', self.user, process_2)

        self.data = Data.objects.create(
            name='Temporary data',
            contributor=self.user,
            process=self.process,
            input={'some_value': 42}
        )
        assign_perm('view_data', self.user, self.data)

        self.get_or_create_view = DataViewSet.as_view({'post': 'get_or_create'})
        self.factory = APIRequestFactory()

    def tearDown(self):
        for data in Data.objects.all():
            data_dir = os.path.join(settings.FLOW_EXECUTOR['DATA_DIR'], str(data.id))
            shutil.rmtree(data_dir, ignore_errors=True)

        super(GetOrCreateTestCase, self).tearDown()

    def test_get_same(self):
        request = self.factory.post(
            '',
            {'name': 'Data object', 'input': {'some_value': 42}, 'process': 'tmp-process'},
            format='json'
        )
        force_authenticate(request, user=self.user)

        response = self.get_or_create_view(request)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data['id'], self.data.pk)

    def test_use_defaults(self):
        request = self.factory.post(
            '',
            {'name': 'Data object', 'input': {}, 'process': 'tmp-process'},
            format='json'
        )
        force_authenticate(request, user=self.user)

        response = self.get_or_create_view(request)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data['id'], self.data.pk)

    def test_missing_permission(self):
        remove_perm('view_data', self.user, self.data)

        request = self.factory.post(
            '',
            {'name': 'Data object', 'input': {'some_value': 42}, 'process': 'tmp-process'},
            format='json'
        )
        force_authenticate(request, user=self.user)

        response = self.get_or_create_view(request)
        self.assertEqual(response.status_code, 201)
        self.assertNotEqual(response.data['id'], self.data.pk)

    def test_different_input(self):
        request = self.factory.post(
            '',
            {'name': 'Data object', 'input': {'some_value': 43}, 'process': 'tmp-process'},
            format='json'
        )
        force_authenticate(request, user=self.user)

        response = self.get_or_create_view(request)
        self.assertEqual(response.status_code, 201)
        self.assertNotEqual(response.data['id'], self.data.pk)

    def test_different_process(self):
        request = self.factory.post(
            '',
            {'name': 'Data object', 'input': {'some_value': 43}, 'process': 'another-process'},
            format='json'
        )
        force_authenticate(request, user=self.user)

        response = self.get_or_create_view(request)
        self.assertEqual(response.status_code, 201)
        self.assertNotEqual(response.data['id'], self.data.pk)

    def test_different_process_version(self):
        self.process.version = '2.0.0'
        self.process.save()

        request = self.factory.post(
            '',
            {'name': 'Data object', 'input': {'some_value': 42}, 'process': 'tmp-process'},
            format='json'
        )
        force_authenticate(request, user=self.user)

        response = self.get_or_create_view(request)
        self.assertEqual(response.status_code, 201)
        self.assertNotEqual(response.data['id'], self.data.pk)

    def test_raw_process(self):
        self.process.persistence = Process.PERSISTENCE_RAW
        self.process.save()

        request = self.factory.post(
            '',
            {'name': 'Data object', 'input': {'some_value': 42}, 'process': 'tmp-process'},
            format='json'
        )
        force_authenticate(request, user=self.user)

        response = self.get_or_create_view(request)
        self.assertEqual(response.status_code, 201)
        self.assertNotEqual(response.data['id'], self.data.pk)


@patch('resolwe.flow.models.utils.os')
class HydrateFileSizeUnitTest(TestCase):

    def setUp(self):
        super(HydrateFileSizeUnitTest, self).setUp()

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


class StorageModelTestCase(TestCase):

    def setUp(self):
        super(StorageModelTestCase, self).setUp()

        self.proc = Process.objects.create(
            name='Test process',
            contributor=self.contributor,
            output_schema=[
                {'name': 'json_field', 'type': 'basic:json:'},
            ],
        )

    def test_save_storage(self):
        """`basic:json:` fields are stored in Storage"""
        data = Data.objects.create(
            name='Test data',
            contributor=self.contributor,
            process=self.proc,
        )

        data.output = {'json_field': {'foo': 'bar'}}
        data.status = Data.STATUS_DONE
        data.save()

        self.assertEqual(Storage.objects.count(), 1)
        storage = Storage.objects.first()
        self.assertEqual(data.output['json_field'], storage.pk)

    def test_save_storage_file(self):
        """File is loaded and saved to storage"""
        data = Data.objects.create(
            name='Test data',
            contributor=self.contributor,
            process=self.proc,
        )

        data.output = {'json_field': 'json.txt'}
        data.status = Data.STATUS_DONE

        # NOTE: io.StringIO expects a Python 3 (unicode) string
        json_file = io.StringIO(six.u(json.dumps({'foo': 'bar'})))

        isfile_mock = MagicMock(return_value=True)
        open_mock = MagicMock(return_value=json_file)
        with patch.object(os.path, 'isfile', isfile_mock):
            with patch.object(builtins, 'open', open_mock):
                data.save()

        self.assertEqual(Storage.objects.count(), 1)
        storage = Storage.objects.first()
        self.assertEqual(data.output['json_field'], storage.pk)
        self.assertEqual(storage.json, {'foo': 'bar'})

    def test_delete_data(self):
        """`Storage` is deleted when `Data` object is deleted"""
        data = Data.objects.create(
            name='Test data',
            contributor=self.contributor,
            process=self.proc,
        )

        data.output = {'json_field': {'foo': 'bar'}}
        data.status = Data.STATUS_DONE
        data.save()

        self.assertEqual(Storage.objects.count(), 1)

        data.delete()
        self.assertEqual(Storage.objects.count(), 0)


class UtilsTestCase(TestCase):

    def test_render_template(self):
        process_mock = MagicMock(requirements={'expression-engine': 'jinja'})
        template = render_template(process_mock, '{{ 1 | increase }}', {})
        self.assertEqual(template, '2')

    def test_render_template_error(self):
        process_mock = MagicMock(requirements={'expression-engine': 'jinja'})
        with self.assertRaises(EvaluationError):
            render_template(process_mock, '{{ 1 | missing_increase }}', {})
