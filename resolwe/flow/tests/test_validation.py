# pylint: disable=missing-docstring
from __future__ import absolute_import, division, print_function, unicode_literals

from mock import MagicMock, patch
import unittest

from django.contrib.auth import get_user_model
from django.core.exceptions import ValidationError
from django.test import TestCase

from resolwe.flow.models import Data, DescriptorSchema, Process, Storage, validate_schema


class ValidationTest(TestCase):
    def test_validating_data_object(self):
        """Diferent validations are performed depending on status"""
        user = get_user_model().objects.create(username="test_user")
        proc = Process.objects.create(
            name='Test process',
            contributor=user,
            input_schema=[
                {'name': 'value', 'type': 'basic:integer:', 'required': True}
            ],
            output_schema=[
                {'name': 'result', 'type': 'basic:string:', 'required': True}
            ]
        )
        descriptor_schema = DescriptorSchema.objects.create(
            name='Test descriptor schema',
            contributor=user,
            schema=[
                {'name': 'description', 'type': 'basic:string:', 'required': True}
            ]
        )

        data = {
            'name': 'Test data',
            'contributor': user,
            'descriptor_schema': descriptor_schema,
            'process': proc,
        }

        with self.assertRaises(ValidationError):
            Data.objects.create(input={}, descriptor={'description': 'Universal answer'}, **data)

        with self.assertRaises(ValidationError):
            Data.objects.create(input={'value': 42}, descriptor={}, **data)

        with self.assertRaises(ValidationError):
            Data.objects.create(input={}, descriptor={}, **data)

        d = Data.objects.create(input={'value': 42}, descriptor={'description': 'Universal answer'}, **data)

        d.status = Data.STATUS_DONE
        with self.assertRaises(ValidationError):
            d.save()

        d.output = {'result': 'forty-two'}
        d.save()

    def test_referenced_storage(self):
        user = get_user_model().objects.create(username="test_user")
        proc = Process.objects.create(
            name='Test process',
            contributor=user,
            output_schema=[
                {'name': 'big_result', 'type': 'basic:json:', 'required': True}
            ]
        )

        data = {
            'name': 'Test data',
            'contributor': user,
            'process': proc,
        }

        d = Data.objects.create(**data)
        d.status = Data.STATUS_DONE

        # `Data` object with referenced non-existing `Storage`
        d.output = {'big_result': 245}
        with self.assertRaises(ValidationError):
            d.save()

        storage = Storage.objects.create(
            name="storage",
            contributor=user,
            data_id=d.pk,
            json={'value': 42}
        )
        d.output = {'big_result': storage.pk}
        d.save()

    def test_referenced_data(self):
        user = get_user_model().objects.create(username="test_user")
        proc1 = Process.objects.create(
            name='Referenced process',
            contributor=user,
            type='data:referenced:object:'
        )
        proc2 = Process.objects.create(
            name='Test process',
            contributor=user,
            input_schema=[
                {'name': 'data_object', 'type': 'data:referenced:object:'}
            ]
        )
        d = Data.objects.create(
            name='Referenced object',
            contributor=user,
            process=proc1
        )

        data = {
            'name': 'Test data',
            'contributor': user,
            'process': proc2,
            'input': {'data_object': d.pk}
        }

        Data.objects.create(**data)

        # less specific type
        proc2.input_schema = [
            {'name': 'data_object', 'type': 'data:referenced:'}
        ]
        Data.objects.create(**data)

        # wrong type
        proc2.input_schema = [
            {'name': 'data_object', 'type': 'data:wrong:type'}
        ]
        with self.assertRaises(ValidationError):
            Data.objects.create(**data)

        # non-existing `Data` object
        data['input'] = {'data_object': 631}
        with self.assertRaises(ValidationError):
            Data.objects.create(**data)

    def test_delete_input(self):
        user = get_user_model().objects.create(username="test_user")
        proc1 = Process.objects.create(
            name='Referenced process',
            contributor=user,
            type='data:referenced:object:'
        )
        proc2 = Process.objects.create(
            name='Test process',
            contributor=user,
            input_schema=[
                {'name': 'data_object', 'type': 'data:referenced:object:'}
            ]
        )
        d1 = Data.objects.create(
            name='Referenced object',
            contributor=user,
            process=proc1
        )
        d2 = Data.objects.create(
            name='Test data',
            contributor=user,
            process=proc2,
            input={'data_object': d1.pk}
        )

        d1.delete()
        d2.name = 'New name'
        d2.save()


class ValidationUnitTest(unittest.TestCase):
    def test_required(self):
        schema = [
            {'name': 'value', 'type': 'basic:integer:', 'required': True},
            {'name': 'description', 'type': 'basic:string:', 'required': False},
            {'name': 'comment', 'type': 'basic:string:'},  # implicit `required=False
        ]

        instance = {'description': 'test'}
        with self.assertRaises(ValidationError):
            validate_schema(instance, schema)

        instance = {'value': 42}
        validate_schema(instance, schema)

        instance = {'value': 42, 'description': 'test', 'comment': 'Lorem ipsum'}
        validate_schema(instance, schema)

        instance = {}
        validate_schema(instance, schema, test_required=False)

    def test_missing_in_schema(self):
        schema = [
            {'name': 'result', 'type': 'basic:integer:', 'required': False}
        ]

        instance = {'res': 42}
        with self.assertRaises(KeyError):
            validate_schema(instance, schema)

    def test_file_prefix(self):
        schema = [
            {'name': 'result', 'type': 'basic:file:'},
        ]
        instance = {'result': {'file': 'result.txt'}}

        with patch('resolwe.flow.models.os') as os_mock:
            validate_schema(instance, schema)
            self.assertEqual(os_mock.path.isfile.call_count, 0)

        # missing file
        with patch('resolwe.flow.models.os') as os_mock:
            os_mock.path.isfile = MagicMock(return_value=False)
            with self.assertRaises(ValidationError):
                validate_schema(instance, schema, path_prefix='/home/genialis/')

        with patch('resolwe.flow.models.os') as os_mock:
            os_mock.path.isfile = MagicMock(return_value=True)
            validate_schema(instance, schema, path_prefix='/home/genialis/')

        instance = {'result': {'file': 'result.txt', 'refs': ['user1.txt', 'user2.txt']}}

        with patch('resolwe.flow.models.os') as os_mock:
            os_mock.path.isfile = MagicMock(return_value=True)
            validate_schema(instance, schema, path_prefix='/home/genialis/')

        # missing second `refs` file
        with patch('resolwe.flow.models.os') as os_mock:
            os_mock.path.isfile = MagicMock(side_effect=[True, True, False])
            with self.assertRaises(ValidationError):
                validate_schema(instance, schema, path_prefix='/home/genialis/')

    def test_string_field(self):
        schema = [
            {'name': 'string', 'type': 'basic:string:'},
            {'name': 'text', 'type': 'basic:text:'},
        ]

        instance = {'string': 'Test string', 'text': 'Test text'}
        validate_schema(instance, schema)

        instance = {'string': 42, 'text': 42}
        with self.assertRaises(ValidationError):
            validate_schema(instance, schema)

    def test_boolean_field(self):
        schema = [
            {'name': 'true', 'type': 'basic:boolean:'},
            {'name': 'false', 'type': 'basic:boolean:'},
        ]

        instance = {'true': True, 'false': False}
        validate_schema(instance, schema)

        instance = {'true': 'true', 'false': 'false'}
        with self.assertRaises(ValidationError):
            validate_schema(instance, schema)

        instance = {'true': 1, 'false': 0}
        with self.assertRaises(ValidationError):
            validate_schema(instance, schema)

        instance = {'true': 'foo', 'false': 'bar'}
        with self.assertRaises(ValidationError):
            validate_schema(instance, schema)

    def test_integer_field(self):
        schema = [
            {'name': 'value', 'type': 'basic:integer:'},
        ]

        instance = {'value': 42}
        validate_schema(instance, schema)

        instance = {'value': 42.0}
        with self.assertRaises(ValidationError):
            validate_schema(instance, schema)

        instance = {'value': 'forty-two'}
        with self.assertRaises(ValidationError):
            validate_schema(instance, schema)

    def test_decimal_field(self):
        schema = [
            {'name': 'value', 'type': 'basic:decimal:'},
        ]

        instance = {'value': 42}
        validate_schema(instance, schema)

        instance = {'value': 42.0}
        validate_schema(instance, schema)

        instance = {'value': 'forty-two'}
        with self.assertRaises(ValidationError):
            validate_schema(instance, schema)

    def test_date_field(self):
        schema = [
            {'name': 'date', 'type': 'basic:date:'},
        ]

        instance = {'date': '2000-12-31'}
        validate_schema(instance, schema)

        instance = {'date': '2000/01/01'}
        with self.assertRaises(ValidationError):
            validate_schema(instance, schema)

        instance = {'date': '31 04 2000'}
        with self.assertRaises(ValidationError):
            validate_schema(instance, schema)

        instance = {'date': '21.06.2000'}
        with self.assertRaises(ValidationError):
            validate_schema(instance, schema)

        instance = {'date': '2000-1-1'}
        with self.assertRaises(ValidationError):
            validate_schema(instance, schema)

        instance = {'date': '2000 apr 8'}
        with self.assertRaises(ValidationError):
            validate_schema(instance, schema)

    def test_datetime_field(self):
        schema = [
            {'name': 'date', 'type': 'basic:datetime:'},
        ]

        instance = {'date': '2000-06-21 00:00'}
        validate_schema(instance, schema)

        instance = {'date': '2000 06 21 24:00'}
        with self.assertRaises(ValidationError):
            validate_schema(instance, schema)

        instance = {'date': '2000/06/21 2:03'}
        with self.assertRaises(ValidationError):
            validate_schema(instance, schema)

        instance = {'date': '2000-06-21 2:3'}  # XXX: Is this ok?
        validate_schema(instance, schema)

        instance = {'date': '2000-06-21'}
        with self.assertRaises(ValidationError):
            validate_schema(instance, schema)

        instance = {'date': '2000-06-21 12pm'}
        with self.assertRaises(ValidationError):
            validate_schema(instance, schema)

    def test_data_field(self):
        schema = [
            {'name': 'data_list', 'type': 'data:test:upload:'}
        ]
        instance = {
            'data_list': 1
        }

        with patch('resolwe.flow.models.Data') as data_mock:
            value_mock = MagicMock(**{
                'exists.return_value': True,
                'first.return_value': {'process__type': 'data:test:upload:'},
            })
            filter_mock = MagicMock(**{'values.return_value': value_mock})
            data_mock.objects.filter.return_value = filter_mock

            validate_schema(instance, schema)

        # subtype is OK
        with patch('resolwe.flow.models.Data') as data_mock:
            value_mock = MagicMock(**{
                'exists.return_value': True,
                'first.return_value': {'process__type': 'data:test:upload:subtype:'},
            })
            filter_mock = MagicMock(**{'values.return_value': value_mock})
            data_mock.objects.filter.return_value = filter_mock

            validate_schema(instance, schema)

        # missing `Data` object
        with patch('resolwe.flow.models.Data') as data_mock:
            value_mock = MagicMock(**{
                'exists.return_value': False,
                'first.return_value': {'process__type': 'data:test:upload:'},
            })
            filter_mock = MagicMock(**{'values.return_value': value_mock})
            data_mock.objects.filter.return_value = filter_mock

            with self.assertRaises(ValidationError):
                validate_schema(instance, schema)

        # `Data` object of wrong type
        with patch('resolwe.flow.models.Data') as data_mock:
            value_mock = MagicMock(**{
                'exists.return_value': True,
                'first.return_value': {'process__type': 'data:test:wrong:'},
            })
            filter_mock = MagicMock(**{'values.return_value': value_mock})
            data_mock.objects.filter.return_value = filter_mock

            with self.assertRaises(ValidationError):
                validate_schema(instance, schema)

    def test_file_field(self):
        schema = [
            {'name': 'result', 'type': 'basic:file:', 'validate_regex': '^.*\.txt$'},
        ]

        instance = {'result': {
            'file': 'result_file.txt'
        }}
        validate_schema(instance, schema)

        instance = {'result': {
            'file_temp': '12345',
            'file': 'result_file.txt'
        }}
        validate_schema(instance, schema)

        instance = {'result': {
            'file_temp': '12345',
            'is_remote': True,
            'file': 'result_file.txt'
        }}
        validate_schema(instance, schema)

        instance = {'result': {
            'file': 'result_file.txt',
            'refs': ['01.txt', '02.txt']
        }}
        validate_schema(instance, schema)

        # non-boolean `is_remote`
        instance = {'result': {
            'file_temp': '12345',
            'is_remote': 'ftp',
            'file': 'result_file.txt'
        }}
        with self.assertRaises(ValidationError):
            validate_schema(instance, schema)

        # missing `file`
        instance = {'result': {
            'file_temp': '12345',
        }}
        with self.assertRaises(ValidationError):
            validate_schema(instance, schema)

        # wrong file extension
        instance = {'result': {
            'file': 'result_file.tar.gz'
        }}
        with self.assertRaises(ValidationError):
            validate_schema(instance, schema)

    def test_dir_field(self):
        schema = [
            {'name': 'result', 'type': 'basic:dir:'},
        ]

        instance = {'result': {
            'dir': 'results'
        }}
        validate_schema(instance, schema)

        instance = {'result': {
            'dir': 'result',
            'refs': ['01.txt', '02.txt']
        }}
        validate_schema(instance, schema)

        # missing `dir`
        instance = {'result': {
            'refs': ['01.txt', '02.txt']
        }}
        with self.assertRaises(ValidationError):
            validate_schema(instance, schema)

    def test_url_field(self):
        schema = [
            {'name': 'webpage', 'type': 'basic:url:view:'},
        ]

        instance = {'webpage': {'url': 'http://www.genialis.com'}}
        validate_schema(instance, schema)

        instance = {'webpage': {
            'url': 'http://www.genialis.com',
            'name': 'Genialis',
            'refs': ['http://www.genialis.com/jobs']
        }}
        validate_schema(instance, schema)

        # wrong type
        schema = [
            {'name': 'webpage', 'type': 'basic:url:'},
        ]
        instance = {'webpage': {'url': 'http://www.genialis.com'}}
        with self.assertRaises(ValidationError):
            validate_schema(instance, schema)

    def test_json_field(self):
        schema = [
            {'name': 'big_dict', 'type': 'basic:json:'}
        ]

        # json not saved in `Storage`
        instance = {'big_dict': {'foo': 'bar'}}
        with self.assertRaises(ValidationError):
            validate_schema(instance, schema)

        with patch('resolwe.flow.models.Storage') as storage_mock:
            filter_mock = MagicMock()
            filter_mock.exists.return_value = True
            storage_mock.objects.filter.return_value = filter_mock

            instance = {'big_dict': 5}
            validate_schema(instance, schema)

        # non existing `Storage`
        with patch('resolwe.flow.models.Storage') as storage_mock:
            filter_mock = MagicMock()
            filter_mock.exists.return_value = False
            storage_mock.objects.filter.return_value = filter_mock

            instance = {'big_dict': 5}
            with self.assertRaises(ValidationError):
                validate_schema(instance, schema)

    def test_list_string_field(self):
        schema = [
            {'name': 'list', 'type': 'list:basic:string:'}
        ]

        instance = {'list': ['foo', 'bar']}
        validate_schema(instance, schema)

        instance = {'list': []}
        validate_schema(instance, schema)

        instance = {'list': ''}
        with self.assertRaises(ValidationError):
            validate_schema(instance, schema)

        instance = {'list': 'foo bar'}
        with self.assertRaises(ValidationError):
            validate_schema(instance, schema)

    def test_list_data_field(self):
        schema = [
            {'name': 'data_list', 'type': 'list:data:test:upload:'}
        ]
        instance = {
            'data_list': [1, 3, 4]
        }

        with patch('resolwe.flow.models.Data') as data_mock:
            value_mock = MagicMock(**{
                'exists.return_value': True,
                'first.return_value': {'process__type': 'data:test:upload:'},
            })
            filter_mock = MagicMock(**{'values.return_value': value_mock})
            data_mock.objects.filter.return_value = filter_mock

            validate_schema(instance, schema)

        # subtypes are OK
        with patch('resolwe.flow.models.Data') as data_mock:
            value_mock = MagicMock(**{
                'exists.return_value': True,
                'first.side_effect': [
                    {'process__type': 'data:test:upload:subtype1:'},
                    {'process__type': 'data:test:upload:'},
                    {'process__type': 'data:test:upload:subtype2:'},
                ],
            })
            filter_mock = MagicMock(**{'values.return_value': value_mock})
            data_mock.objects.filter.return_value = filter_mock

            validate_schema(instance, schema)

        # one object does not exist
        with patch('resolwe.flow.models.Data') as data_mock:
            value_mock = MagicMock(**{
                'exists.side_effect': [True, False, True],
                'first.return_value': {'process__type': 'data:test:upload:'},
            })
            filter_mock = MagicMock(**{'values.return_value': value_mock})
            data_mock.objects.filter.return_value = filter_mock

            with self.assertRaises(ValidationError):
                validate_schema(instance, schema)

        # one object of wrong type
        with patch('resolwe.flow.models.Data') as data_mock:
            value_mock = MagicMock(**{
                'exists.return_value': True,
                'first.side_effect': [
                    {'process__type': 'data:test:upload:'},
                    {'process__type': 'data:test:upload:'},
                    {'process__type': 'data:test:wrong:'},
                ],
            })
            filter_mock = MagicMock(**{'values.return_value': value_mock})
            data_mock.objects.filter.return_value = filter_mock

            with self.assertRaises(ValidationError):
                validate_schema(instance, schema)

    def test_list_file_field(self):
        schema = [
            {'name': 'result', 'type': 'list:basic:file:', 'validate_regex': '^.*\.txt$'},
        ]
        instance = {'result': [
            {'file': 'result01.txt'},
            {'file': 'result02.txt', 'size': 14, 'refs': ['results.tar.gz']},
        ]}
        validate_schema(instance, schema)

        # wrong extension
        instance = {'result': [
            {'file': 'result01.txt'},
            {'file': 'result02.tar.gz'},
        ]}
        with self.assertRaises(ValidationError):
            validate_schema(instance, schema)

    def test_list_dir_field(self):
        pass  # TODO

    def test_list_url_field(self):
        schema = [
            {'name': 'webpage', 'type': 'list:basic:url:view:'},
        ]

        instance = {'webpage': [
            {'url': 'http://www.genialis.com', 'refs': ['http://www.genialis.com/jobs']},
            {'url': 'http://www.dictyexpress.org'},
        ]}
        validate_schema(instance, schema)

        instance = {'webpage': {'url': 'http://www.dictyexpress.org'}}
        with self.assertRaises(ValidationError):
            validate_schema(instance, schema)

    def test_groups(self):
        schema = [
            {'name': 'test_group', 'group': [
                {'name': 'result_file', 'type': 'basic:file:', 'validate_regex': '^.*\.txt$'},
                {'name': 'description', 'type': 'basic:string:'}
            ]}
        ]

        instance = {'test_group': {
            'result_file': {'file': 'results.txt'},
            'description': 'This are results',
        }}
        validate_schema(instance, schema)

        # wrong file extension
        instance = {'test_group': {
            'result_file': {'file': 'results.tar.gz'},
            'description': 'This are results',
        }}
        with self.assertRaises(ValidationError):
            validate_schema(instance, schema)

        # wrong description type
        instance = {'test_group': {
            'result_file': {'file': 'results.tar.gz'},
            'description': 6,
        }}
        with self.assertRaises(ValidationError):
            validate_schema(instance, schema)

        # missing description
        instance = {'test_group': {
            'result_file': {'file': 'results.tar.gz'},
        }}
        with self.assertRaises(ValidationError):
            validate_schema(instance, schema)
