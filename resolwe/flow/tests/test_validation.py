# pylint: disable=missing-docstring,too-many-lines
from unittest.mock import MagicMock, patch

from django.contrib.auth import get_user_model
from django.core.exceptions import ValidationError

from resolwe.flow.models import Collection, Data, DescriptorSchema, Entity, Process
from resolwe.flow.models.utils import validate_data_object, validate_schema
from resolwe.test import TestCase
from resolwe.test.utils import create_data_location


class ValidationTest(TestCase):
    def setUp(self):
        super().setUp()

        self.user = get_user_model().objects.create(username="test_user")

    def test_validating_data_object(self):
        """Diferent validations are performed depending on status"""
        proc = Process.objects.create(
            name="Test process",
            contributor=self.user,
            input_schema=[
                {"name": "value", "type": "basic:integer:", "required": True}
            ],
            output_schema=[
                {"name": "result", "type": "basic:string:", "required": True}
            ],
        )

        data = {
            "name": "Test data",
            "contributor": self.user,
            "process": proc,
        }

        with self.assertRaisesRegex(ValidationError, '"value" not given'):
            validate_data_object(Data.objects.create(input={}, **data))

        with self.assertRaisesRegex(ValidationError, "Required fields .* not given"):
            validate_data_object(Data.objects.create(input={}, **data))

        d = Data.objects.create(input={"value": 42}, **data)

        d.status = Data.STATUS_DONE
        with self.assertRaisesRegex(ValidationError, '"result" not given'):
            d.save()
            validate_data_object(d)

        d.output = {"result": "forty-two"}
        d.save()
        validate_data_object(d)

    def test_validate_data_descriptor(self):
        proc = Process.objects.create(name="Test process", contributor=self.user)
        descriptor_schema = DescriptorSchema.objects.create(
            name="Test descriptor schema",
            contributor=self.user,
            schema=[{"name": "description", "type": "basic:string:", "required": True}],
        )

        data = Data.objects.create(
            name="Test descriptor",
            contributor=self.user,
            process=proc,
            descriptor={},
            descriptor_schema=descriptor_schema,
        )
        self.assertEqual(data.descriptor_dirty, True)

        data.descriptor = {"description": "some value"}
        data.save()
        self.assertEqual(data.descriptor_dirty, False)

        data.descriptor = {}
        data.save()
        self.assertEqual(data.descriptor_dirty, True)

        data.descriptor = {"description": 42}
        with self.assertRaisesRegex(ValidationError, "is not valid"):
            data.save()

    def test_validate_collection_descriptor(self):
        descriptor_schema = DescriptorSchema.objects.create(
            name="Test descriptor schema",
            contributor=self.user,
            schema=[{"name": "description", "type": "basic:string:", "required": True}],
        )

        collection = Collection.objects.create(
            name="Test descriptor",
            contributor=self.user,
            descriptor_schema=descriptor_schema,
        )
        self.assertEqual(collection.descriptor_dirty, True)

        collection.descriptor = {"description": "some value"}
        collection.save()
        self.assertEqual(collection.descriptor_dirty, False)

        collection.descriptor = {}
        collection.save()
        self.assertEqual(collection.descriptor_dirty, True)

        collection.descriptor = {"description": 42}
        with self.assertRaisesRegex(ValidationError, "is not valid"):
            collection.save()

    def test_validate_entity_descriptor(self):
        descriptor_schema = DescriptorSchema.objects.create(
            name="Test descriptor schema",
            contributor=self.user,
            schema=[{"name": "description", "type": "basic:string:", "required": True}],
        )

        entity = Entity.objects.create(
            name="Test descriptor",
            contributor=self.user,
            descriptor_schema=descriptor_schema,
        )
        self.assertEqual(entity.descriptor_dirty, True)

        entity.descriptor = {"description": "some value"}
        entity.save()
        self.assertEqual(entity.descriptor_dirty, False)

        entity.descriptor = {}
        entity.save()
        self.assertEqual(entity.descriptor_dirty, True)

        entity.descriptor = {"description": 42}
        with self.assertRaisesRegex(ValidationError, "is not valid"):
            entity.save()

    def test_referenced_storage(self):
        proc = Process.objects.create(
            name="Test process",
            contributor=self.user,
            output_schema=[
                {"name": "big_result", "type": "basic:json:", "required": True}
            ],
        )

        data = {
            "name": "Test data",
            "contributor": self.user,
            "process": proc,
        }

        d = Data.objects.create(**data)
        d.status = Data.STATUS_DONE

        # `Data` object with referenced non-existing `Storage`
        d.output = {"big_result": 245}
        with self.assertRaisesRegex(ValidationError, "`Storage` object does not exist"):
            d.save()
            validate_data_object(d)

        d.storages.create(name="storage", contributor=self.user, json={"value": 42})
        self.assertEqual(d.storages.count(), 1)
        d.output = {"big_result": d.storages.first().id}
        d.save()
        validate_data_object(d)

    def test_referenced_data(self):
        proc1 = Process.objects.create(
            name="Referenced process",
            contributor=self.user,
            type="data:referenced:object:",
        )
        proc2 = Process.objects.create(
            name="Test process",
            contributor=self.user,
            input_schema=[{"name": "data_object", "type": "data:referenced:object:"}],
        )
        d = Data.objects.create(
            name="Referenced object", contributor=self.user, process=proc1
        )
        validate_data_object(d)

        data = {
            "name": "Test data",
            "contributor": self.user,
            "process": proc2,
            "input": {"data_object": d.pk},
        }

        validate_data_object(Data.objects.create(**data))

        # less specific type
        proc2.input_schema = [{"name": "data_object", "type": "data:referenced:"}]
        validate_data_object(Data.objects.create(**data))

        # wrong type
        proc2.input_schema = [{"name": "data_object", "type": "data:wrong:type:"}]
        with self.assertRaisesRegex(
            ValidationError, "Data object of type .* is required"
        ):
            validate_data_object(Data.objects.create(**data))

        # non-existing `Data` object
        data["input"] = {"data_object": 631}
        with self.assertRaisesRegex(ValidationError, "`Data` object does not exist"):
            validate_data_object(Data.objects.create(**data))

    def test_delete_input(self):
        proc1 = Process.objects.create(
            name="Referenced process",
            contributor=self.user,
            type="data:referenced:object:",
        )
        proc2 = Process.objects.create(
            name="Test process",
            contributor=self.user,
            input_schema=[{"name": "data_object", "type": "data:referenced:object:"}],
        )
        data1 = Data.objects.create(
            name="Referenced object", contributor=self.user, process=proc1
        )
        data2 = Data.objects.create(
            name="Test data",
            contributor=self.user,
            process=proc2,
            input={"data_object": data1.pk},
        )
        validate_data_object(data1)
        validate_data_object(data2)

        data1.delete()
        data2.name = "New name"
        data2.save()
        validate_data_object(data2, skip_missing_data=True)


class ValidationUnitTest(TestCase):
    def test_required(self):
        schema = [
            {"name": "value", "type": "basic:integer:", "required": True},
            {
                "name": "description",
                "type": "basic:string:",
            },  # implicit `required=True`
            {"name": "comment", "type": "basic:string:", "required": False},
        ]

        instance = {"description": "test"}
        with self.assertRaisesRegex(ValidationError, '"value" not given.'):
            validate_schema(instance, schema)

        instance = {"value": 42}
        with self.assertRaisesRegex(ValidationError, '"description" not given.'):
            validate_schema(instance, schema)

        instance = {"value": 42, "description": "universal answer"}
        validate_schema(instance, schema)

        instance = {"value": 42, "description": "universal answer", "comment": None}
        validate_schema(instance, schema)

        instance = {"value": 42, "description": "test", "comment": "Lorem ipsum"}
        validate_schema(instance, schema)

        instance = {}
        validate_schema(instance, schema, test_required=False)

    def test_choices(self):
        schema = [
            {
                "name": "value",
                "type": "basic:integer:",
                "choices": [{"value": 7, "label": "7"}, {"value": 13, "label": "13"}],
            },
        ]

        instance = {"value": 7}
        validate_schema(instance, schema)

        instance = {"value": 8}
        error_msg = "Value of field 'value' must match one of predefined choices. Current value: 8"
        with self.assertRaisesRegex(ValidationError, error_msg):
            validate_schema(instance, schema)

        schema = [
            {
                "name": "value",
                "type": "basic:integer:",
                "choices": [{"value": 7, "label": "7"}, {"value": 13, "label": "13"}],
                "allow_custom_choice": True,
            },
        ]

        instance = {"value": 7}
        validate_schema(instance, schema)

        instance = {"value": 8}
        validate_schema(instance, schema)

    def test_range(self):
        schema = [
            {"name": "value", "type": "basic:integer:", "range": [0, 5],},
        ]

        instance = {"value": 5}
        validate_schema(instance, schema)
        instance = {"value": 0}
        validate_schema(instance, schema)
        instance = {"value": 3}
        validate_schema(instance, schema)

        instance = {"value": 7}
        error_msg = (
            "Value of field 'value' is out of range. It should be between 0 and 5."
        )
        with self.assertRaisesRegex(ValidationError, error_msg):
            validate_schema(instance, schema)

        schema = [
            {"name": "value", "type": "basic:decimal:", "range": [0, 5],},
        ]

        instance = {"value": 7}
        error_msg = (
            "Value of field 'value' is out of range. It should be between 0 and 5."
        )
        with self.assertRaisesRegex(ValidationError, error_msg):
            validate_schema(instance, schema)

        schema = [
            {"name": "value", "type": "list:basic:integer:", "range": [0, 5],},
        ]

        instance = {"value": [0, 3, 5]}
        validate_schema(instance, schema)

        instance = {"value": [0, 3, 7]}
        error_msg = (
            "Value of field 'value' is out of range. It should be between 0 and 5."
        )
        with self.assertRaisesRegex(ValidationError, error_msg):
            validate_schema(instance, schema)

        schema = [
            {"name": "value", "type": "list:basic:decimal:", "range": [0, 5],},
        ]

        instance = {"value": [0, 3, 7]}
        error_msg = (
            "Value of field 'value' is out of range. It should be between 0 and 5."
        )
        with self.assertRaisesRegex(ValidationError, error_msg):
            validate_schema(instance, schema)

    def test_missing_in_schema(self):
        schema = [{"name": "result", "type": "basic:integer:", "required": False}]

        instance = {"res": 42}
        with self.assertRaisesRegex(ValidationError, r"\(res\) missing in schema"):
            validate_schema(instance, schema)

    def test_file_prefix(self):
        schema = [
            {"name": "result", "type": "basic:file:"},
        ]
        path_instance = MagicMock()
        instance = {"result": {"file": "result.txt"}}

        data_location = create_data_location(subpath="1")

        with patch("resolwe.flow.models.utils.Path") as path_mock:
            path_mock.return_value = path_instance
            validate_schema(instance, schema)
            path_instance.exists = MagicMock(return_value=True)
            self.assertEqual(path_instance.is_file.call_count, 0)

        # missing file
        with patch("resolwe.flow.models.utils.Path") as path_mock:
            path_mock.return_value = path_instance
            path_instance.exists = MagicMock(return_value=False)
            path_instance.__truediv__ = MagicMock(return_value=path_instance)
            with self.assertRaisesRegex(
                ValidationError, "Referenced path .* does not exist."
            ):
                validate_schema(instance, schema, data_location=data_location)
            self.assertEqual(path_instance.exists.call_count, 1)

        # File is not a normal file
        with patch("resolwe.flow.models.utils.Path") as path_mock:
            path_mock.return_value = path_instance
            path_instance.exists = MagicMock(return_value=True)
            path_instance.is_file = MagicMock(return_value=False)
            path_instance.__truediv__ = MagicMock(return_value=path_instance)
            with self.assertRaisesRegex(
                ValidationError, "Referenced path .* is not a file."
            ):
                validate_schema(instance, schema, data_location=data_location)
            self.assertEqual(path_instance.is_file.call_count, 1)

        with patch("resolwe.flow.models.utils.Path") as path_mock:
            path_mock.return_value = path_instance
            path_instance.is_file = MagicMock(return_value=True)
            path_instance.__truediv__ = MagicMock(return_value=path_instance)
            validate_schema(instance, schema, data_location=data_location)
            self.assertEqual(path_instance.is_file.call_count, 1)

        instance = {
            "result": {"file": "result.txt", "refs": ["user1.txt", "user2.txt"]}
        }

        with patch("resolwe.flow.models.utils.Path") as path_mock:
            path_mock.return_value = path_instance
            path_instance.is_file = MagicMock(return_value=True)
            path_instance.__truediv__ = MagicMock(return_value=path_instance)
            validate_schema(instance, schema, data_location=data_location)
            self.assertEqual(path_instance.is_file.call_count, 3)

        # missing second `refs` file
        with patch("resolwe.flow.models.utils.Path") as path_mock:
            path_mock.return_value = path_instance
            path_instance.is_file = MagicMock(side_effect=[True, True, False])
            path_instance.is_dir = MagicMock(return_value=False)
            path_instance.__truediv__ = MagicMock(return_value=path_instance)
            message = "Path referenced in `refs` .* is neither a file or directory."
            with self.assertRaisesRegex(ValidationError, message):
                validate_schema(instance, schema, data_location=data_location)
            self.assertEqual(path_instance.is_file.call_count, 3)
            self.assertEqual(path_instance.is_dir.call_count, 1)

    def test_dir_prefix(self):
        schema = [
            {"name": "result", "type": "basic:dir:"},
        ]
        path_instance = MagicMock()
        instance = {"result": {"dir": "results"}}
        data_location = create_data_location(subpath="1")

        # dir validation is not called if `data_location` is not given
        with patch("resolwe.flow.models.utils.Path") as path_mock:
            path_mock.return_value = path_instance
            validate_schema(instance, schema)
            self.assertEqual(path_instance.is_file.call_count, 0)
        path_instance.reset_mock()

        with patch("resolwe.flow.models.utils.Path") as path_mock:
            path_mock.return_value = path_instance
            path_instance.exists = lambda: True
            path_instance.__truediv__ = MagicMock(return_value=path_instance)
            path_instance.is_dir = MagicMock(return_value=True)
            validate_schema(instance, schema, data_location=data_location)
            self.assertEqual(path_instance.is_dir.call_count, 1)
        path_instance.reset_mock()

        # missing path
        with patch("resolwe.flow.models.utils.Path") as path_mock:
            path_mock.return_value = path_instance
            path_instance.exists = MagicMock(return_value=False)
            with self.assertRaisesRegex(
                ValidationError, "Referenced path .* does not exist."
            ):
                validate_schema(instance, schema, data_location=data_location)
            self.assertEqual(path_instance.exists.call_count, 1)
        path_instance.reset_mock()

        # not a dir
        with patch("resolwe.flow.models.utils.Path") as path_mock:
            path_mock.return_value = path_instance
            path_instance.exists = MagicMock(return_value=True)
            path_instance.is_dir = MagicMock(return_value=False)
            with self.assertRaisesRegex(
                ValidationError, "Referenced path .* is not a directory"
            ):
                validate_schema(instance, schema, data_location=data_location)
            self.assertEqual(path_instance.is_dir.call_count, 1)
        path_instance.reset_mock()

        instance = {"result": {"dir": "results", "refs": ["file01.txt", "file02.txt"]}}

        with patch("resolwe.flow.models.utils.Path") as path_mock:
            path_mock.return_value = path_instance
            path_instance.is_file = MagicMock(return_value=True)
            path_instance.is_dir = MagicMock(return_value=True)
            path_instance.__truediv__ = MagicMock(return_value=path_instance)

            validate_schema(instance, schema, data_location=data_location)
            self.assertEqual(path_instance.is_dir.call_count, 1)
            self.assertEqual(path_instance.is_file.call_count, 2)
        path_instance.reset_mock()

        # missing second `refs` file
        with patch("resolwe.flow.models.utils.Path") as path_mock:
            path_mock.return_value = path_instance
            path_instance.is_file = MagicMock(side_effect=[True, False])
            path_instance.is_dir = MagicMock(side_effect=[True, False])
            path_instance.__truediv__ = MagicMock(return_value=path_instance)
            message = "Path referenced in `refs` .* is neither a file or directory"
            with self.assertRaisesRegex(ValidationError, message):
                validate_schema(instance, schema, data_location=data_location)
            self.assertEqual(path_instance.is_dir.call_count, 2)
            self.assertEqual(path_instance.is_file.call_count, 2)

    def test_string_field(self):
        schema = [
            {"name": "string", "type": "basic:string:"},
            {"name": "text", "type": "basic:text:"},
        ]

        instance = {"string": "Test string", "text": "Test text"}
        validate_schema(instance, schema)

        instance = {"string": 42, "text": 42}
        with self.assertRaisesRegex(ValidationError, "is not valid"):
            validate_schema(instance, schema)

    def test_boolean_field(self):
        schema = [
            {"name": "true", "type": "basic:boolean:"},
            {"name": "false", "type": "basic:boolean:"},
        ]

        instance = {"true": True, "false": False}
        validate_schema(instance, schema)

        instance = {"true": "true", "false": "false"}
        with self.assertRaisesRegex(ValidationError, "is not valid"):
            validate_schema(instance, schema)

        instance = {"true": 1, "false": 0}
        with self.assertRaisesRegex(ValidationError, "is not valid"):
            validate_schema(instance, schema)

        instance = {"true": "foo", "false": "bar"}
        with self.assertRaisesRegex(ValidationError, "is not valid"):
            validate_schema(instance, schema)

    def test_integer_field(self):
        schema = [
            {"name": "value", "type": "basic:integer:"},
        ]

        instance = {"value": 42}
        validate_schema(instance, schema)

        instance = {"value": 42.0}
        with self.assertRaisesRegex(ValidationError, "is not valid"):
            validate_schema(instance, schema)

        instance = {"value": "forty-two"}
        with self.assertRaisesRegex(ValidationError, "is not valid"):
            validate_schema(instance, schema)

    def test_decimal_field(self):
        schema = [
            {"name": "value", "type": "basic:decimal:"},
        ]

        instance = {"value": 42}
        validate_schema(instance, schema)

        instance = {"value": 42.0}
        validate_schema(instance, schema)

        instance = {"value": "forty-two"}
        with self.assertRaisesRegex(ValidationError, "is not valid"):
            validate_schema(instance, schema)

    def test_date_field(self):
        schema = [
            {"name": "date", "type": "basic:date:"},
        ]

        instance = {"date": "2000-12-31"}
        validate_schema(instance, schema)

        instance = {"date": "2000/01/01"}
        with self.assertRaisesRegex(ValidationError, "is not valid"):
            validate_schema(instance, schema)

        instance = {"date": "31 04 2000"}
        with self.assertRaisesRegex(ValidationError, "is not valid"):
            validate_schema(instance, schema)

        instance = {"date": "21.06.2000"}
        with self.assertRaisesRegex(ValidationError, "is not valid"):
            validate_schema(instance, schema)

        instance = {"date": "2000-1-1"}
        with self.assertRaisesRegex(ValidationError, "is not valid"):
            validate_schema(instance, schema)

        instance = {"date": "2000 apr 8"}
        with self.assertRaisesRegex(ValidationError, "is not valid"):
            validate_schema(instance, schema)

    def test_datetime_field(self):
        schema = [
            {"name": "date", "type": "basic:datetime:"},
        ]

        instance = {"date": "2000-06-21 00:00"}
        validate_schema(instance, schema)

        instance = {"date": "2000 06 21 24:00"}
        with self.assertRaises(ValidationError):
            validate_schema(instance, schema)

        instance = {"date": "2000/06/21 2:03"}
        with self.assertRaisesRegex(ValidationError, "is not valid"):
            validate_schema(instance, schema)

        instance = {"date": "2000-06-21 2:3"}  # XXX: Is this ok?
        validate_schema(instance, schema)

        instance = {"date": "2000-06-21"}
        with self.assertRaisesRegex(ValidationError, "is not valid"):
            validate_schema(instance, schema)

        instance = {"date": "2000-06-21 12pm"}
        with self.assertRaisesRegex(ValidationError, "is not valid"):
            validate_schema(instance, schema)

    def test_data_field(self):
        schema = [{"name": "data_list", "type": "data:test:upload:"}]
        instance = {"data_list": 1}

        with patch("resolwe.flow.models.data.Data") as data_mock:
            value_mock = MagicMock(
                **{
                    "exists.return_value": True,
                    "first.return_value": {"process__type": "data:test:upload:"},
                }
            )
            filter_mock = MagicMock(**{"values.return_value": value_mock})
            data_mock.objects.filter.return_value = filter_mock

            validate_schema(instance, schema)

            self.assertEqual(value_mock.exists.call_count, 1)
            self.assertEqual(value_mock.first.call_count, 1)

        # subtype is OK
        with patch("resolwe.flow.models.data.Data") as data_mock:
            value_mock = MagicMock(
                **{
                    "exists.return_value": True,
                    "first.return_value": {
                        "process__type": "data:test:upload:subtype:"
                    },
                }
            )
            filter_mock = MagicMock(**{"values.return_value": value_mock})
            data_mock.objects.filter.return_value = filter_mock

            validate_schema(instance, schema)

            self.assertEqual(value_mock.exists.call_count, 1)
            self.assertEqual(value_mock.first.call_count, 1)

        # missing `Data` object
        with patch("resolwe.flow.models.data.Data") as data_mock:
            value_mock = MagicMock(
                **{
                    "exists.return_value": False,
                    "first.return_value": {"process__type": "data:test:upload:"},
                }
            )
            filter_mock = MagicMock(**{"values.return_value": value_mock})
            data_mock.objects.filter.return_value = filter_mock

            with self.assertRaisesRegex(
                ValidationError, "`Data` object does not exist"
            ):
                validate_schema(instance, schema)

            self.assertEqual(value_mock.exists.call_count, 1)
            self.assertEqual(value_mock.first.call_count, 0)

            validate_schema(instance, schema, skip_missing_data=True)

        # `Data` object of wrong type
        with patch("resolwe.flow.models.data.Data") as data_mock:
            value_mock = MagicMock(
                **{
                    "exists.return_value": True,
                    "first.return_value": {"process__type": "data:test:wrong:"},
                }
            )
            filter_mock = MagicMock(**{"values.return_value": value_mock})
            data_mock.objects.filter.return_value = filter_mock

            with self.assertRaisesRegex(
                ValidationError, "Data object of type .* is required"
            ):
                validate_schema(instance, schema)

            self.assertEqual(value_mock.exists.call_count, 1)
            self.assertEqual(value_mock.first.call_count, 1)

        # data `id` shouldn't be string
        instance = {"data_list": "1"}

        with self.assertRaisesRegex(ValidationError, "is not valid"):
            validate_schema(instance, schema)

    def test_file_field(self):
        schema = [
            {"name": "result", "type": "basic:file:", "validate_regex": r"^.*\.txt$"},
        ]

        instance = {"result": {"file": "result_file.txt", "size": 13}}
        validate_schema(instance, schema)

        instance = {"result": {"file_temp": "12345", "file": "result_file.txt",}}
        validate_schema(instance, schema)

        instance = {
            "result": {
                "file_temp": "12345",
                "is_remote": True,
                "file": "result_file.txt",
            }
        }
        validate_schema(instance, schema)

        instance = {
            "result": {"file": "result_file.txt", "refs": ["01.txt", "02.txt"],}
        }
        validate_schema(instance, schema)

        # non-boolean `is_remote`
        instance = {
            "result": {
                "file_temp": "12345",
                "is_remote": "ftp",
                "file": "result_file.txt",
            }
        }
        with self.assertRaisesRegex(ValidationError, "is not valid"):
            validate_schema(instance, schema)

        # missing `file`
        instance = {"result": {"file_temp": "12345",}}
        with self.assertRaisesRegex(ValidationError, "is not valid"):
            validate_schema(instance, schema)

        # wrong file extension
        instance = {"result": {"file": "result_file.tar.gz",}}
        with self.assertRaisesRegex(
            ValidationError, "File name .* does not match regex"
        ):
            validate_schema(instance, schema)

    def test_html_file_field(self):
        schema = [
            {"name": "html_result", "type": "basic:file:html:"},
        ]

        instance = {
            "html_result": {"file": "index.htmls", "refs": ["some.js", "some.css"]}
        }
        validate_schema(instance, schema)

    def test_dir_field(self):
        schema = [
            {"name": "result", "type": "basic:dir:"},
        ]

        instance = {"result": {"dir": "results", "size": 32156}}
        validate_schema(instance, schema)

        instance = {"result": {"dir": "result", "refs": ["01.txt", "02.txt"]}}
        validate_schema(instance, schema)

        # missing `dir`
        instance = {"result": {"refs": ["01.txt", "02.txt"],}}
        with self.assertRaisesRegex(ValidationError, "is not valid"):
            validate_schema(instance, schema)

    def test_url_field(self):
        schema = [
            {"name": "url_view", "type": "basic:url:view:", "required": False},
            {"name": "url_download", "type": "basic:url:download:", "required": False},
            {"name": "url_link", "type": "basic:url:link:", "required": False},
        ]

        instance = {
            "url_view": {"url": "http://www.genialis.com"},
            "url_download": {"url": "http://www.genialis.com"},
            "url_link": {"url": "http://www.genialis.com"},
        }
        validate_schema(instance, schema)

        instance = {
            "url_view": {
                "url": "http://www.genialis.com",
                "name": "Genialis",
                "refs": ["http://www.genialis.com/jobs"],
            }
        }
        validate_schema(instance, schema)

        # wrong type
        schema = [
            {"name": "webpage", "type": "basic:url:"},
        ]
        instance = {"webpage": {"url": "http://www.genialis.com"}}
        with self.assertRaisesRegex(ValidationError, "is not valid"):
            validate_schema(instance, schema)

    def test_json_field(self):
        schema = [{"name": "big_dict", "type": "basic:json:"}]

        # json not saved in `Storage`
        instance = {"big_dict": {"foo": "bar"}}
        with self.assertRaisesRegex(ValidationError, "is not valid"):
            validate_schema(instance, schema)

        with patch("resolwe.flow.models.storage.Storage") as storage_mock:
            filter_mock = MagicMock()
            filter_mock.exists.return_value = True
            storage_mock.objects.filter.return_value = filter_mock

            instance = {"big_dict": 5}
            validate_schema(instance, schema)

            self.assertEqual(filter_mock.exists.call_count, 1)

        # non existing `Storage`
        with patch("resolwe.flow.models.storage.Storage") as storage_mock:
            filter_mock = MagicMock()
            filter_mock.exists.return_value = False
            storage_mock.objects.filter.return_value = filter_mock

            instance = {"big_dict": 5}
            with self.assertRaisesRegex(
                ValidationError, "`Storage` object does not exist"
            ):
                validate_schema(instance, schema)

            self.assertEqual(filter_mock.exists.call_count, 1)

    def test_list_string_field(self):
        schema = [{"name": "list", "type": "list:basic:string:"}]

        instance = {"list": ["foo", "bar"]}
        validate_schema(instance, schema)

        instance = {"list": []}
        validate_schema(instance, schema)

        instance = {"list": ""}
        with self.assertRaisesRegex(ValidationError, "is not valid"):
            validate_schema(instance, schema)

        instance = {"list": "foo bar"}
        with self.assertRaisesRegex(ValidationError, "is not valid"):
            validate_schema(instance, schema)

    def test_list_integer_field(self):
        schema = [
            {"name": "value", "type": "list:basic:integer:"},
        ]

        instance = {"value": [42, 43]}
        validate_schema(instance, schema)

        instance = {"value": 42}
        with self.assertRaisesRegex(ValidationError, "is not valid"):
            validate_schema(instance, schema)

        instance = {"value": [42, 43.0]}
        with self.assertRaisesRegex(ValidationError, "is not valid"):
            validate_schema(instance, schema)

        instance = {"value": [42, "43"]}
        with self.assertRaisesRegex(ValidationError, "is not valid"):
            validate_schema(instance, schema)

    def test_list_data_field(self):
        schema = [{"name": "data_list", "type": "list:data:test:upload:"}]
        instance = {"data_list": [1, 3, 4]}

        with patch("resolwe.flow.models.data.Data") as data_mock:
            value_mock = MagicMock(
                **{
                    "exists.return_value": True,
                    "first.return_value": {"process__type": "data:test:upload:"},
                }
            )
            filter_mock = MagicMock(**{"values.return_value": value_mock})
            data_mock.objects.filter.return_value = filter_mock

            validate_schema(instance, schema)

            self.assertEqual(value_mock.exists.call_count, 3)
            self.assertEqual(value_mock.first.call_count, 3)

        # subtypes are OK
        with patch("resolwe.flow.models.data.Data") as data_mock:
            value_mock = MagicMock(
                **{
                    "exists.return_value": True,
                    "first.side_effect": [
                        {"process__type": "data:test:upload:subtype1:"},
                        {"process__type": "data:test:upload:"},
                        {"process__type": "data:test:upload:subtype2:"},
                    ],
                }
            )
            filter_mock = MagicMock(**{"values.return_value": value_mock})
            data_mock.objects.filter.return_value = filter_mock

            validate_schema(instance, schema)

            self.assertEqual(value_mock.exists.call_count, 3)
            self.assertEqual(value_mock.first.call_count, 3)

        # one object does not exist
        with patch("resolwe.flow.models.data.Data") as data_mock:
            value_mock = MagicMock(
                **{
                    "exists.side_effect": [True, False, True],
                    "first.return_value": {"process__type": "data:test:upload:"},
                }
            )
            filter_mock = MagicMock(**{"values.return_value": value_mock})
            data_mock.objects.filter.return_value = filter_mock

            with self.assertRaisesRegex(
                ValidationError, "`Data` object does not exist"
            ):
                validate_schema(instance, schema)

            self.assertEqual(value_mock.exists.call_count, 2)
            self.assertEqual(value_mock.first.call_count, 1)

        # one object of wrong type
        with patch("resolwe.flow.models.data.Data") as data_mock:
            value_mock = MagicMock(
                **{
                    "exists.return_value": True,
                    "first.side_effect": [
                        {"process__type": "data:test:upload:"},
                        {"process__type": "data:test:upload:"},
                        {"process__type": "data:test:wrong:"},
                    ],
                }
            )
            filter_mock = MagicMock(**{"values.return_value": value_mock})
            data_mock.objects.filter.return_value = filter_mock

            with self.assertRaisesRegex(
                ValidationError, "Data object of type .* is required"
            ):
                validate_schema(instance, schema)

            self.assertEqual(value_mock.exists.call_count, 3)
            self.assertEqual(value_mock.first.call_count, 3)

    def test_list_file_field(self):
        schema = [
            {
                "name": "result",
                "type": "list:basic:file:",
                "validate_regex": r"^.*\.txt$",
            },
        ]
        instance = {
            "result": [
                {"file": "result01.txt"},
                {"file": "result02.txt", "size": 14, "refs": ["results.tar.gz"]},
            ]
        }
        validate_schema(instance, schema)

        # wrong extension
        instance = {"result": [{"file": "result01.txt"}, {"file": "result02.tar.gz"},]}
        with self.assertRaisesRegex(
            ValidationError, "File name .* does not match regex"
        ):
            validate_schema(instance, schema)

    def test_list_dir_field(self):
        schema = [
            {"name": "result", "type": "list:basic:dir:"},
        ]

        instance = {
            "result": [
                {
                    "dir": "results01",
                    "size": 32156,
                    "refs": ["result01.txt", "result02.txt"],
                },
                {"dir": "results02"},
            ]
        }
        validate_schema(instance, schema)

        # missing `dir`
        instance = {
            "result": [{"size": 32156, "refs": ["result01.txt", "result02.txt"]},]
        }
        with self.assertRaisesRegex(ValidationError, "is not valid"):
            validate_schema(instance, schema)

    def test_list_url_field(self):
        schema = [
            {"name": "webpage", "type": "list:basic:url:view:"},
        ]

        instance = {
            "webpage": [
                {
                    "url": "http://www.genialis.com",
                    "refs": ["http://www.genialis.com/jobs"],
                },
                {"url": "http://www.dictyexpress.org"},
            ]
        }
        validate_schema(instance, schema)

        instance = {"webpage": {"url": "http://www.dictyexpress.org"}}
        with self.assertRaisesRegex(ValidationError, "is not valid"):
            validate_schema(instance, schema)

    def test_groups(self):
        schema = [
            {
                "name": "test_group",
                "group": [
                    {
                        "name": "result_file",
                        "type": "basic:file:",
                        "validate_regex": r"^.*\.txt$",
                    },
                    {"name": "description", "type": "basic:string:", "required": True},
                ],
            }
        ]

        instance = {
            "test_group": {
                "result_file": {"file": "results.txt"},
                "description": "This are results",
            }
        }
        validate_schema(instance, schema)

        # wrong file extension
        instance = {
            "test_group": {
                "result_file": {"file": "results.tar.gz"},
                "description": "This are results",
            }
        }
        with self.assertRaisesRegex(
            ValidationError, "File name .* does not match regex"
        ):
            validate_schema(instance, schema)

        # wrong description type
        instance = {
            "test_group": {"result_file": {"file": "results.txt"}, "description": 6,}
        }
        with self.assertRaisesRegex(ValidationError, "is not valid"):
            validate_schema(instance, schema)

        # missing description
        instance = {"test_group": {"result_file": {"file": "results.txt"},}}
        with self.assertRaisesRegex(ValidationError, '"description" not given'):
            validate_schema(instance, schema)
