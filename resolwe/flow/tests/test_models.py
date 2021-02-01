# pylint: disable=missing-docstring,too-many-lines
import os
import shutil
from datetime import timedelta
from unittest.mock import MagicMock, PropertyMock, patch

from django.contrib.auth import get_user_model
from django.core.exceptions import ValidationError
from django.db import transaction
from django.utils.timezone import now

from guardian.shortcuts import assign_perm, get_perms, remove_perm
from rest_framework.test import APIRequestFactory, APITestCase, force_authenticate

from resolwe.flow.expression_engines import EvaluationError
from resolwe.flow.models import (
    Collection,
    DataMigrationHistory,
    DescriptorSchema,
    Entity,
    Process,
    Storage,
)
from resolwe.flow.models.data import Data, DataDependency, hydrate_size, render_template
from resolwe.flow.models.utils import hydrate_input_references, referenced_files
from resolwe.flow.views import DataViewSet
from resolwe.test import TestCase, TransactionTestCase
from resolwe.test.utils import create_data_location, save_storage


class DataModelNameTest(TransactionTestCase):
    def test_name(self):
        process = Process.objects.create(
            slug="test-first",
            type="data:test:first:",
            contributor=self.contributor,
            data_name="Process based data name",
            output_schema=[
                {
                    "name": "stat",
                    "type": "basic:string:",
                    "required": False,
                }
            ],
            run={"language": "bash", "program": "echo test"},
        )

        data = Data.objects.create(contributor=self.contributor, process=process)
        data.refresh_from_db()

        self.assertEqual(data.name, "Process based data name")
        self.assertFalse(data.named_by_user)

        data.name = "Name changed by user"
        data.save()
        data.refresh_from_db()

        self.assertEqual(data.name, "Name changed by user")
        self.assertTrue(data.named_by_user)

        data = Data.objects.create(
            name="Explicit data name", contributor=self.contributor, process=process
        )
        data.refresh_from_db()

        self.assertEqual(data.name, "Explicit data name")
        self.assertTrue(data.named_by_user)

        process = Process.objects.create(
            slug="test-second",
            type="test:second",
            contributor=self.contributor,
            requirements={"expression-engine": "jinja"},
            data_name="Process based data name, value: {{src.stat}}",
            input_schema=[
                {
                    "name": "src",
                    "type": "data:test:first:",
                    "required": False,
                }
            ],
            run={"language": "bash", "program": "echo test"},
        )

        with transaction.atomic():
            second = Data.objects.create(
                contributor=self.contributor, process=process, input={"src": data.id}
            )

            data.output = {"stat": "42"}
            data.status = "OK"
            data.save()

            self.assertEqual(second.name, "Process based data name, value: ")
            self.assertFalse(second.named_by_user)

        second = Data.objects.get(id=second.id)

        self.assertEqual(second.name, "Process based data name, value: 42")
        self.assertFalse(second.named_by_user)

        with transaction.atomic():
            data.output = {}
            data.status = "RE"
            data.save()

            second = Data.objects.create(
                contributor=self.contributor, process=process, input={"src": data.id}
            )

            second.name = "User' data name"
            second.save()

            data.output = {"stat": "42"}
            data.status = "OK"
            data.save()

            self.assertEqual(second.name, "User' data name")
            self.assertTrue(second.named_by_user)

        second = Data.objects.get(id=second.id)

        self.assertEqual(second.name, "User' data name")
        self.assertTrue(second.named_by_user)


class DataModelTest(TestCase):
    def test_delete_chunked(self):
        process = Process.objects.create(contributor=self.contributor)
        Data.objects.bulk_create(
            [
                Data(name=str(i), contributor=self.contributor, process=process, size=0)
                for i in range(25)
            ]
        )

        Data.objects.all().delete_chunked(chunk_size=10)
        self.assertFalse(Data.objects.exists())

    def test_trim_name(self):
        process = Process.objects.create(
            contributor=self.contributor, data_name="test" * 50
        )
        data = Data.objects.create(contributor=self.contributor, process=process)

        self.assertEqual(len(data.name), 100)
        self.assertEqual(data.name[-3:], "...")

    def test_hydrate_file_size(self):
        """Hydrate file size.

        NOTE: It is no longer called every time the data object is saved os it
        needs to be called explicitly. The sizes are calculated and  reported
        by the communication container when object processing is done.
        """
        proc = Process.objects.create(
            name="Test process",
            contributor=self.contributor,
            output_schema=[{"name": "output_file", "type": "basic:file:"}],
        )

        data = Data.objects.create(
            name="Test data",
            contributor=self.contributor,
            process=proc,
        )

        data.output = {"output_file": {"file": "output.txt"}}

        data_location = create_data_location()
        data_location.data.add(data)

        dir_path = data.location.get_path()
        os.makedirs(dir_path)

        with self.assertRaises(ValidationError):
            hydrate_size(data)

        file_path = os.path.join(dir_path, "output.txt")
        with open(file_path, "w") as fn:
            fn.write("foo bar")

        hydrate_size(data)
        self.assertEqual(data.output["output_file"]["size"], 7)

    def test_dependencies_single(self):
        process = Process.objects.create(
            slug="test-dependencies",
            type="data:test:dependencies:",
            contributor=self.contributor,
            input_schema=[
                {
                    "name": "src",
                    "type": "data:test:dependencies:",
                    "required": False,
                }
            ],
        )

        first = Data.objects.create(
            name="First data", contributor=self.contributor, process=process
        )

        second = Data.objects.create(
            name="Second data",
            contributor=self.contributor,
            process=process,
            input={"src": first.id},
        )

        third = Data.objects.create(
            name="Third data",
            contributor=self.contributor,
            process=process,
            input={"src": first.id},
        )

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
        self.assertEqual(
            {d.kind for d in first.children_dependency.all()}, {DataDependency.KIND_IO}
        )
        self.assertEqual(
            {d.kind for d in second.parents_dependency.all()}, {DataDependency.KIND_IO}
        )
        self.assertEqual(
            {d.kind for d in third.parents_dependency.all()}, {DataDependency.KIND_IO}
        )

    def test_dependencies_list(self):
        process = Process.objects.create(
            slug="test-dependencies-list",
            type="data:test:dependencies:list:",
            contributor=self.contributor,
            input_schema=[
                {
                    "name": "src",
                    "type": "list:data:test:dependencies:list:",
                    "required": False,
                }
            ],
        )

        first = Data.objects.create(
            name="First data", contributor=self.contributor, process=process
        )

        second = Data.objects.create(
            name="Second data",
            contributor=self.contributor,
            process=process,
            input={"src": [first.id]},
        )

        third = Data.objects.create(
            name="Third data",
            contributor=self.contributor,
            process=process,
            input={"src": [first.id, second.id]},
        )

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
        self.assertEqual(
            {d.kind for d in first.children_dependency.all()}, {DataDependency.KIND_IO}
        )
        self.assertEqual(
            {d.kind for d in second.children_dependency.all()}, {DataDependency.KIND_IO}
        )
        self.assertEqual(
            {d.kind for d in second.parents_dependency.all()}, {DataDependency.KIND_IO}
        )
        self.assertEqual(
            {d.kind for d in third.parents_dependency.all()}, {DataDependency.KIND_IO}
        )


class EntityModelTest(TestCase):
    def setUp(self):
        super().setUp()

        DescriptorSchema.objects.create(
            name="Sample", slug="sample", contributor=self.contributor
        )
        self.process = Process.objects.create(
            name="Test process",
            type="data:test:",
            contributor=self.contributor,
            entity_type="sample",
            entity_descriptor_schema="sample",
        )
        # Entity is created automatically when Data object is created
        self.data = Data.objects.create(
            name="Test data", contributor=self.contributor, process=self.process
        )

    def test_delete_data(self):
        # Create another Data object and add it to the same Entity.
        data_2 = Data.objects.create(
            name="Test data 2", contributor=self.contributor, process=self.process
        )
        data_2.entity.delete()
        data_2.entity = self.data.entity
        data_2.save()
        data_2.delete()

        self.assertEqual(Entity.objects.count(), 1)

    def test_new_sample(self):
        data = Data.objects.create(
            name="Test data",
            contributor=self.contributor,
            process=self.process,
            tags=["foo", "bar"],
        )
        entity = Entity.objects.last()
        self.assertTrue(entity.data.filter(pk=data.pk).exists())

        # Make sure tags are copied.
        self.assertEqual(entity.tags, data.tags)

    def test_entity_inheritance(self):
        # Prepare Data objects with entities.
        process_entity = self.process
        data_1 = self.data
        data_2 = Data.objects.create(
            contributor=self.contributor, process=process_entity
        )
        # Prepare Data objects without entities.
        process_no_entity = Process.objects.create(
            type="data:test:", contributor=self.contributor
        )
        data_3 = Data.objects.create(
            contributor=self.contributor, process=process_no_entity
        )
        # Prepare test process.
        test_process = Process.objects.create(
            contributor=self.contributor,
            entity_type="sample",
            entity_descriptor_schema="sample",
            input_schema=[
                {"name": "data_list", "type": "list:data:test:", "required": False},
                {"name": "data", "type": "data:test:", "required": False},
            ],
        )

        # Single Entity - Data object should be added to it.
        data = Data.objects.create(
            contributor=self.contributor,
            process=test_process,
            input={
                "data_list": [data_1.pk, data_3.pk],
            },
        )
        self.assertEqual(data.entity, data_1.entity)

        # Multiple Entities - Data object should be added to none of them.
        data = Data.objects.create(
            contributor=self.contributor,
            process=test_process,
            input={
                "data_list": [data_1.pk, data_2.pk],
            },
        )
        self.assertEqual(data.entity, None)

        # Multiple Entities with entity_always_create = True - Data object should be added to a whole new entity.
        test_process.entity_always_create = True
        test_process.save()
        data = Data.objects.create(
            contributor=self.contributor,
            process=test_process,
            input={
                "data": data_1.pk,
                "data_list": [data_2.pk, data_3.pk],
            },
        )
        self.assertEqual(data.entity, Entity.objects.last())
        test_process.entity_always_create = False
        test_process.save()

        # Multiple Entities with entity_input defined - Data object should be added to it.
        test_process.entity_input = "data"
        test_process.save()
        data = Data.objects.create(
            contributor=self.contributor,
            process=test_process,
            input={
                "data": data_1.pk,
                "data_list": [data_2.pk],
            },
        )
        self.assertEqual(data.entity, data_1.entity)
        test_process.entity_input = "data_list"
        test_process.save()
        data = Data.objects.create(
            contributor=self.contributor,
            process=test_process,
            input={
                "data": data_1.pk,
                "data_list": [data_2.pk],
            },
        )
        self.assertEqual(data.entity, data_2.entity)
        test_process.entity_input = None
        test_process.save()

        # Entities of different types - Data object should be added to the right one.
        entity_2 = data_2.entity
        entity_2.type = "something_else"
        entity_2.save()
        data = Data.objects.create(
            contributor=self.contributor,
            process=test_process,
            input={
                "data_list": [data_1.pk, data_2.pk],
            },
        )
        self.assertEqual(data.entity, data_1.entity)
        entity_2.type = "sample"
        entity_2.save()


class GetOrCreateTestCase(APITestCase):
    def setUp(self):
        super().setUp()

        user_model = get_user_model()
        self.user = user_model.objects.create(username="test_user", password="test_pwd")

        self.process = Process.objects.create(
            name="Temporary process",
            contributor=self.user,
            slug="tmp-process",
            persistence=Process.PERSISTENCE_TEMP,
            input_schema=[
                {"name": "some_value", "type": "basic:integer:", "default": 42}
            ],
        )
        assign_perm("view_process", self.user, self.process)

        process_2 = Process.objects.create(
            name="Another process",
            contributor=self.user,
            slug="another-process",
            persistence=Process.PERSISTENCE_TEMP,
            input_schema=[{"name": "some_value", "type": "basic:integer:"}],
        )
        assign_perm("view_process", self.user, process_2)

        self.data = Data.objects.create(
            name="Temporary data",
            contributor=self.user,
            process=self.process,
            input={"some_value": 42},
        )
        assign_perm("view_data", self.user, self.data)

        self.get_or_create_view = DataViewSet.as_view({"post": "get_or_create"})
        self.factory = APIRequestFactory()

    def tearDown(self):
        for data in Data.objects.all():
            if data.location:
                data_dir = data.location.get_path()
                shutil.rmtree(data_dir, ignore_errors=True)

        super().tearDown()

    def test_get_same(self):
        request = self.factory.post(
            "",
            {
                "name": "Data",
                "input": {"some_value": 42},
                "process": {"slug": "tmp-process"},
            },
            format="json",
        )
        force_authenticate(request, user=self.user)

        response = self.get_or_create_view(request)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data["id"], self.data.pk)

    def test_use_defaults(self):
        request = self.factory.post(
            "",
            {"name": "Data", "input": {}, "process": {"slug": "tmp-process"}},
            format="json",
        )
        force_authenticate(request, user=self.user)

        response = self.get_or_create_view(request)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data["id"], self.data.pk)

    def test_missing_permission(self):
        remove_perm("view_data", self.user, self.data)

        request = self.factory.post(
            "",
            {
                "name": "Data",
                "input": {"some_value": 42},
                "process": {"slug": "tmp-process"},
            },
            format="json",
        )
        force_authenticate(request, user=self.user)

        response = self.get_or_create_view(request)
        self.assertEqual(response.status_code, 201)
        self.assertNotEqual(response.data["id"], self.data.pk)

    def test_different_input(self):
        request = self.factory.post(
            "",
            {
                "name": "Data",
                "input": {"some_value": 43},
                "process": {"slug": "tmp-process"},
            },
            format="json",
        )
        force_authenticate(request, user=self.user)

        response = self.get_or_create_view(request)
        self.assertEqual(response.status_code, 201)
        self.assertNotEqual(response.data["id"], self.data.pk)

    def test_different_process(self):
        request = self.factory.post(
            "",
            {
                "name": "Data",
                "input": {"some_value": 43},
                "process": {"slug": "another-process"},
            },
            format="json",
        )
        force_authenticate(request, user=self.user)

        response = self.get_or_create_view(request)
        self.assertEqual(response.status_code, 201)
        self.assertNotEqual(response.data["id"], self.data.pk)

    def test_different_process_version(self):
        self.process.version = "2.0.0"
        self.process.save()

        request = self.factory.post(
            "",
            {
                "name": "Data",
                "input": {"some_value": 42},
                "process": {"slug": "tmp-process"},
            },
            format="json",
        )
        force_authenticate(request, user=self.user)

        response = self.get_or_create_view(request)
        self.assertEqual(response.status_code, 201)
        self.assertNotEqual(response.data["id"], self.data.pk)

    def test_raw_process(self):
        self.process.persistence = Process.PERSISTENCE_RAW
        self.process.save()

        request = self.factory.post(
            "",
            {
                "name": "Data",
                "input": {"some_value": 42},
                "process": {"slug": "tmp-process"},
            },
            format="json",
        )
        force_authenticate(request, user=self.user)

        response = self.get_or_create_view(request)
        self.assertEqual(response.status_code, 201)
        self.assertNotEqual(response.data["id"], self.data.pk)


class DuplicateTestCase(TestCase):
    def test_data_duplicate(self):
        process1 = Process.objects.create(
            contributor=self.user,
            type="data:test:first:",
        )
        process2 = Process.objects.create(
            contributor=self.user,
            input_schema=[
                {"name": "data_field", "type": "data:test:first:"},
            ],
            output_schema=[
                {"name": "json_field", "type": "basic:json:"},
            ],
        )

        input_data = Data.objects.create(
            name="Data 1",
            contributor=self.user,
            process=process1,
        )
        input_data.status = Data.STATUS_DONE
        input_data.save()

        data2 = Data.objects.create(
            name="Data 2",
            contributor=self.user,
            process=process2,
            started=now(),
            input={"data_field": input_data.id},
        )
        data_location = create_data_location()
        data_location.data.add(data2)
        data2.output = {"json_field": {"foo": "bar"}}
        data2.status = Data.STATUS_DONE
        save_storage(data2)
        data2.save()

        data2.migration_history.create(migration="migration_1")
        data2.migration_history.create(migration="migration_2")

        # Duplicate.
        duplicates = Data.objects.filter(id=data2.id).duplicate(self.contributor)
        self.assertEqual(len(duplicates), 1)
        duplicate = duplicates[0]
        self.assertTrue(duplicate.is_duplicate())

        # Convert original and duplicated to dict.
        data2_dict = Data.objects.filter(id=data2.id).values()[0]
        duplicate_dict = Data.objects.filter(id=duplicate.id).values()[0]

        # Pop fields that should differ and assert the remaining.
        fields_to_differ = (
            "id",
            "slug",
            "contributor_id",
            "name",
            "duplicated",
            "modified",
            "search",
        )
        for model_dict in (data2_dict, duplicate_dict):
            for field in fields_to_differ:
                model_dict.pop(field)

        self.assertDictEqual(data2_dict, duplicate_dict)

        # Assert location.
        self.assertEqual(data2.location.id, duplicate.location.id)

        # Assert fields that differ.
        self.assertEqual(duplicate.slug, "copy-of-data-2")
        self.assertEqual(duplicate.name, "Copy of Data 2")
        self.assertEqual(duplicate.contributor.username, "contributor")
        self.assertAlmostEqual(duplicate.duplicated, now(), delta=timedelta(seconds=3))
        self.assertAlmostEqual(duplicate.modified, now(), delta=timedelta(seconds=3))

        # Assert dependencies.
        self.assertEqual(
            DataDependency.objects.filter(kind=DataDependency.KIND_IO).count(), 2
        )
        self.assertTrue(
            DataDependency.objects.filter(
                kind=DataDependency.KIND_IO, parent=input_data, child=data2
            ).exists()
        )
        self.assertTrue(
            DataDependency.objects.filter(
                kind=DataDependency.KIND_IO, parent=input_data, child=duplicate
            ).exists()
        )
        self.assertEqual(
            DataDependency.objects.filter(kind=DataDependency.KIND_DUPLICATE).count(), 1
        )
        self.assertTrue(
            DataDependency.objects.filter(
                kind=DataDependency.KIND_DUPLICATE, parent=data2, child=duplicate
            ).exists()
        )
        self.assertEqual(
            DataDependency.objects.filter(kind=DataDependency.KIND_SUBPROCESS).count(),
            0,
        )

        # Assert storage
        self.assertEqual(Storage.objects.count(), 1)
        self.assertEqual(data2.storages.first(), Storage.objects.first())
        self.assertEqual(duplicate.storages.first(), Storage.objects.first())

        # Assert migration history
        self.assertEqual(DataMigrationHistory.objects.count(), 4)
        self.assertEqual(
            data2.migration_history.earliest("created").migration, "migration_1"
        )
        self.assertEqual(
            data2.migration_history.latest("created").migration, "migration_2"
        )
        self.assertEqual(
            duplicate.migration_history.earliest("created").migration, "migration_1"
        )
        self.assertEqual(
            duplicate.migration_history.latest("created").migration, "migration_2"
        )

        # Assert permissions.
        self.assertEqual(len(get_perms(self.contributor, duplicate)), 4)

    def test_data_duplicate_duplicate(self):
        process1 = Process.objects.create(
            contributor=self.user,
            type="data:test:first:",
        )
        process2 = Process.objects.create(
            contributor=self.user,
            input_schema=[
                {"name": "data_field", "type": "data:test:first:"},
            ],
            output_schema=[
                {"name": "json_field", "type": "basic:json:"},
            ],
        )
        input_data = Data.objects.create(contributor=self.user, process=process1)

        data = Data.objects.create(
            contributor=self.user, process=process2, input={"data_field": input_data.id}
        )
        data_location = create_data_location()
        data_location.data.add(data)
        data.output = {"json_field": {"foo": "bar"}}
        data.status = Data.STATUS_DONE
        save_storage(data)
        data.save()
        data.migration_history.create(migration="migration_1")

        # Duplicate.
        duplicate = data.duplicate(self.contributor)
        duplicate_of_duplicate = duplicate.duplicate(self.contributor)

        self.assertEqual(
            duplicate.contributor.id, duplicate_of_duplicate.contributor.id
        )

        # Assert location
        self.assertEqual(duplicate.location.id, duplicate_of_duplicate.location.id)

        # Assert dependencies.
        self.assertEqual(
            DataDependency.objects.filter(kind=DataDependency.KIND_IO).count(), 3
        )
        self.assertEqual(
            DataDependency.objects.filter(kind=DataDependency.KIND_DUPLICATE).count(), 2
        )
        self.assertEqual(
            DataDependency.objects.filter(kind=DataDependency.KIND_SUBPROCESS).count(),
            0,
        )
        self.assertTrue(
            DataDependency.objects.filter(
                kind=DataDependency.KIND_IO,
                parent=input_data,
                child=duplicate_of_duplicate,
            ).exists()
        )

        # Assert storage
        self.assertEqual(
            duplicate_of_duplicate.storages.first().id, Storage.objects.first().id
        )

        # Assert migration history
        self.assertEqual(DataMigrationHistory.objects.count(), 3)
        self.assertEqual(
            duplicate_of_duplicate.migration_history.latest("created").migration,
            "migration_1",
        )

        # Assert permissions.
        self.assertEqual(len(get_perms(self.contributor, duplicate_of_duplicate)), 4)

    def test_input_rewiring(self):
        process1 = Process.objects.create(
            contributor=self.user,
            type="data:test:first:",
        )
        process2 = Process.objects.create(
            contributor=self.user,
            type="data:test:second:",
            input_schema=[
                {"name": "data_field1", "type": "data:test:first:"},
                {"name": "data_field2", "type": "data:test:first:"},
                {"name": "data_list_field", "type": "list:data:test:first:"},
            ],
        )

        data1 = Data.objects.create(
            contributor=self.user,
            process=process1,
            status=Data.STATUS_DONE,
        )

        should_not_be_rewritten = data1.duplicate(self.user)

        data2 = Data.objects.create(
            contributor=self.user,
            process=process2,
            input={
                "data_field1": data1.id,
                "data_field2": should_not_be_rewritten.id,
                "data_list_field": [data1.id, should_not_be_rewritten.id],
            },
            status=Data.STATUS_DONE,
        )

        # Duplicate.
        duplicates = (
            Data.objects.filter(id__in=[data1.id, data2.id])
            .order_by("id")
            .duplicate(self.contributor)
        )
        duplicate1, duplicate2 = duplicates

        self.assertEqual(duplicate2.input["data_field1"], duplicate1.id)
        self.assertEqual(duplicate2.input["data_field2"], should_not_be_rewritten.id)
        self.assertEqual(
            duplicate2.input["data_list_field"],
            [duplicate1.id, should_not_be_rewritten.id],
        )

    def test_data_status_not_done(self):
        process = Process.objects.create(contributor=self.user)
        data = Data.objects.create(
            contributor=self.user, process=process, status=Data.STATUS_WAITING
        )
        data2 = Data.objects.create(
            contributor=self.user, process=process, status=Data.STATUS_DONE
        )

        with self.assertRaisesMessage(
            ValidationError,
            "Data object must have done or error status to be duplicated",
        ):
            Data.objects.filter(id__in=[data.id, data2.id]).duplicate(self.contributor)

    def test_data_long_name(self):
        process = Process.objects.create(contributor=self.user)

        long_name = "a" * (Data._meta.get_field("name").max_length - 1)
        data = Data.objects.create(
            name=long_name,
            contributor=self.user,
            process=process,
            status=Data.STATUS_DONE,
        )

        duplicate = Data.objects.filter(id=data.id).duplicate(self.contributor)[0]

        self.assertTrue(duplicate.name.startswith("Copy of "))
        self.assertTrue(duplicate.name.endswith("..."))
        self.assertEqual(len(duplicate.name), Data._meta.get_field("name").max_length)

    def test_entity_duplicate(self):
        process = Process.objects.create(contributor=self.user)
        data = Data.objects.create(
            name="Data 1",
            contributor=self.user,
            process=process,
            status=Data.STATUS_DONE,
        )
        assign_perm("view_data", self.contributor, data)
        data2 = Data.objects.create(
            name="Data 2",
            contributor=self.user,
            process=process,
            status=Data.STATUS_DONE,
        )
        assign_perm("view_data", self.contributor, data2)
        entity = Entity.objects.create(name="Entity", contributor=self.user)
        entity.data.add(data, data2)

        # Add to collection.
        collection = Collection.objects.create(contributor=self.user)
        collection.entity_set.add(entity)
        data.collection = collection
        data.save()
        data2.collection = collection
        data2.save()

        # Duplicate.
        entities = Entity.objects.filter(id=entity.id).duplicate(self.contributor)
        self.assertEqual(len(entities), 1)
        duplicate = entities[0]

        entity_dict = Entity.objects.filter(id=entity.id).values()[0]
        duplicate_dict = Entity.objects.filter(id=duplicate.id).values()[0]

        data1_dict = Data.objects.filter(id=data.id).values()[0]
        data1_duplicate = duplicate.data.get(name="Copy of Data 1")
        data1_duplicate_dict = Data.objects.filter(id=data1_duplicate.id).values()[0]

        data2_dict = Data.objects.filter(id=data2.id).values()[0]
        data2_duplicate = duplicate.data.get(name="Copy of Data 2")
        data2_duplicate_dict = Data.objects.filter(id=data2_duplicate.id).values()[0]

        self.assertTrue(duplicate.is_duplicate())
        self.assertTrue(data1_duplicate.is_duplicate())

        # Pop fields that should differ and assert the remaining.
        fields_to_differ = (
            "id",
            "slug",
            "contributor_id",
            "name",
            "duplicated",
            "modified",
            "collection_id",
            "entity_id",
            "search",
        )
        for model_dict in (
            entity_dict,
            duplicate_dict,
            data1_dict,
            data1_duplicate_dict,
            data2_dict,
            data2_duplicate_dict,
        ):
            for field in fields_to_differ:
                model_dict.pop(field, "")

        self.assertDictEqual(entity_dict, duplicate_dict)
        self.assertDictEqual(data1_dict, data1_duplicate_dict)
        self.assertDictEqual(data2_dict, data2_duplicate_dict)

        # Assert fields that differ.
        self.assertEqual(duplicate.slug, "copy-of-entity")
        self.assertEqual(duplicate.name, "Copy of Entity")
        self.assertEqual(duplicate.contributor.username, "contributor")
        self.assertAlmostEqual(duplicate.duplicated, now(), delta=timedelta(seconds=3))
        self.assertAlmostEqual(duplicate.modified, now(), delta=timedelta(seconds=3))
        self.assertEqual(duplicate.collection, None)

        # Assert collection is not altered.
        self.assertEqual(Collection.objects.count(), 1)
        self.assertEqual(collection.entity_set.count(), 1)
        self.assertEqual(collection.entity_set.first().name, "Entity")
        self.assertEqual(collection.data.count(), 2)
        collection_data = collection.data.all().order_by("name")
        self.assertEqual(collection_data[0].name, "Data 1")
        self.assertEqual(collection_data[1].name, "Data 2")

        # Assert permissions.
        self.assertEqual(len(get_perms(self.contributor, duplicate)), 4)

    def test_entity_duplicate_inherit(self):
        process = Process.objects.create(contributor=self.user)
        data = Data.objects.create(
            contributor=self.user, process=process, status=Data.STATUS_DONE
        )
        assign_perm("view_data", self.user, data)
        entity = Entity.objects.create(contributor=self.user)
        entity.data.add(data)

        # Add to collection.
        collection = Collection.objects.create(contributor=self.user)
        assign_perm("view_collection", self.user, collection)
        assign_perm("edit_collection", self.user, collection)
        assign_perm("view_collection", self.contributor, collection)
        collection.entity_set.add(entity)
        collection.data.add(data)

        # Duplicate.
        duplicated_entity1 = Entity.objects.filter(id=entity.id).duplicate(
            self.user, inherit_collection=False
        )[0]

        self.assertEqual(collection.entity_set.count(), 1)
        self.assertEqual(collection.entity_set.first().id, entity.id)
        self.assertEqual(collection.data.count(), 1)
        self.assertEqual(collection.data.first().id, data.id)

        # Assert permissions.
        self.assertEqual(duplicated_entity1.data.count(), 1)
        self.assertCountEqual(
            get_perms(self.user, collection), ["view_collection", "edit_collection"]
        )
        self.assertEqual(len(get_perms(self.user, duplicated_entity1)), 4)
        self.assertEqual(len(get_perms(self.user, duplicated_entity1.data.first())), 4)
        self.assertListEqual(
            get_perms(self.contributor, collection), ["view_collection"]
        )
        self.assertListEqual(get_perms(self.contributor, duplicated_entity1), [])
        self.assertListEqual(
            get_perms(self.contributor, duplicated_entity1.data.first()), []
        )

        with self.assertRaises(ValidationError):
            Entity.objects.filter(id=entity.id).duplicate(
                self.contributor, inherit_collection=True
            )[0]

        duplicated_entity2 = Entity.objects.filter(id=entity.id).duplicate(
            self.user, inherit_collection=True
        )[0]

        self.assertEqual(collection.entity_set.count(), 2)
        self.assertEqual(collection.entity_set.first().id, entity.id)
        self.assertEqual(collection.entity_set.last().id, duplicated_entity2.id)
        self.assertEqual(collection.data.count(), 2)
        self.assertEqual(collection.data.first().id, data.id)
        self.assertEqual(collection.data.last().id, duplicated_entity2.data.first().id)

        # Assert permissions.
        self.assertEqual(duplicated_entity2.data.count(), 1)
        self.assertCountEqual(
            get_perms(self.user, collection), ["view_collection", "edit_collection"]
        )
        self.assertEqual(len(get_perms(self.user, duplicated_entity2)), 4)
        self.assertEqual(len(get_perms(self.user, duplicated_entity2.data.first())), 4)
        self.assertListEqual(
            get_perms(self.contributor, collection), ["view_collection"]
        )

    def test_collection_duplicate(self):
        entity = Entity.objects.create(name="Entity", contributor=self.user)
        assign_perm("view_entity", self.contributor, entity)

        # Add to collection.
        collection = Collection.objects.create(name="Collection", contributor=self.user)
        collection.entity_set.add(entity)

        # Duplicate.
        collections = Collection.objects.filter(id=collection.id).duplicate(
            self.contributor
        )
        self.assertEqual(len(collections), 1)
        duplicate = collections[0]

        collection_dict = Collection.objects.filter(id=collection.id).values()[0]
        duplicate_dict = Collection.objects.filter(id=duplicate.id).values()[0]

        entity_dict = Entity.objects.filter(id=entity.id).values()[0]
        entity_duplicate = duplicate.entity_set.first()
        entity_duplicate_dict = Entity.objects.filter(id=entity_duplicate.id).values()[
            0
        ]

        self.assertTrue(duplicate.is_duplicate())
        self.assertTrue(entity_duplicate.is_duplicate())

        # Pop fields that should differ and assert the remaining.
        fields_to_differ = (
            "id",
            "slug",
            "contributor_id",
            "name",
            "duplicated",
            "modified",
            "collection_id",
            "search",
        )
        for model_dict in (
            collection_dict,
            duplicate_dict,
            entity_dict,
            entity_duplicate_dict,
        ):
            for field in fields_to_differ:
                model_dict.pop(field, "")

        self.assertDictEqual(collection_dict, duplicate_dict)
        self.assertDictEqual(entity_dict, entity_duplicate_dict)

        # Assert fields that differ.
        self.assertEqual(duplicate.slug, "copy-of-collection")
        self.assertEqual(duplicate.name, "Copy of Collection")
        self.assertEqual(duplicate.contributor.username, "contributor")
        self.assertAlmostEqual(duplicate.duplicated, now(), delta=timedelta(seconds=3))
        self.assertAlmostEqual(duplicate.modified, now(), delta=timedelta(seconds=3))

        self.assertEqual(entity_duplicate.slug, "copy-of-entity")
        self.assertEqual(entity_duplicate.name, "Copy of Entity")
        self.assertEqual(entity_duplicate.contributor.username, "contributor")
        self.assertAlmostEqual(
            entity_duplicate.duplicated, now(), delta=timedelta(seconds=3)
        )
        self.assertAlmostEqual(
            entity_duplicate.modified, now(), delta=timedelta(seconds=3)
        )

        # Assert duplicated entity and data objects are in collection.
        self.assertEqual(duplicate.entity_set.count(), 1)
        self.assertEqual(duplicate.entity_set.first().name, "Copy of Entity")

        # Assert permissions.
        self.assertEqual(len(get_perms(self.contributor, duplicate)), 4)


class ProcessModelTest(TestCase):
    def test_process_isactive(self):
        process = Process.objects.create(contributor=self.contributor)
        # is_active is true by default
        self.assertIs(process.is_active, True)
        process.is_active = False
        process.save()
        process_fetched = Process.objects.get(pk=process.pk)
        # the is_active flag is saved
        self.assertFalse(process_fetched.is_active)


@patch("resolwe.flow.models.utils.hydrate.Path")
class HydrateFileSizeUnitTest(TestCase):
    def create_data(self, path_mock, contributor, process):
        # Mock isfile and getsize for data creation to pass.
        # Make sure to set these two values to desired
        # values after calling this method.
        path_mock.return_value = MagicMock(
            stat=lambda: MagicMock(st_size=0), is_file=lambda: True
        )

        data = Data.objects.create(contributor=contributor, process=process)
        data_location = create_data_location()
        data_location.data.add(data)

        data.output = {"test_file": {"file": "test_file.tmp"}}
        data.save()

        return data

    def setUp(self):
        super().setUp()

        self.process = Process.objects.create(
            contributor=self.contributor,
            output_schema=[
                {"name": "test_file", "type": "basic:file:", "required": False},
                {"name": "file_list", "type": "list:basic:file:", "required": False},
            ],
        )

    def test_done_data(self, path_mock):
        data = self.create_data(path_mock, self.contributor, self.process)

        path_mock.return_value = MagicMock(
            stat=lambda: MagicMock(st_size=42000), is_file=lambda: True
        )

        hydrate_size(data)
        self.assertEqual(data.output["test_file"]["size"], 42000)

    @patch("resolwe.flow.models.utils.hydrate.os")
    def test_data_with_refs(self, os_mock, path_mock):
        data = self.create_data(path_mock, self.contributor, self.process)
        stat_mock = MagicMock()
        type(stat_mock).st_size = PropertyMock(side_effect=[42000, 8000, 42])
        path_mock.return_value = MagicMock(is_file=lambda: True, stat=lambda: stat_mock)
        data.output = {
            "test_file": {
                "file": "test_file.tmp",
                "refs": ["ref_file1.tmp", "ref_file2.tmp"],
            }
        }
        hydrate_size(data)
        self.assertEqual(data.output["test_file"]["size"], 42000)
        self.assertEqual(data.output["test_file"]["total_size"], 50042)
        self.assertEqual(data.size, 50042)

    @patch("resolwe.flow.models.utils.hydrate.os")
    def test_list(self, os_mock, path_mock):
        data = self.create_data(path_mock, self.contributor, self.process)

        stat_mock = MagicMock()
        type(stat_mock).st_size = PropertyMock(side_effect=[34, 42000, 42])
        path_mock.return_value = MagicMock(
            is_file=lambda: True,
            stat=lambda: stat_mock,
            __fspath__=lambda: "my_mocked_file_name",
        )

        data.output = {
            "file_list": [
                {
                    "file": "test_01.tmp",
                },
                {"file": "test_02.tmp", "refs": ["ref_file1.tmp"]},
            ]
        }
        hydrate_size(data)
        self.assertEqual(data.output["file_list"][0]["size"], 34)
        self.assertEqual(data.output["file_list"][0]["total_size"], 34)
        self.assertEqual(data.output["file_list"][1]["size"], 42000)
        self.assertEqual(data.output["file_list"][1]["total_size"], 42042)
        self.assertEqual(data.size, 34 + 42042)

    def test_change_size(self, path_mock):
        """Size is not changed after object is done."""
        data = self.create_data(path_mock, self.contributor, self.process)

        path_mock.return_value = MagicMock(
            stat=lambda: MagicMock(st_size=42000), is_file=lambda: True
        )

        hydrate_size(data)
        self.assertEqual(data.output["test_file"]["size"], 42000)

        path_mock.return_value = MagicMock(
            stat=lambda: MagicMock(st_size=43000), is_file=lambda: True
        )
        hydrate_size(data)
        self.assertEqual(data.output["test_file"]["size"], 43000)

        data.status = Data.STATUS_DONE
        path_mock.return_value = MagicMock(
            stat=lambda: MagicMock(st_size=44000), is_file=lambda: True
        )
        hydrate_size(data)
        self.assertEqual(data.output["test_file"]["size"], 43000)

    def test_missing_file(self, path_mock):
        data = self.create_data(path_mock, self.contributor, self.process)

        path_mock.return_value = MagicMock(is_file=lambda: False)
        with self.assertRaises(ValidationError):
            hydrate_size(data)


class StorageModelTestCase(TestCase):
    def setUp(self):
        super().setUp()
        self.proc = Process.objects.create(
            name="Test process",
            contributor=self.contributor,
            output_schema=[
                {"name": "json_field", "type": "basic:json:"},
            ],
        )

    def test_delete_data(self):
        """Orphaned storages are deleted when data object is deleted"""
        storage_referenced_by_both = Storage.objects.create(
            contributor=self.user, json={}
        )

        data_1 = Data.objects.create(contributor=self.contributor, process=self.proc)
        data_1.storages.create(contributor=self.user, json={})
        data_1.storages.add(storage_referenced_by_both)

        data_2 = Data.objects.create(contributor=self.contributor, process=self.proc)
        data_2.storages.add(storage_referenced_by_both)
        data_2.save()

        Storage.objects.create(contributor=self.user, json={})

        self.assertEqual(Data.objects.count(), 2)
        self.assertEqual(Storage.objects.count(), 3)

        # Delete first object.
        data_1.delete()
        self.assertEqual(Data.objects.count(), 1)
        self.assertEqual(Storage.objects.count(), 2)

        # Delete second object.
        data_2.delete()
        self.assertEqual(Data.objects.count(), 0)
        self.assertEqual(Storage.objects.count(), 1)

    def test_storage_manager(self):
        data = Data.objects.create(
            name="Test data",
            contributor=self.contributor,
            process=self.proc,
        )
        data.output = {"json_field": {"foo": {"moo": "bar"}}}
        data.status = Data.STATUS_DONE
        save_storage(data)
        data.save()

        # Annotation with specific JSON field.
        storage = Storage.objects.with_json_path(["foo", "moo"])[0]
        self.assertEqual(storage.json_foo_moo, "bar")

        storage = Storage.objects.with_json_path("foo.moo")[0]
        self.assertEqual(storage.json_foo_moo, "bar")

        storage = Storage.objects.with_json_path(["foo", "moo"], field="result")[0]
        self.assertEqual(storage.result, "bar")

        # Extract specific JSON path.
        value = Storage.objects.get_json_path(["foo", "moo"])[0]
        self.assertEqual(value, "bar")

        value = Storage.objects.get_json_path("foo.moo")[0]
        self.assertEqual(value, "bar")


class UtilsTestCase(TestCase):
    def test_render_template(self):
        process_mock = MagicMock(requirements={"expression-engine": "jinja"})
        template = render_template(process_mock, "{{ 1 | increase }}", {})
        self.assertEqual(template, "2")

    def test_render_template_error(self):
        process_mock = MagicMock(requirements={"expression-engine": "jinja"})
        with self.assertRaises(EvaluationError):
            render_template(process_mock, "{{ 1 | missing_increase }}", {})

    def test_hydrate_input_references(self):
        DescriptorSchema.objects.create(
            name="Sample", slug="sample", contributor=self.contributor
        )

        descriptor_schema = DescriptorSchema.objects.create(
            contributor=self.contributor,
            schema=[
                {
                    "name": "annotation",
                    "type": "basic:string:",
                },
            ],
        )

        process = Process.objects.create(
            contributor=self.contributor,
            type="data:test:",
            output_schema=[
                {
                    "name": "file",
                    "type": "basic:file:",
                },
                {
                    "name": "file_list",
                    "type": "list:basic:file:",
                },
                {
                    "name": "dir",
                    "type": "basic:dir:",
                },
                {
                    "name": "dir_list",
                    "type": "list:basic:dir:",
                },
            ],
            entity_type="sample",
            entity_descriptor_schema="sample",
        )

        data = Data.objects.create(
            name="test",
            contributor=self.contributor,
            status=Data.STATUS_ERROR,
            process=process,
            # Workaround for skipping the validation.
            descriptor_schema=descriptor_schema,
            descriptor={
                "annotation": "my-annotation",
            },
            size=0,
        )
        data_location = create_data_location()
        data_location.data.add(data)
        data.output = {
            "file": {"file": "some-file", "refs": ["ref1"]},
            "file_list": [
                {"file": "some-file", "refs": ["ref2"]},
                {"file": "another-file"},
            ],
            "dir": {"dir": "some-dir", "refs": ["ref3"]},
            "dir_list": [
                {"dir": "some-dir", "refs": ["ref4"]},
                {"dir": "another-dir"},
            ],
        }
        data.save()

        input_schema = [
            {
                "name": "data",
                "type": "data:test:",
            },
        ]
        input_ = {"data": data.pk}
        hydrate_input_references(input_, input_schema)

        # TODO: why this worked?
        # path_prefix = data.location.get_path()
        from resolwe.flow.managers import manager

        path_prefix = manager.get_executor().resolve_data_path(data=data)

        self.assertEqual(
            input_["data"]["__descriptor"], {"annotation": "my-annotation"}
        )
        self.assertEqual(input_["data"]["__type"], "data:test:")
        self.assertEqual(input_["data"]["__id"], data.id)

        self.assertEqual(input_["data"]["file"]["file"].data_id, data.id)
        self.assertEqual(input_["data"]["file"]["file"].file_name, "some-file")
        self.assertEqual(
            str(input_["data"]["file"]["file"]), os.path.join(path_prefix, "some-file")
        )

        self.assertEqual(input_["data"]["file"]["refs"][0].data_id, data.id)
        self.assertEqual(input_["data"]["file"]["refs"][0].file_name, "ref1")
        self.assertEqual(
            str(input_["data"]["file"]["refs"][0]), os.path.join(path_prefix, "ref1")
        )

        self.assertEqual(input_["data"]["file_list"][0]["file"].data_id, data.id)
        self.assertEqual(input_["data"]["file_list"][0]["file"].file_name, "some-file")
        self.assertEqual(
            str(input_["data"]["file_list"][0]["file"]),
            os.path.join(path_prefix, "some-file"),
        )

        self.assertEqual(input_["data"]["file_list"][0]["refs"][0].data_id, data.id)
        self.assertEqual(input_["data"]["file_list"][0]["refs"][0].file_name, "ref2")
        self.assertEqual(
            str(input_["data"]["file_list"][0]["refs"][0]),
            os.path.join(path_prefix, "ref2"),
        )

        self.assertEqual(input_["data"]["file_list"][1]["file"].data_id, data.id)
        self.assertEqual(
            input_["data"]["file_list"][1]["file"].file_name, "another-file"
        )
        self.assertEqual(
            str(input_["data"]["file_list"][1]["file"]),
            os.path.join(path_prefix, "another-file"),
        )

        self.assertEqual(input_["data"]["dir"]["dir"].data_id, data.id)
        self.assertEqual(input_["data"]["dir"]["dir"].file_name, "some-dir")
        self.assertEqual(
            str(input_["data"]["dir"]["dir"]), os.path.join(path_prefix, "some-dir")
        )

        self.assertEqual(input_["data"]["dir"]["refs"][0].data_id, data.id)
        self.assertEqual(input_["data"]["dir"]["refs"][0].file_name, "ref3")
        self.assertEqual(
            str(input_["data"]["dir"]["refs"][0]), os.path.join(path_prefix, "ref3")
        )

        self.assertEqual(input_["data"]["dir_list"][0]["dir"].data_id, data.id)
        self.assertEqual(input_["data"]["dir_list"][0]["dir"].file_name, "some-dir")
        self.assertEqual(
            str(input_["data"]["dir_list"][0]["dir"]),
            os.path.join(path_prefix, "some-dir"),
        )

        self.assertEqual(input_["data"]["dir_list"][0]["refs"][0].data_id, data.id)
        self.assertEqual(input_["data"]["dir_list"][0]["refs"][0].file_name, "ref4")
        self.assertEqual(
            str(input_["data"]["dir_list"][0]["refs"][0]),
            os.path.join(path_prefix, "ref4"),
        )

        self.assertEqual(input_["data"]["dir_list"][1]["dir"].data_id, data.id)
        self.assertEqual(input_["data"]["dir_list"][1]["dir"].file_name, "another-dir")
        self.assertEqual(
            str(input_["data"]["dir_list"][1]["dir"]),
            os.path.join(path_prefix, "another-dir"),
        )

        self.assertEqual(input_["data"]["__entity_name"], "test")
        self.assertEqual(input_["data"]["__entity_id"], data.entity.id)

    def _test_referenced_files(self, field_schema, output):
        data_mock = MagicMock(
            output=output,
            process=MagicMock(output_schema=field_schema),
            descriptor={},
            descriptor_schema=[],
        )
        refs = referenced_files(data_mock)
        refs.remove("jsonout.txt")
        refs.remove("stdout.txt")
        return refs

    def test_referenced_basic_file(self):
        field_type = "basic:file:"
        output = {"sample": {"file": "sample_file"}}
        field_schema = [
            {"name": "sample", "label": "Sample output", "type": field_type}
        ]
        refs = self._test_referenced_files(field_schema, output)
        self.assertEqual(refs, ["sample_file"])

        field_schema = [
            {"name": "sample", "label": "Sample output", "type": field_type}
        ]
        output = {"sample": {"file": "dir/sample_file"}}

        refs = self._test_referenced_files(field_schema, output)
        self.assertEqual(refs, ["dir/sample_file"])

        field_schema = [
            {"name": "a", "label": "Sample output", "type": field_type},
            {"name": "b", "label": "Sample output", "type": field_type},
        ]
        output = {"a": {"file": "file1"}, "b": {"file": "file2"}}
        refs = self._test_referenced_files(field_schema, output)
        self.assertSetEqual(set(refs), {"file1", "file2"})

    def test_referenced_list_basic_file(self):
        field_schema = [
            {"name": "sample", "label": "Sample output", "type": "list:basic:file:"}
        ]
        output = {"sample": [{"file": "a"}, {"file": "b"}]}
        refs = self._test_referenced_files(field_schema, output)
        self.assertSetEqual(set(refs), {"a", "b"})

        field_schema = [
            {"name": "sample1", "label": "Sample output", "type": "list:basic:file:"},
            {"name": "sample2", "label": "Sample output", "type": "list:basic:file:"},
        ]
        output = {
            "sample1": [{"file": "a"}, {"file": "b"}],
            "sample2": [{"file": "c"}, {"file": "d"}],
        }
        refs = self._test_referenced_files(field_schema, output)
        self.assertSetEqual(set(refs), {"a", "b", "c", "d"})

    def test_referenced_basic_dir(self):
        field_schema = [
            {"name": "sample", "label": "Sample output", "type": "basic:dir:"}
        ]
        output = {"sample": {"dir": "sample_dir"}}
        refs = self._test_referenced_files(field_schema, output)
        self.assertEqual(refs, ["sample_dir"])

        field_schema = [
            {"name": "sample", "label": "Sample output", "type": "basic:dir:"}
        ]
        output = {"sample": {"dir": "dir/dir"}}
        refs = self._test_referenced_files(field_schema, output)
        self.assertEqual(refs, ["dir/dir"])

        field_schema = [
            {"name": "a", "label": "Sample output", "type": "basic:dir:"},
            {"name": "b", "label": "Sample output", "type": "basic:dir:"},
        ]
        output = {"a": {"dir": "dir1"}, "b": {"dir": "dir2"}}
        refs = self._test_referenced_files(field_schema, output)
        self.assertSetEqual(set(refs), {"dir1", "dir2"})

    def test_referenced_list_basic_dir(self):
        field_schema = [
            {"name": "sample", "label": "Sample output", "type": "list:basic:dir:"}
        ]
        output = {"sample": [{"dir": "a"}, {"dir": "b"}]}
        refs = self._test_referenced_files(field_schema, output)
        self.assertSetEqual(set(refs), {"a", "b"})

        field_schema = [
            {"name": "sample1", "label": "Sample output", "type": "list:basic:dir:"},
            {"name": "sample2", "label": "Sample output", "type": "list:basic:dir:"},
        ]
        output = {
            "sample1": [{"dir": "a/1"}, {"dir": "b"}],
            "sample2": [{"dir": "c"}, {"dir": "d"}],
        }
        refs = self._test_referenced_files(field_schema, output)
        self.assertSetEqual(set(refs), {"a/1", "b", "c", "d"})

    def test_referenced_complex(self):
        field_schema = [
            {"name": "sample1", "label": "Sample output", "type": "basic:file:"},
            {"name": "sample2", "label": "Sample output", "type": "list:basic:file:"},
            {"name": "sample3", "label": "Sample output", "type": "basic:file:s"},
            {
                "name": "sample4",
                "label": "Sample output",
                "type": "list:basic:file:s",
            },
            {"name": "sample5", "label": "Sample output", "type": "basic:dir:"},
            {
                "name": "sample6",
                "label": "Sample output",
                "type": "list:basic:dir:",
            },
            {"name": "sample7", "label": "Sample output", "type": "basic:dir:s"},
            {
                "name": "sample8",
                "label": "Sample output",
                "type": "list:basic:dir:s",
            },
        ]
        output = {
            "sample1": {"file": "file1"},
            "sample2": [{"file": "file2"}, {"file": "file3"}],
            "sample3": {"file": "file4"},
            "sample4": [{"file": "file5"}, {"file": "file6"}],
            "sample5": {"dir": "dir1"},
            "sample6": [{"dir": "dir2"}, {"dir": "dir3"}],
            "sample7": {"dir": "dir4"},
            "sample8": [{"dir": "dir5"}, {"dir": "dir6"}],
        }
        refs = self._test_referenced_files(field_schema, output)
        files = {"file{}".format(i) for i in range(1, 6 + 1)}
        dirs = {"dir{}".format(i) for i in range(1, 6 + 1)}
        self.assertSetEqual(set(refs), files | dirs)

    def test_referenced_complex_descriptor(self):
        field_schema = [
            {"name": "sample1", "label": "Sample output", "type": "basic:file:"},
            {"name": "sample2", "label": "Sample output", "type": "list:basic:file:"},
            {"name": "sample3", "label": "Sample output", "type": "basic:file:s"},
            {
                "name": "sample4",
                "label": "Sample output",
                "type": "list:basic:file:s",
            },
            {"name": "sample5", "label": "Sample output", "type": "basic:dir:"},
            {
                "name": "sample6",
                "label": "Sample output",
                "type": "list:basic:dir:",
            },
            {"name": "sample7", "label": "Sample output", "type": "basic:dir:s"},
            {
                "name": "sample8",
                "label": "Sample output",
                "type": "list:basic:dir:s",
            },
        ]
        output = {
            "sample1": {"file": "file1"},
            "sample2": [{"file": "file2"}, {"file": "file3"}],
            "sample3": {"file": "file4"},
            "sample4": [{"file": "file5"}, {"file": "file6"}],
            "sample5": {"dir": "dir1"},
            "sample6": [{"dir": "dir2"}, {"dir": "dir3"}],
            "sample7": {"dir": "dir4"},
            "sample8": [{"dir": "dir5"}, {"dir": "dir6"}],
        }
        data_mock = MagicMock(
            output={},
            process=MagicMock(output_schema=[]),
            descriptor=output,
            descriptor_schema=MagicMock(schema=field_schema),
        )
        refs = referenced_files(data_mock)
        refs.remove("jsonout.txt")
        refs.remove("stdout.txt")

        refs = self._test_referenced_files(field_schema, output)
        files = {"file{}".format(i) for i in range(1, 6 + 1)}
        dirs = {"dir{}".format(i) for i in range(1, 6 + 1)}
        self.assertSetEqual(set(refs), files | dirs)

    def test_referenced_mix(self):
        field_schema = [
            {"name": "sample1", "label": "Sample output", "type": "basic:file:"},
        ]
        output = {
            "sample1": {"file": "file1"},
        }

        field_schema_descriptor = [
            {"name": "sample2", "label": "Sample output", "type": "basic:file:"},
        ]
        output_descriptor = {
            "sample2": {"file": "file2"},
        }

        data_mock = MagicMock(
            output=output,
            process=MagicMock(output_schema=field_schema),
            descriptor=output_descriptor,
            descriptor_schema=MagicMock(schema=field_schema_descriptor),
        )
        refs = referenced_files(data_mock)
        refs.remove("jsonout.txt")
        refs.remove("stdout.txt")
        self.assertSetEqual(set(refs), {"file1", "file2"})
