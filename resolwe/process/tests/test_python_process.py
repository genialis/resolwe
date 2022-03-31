# pylint: disable=missing-docstring
import os
import sys
import unittest

from django.test import LiveServerTestCase, override_settings

import resolwe.permissions.models
from resolwe.flow.models import (
    Collection,
    Data,
    DescriptorSchema,
    Entity,
    Process,
    Relation,
    RelationPartition,
    RelationType,
    Storage,
)
from resolwe.permissions.models import Permission, get_anonymous_user
from resolwe.test import (
    ProcessTestCase,
    tag_process,
    with_docker_executor,
    with_resolwe_host,
)

PROCESSES_DIR = os.path.join(os.path.dirname(__file__), "processes")
WORKFLOWS_DIR = os.path.join(os.path.dirname(__file__), "workflows")
DESCRIPTORS_DIR = os.path.join(os.path.dirname(__file__), "descriptors")
FILES_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "files")


class PythonProcessTest(ProcessTestCase):
    def setUp(self):
        super().setUp()
        self._register_schemas(
            processes_paths=[PROCESSES_DIR, WORKFLOWS_DIR],
            descriptors_paths=[DESCRIPTORS_DIR],
        )
        self.files_path = FILES_PATH
        # Force reading anonymous user from the database for every test.
        resolwe.permissions.models.ANONYMOUS_USER = None

    @with_docker_executor
    def test_registration(self):
        process = Process.objects.get(slug="test-python-process")

        self.assertEqual(process.slug, "test-python-process")
        self.assertEqual(process.name, "Test Python Process")
        self.assertEqual(process.version, "0.1.2")
        self.assertEqual(process.type, "data:python:")
        self.assertEqual(process.category, "analyses:")
        self.assertEqual(process.scheduling_class, Process.SCHEDULING_CLASS_BATCH)
        self.assertEqual(process.persistence, Process.PERSISTENCE_CACHED)
        self.assertEqual(process.description, "This is a process description.")
        self.assertEqual(process.data_name, "Foo: {{input_data | name}}")
        self.assertEqual(process.entity_type, "sample")
        self.assertEqual(process.entity_descriptor_schema, "sample")
        self.assertEqual(process.entity_input, "input_data")
        self.assertEqual(
            process.requirements,
            {
                "expression-engine": "jinja",
                "executor": {
                    "docker": {
                        "image": "public.ecr.aws/s4q6j6e8/resolwe/base:ubuntu-20.04"
                    }
                },
            },
        )

        for field in process.input_schema:
            if field["name"] == "my_group":
                self.assertEqual(field["group"][0]["name"], "foo")
                self.assertEqual(field["group"][1]["name"], "bar")
                break
        else:
            self.fail("Field my_group not found in test-python-process")

        for field in process.input_schema:
            if field["name"] == "bar":
                self.assertEqual(field["relation"]["type"], "group")
                self.assertEqual(field["relation"]["npartitions"], "none")
                break
        else:
            self.fail("Field bar not found in test-python-process")

        for field in process.input_schema:
            if field["name"] == "baz":
                self.assertEqual(field["relation"]["type"], "group")
                self.assertEqual(field["relation"]["npartitions"], 1)
                break
        else:
            self.fail("Field baz not found in test-python-process")

        for field in process.input_schema:
            if field["name"] == "baz_list":
                self.assertEqual(field["relation"]["type"], "group")
                self.assertEqual(field["relation"]["npartitions"], 1)
                break
        else:
            self.fail("Field baz_list not found in test-python-process")

        for field in process.input_schema:
            if field["name"] == "my_float":
                self.assertEqual(field["range"], [0.0, 1.0])
                break
        else:
            self.fail("Field my_float not found in test-python-process")
        for field in process.input_schema:
            if field["name"] == "integer":
                self.assertEqual(field["range"], [0, 100])
                break
        else:
            self.fail("Field integer not found in test-python-process")

        # Make sure that process with inheritance from `module.Class` is also registered.
        process = Process.objects.get(slug="test-python-process-2")

    @with_docker_executor
    @tag_process("test-python-process-annotate-entity")
    def test_annotation(self):
        data = self.run_process("test-python-process-annotate-entity")
        self.assertIsNotNone(data.entity)
        dsc = data.entity.descriptor
        self.assertIn("general", dsc)
        self.assertIn("species", dsc["general"])
        self.assertEqual(dsc["general"]["species"], "Valid")
        self.assertIn("description", dsc["general"])
        self.assertEqual(dsc["general"]["description"], "desc")

    @with_docker_executor
    @tag_process("test-python-process", "test-save-file", "entity-process")
    def test_python_process(self):
        with self.preparation_stage():
            input_data = self.run_process(
                "test-save-file", {"input_file": "testfile.txt"}
            )
            input_data.name = "bar"
            input_data.save()

            input_entity = self.run_process("entity-process")

            storage = input_data.storages.create(
                name="storage", contributor=self.user, json={"value": 42}
            )

        data = self.run_process(
            "test-python-process",
            {
                "my_field": "bar",
                "my_list": ["one", "two", "three"],
                "bar": input_data.pk,
                "url": {"url": "https://www.genialis.com"},
                "input_data": input_data.pk,
                "input_entity_data": input_entity.pk,
                "integer": 42,
                "my_float": 0.42,
                "my_json": storage.pk,
                "my_group": {
                    "bar": "my string",
                    "foo": 21,
                },
            },
        )

        self.assertFields(data, "string_output", "OK")
        self.assertFields(data, "list_string_output", ["foo", "bar"])
        self.assertFile(data, "file_output", "testfile.txt")
        self.assertFiles(data, "list_file_output", ["testfile.txt", "testfile2.txt"])

        # Non-deterministic output.
        del data.output["dir_output"]["size"]
        del data.output["dir_output"]["total_size"]

        self.assertEqual(data.output["dir_output"], {"dir": "test/"})
        self.assertEqual(data.output["input_data_name"], "bar")
        self.assertEqual(data.output["input_entity_name"], "Data with entity")
        self.assertEqual(
            data.output["docker_image"],
            "public.ecr.aws/s4q6j6e8/resolwe/base:ubuntu-20.04",
        )
        self.assertEqual(data.name, "Foo: bar")

        entity = Entity.objects.get(data=data)
        self.assertEqual(entity.data.first(), data)

    @with_docker_executor
    @tag_process("test-python-process-group-field")
    def test_python_process_group(self):
        # Run with explicitly given inputs.
        data = self.run_process(
            "test-python-process-group-field",
            {
                "my_group": {
                    "foo": 123,
                    "bar": "foobar",
                },
                "my_group2": {
                    "foo": 124,
                },
                "my_subgroup": {"subgroup": {"foo": 3}},
            },
        )
        self.assertFields(data, "out_foo", 123)
        self.assertFields(data, "out_bar", "foobar")
        self.assertFields(data, "out_foo2", 124)
        self.assertFields(data, "out_subgroup", 3)

        # Run with no inputs - check that default values are used.
        data = self.run_process("test-python-process-group-field")
        self.assertFields(data, "out_foo", 42)
        self.assertFalse(hasattr(data.output, "out_bar"))
        self.assertFalse(hasattr(data.output, "out_bar2"))
        self.assertFields(data, "out_subgroup", 2)

    @with_docker_executor
    @tag_process("test-python-process-json")
    def test_python_process_json(self):
        """Test that data object with json output can be given as input."""
        with self.preparation_stage():
            input_data = self.run_process("test-output-json")

        self.run_process(
            "test-python-process-json",
            {
                "data": input_data.pk,
            },
        )

    @with_docker_executor
    @tag_process("test-non-required-data-inputs")
    def test_non_required_data_input(self):
        """Test workflow with non-required data inputs"""
        with self.preparation_stage():
            input_data = self.run_process("test-output-json")

        self.run_process(
            "test-non-required-data-inputs",
            {
                "data": input_data.pk,
            },
        )

        data = Data.objects.get(process__slug="test-python-process-json")
        self.assertEqual(data.status, "OK")

    @with_docker_executor
    @tag_process("process-with-workflow-input")
    def test_workflow_as_list_input(self):
        """Test workflow with non-required data inputs"""
        with self.preparation_stage():
            workflow = self.run_process("simple-workflow")

        data = self.run_process("process-with-workflow-input", {"data": workflow.pk})

        data.refresh_from_db()
        self.assertEqual(data.status, "OK")

    @with_docker_executor
    @tag_process("test-python-process-error")
    def test_error(self):
        """Test process that raises exception"""
        data = self.run_process(
            "test-python-process-error", assert_status=Data.STATUS_ERROR
        )
        self.assertEqual(data.process_error[0], "Value error in ErrorProcess")

    @with_docker_executor
    @tag_process("test-python-process-file")
    def test_import_file(self):
        """Test import file"""
        inputs = {"src": "testfile.txt"}
        data = self.run_process("test-python-process-file", inputs)
        self.assertEqual(data.output["dst"]["file"], "testfile.txt")
        self.assertEqual(data.output["dst"]["size"], 15)

    @with_docker_executor
    @tag_process("process-with-choices-input")
    def test_process_with_choices(self):
        """Test process that does not have a predefined choice as an input."""
        data = self.run_process("process-with-choices-input", {"string_input": "baz"})
        self.assertFields(data, "string_output", "baz")

    @with_docker_executor
    @tag_process("test-process-relations")
    def test_python_process_relations(self):
        """Test relations in Python process.

        Make two Data (with corresponding entities) in series relation.
        """
        collection = Collection.objects.create(
            name="collection", contributor=self.contributor
        )

        with self.preparation_stage():
            # From collection 1
            start = self.run_process("entity-process")
            end = self.run_process("entity-process")

            start.collection = collection
            end.collection = collection
            start.save()
            end.save()

            # Set relation between the start and end object's entities.
            rel_type_series = RelationType.objects.create(name="series", ordered=True)
            relation = Relation.objects.create(
                contributor=self.contributor,
                collection=collection,
                type=rel_type_series,
                category="time-series",
                unit=Relation.UNIT_HOUR,
            )

            RelationPartition.objects.create(
                relation=relation,
                entity=start.entity,
                label="start",
                position=1,
            )
            RelationPartition.objects.create(
                relation=relation,
                entity=end.entity,
                label="end",
                position=2,
            )

            # Prepare also another data that is not inside entity and is in another collection
            other = self.run_process("test-python-process-2")
            collection_2 = Collection.objects.create(
                name="Collection 2", contributor=self.contributor
            )
            collection_2.data.add(other)

        data = self.run_process(
            "test-process-relations", {"data": [start.pk, end.pk, other.pk]}
        )

        data.refresh_from_db()
        self.assertEqual(data.output["relation_id"], relation.id)
        self.assertEqual(data.output["relation_type"], "series")
        self.assertEqual(data.output["relation_ordered"], "True")
        self.assertEqual(data.output["relation_category"], "time-series")
        self.assertEqual(data.output["relation_unit"], "hr")
        self.assertEqual(data.output["relation_partition_label"], "start")
        self.assertEqual(data.output["relation_partition_position"], 1)

    @with_docker_executor
    @tag_process("data-name-process")
    def test_data_name(self):
        """Test self.name property."""
        with self.preparation_stage():
            data_input = self.run_process("entity-process")

        data = self.run_process("data-name-process", {"data_input": data_input.id})
        self.assertEqual(data.output["name"], "Data with entity")

    @with_docker_executor
    @tag_process("create-collection")
    def test_create_collection(self):
        """Test process that creates object"""
        collection_name = "Python process collection"
        self.assertFalse(Collection.objects.filter(name=collection_name).exists())
        self.run_process("create-collection", {"collection_name": collection_name})
        Collection.objects.get(name=collection_name)

    @with_docker_executor
    @tag_process("storage-objects-test")
    def test_storage_objects(self):
        """Test storage access from python process."""
        data = self.run_process("storage-objects-test")
        self.assertEquals(data.storages.count(), 2)
        storage_string = Storage.objects.get(pk=data.output["output_string"])
        self.assertEqual(storage_string.json, ["valid", "json"])
        storage_file = Storage.objects.get(pk=data.output["output_file"])
        self.assertEqual(storage_file.json, ["valid", "json", "file"])

    @with_docker_executor
    @tag_process("filter-collection")
    def test_filter_collection(self):
        """Test process that filters object"""
        collection_name = "Python process collection"
        number_of_collections = 2
        self.assertFalse(Collection.objects.filter(name=collection_name).exists())
        for _ in range(number_of_collections):
            Collection.objects.create(name=collection_name, contributor=self.user)
        data = self.run_process(
            "filter-collection", {"collection_name": collection_name}
        )
        self.assertEqual(data.output["number_of_collections"], number_of_collections)

    @with_docker_executor
    @tag_process("get-collection")
    def test_get_collection(self):
        """Test process that gets object"""
        collection_name = "Python process collection"
        self.assertFalse(Collection.objects.filter(name=collection_name).exists())
        collection = Collection.objects.create(
            name=collection_name, contributor=self.user
        )
        data = self.run_process("get-collection", {"collection_name": collection_name})
        self.assertEqual(data.output["collection_slug"], collection.slug)

    @with_docker_executor
    @tag_process("parent-process-schema", "test-python-process-requirements")
    def test_get_parent_schema(self):
        """Test access to parent object schema.

        Even when user has no permission to access the process of the parent
        data object.
        """
        anonymous_user = get_anonymous_user()
        main_process = Process.objects.get(slug="parent-process-schema")
        main_process.set_permission(Permission.VIEW, anonymous_user)
        input_process = Process.objects.get(slug="test-python-process-requirements")
        input_process.set_permission(Permission.NONE, anonymous_user)

        inputs = {
            "input1": self.run_process(input_process.slug).id,
            "input2": self.run_process(input_process.slug).id,
        }
        self.run_process(main_process.slug, inputs, contributor=anonymous_user)

    @with_docker_executor
    @tag_process("get-latest-process")
    def test_get_process(self):
        """Test process that gets process by slug.

        The catch here is that multiple objects are returned, as there can be
        multiple processes with the same slug but different versions. In such
        case the object with the process with the latest version must be
        returned.
        """
        process_slug = "multiple-versions"
        self.assertFalse(Process.objects.filter(slug=process_slug).exists())

        process1 = Process.objects.create(
            persistence=Process.PERSISTENCE_TEMP,
            version="1.0.0",
            slug=process_slug,
            contributor=self.admin,
        )
        data = self.run_process("get-latest-process", {"process_slug": process_slug})
        self.assertEqual(data.output["process_pk"], process1.pk)

        process2 = Process.objects.create(
            contributor=self.admin,
            persistence=Process.PERSISTENCE_TEMP,
            version="1.0.1",
            slug=process_slug,
        )
        data = self.run_process("get-latest-process", {"process_slug": process_slug})
        self.assertEqual(data.output["process_pk"], process2.pk)

    @with_docker_executor
    @tag_process("create-data")
    def test_create_data(self):
        """Test process that creates data object."""
        collection_name = "Python process collection"
        data_name = "Data name"
        self.assertFalse(Collection.objects.filter(name=collection_name).exists())
        self.run_process(
            "create-data", {"collection_name": collection_name, "data_name": data_name}
        )
        Collection.objects.get(name=collection_name)
        Data.objects.get(name=data_name)

    @with_docker_executor
    @tag_process("assign-entity-tags")
    def test_assign_tags_entity(self):
        """Assign tags to entity."""
        data = self.run_process(
            "assign-entity-tags",
            {
                "data_name": "data_name",
                "sample_name": "sample_name",
                "tags": ["first", "second"],
            },
        )
        self.assertEqual(data.name, "data_name")
        self.assertEqual(data.entity.name, "sample_name")
        self.assertEqual(data.entity.tags, ["first", "second"])

    @with_docker_executor
    @tag_process("change-entity-name")
    def test_change_entity_name(self):
        """Assign tags to entity."""

        entity = Entity.objects.create(name="Entity", contributor=self.user)
        process = Process.objects.get(slug="change-entity-name")
        process.set_permission(Permission.VIEW, self.user)

        data = self.run_process(
            "change-entity-name",
            {"entity_id": entity.pk, "entity_name": "New entity name"},
            contributor=self.user,
            assert_status=Data.STATUS_ERROR,
        )
        self.assertEqual(
            data.process_error,
            ["No objects match the given criteria or no permission to read object."],
        )
        entity.refresh_from_db()
        self.assertEqual(entity.name, "Entity")

        entity.set_permission(Permission.VIEW, self.user)
        data = self.run_process(
            "change-entity-name",
            {"entity_id": entity.pk, "entity_name": "New entity name"},
            contributor=self.user,
            assert_status=Data.STATUS_ERROR,
        )
        self.assertEqual(len(data.process_error), 1)

        self.assertTrue(
            f"No edit permission for entity with id {entity.pk}."
            in data.process_error[0]
        )
        entity.refresh_from_db()
        self.assertEqual(entity.name, "Entity")

        entity.set_permission(Permission.EDIT, self.user)
        data = self.run_process(
            "change-entity-name",
            {"entity_id": entity.pk, "entity_name": "New entity name"},
            contributor=self.user,
        )
        self.assertEqual(len(data.process_error), 0)
        entity.refresh_from_db()
        self.assertEqual(entity.name, "New entity name")

    @with_docker_executor
    @tag_process("change-entity-descriptor")
    def test_change_descriptor(self):
        """Assign descriptor to entity."""

        descriptor_schema = DescriptorSchema.objects.create(
            name="Descriptor schema",
            contributor=self.contributor,
            schema=[
                {
                    "name": "Description",
                    "type": "basic:string:",
                    "default": "default value",
                }
            ],
        )
        entity = Entity.objects.create(
            name="Entity", contributor=self.user, descriptor_schema=descriptor_schema
        )

        self.run_process(
            "change-entity-descriptor",
            {
                "entity_id": entity.pk,
                "description": "New description",
            },
        )
        entity.refresh_from_db()
        self.assertEqual(
            entity.descriptor,
            {"Description": "New description"},
        )


class PythonProcessRequirementsTest(ProcessTestCase):
    def setUp(self):
        super().setUp()
        self._register_schemas(
            processes_paths=[PROCESSES_DIR, WORKFLOWS_DIR],
            descriptors_paths=[DESCRIPTORS_DIR],
        )
        self.files_path = FILES_PATH

    @with_docker_executor
    @tag_process("test-python-process-requirements")
    def test_defaults(self):
        data = self.run_process("test-python-process-requirements")
        self.assertEqual(data.output["cores"], 2)
        self.assertEqual(data.output["memory"], 4096)
        self.assertEqual(data.output["storage"], 200)

    @with_docker_executor
    @override_settings(FLOW_PROCESS_MAX_CORES=1)
    @tag_process("test-python-process-requirements")
    def test_max_cores(self):
        data = self.run_process("test-python-process-requirements")
        self.assertEqual(data.output["cores"], 1)
        self.assertEqual(data.output["memory"], 4096)
        self.assertEqual(data.output["storage"], 200)

    @with_docker_executor
    @override_settings(
        FLOW_PROCESS_RESOURCE_OVERRIDES={
            "memory": {"test-python-process-requirements": 2048},
            "storage": {"test-python-process-requirements": 300},
        }
    )
    @tag_process("test-python-process-requirements")
    def test_resource_environment_override(self):
        data = self.run_process("test-python-process-requirements")
        self.assertEqual(data.output["cores"], 2)
        self.assertEqual(data.output["memory"], 2048)
        self.assertEqual(data.output["storage"], 300)

    @with_docker_executor
    @override_settings(
        FLOW_PROCESS_RESOURCE_OVERRIDES={
            "memory": {"test-python-process-requirements": 2048},
            "storage": {"test-python-process-requirements": 300},
        }
    )
    @tag_process("test-python-process-requirements")
    def test_resource_data_override(self):
        data = self.run_process(
            "test-python-process-requirements",
            process_resources={"cores": 3, "storage": 500, "memory": 50000},
        )
        self.assertEqual(data.output["cores"], 3)
        self.assertEqual(data.output["memory"], 50000)
        self.assertEqual(data.output["storage"], 500)


class PythonProcessDataBySlugTest(ProcessTestCase, LiveServerTestCase):
    def setUp(self):
        super().setUp()
        self._register_schemas(
            processes_paths=[PROCESSES_DIR], descriptors_paths=[DESCRIPTORS_DIR]
        )
        self.files_path = FILES_PATH

    @unittest.skipUnless(
        sys.platform.startswith("linux"),
        "Accessing live Resolwe host from a Docker container on non-Linux systems is not possible yet.",
    )
    @with_resolwe_host
    @with_docker_executor
    @tag_process("test-python-process-data-id-by-slug", "test-python-process-2")
    def test_process_data_by_slug(self):
        """Test that data object with json output can be given as input."""
        with self.preparation_stage():
            input_data = self.run_process("test-python-process-2")

        input_data = Data.objects.get(id=input_data.id)

        data = self.run_process(
            "test-python-process-data-id-by-slug",
            {
                "slug": input_data.slug,
            },
        )

        self.assertEqual(data.output["data_id"], input_data.pk)
