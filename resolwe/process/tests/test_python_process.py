# pylint: disable=missing-docstring
import os
import sys
import unittest

from django.contrib.auth.models import AnonymousUser
from django.test import LiveServerTestCase, override_settings

from guardian.shortcuts import assign_perm

from resolwe.flow.models import (
    Collection,
    Data,
    Entity,
    Process,
    Relation,
    RelationPartition,
    RelationType,
)
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
                "executor": {"docker": {"image": "resolwe/base:ubuntu-18.04",}},
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
                "my_group": {"bar": "my string", "foo": 21,},
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
        self.assertEqual(data.output["docker_image"], "resolwe/base:ubuntu-18.04")
        self.assertEqual(data.name, "Foo: bar")

        entity = Entity.objects.get(data=data)
        self.assertEqual(entity.data.first(), data)

    @with_docker_executor
    @tag_process("test-python-process-group-field")
    def test_python_process_group(self):
        # Run with explicitly given inputs.
        data = self.run_process(
            "test-python-process-group-field",
            {"my_group": {"foo": 123, "bar": "foobar",}, "my_group2": {"foo": 124,},},
        )
        self.assertFields(data, "out_foo", 123)
        self.assertFields(data, "out_bar", "foobar")
        self.assertFields(data, "out_foo2", 124)

        # Run with no inputs - check that default values are used.
        data = self.run_process("test-python-process-group-field")
        self.assertFields(data, "out_foo", 42)
        self.assertFalse(hasattr(data.output, "out_bar"))
        self.assertFalse(hasattr(data.output, "out_bar2"))

    @with_docker_executor
    @tag_process("test-python-process-json")
    def test_python_process_json(self):
        """Test that data object with json output can be given as input."""
        with self.preparation_stage():
            input_data = self.run_process("test-output-json")

        self.run_process("test-python-process-json", {"data": input_data.pk,})

    @with_docker_executor
    @tag_process("test-non-required-data-inputs")
    def test_non_required_data_input(self):
        """Test workflow with non-required data inputs"""
        with self.preparation_stage():
            input_data = self.run_process("test-output-json")

        self.run_process("test-non-required-data-inputs", {"data": input_data.pk,})

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
        with self.preparation_stage():
            # From collection 1
            start = self.run_process("entity-process")
            end = self.run_process("entity-process")

            # Set relation between the start and end object's entities.
            rel_type_series = RelationType.objects.create(name="series", ordered=True)
            relation = Relation.objects.create(
                contributor=self.contributor,
                collection=self.collection,
                type=rel_type_series,
                category="time-series",
                unit=Relation.UNIT_HOUR,
            )
            assign_perm("view_relation", self.contributor, relation)
            RelationPartition.objects.create(
                relation=relation, entity=start.entity, label="start", position=1,
            )
            RelationPartition.objects.create(
                relation=relation, entity=end.entity, label="end", position=2,
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

    @with_docker_executor
    @override_settings(FLOW_PROCESS_MAX_CORES=1)
    @tag_process("test-python-process-requirements")
    def test_max_cores(self):
        data = self.run_process("test-python-process-requirements")
        self.assertEqual(data.output["cores"], 1)
        self.assertEqual(data.output["memory"], 4096)

    @with_docker_executor
    @override_settings(
        FLOW_PROCESS_RESOURCE_OVERRIDES={
            "memory": {"test-python-process-requirements": 2048}
        }
    )
    @tag_process("test-python-process-requirements")
    def test_resource_override(self):
        data = self.run_process("test-python-process-requirements")
        self.assertEqual(data.output["cores"], 2)
        self.assertEqual(data.output["memory"], 2048)


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
        assign_perm("view_data", AnonymousUser(), input_data)

        data = self.run_process(
            "test-python-process-data-id-by-slug", {"slug": input_data.slug,}
        )

        self.assertEqual(data.output["data_id"], input_data.pk)
