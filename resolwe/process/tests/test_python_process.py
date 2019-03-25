# pylint: disable=missing-docstring
import os

from django.contrib.auth.models import AnonymousUser
from django.test import LiveServerTestCase

from guardian.shortcuts import assign_perm

from resolwe.flow.models import Data, Entity, Process
from resolwe.test import ProcessTestCase, tag_process, with_docker_executor, with_resolwe_host

PROCESSES_DIR = os.path.join(os.path.dirname(__file__), 'processes')
DESCRIPTORS_DIR = os.path.join(os.path.dirname(__file__), 'descriptors')
FILES_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'files')


class PythonProcessTest(ProcessTestCase):
    def setUp(self):
        super().setUp()
        self._register_schemas(processes_paths=[PROCESSES_DIR], descriptors_paths=[DESCRIPTORS_DIR])
        self.files_path = FILES_PATH

    @with_docker_executor
    def test_registration(self):
        process = Process.objects.get(slug='test-python-process')

        self.assertEqual(process.slug, 'test-python-process')
        self.assertEqual(process.name, 'Test Python Process')
        self.assertEqual(process.version, '0.1.2')
        self.assertEqual(process.type, 'data:python:')
        self.assertEqual(process.category, 'analyses:')
        self.assertEqual(process.persistence, Process.PERSISTENCE_RAW)
        self.assertEqual(process.description, 'This is a process description.')
        self.assertEqual(process.data_name, 'Foo: {{input_data | name}}')
        self.assertEqual(process.entity_type, 'sample')
        self.assertEqual(process.entity_descriptor_schema, 'sample')
        self.assertEqual(process.entity_input, 'input_data')
        self.assertEqual(process.requirements, {
            'expression-engine': 'jinja',
            'executor': {
                'docker': {
                    'image': 'resolwe/base:ubuntu-18.04',
                }
            }
        })
        self.assertEqual(process.scheduling_class, Process.SCHEDULING_CLASS_BATCH)

        for field in process.input_schema:
            if field['name'] == 'my_group':
                self.assertEqual(field['group'][0]['name'], 'foo')
                self.assertEqual(field['group'][1]['name'], 'bar')
                break
        else:
            self.fail("Field my_group not found in test-python-process")

        # Make sure that process with inheritance from `module.Class` is also registered.
        process = Process.objects.get(slug='test-python-process-2')

    @with_docker_executor
    @tag_process('test-python-process')
    def test_python_process(self):
        with self.preparation_stage():
            input_data = self.run_process('test-save-number', {'number': 19})
            input_data.name = "bar"
            input_data.save()

            input_entity = self.run_process('entity-process')

            storage = input_data.storages.create(
                name="storage",
                contributor=self.user,
                json={'value': 42}
            )

        data = self.run_process('test-python-process', {
            'my_field': "bar",
            'my_list': ["one", "two", "three"],
            'bar': input_data.pk,
            'url': {'url': "https://www.genialis.com"},
            'input_data': input_data.pk,
            'input_entity_data': input_entity.pk,
            'integer': 42,
            'my_float': 0.42,
            'my_json': storage.pk,
            'my_group': {
                'bar': 'my string',
                'foo': 21,
            }
        })

        self.assertFields(data, 'string_output', 'OK')
        self.assertFields(data, 'list_string_output', ['foo', 'bar'])
        self.assertFile(data, 'file_output', 'testfile.txt')
        self.assertFiles(data, 'list_file_output', ['testfile.txt', 'testfile2.txt'])

        # Non-deterministic output.
        del data.output['dir_output']['size']
        del data.output['dir_output']['total_size']

        self.assertEqual(data.output['dir_output'], {'dir': 'test/'})
        self.assertEqual(data.output['input_entity_name'], 'Data with entity')
        self.assertEqual(data.output['docker_image'], 'resolwe/base:ubuntu-18.04')
        self.assertEqual(data.name, 'Foo: bar')

        entity = Entity.objects.get(data=data)
        self.assertEqual(entity.data.first(), data)

    @with_docker_executor
    @tag_process('test-python-process-json')
    def test_python_process_json(self):
        """Test that data object with json output can be given as input."""
        with self.preparation_stage():
            input_data = self.run_process('test-output-json')

        self.run_process('test-python-process-json', {
            'data': input_data.pk,
        })


class PythonProcessDataBySlugTest(ProcessTestCase, LiveServerTestCase):

    def setUp(self):
        super().setUp()
        self._register_schemas(processes_paths=[PROCESSES_DIR], descriptors_paths=[DESCRIPTORS_DIR])
        self.files_path = FILES_PATH

    @with_resolwe_host
    @with_docker_executor
    @tag_process('test-python-process-data-id-by-slug', 'test-python-process-2')
    def test_process_data_by_slug(self):
        """Test that data object with json output can be given as input."""
        with self.preparation_stage():
            input_data = self.run_process('test-python-process-2')

        input_data = Data.objects.get(id=input_data.id)
        assign_perm("view_data", AnonymousUser(), input_data)

        data = self.run_process('test-python-process-data-id-by-slug', {
            'slug': input_data.slug,
        })

        self.assertEqual(data.output['data_id'], input_data.pk)
