# pylint: disable=missing-docstring
import os

from resolwe.flow.models import Entity, Process, Storage
from resolwe.test import ProcessTestCase, tag_process, with_docker_executor

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
        process = Process.objects.filter(slug='test-python-process').latest()

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

    @with_docker_executor
    @tag_process('test-python-process')
    def test_python_process(self):
        with self.preparation_stage():
            input_data = self.run_process('test-save-number', {'number': 19})
            input_data.name = "bar"
            input_data.save()

            storage = Storage.objects.create(
                name="storage",
                contributor=self.user,
                data_id=input_data.pk,
                json={'value': 42}
            )

        data = self.run_process('test-python-process', {
            'my_field': "bar",
            'my_list': ["one", "two", "three"],
            'bar': input_data.pk,
            'url': {'url': "https://www.genialis.com"},
            'input_data': input_data.pk,
            'integer': 42,
            'my_float': 0.42,
            'my_json': storage.pk,
            'my_group': {
                'bar': 'my string',
                'foo': 21,
            }
        })

        self.assertFields(data, 'my_output', 'OK')
        self.assertFile(data, 'file_output', 'testfile.txt')

        # Non-deterministic output.
        del data.output['dir_output']['size']
        del data.output['dir_output']['total_size']

        self.assertEqual(data.output['dir_output'], {'dir': 'test/'})

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
