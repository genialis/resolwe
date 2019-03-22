from resolwe.process import *


class PythonProcessDataIdBySlug(Process):
    """The process is used for testing get_data_id_by_slug."""
    slug = 'test-python-process-data-id-by-slug'
    name = "Test Python Process Data ID by Slug"
    version = '1.0.0'
    process_type = 'data:python:dataidbyslug'
    requirements = {
        'executor': {
            'docker': {
                'image': 'resolwe/base:ubuntu-18.04',
            }
        },
        'resources': {
            'network': True,
        }
    }

    class Input:
        """Input fields."""
        slug = StringField(label="Slug")

    class Output:
        data_id = IntegerField(label="Data ID")

    def run(self, inputs, outputs):
        data_id = self.get_data_id_by_slug(inputs.slug)
        outputs.data_id = data_id
