from resolwe.process import *


class PythonProcessJson(Process):
    """This is a process description."""
    slug = 'test-python-process-json'
    name = "Python Process that uses JSON field"
    process_type = 'data:python'
    version = '0.1.2'
    requirements = {
        'executor': {
            'docker': {
                'image': 'resolwe/base:ubuntu-18.04',
            }
        }
    }

    class Input:
        """Input fields."""
        data = DataField('test', label="My input data")
        data2 = DataField('test', label="My second non-required input data", required=False)

    def run(self, inputs, outputs):
        print('Input data:', inputs.data)
        print('Input data:', inputs.data.storage)
