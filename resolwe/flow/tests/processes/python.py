from resolwe.process import *


class PythonProcess(Process):
    """This is a process description."""
    slug = 'test-python-process'
    name = "Test Python Process"
    process_type = 'data:python'
    version = '0.1.2'
    category = 'analyses'
    scheduling_class = SchedulingClass.BATCH
    requirements = {
        'executor': {
            'docker': {
                'image': 'resolwe/base:ubuntu-18.04',
            }
        }
    }

    class Input:
        """Input fields."""
        my_field = StringField(label="My field")
        my_list = ListField(StringField(), label="My list")
        input_data = DataField('test:save', label="My input data")
        bar = DataField(data_type='test:save', label="My bar")
        url = UrlField(UrlField.DOWNLOAD, label="My URL")
        integer = IntegerField(label="My integer")
        my_float = FloatField(label="My float")
        my_json = JsonField(label="Blah blah")
        my_optional = StringField(label="Optional", required=False, default='default value')

        class MyGroup:
            bar = StringField(label="Bar")
            foo = IntegerField(label="Foo")

        my_group = GroupField(MyGroup, label="My group")

    class Output:
        my_output = StringField(label="My output")

    def run(self, inputs, outputs):
        print('All inputs are:', inputs)
        print('Input data:', inputs.input_data)
        print('Input data ID:', inputs.input_data.id)
        print('Input data number output:', inputs.input_data.number)
        print('Input data type:', inputs.input_data.type)
        print('Input data descriptor:', inputs.input_data.descriptor)
        print('Group bar:', inputs.my_group.bar)
        print('Group foo:', inputs.my_group.foo)

        bar = Cmd['ls']['-l', '-a', '/'] | Cmd['grep']['python']
        print('hello world:\n', bar())

        outputs.my_output = 'OK'
