from resolwe.process import Process, StringField


class PythonProcessExample(Process):
    """This process is used for testing of autoprocess Sphinx directive."""

    slug = "test-autoprocess-python"
    name = "Test autoprocess Python"
    version = "1.0.0"
    process_type = "data:python:autoprocess"

    class Input:
        """Input fields."""

        foo = StringField(label="Foo")

    class Output:

        foo = StringField(label="Foo")

    def run(self, inputs, outputs):
        outputs.foo = inputs.foo
