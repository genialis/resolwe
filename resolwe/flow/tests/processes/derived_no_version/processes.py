from resolwe.process import IntegerField, Process


class BaseAbstractClass(Process):
    version = "1.2.3"
    requirements = {"expression-engine": "jinja"}
    process_type = "data:basic"
    abstract = True

    class Input:
        """Input fields."""

        num = IntegerField(label="Number")

    class Output:
        """Output fields."""

        parent_out = IntegerField(label="Number")

    def run(self, inputs, outputs):
        outputs.parent_out = inputs.num


class InheritedProcess(BaseAbstractClass):
    """Test simple process inheritance."""

    slug = "test-python-process-inheritance"
    name = "Inheritance test"
    version = "1.2.4"

    class Output:
        """Output fields."""

        children_out = IntegerField(label="Number")

    def run(self, inputs, outputs):
        outputs.children_out = inputs.num
