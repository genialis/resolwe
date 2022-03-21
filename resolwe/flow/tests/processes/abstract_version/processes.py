from resolwe.process import Process


class BaseAbstractClassNoVersion(Process):
    """The version in abstract class must be defined."""

    requirements = {"expression-engine": "jinja"}
    process_type = "data:basic"
    abstract = True

    def run(self, inputs, outputs):
        pass
