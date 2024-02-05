"""Workflow execution engines."""

from resolwe.flow.engine import BaseEngine


class BaseExecutionEngine(BaseEngine):
    """A workflow execution engine."""

    def evaluate(self, data):
        """Return the code needed to compute a given Data object."""
        raise NotImplementedError

    def get_expression_engine(self, name):
        """Return an expression engine by its name."""
        return self.manager.get_expression_engine(name)

    def get_output_schema(self, process):
        """Return any additional output schema for the process."""
        return []

    def discover_process(self, path):
        """Perform process discovery in given path.

        This method will be called during process registration and
        should return a list of dictionaries with discovered process
        schemas.
        """
        return []
