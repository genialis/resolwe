"""An execution engine for Python processes."""

from pathlib import Path

from resolwe.flow.execution_engines.base import BaseExecutionEngine
from resolwe.flow.executors.constants import BOOTSTRAP_PYTHON_RUNTIME
from resolwe.process.parser import SafeParser


class ExecutionEngine(BaseExecutionEngine):
    """An execution engine that outputs bash programs."""

    name = "python"

    def discover_process(self, path):
        """Perform process discovery in given path.

        This method will be called during process registration and
        should return a list of dictionaries with discovered process
        schemas.
        """
        if not path.lower().endswith(".py"):
            return []
        with open(path) as handle:
            parser = SafeParser(handle.read())
            processes = parser.parse()
            return [process.to_schema() for process in processes]

    def evaluate(self, data):
        """Evaluate the code needed to compute a given Data object."""
        bootstrap_filename = Path("/") / BOOTSTRAP_PYTHON_RUNTIME
        return f"python3 {bootstrap_filename}"
