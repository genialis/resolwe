"""An execution engine for Python processes."""
import os

from resolwe.flow.execution_engines.base import BaseExecutionEngine
from resolwe.process.parser import SafeParser

PYTHON_RUNTIME_DIRNAME = "python_runtime"
PYTHON_RUNTIME_ROOT = "/"
PYTHON_RUNTIME_VOLUME = os.path.join(PYTHON_RUNTIME_ROOT, PYTHON_RUNTIME_DIRNAME)


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
        parser = SafeParser(open(path).read())
        processes = parser.parse()
        return [process.to_schema() for process in processes]

    def evaluate(self, data):
        """Evaluate the code needed to compute a given Data object."""
        return 'PYTHONPATH="{runtime}" python3 -u -m resolwe.process'.format(
            runtime=PYTHON_RUNTIME_VOLUME
        )

    def prepare_volumes(self):
        """Mount additional volumes."""
        return {PYTHON_RUNTIME_DIRNAME: PYTHON_RUNTIME_VOLUME}
