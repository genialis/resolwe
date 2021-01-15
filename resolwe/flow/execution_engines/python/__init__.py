"""An execution engine for Python processes."""
import inspect
import os
import shutil

from resolwe.flow.execution_engines.base import BaseExecutionEngine
from resolwe.process.parser import SafeParser
from resolwe.process.runtimes import python_runtimes_manager

PYTHON_RUNTIME_DIRNAME = "python_runtime"
PYTHON_RUNTIME_ROOT = "/"
PYTHON_RUNTIME_VOLUME = os.path.join(PYTHON_RUNTIME_ROOT, PYTHON_RUNTIME_DIRNAME)
PYTHON_PROGRAM_ROOT = "/"
PYTHON_PROGRAM_FILENAME = "python_process.py"
PYTHON_PROGRAM_VOLUME = os.path.join(PYTHON_PROGRAM_ROOT, PYTHON_PROGRAM_FILENAME)


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
        return 'PYTHONPATH="{runtime}" python3 -u -m resolwe.process {program} '.format(
            runtime=PYTHON_RUNTIME_VOLUME,
            program=PYTHON_PROGRAM_VOLUME,
        )

    def prepare_runtime(self, runtime_dir, data):
        """Prepare runtime directory.

        Preparation consists of:
        1. Iterating over registered python process runtimes and copy them to
           python runtime subdirectory of the runtime_dir.
        2. Copying the source code (aka program) to the file inside runtime
           directory.
        3. Returning mapping between Python runtimes and Python program.
        """
        source = data.process.run.get("program", "")
        parser = SafeParser(source)
        for base_class_name in parser.base_classes():
            for module in python_runtimes_manager.necessary_modules(base_class_name):
                src_dir = os.path.dirname(inspect.getsourcefile(module))
                dest_package_dir = os.path.join(
                    runtime_dir, PYTHON_RUNTIME_DIRNAME, *module.__name__.split(".")
                )
                shutil.copytree(src_dir, dest_package_dir)
                os.chmod(dest_package_dir, 0o755)

        # Write python source file.
        program_path = os.path.join(runtime_dir, PYTHON_PROGRAM_FILENAME)
        with open(program_path, "w") as file:
            file.write(source)
        os.chmod(program_path, 0o755)

        # Generate volume maps required to expose needed files.
        volume_maps = {
            PYTHON_RUNTIME_DIRNAME: PYTHON_RUNTIME_VOLUME,
            PYTHON_PROGRAM_FILENAME: PYTHON_PROGRAM_VOLUME,
        }
        return volume_maps
