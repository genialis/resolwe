"""An execution engine for Python processes."""
import copy
import inspect
import json
import os
import shlex
import shutil

from resolwe.flow.execution_engines.base import BaseExecutionEngine
from resolwe.flow.models.utils import hydrate_input_references, hydrate_input_uploads
from resolwe.process.parser import SafeParser

PYTHON_RUNTIME_DIRNAME = "python_runtime"
PYTHON_RUNTIME_ROOT = "/"
PYTHON_RUNTIME_VOLUME = os.path.join(PYTHON_RUNTIME_ROOT, PYTHON_RUNTIME_DIRNAME)
PYTHON_PROGRAM_ROOT = "/"
PYTHON_PROGRAM_FILENAME = "python_process.py"
PYTHON_PROGRAM_VOLUME = os.path.join(PYTHON_PROGRAM_ROOT, PYTHON_PROGRAM_FILENAME)
PYTHON_INPUTS_FILENAME = "inputs.json"
PYTHON_INPUTS_ROOT = "/"
PYTHON_INPUTS_VOLUME = os.path.join(PYTHON_INPUTS_ROOT, PYTHON_INPUTS_FILENAME)
PYTHON_REQUIREMENTS_FILENAME = "requirements.json"
PYTHON_REQUIREMENTS_ROOT = "/"
PYTHON_REQUIREMENTS_VOLUME = os.path.join(
    PYTHON_INPUTS_ROOT, PYTHON_REQUIREMENTS_FILENAME
)


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
        return (
            'PYTHONPATH="{runtime}" python3 -u -m resolwe.process {program} '
            "--slug {slug} "
            "--inputs {inputs} "
            "--requirements {requirements}".format(
                runtime=PYTHON_RUNTIME_VOLUME,
                program=PYTHON_PROGRAM_VOLUME,
                slug=shlex.quote(data.process.slug),
                inputs=PYTHON_INPUTS_VOLUME,
                requirements=PYTHON_REQUIREMENTS_VOLUME,
            )
        )

    def prepare_runtime(self, runtime_dir, data):
        """Prepare runtime directory."""
        # Copy over Python process runtime (resolwe.process).
        import resolwe.process as runtime_package

        src_dir = os.path.dirname(inspect.getsourcefile(runtime_package))
        dest_package_dir = os.path.join(
            runtime_dir, PYTHON_RUNTIME_DIRNAME, "resolwe", "process"
        )
        shutil.copytree(src_dir, dest_package_dir)
        os.chmod(dest_package_dir, 0o755)

        # Write python source file.
        source = data.process.run.get("program", "")
        program_path = os.path.join(runtime_dir, PYTHON_PROGRAM_FILENAME)
        with open(program_path, "w") as file:
            file.write(source)
        os.chmod(program_path, 0o755)

        # Write serialized inputs.
        inputs = copy.deepcopy(data.input)
        hydrate_input_references(inputs, data.process.input_schema)
        hydrate_input_uploads(inputs, data.process.input_schema)
        inputs_path = os.path.join(runtime_dir, PYTHON_INPUTS_FILENAME)

        # XXX: Skip serialization of LazyStorageJSON. We should support
        # LazyStorageJSON in Python processes on the new communication protocol
        def default(obj):
            """Get default value."""
            class_name = obj.__class__.__name__
            if class_name == "LazyStorageJSON":
                return ""

            raise TypeError(f"Object of type {class_name} is not JSON serializable")

        with open(inputs_path, "w") as file:
            json.dump(inputs, file, default=default)

        # Write serialized requirements.
        # Include special 'requirements' variable in the context.
        requirements = copy.deepcopy(data.process.requirements)
        # Inject default values and change resources according to
        # the current Django configuration.
        requirements["resources"] = data.process.get_resource_limits()
        requirements_path = os.path.join(runtime_dir, PYTHON_REQUIREMENTS_FILENAME)

        with open(requirements_path, "w") as file:
            json.dump(requirements, file)

        # Generate volume maps required to expose needed files.
        volume_maps = {
            PYTHON_RUNTIME_DIRNAME: PYTHON_RUNTIME_VOLUME,
            PYTHON_PROGRAM_FILENAME: PYTHON_PROGRAM_VOLUME,
            PYTHON_INPUTS_FILENAME: PYTHON_INPUTS_VOLUME,
            PYTHON_REQUIREMENTS_FILENAME: PYTHON_REQUIREMENTS_VOLUME,
        }

        return volume_maps
