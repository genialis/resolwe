"""Python process runner utility."""

import inspect
import logging
import os
import sys
import tempfile
import types
from base64 import b64decode
from typing import Dict, Type

from communicator import communicator

# Id of the Data object we are processing.
DATA_ID = int(os.getenv("DATA_ID", "-1"))
logger = logging.getLogger(__name__)


if __name__ == "__main__":
    assert communicator is not None

    # Get the python runtime.
    python_runtime = communicator.get_python_runtime()
    tmpdir = tempfile.TemporaryDirectory()
    python_runtime_filename = os.path.join(tmpdir.name, "python_runtime.zip")
    with open(python_runtime_filename, "wb") as python_runtime_handle:
        python_runtime_handle.write(b64decode(python_runtime))

    sys.path.insert(0, python_runtime_filename)
    # From this line on the Python process runtime is available.

    from resolwe.process.descriptor import ValidationError
    from resolwe.process.models import Data
    from resolwe.process.runtime import Process

    python_program = communicator.get_python_program()
    # Get the data object.
    data = Data(DATA_ID)

    module = types.ModuleType("python_program")
    exec(python_program, module.__dict__)

    # Mapping between the process slug and the class containing the process
    # definition.
    processes: Dict[str, Type[Process]] = {}
    for variable in dir(module):
        value = getattr(module, variable)
        if (
            value == Process
            or not inspect.isclass(value)
            or not issubclass(value, Process)
            or "_abstract" in value.__dict__
        ):
            continue
        processes[value._meta.metadata.slug] = value

    try:
        process = processes[data.process.slug]
    except KeyError:
        print("Found the following processes in module '{}':".format(processes))
        print("")
        for slug, process in processes.items():
            print("  {} ({})".format(slug, getattr(process, "name")))
        print("")

        print("ERROR: Unable to find process '{}'.".format(data.process.slug))
        sys.exit(1)

    # Start the process.
    try:
        process(data).start()
    except ValidationError as error:
        print("ERROR: Output field validation failed: {}".format(error.args[0]))
        sys.exit(1)
