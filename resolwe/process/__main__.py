"""Python process runner utility."""
import inspect
import logging
import os
import sys
import types
from typing import Dict, Type

from .communicator import communicator
from .descriptor import ValidationError
from .models import Data
from .runtime import Process

# Id of the Data object we are processing.
DATA_ID = int(os.getenv("DATA_ID", "-1"))
logger = logging.getLogger(__name__)


if __name__ == "__main__":
    assert communicator is not None

    # Get the data object.
    data = Data(DATA_ID)
    module = types.ModuleType("python_program")
    python_program = communicator.get_python_program()
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
