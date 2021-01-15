"""Python process runner utility."""
import argparse
import inspect
import logging
import os
import sys
from importlib import import_module
from pathlib import Path
from typing import Dict, Type

from .descriptor import ValidationError
from .models import Data
from .runtime import Process

# Id of the Data object we are processing.
DATA_ID = int(os.getenv("DATA_ID", "-1"))
logger = logging.getLogger(__name__)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run a Resolwe Python proces")
    parser.add_argument("filename", type=str, help="Python process filename to run")
    args = parser.parse_args()

    # Get the data object.
    data = Data(DATA_ID)

    # Switch to target directory to import the module.
    try:
        # Insert empty string as first path argument due to the issue
        # https://bugs.python.org/issue33053
        sys.path.insert(0, "")
        filename = Path(args.filename).resolve()
        start_dir = Path.cwd()
        os.chdir(filename.parent)
        module = import_module(filename.stem, __package__)
        os.chdir(start_dir)
    except (OSError, ImportError):
        logger.exception("Failed to load Python process from %s.", args.filename)
        sys.exit(1)

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
        print("Found the following processes in module '{}':".format(args.module))
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
