"""Python process runner utility."""
import argparse
import inspect
import json
import os
import sys
from importlib import import_module

from .runtime import Inputs, Process, ValidationError

if __name__ == '__main__':
    # pylint: disable=invalid-name
    parser = argparse.ArgumentParser(description="Run a Resolwe Python proces")
    parser.add_argument('filename', type=str,
                        help="Python process filename to run")
    parser.add_argument('--slug', type=str,
                        help="Slug of the process to run (required if multiple processes are defined)")
    parser.add_argument('--inputs', type=str,
                        help="Path to input JSON file")
    parser.add_argument('--requirements', type=str,
                        help="Path to requirements JSON file")
    args = parser.parse_args()

    # Switch to target directory to import the module.
    try:
        filename = os.path.realpath(args.filename)
        start_dir = os.getcwd()
        os.chdir(os.path.dirname(filename))
        module, _ = os.path.splitext(os.path.basename(filename))
        module = import_module(module, __package__)
        os.chdir(start_dir)
    except (OSError, ImportError):
        print("ERROR: Failed to load Python process from '{}'.".format(args.filename))
        sys.exit(1)

    processes = {}
    for variable in dir(module):
        value = getattr(module, variable)
        if value == Process or not inspect.isclass(value) or not issubclass(value, Process):
            continue

        processes[value._meta.metadata.slug] = value  # pylint: disable=protected-access

    if args.slug:
        try:
            process = processes[args.slug]
        except KeyError:
            print("Found the following processes in module '{}':".format(args.module))
            print("")
            for slug, process in processes.items():
                print("  {} ({})".format(slug, process._meta.metadata.name))  # pylint: disable=protected-access
            print("")

            print("ERROR: Unable to find process '{}'.".format(args.slug))
            sys.exit(1)
    elif len(processes) == 1:
        process = next(iter(processes.values()))
    else:
        print("Found the following processes in module '{}':".format(args.module))
        print("")
        for slug, process in processes.items():
            print("  {} ({})".format(slug, process._meta.metadata.name))  # pylint: disable=protected-access
        print("")

        print("ERROR: Unable to determine which process to run. Pass --slug option.")
        sys.exit(1)

    # Prepare process inputs.
    try:
        inputs = Inputs(process._meta.inputs)  # pylint: disable=protected-access
        if args.inputs:
            with open(args.inputs) as inputs_file:
                data = json.load(inputs_file)

            for key, value in data.items():
                setattr(inputs, key, value)
    except ValidationError as error:
        print("ERROR: Input field validation failed: {}".format(error.args[0]))
        sys.exit(1)

    # Prepare process requirements.
    requirements = None
    try:
        if args.requirements:
            with open(args.requirements) as requirements_file:
                requirements = json.load(requirements_file)
    except Exception as error:  # pylint: disable=broad-except
        print("ERROR: Requirements failed to load: {}".format(str(error)))
        sys.exit(1)

    # TODO: Configure logging.

    # Start the process.
    instance = process()
    try:
        if requirements is not None:
            instance._meta.metadata.requirements = requirements  # pylint: disable=protected-access

        instance.start(inputs)
    except ValidationError as error:
        print("ERROR: Output field validation failed: {}".format(error.args[0]))
        sys.exit(1)
