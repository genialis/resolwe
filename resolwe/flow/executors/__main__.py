"""Main standalone execution stub, used when the executor is run.

It should be run as a module with one argument: the relative module name
of the concrete executor class to use. The current working directory
should be where the ``executors`` module directory is, so that it can be
imported with python's ``-m <module>`` interpreter option.

Usage format:

.. code-block:: none

    /path/to/python -m executors .executor_type

Concrete example, run from the directory where ``./executors/`` is:

.. code-block:: none

    /venv/bin/python -m executors .docker

using the python from the ``venv`` virtualenv.

.. note::

    The startup code adds the concrete class name as needed, so that in
    the example above, what's actually instantiated is
    ``.docker.run.FlowExecutor``.
"""

import argparse
from importlib import import_module

from .protocol import ExecutorFiles  # pylint: disable=import-error
from .run import DATA, SETTINGS


def run_executor():
    """Start the actual execution; instantiate the executor and run."""
    parser = argparse.ArgumentParser(description="Run the specified executor.")
    parser.add_argument('module', help="The module from which to instantiate the concrete executor.")
    args = parser.parse_args()

    module_name = '{}.run'.format(args.module)
    class_name = 'FlowExecutor'

    module = import_module(module_name, __package__)
    executor = getattr(module, class_name)()
    with open(ExecutorFiles.PROCESS_SCRIPT, 'rt') as script_file:
        executor.run(DATA['id'], script_file.read(), SETTINGS['verbosity'])


run_executor()
