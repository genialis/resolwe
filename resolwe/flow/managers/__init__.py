""".. Ignore pydocstyle D400.

=============
Flow Managers
=============

Workflow workload managers.

.. data:: manager

    The global manager instance.

    :type: :class:`~resolwe.flow.managers.dispatcher.Manager`

.. automodule:: resolwe.flow.managers.dispatcher
    :members:
.. automodule:: resolwe.flow.managers.workload_connectors
.. automodule:: resolwe.flow.managers.listener
.. automodule:: resolwe.flow.managers.consumer
    :members:
.. automodule:: resolwe.flow.managers.utils
    :members:

"""

from .dispatcher import Manager
from .listener import ExecutorListener, basic_commands_plugin

# The basic_commands_plugin is exported to allow mocking in tests.
__all__ = ("manager", "listener", "basic_commands_plugin")

manager = Manager()
listener = ExecutorListener()
