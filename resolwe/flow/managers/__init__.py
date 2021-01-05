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
from .listener import ExecutorListener

__all__ = ("manager", "listener")

manager = Manager()
listener = ExecutorListener()
