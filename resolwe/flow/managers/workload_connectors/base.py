""".. Ignore pydocstyle D400.

==========
Base Class
==========

"""

from resolwe.flow.models import Data


class BaseConnector:
    """The abstract base class for workload manager connectors.

    The main :class:`~resolwe.flow.managers.dispatcher.Manager` instance
    in :data:`~resolwe.flow.managers.manager` uses connectors to handle
    communication with concrete backend workload management systems,
    such as Celery and SLURM. The connectors need not worry about how
    jobs are discovered or how they're prepared for execution; this is
    all done by the manager.
    """

    def submit(self, data: Data, argv):
        """Submit the job to the workload management system.

        :param data: The :class:`~resolwe.flow.models.Data` object that
            is to be run.
        :param argv: The argument vector used to spawn the executor.
        """
        raise NotImplementedError(
            "Subclasses of BaseConnector must implement a submit() method."
        )
