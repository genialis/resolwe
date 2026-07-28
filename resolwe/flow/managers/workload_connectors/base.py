""".. Ignore pydocstyle D400.

==========
Base Class
==========

"""

from typing import Dict, Optional, Sequence

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

    def is_active(self, data: Data) -> Optional[bool]:
        """Check if the task for the given data object can still run.

        The listener uses this to requeue data objects that were submitted to
        the workload management system but whose task has vanished (for
        instance when the manager was killed in the middle of the submission
        or when the task was lost due to an external failure).

        :returns: ``True`` when the task is known to be queued or running,
            ``False`` when it is known that the task does not exist or can
            never run again, and ``None`` when this cannot be determined.
            Objects whose state cannot be determined are never requeued, so
            connectors without task introspection must return ``None``.
        """
        return None

    def is_active_bulk(self, data_objects: Sequence[Data]) -> Dict[int, Optional[bool]]:
        """Check the task state for multiple data objects at once.

        Connectors that can answer for many tasks with a single query (for
        instance Kubernetes) should override this method; the default
        implementation checks every object separately.

        :returns: the mapping from the data object primary key to the task
            state as returned by :meth:`is_active`.
        """
        return {data.pk: self.is_active(data) for data in data_objects}
