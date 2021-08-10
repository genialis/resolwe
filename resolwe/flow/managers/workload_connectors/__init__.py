""".. Ignore pydocstyle D400.

===================
Workload Connectors
===================

The workload management system connectors are used as glue between the
Resolwe Manager and various concrete workload management systems that
might be used by it. Since the only functional requirement is job
submission, they can be simple and nearly contextless.

.. automodule:: resolwe.flow.managers.workload_connectors.base
    :members:
.. automodule:: resolwe.flow.managers.workload_connectors.local
    :members:
.. automodule:: resolwe.flow.managers.workload_connectors.celery
    :members:
.. automodule:: resolwe.flow.managers.workload_connectors.slurm
    :members:
.. automodule:: resolwe.flow.managers.workload_connectors.kubernetes
    :members:

"""
