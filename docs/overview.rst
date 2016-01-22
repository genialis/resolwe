========
Overview
========

Resolwe consists of two major components: RESTful API and Dataflow Engine. The
RESTful API is based on the `Django REST Framework`_. It offers complete
control of the dataflow, data and permissions. The Dataflow Engine handles
pipeline execution. It resolves dependencies between `processes` (jobs or
tasks), and executes them on worker nodes. Results are saved to the PostgreSQL
database and clustered file system.

.. _Django REST Framework: http://www.django-rest-framework.org

.. figure:: images/resolwe_02_internals.png

The Dataflow Engine has several layers of execution that can be configured
either on the server or by the individual processes.

.. figure:: images/resolwe_03_dataflow.png

Processes can be executed on a server cluster. In this case the Executor,
Runtime and Expression Engine layers span over multiple worker nodes.

.. figure:: images/resolwe_04_workers.png

Resolwe can be configured for a lightweight desktop use (`e.g.,` by
bioinformatics professionals) or deployed as a complex set-up of multiple
servers and worker nodes. In addition to the components described above,
customizing the configuration of the web server (`e.g.,` NGINX or Apache HTTP
Server), workload manager, and the database offer high scaling potential.

Example of a lightweight configuration: synchronous workload manager that runs
locally, Docker executor and runtime, Django web server, and local file system.

Example of a complex deploy: Slurm workload manager with a range of
computational nodes, Docker executor and runtime on each worker node, NGINX web
server, and a fast file system shared between worker nodes.
