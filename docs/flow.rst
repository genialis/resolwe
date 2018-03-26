===================
Resolwe Flow Design
===================

The Resolwe Flow workflow engine comprises the execution framework and other
layers which make up the internal data model and facilitate dependency
resolution, permissions enforcement and process filtering.

Overview
========

Execution Framework
-------------------

Flow consists of a number of active services which need to be running before job
execution can proceed.

The core message transport and coordination facility, as currently used, is
`Redis`_. It serves as the central status hub for keeping track of shared
dynamic information used by parts of the framework, and as a contact point for
those parts of the framework that run remotely. These connect to well-known
'channels' (specially named Redis list objects), into which they can deposit
JSON-formatted messages and commands.

.. _Redis: https://redis.io

Flow's execution manager, or just the 'manager', is an independent service which
runs as a `Django Channels`_ event consumer. When objects are added to the
database to be executed, they will trigger events for the appropriate channels.
These will be processed by the manager, which will carry out all the preparatory
tasks necessary to start execution and then commuicate with a concrete workload
management system so that the job can eventually be scheduled and run on a
worker node.

.. _Django Channels: https://github.com/django/channels

Finally, the jobs are executed by the aptly named 'executors'. These are run on
worker nodes and act as local execution managers: preparing a controlled
execution environment, running the job's code, collecting results and
communicating them back to the manager which stores them in the database.

Utility Layers
--------------

Django's facilities are used for interfacing with the database, thus all models
used in Flow are actually Django Model objects. The most important two models
are the `Data` model and the `Process` model.

A Data object represents a single instance of data to be processed, i.e. a node
in the flow graph being executed.  It contains properties which mainly concern
execution, such as various process and task IDs, output statuses and the results
produced by executors.

A Process object represents the way in which its Data object will be 'executed',
i.e. the type of node in the flow graph and the associated code. It contains
properties defining its relationship to other nodes in the graph currently being
executed, the permissions that control access rights for users and other
processes, and the actual code that is run by the executors.

The code in the process object can be either final code that is already ready
for execution, or it can be a form of template, for which an 'expression engine'
is needed. An expression engine (the only one currently in use is `Jinja`_)
pre-processes the process' code to produce text that can then be executed by an
'execution engine'.

.. _Jinja: http://jinja.pocoo.org

An execution engine is, simply put, the interpreter that will run the processed
code, just after an additional metadata discovery step. It is done by the
execution engine because the encoding might be language-dependent. The
properties to be discovered include process resource limits, secret
requirements, etc. These properties are passed on to the executor, so that it
can properly set up the target environment. The only currently supported
execution engine is Bash.

Technicalities
==============

The Manager
-----------

Being a Django Channels consumer application, the Flow Manager is entirely
event-driven and mostly contextless. The main input events are data object
creation, processing termination and intermediate executor messages. Once run,
it consists of two distinct servers and a modularized connection framework used
to interface with workload managers used by the deployment site.

Dispatcher
^^^^^^^^^^

This is the central job scheduler. On receipt of an appropriate event through
Django Channels (in this service, only data object creation and processing
termination), the dispatcher will scan the database for outstanding data
objects. For each object found to still not be processed, dependencies will be
calculated and scanned for completion. If all the requirements are satisfied,
its execution cycle will commence. The manager-side of this cycle entails job
pre-processing and a part of the environment preparation steps:

- The data object's process is loaded, its code preprocessed with the
  configured expression engine and the result of that fed into the selected
  execution engine to discover further details about the process' environemntal
  requirements (resource limits).

- The runtime directories on the global shared file system are prepared: file
  dependencies are copied out from the database, the process' processed code
  (as output by the expression engine) is stored into a file that the executor
  will run.

- The executor platform is created by copying the Flow Executor source code to
  the destination (per-data) directories on the shared file system, along with
  serialized (JSON) settings and support metadata (file lists, directory paths,
  Docker configuration and other information the configured executor will need
  for execution).

- After all this is done, control is handed over to the configured 'workload
  connector', see below for a description.

Listener
^^^^^^^^

As the name might imply to some, the purpose of the listener is to listen for
status updates and distressing thoughts sent by executors. The service itself is
an independent (`i.e.` not Django Channels-based) process which waits for events to
arrive on the executor contact point channels in Redis.

The events are JSON-formatted messages and include:

- processing status updates, such as execution progress and any computed output
  values,

- spawn commands, with which a process can request the creation of new data
  objects,

- execution termination, upon which the listener will finalize the Data object
  in question: delete temporary files from the global shared file system, update
  process exit code fields in the database, store the process' standard output
  and standard error sent by the executor and notify the dispatcher about the
  termination, so that any state internal to it may be updated properly,

- ancillary status updates from the executor, such as logging. Because executors
  are running remotely with respect to the manager's host machine, they may not
  have access to any centralized logging infrastructure, so the listener is used
  as a proxy.

Workload Connectors
^^^^^^^^^^^^^^^^^^^

Workload connectors are thin glue libraries which communicate with the concrete
workload managers used on the deployment site. The dispatcher only contains
logic to prepare execution environments and generate the command line necessary
to kick off an executor instance. The purpose of the workload connector is to
submit that command line to the workload manager which will then execute it on
one of its worker nodes.  The currently supported workload managers are
`Celery`_, `SLURM`_ and a simple local dummy for test environments.

.. _Celery: http://www.celeryproject.org

.. _SLURM: https://slurm.schedmd.com

The Executor
------------

The Flow Executor is the program that controls Process execution on a worker
node managed by the site workload manager, for which it is a job. Depending on
the configured executor, it further prepares an execution environment,
configures runtime limitations enforced by the system and spawns the code in the
Process object. The currently supported executor types are a simple local
executor for testing deployments and a `Docker`_-based one.

.. _Docker: https://www.docker.com

Once started, the executor will carry out any additional preparation based on
its type (`e.g.` the Docker executor constructs a command line to create an
instance of a pre-prepared Docker container, with all necessary file system
mappings and communication conduits). After that, it executes the Process code
as prepared by the manager, by running a command to start it (this need not be
anything more complicated than a simple `subprocess.Popen`).

Following this, the executor acts as a proxy between the process and the
database by relaying messages generated by the process to the manager-side
listener. When the process is finished (or when it terminates abnormally), the
executor will send a final cleanup message and terminate, thus finishing the job
from the point of view of the workload manager.

Example Execution, from Start to Finish
=======================================

- Flow services are started: the dispatcher Django Channels application and the
  listener process.

- The user, through any applicable intricacy, creates a Data object.

- Django signals will fire on creation and submit a data scan event to the
  dispatcher through Django Channels.

- The dispatcher will scan the database for outstanding data objects
  (alternatively, only for a specific one, given an ID). The following steps are
  then performed for each discovered data object whose dependencies are all
  processed:

- The runtime directory is populated with data files, executor source and
  configuration files.

- The process code template is run through an expression engine to transform it
  into executable text. This is also scanned with an execution engine to
  discover runtime resource limits and other process-local configuration.

- A command line is generated which can be run on a processing node to start an
  executor.

- The command line is handed over to a workload connector, which submits it as a
  job to the workload manager installed on the site.

- At this point, the dispatcher's job for this data object is done. Eventually,
  the workload manager will start processing the submitted job, thereby spawning
  an executor.

- The executor will prepare a safe runtime context, such as a Docker container,
  configure it with appropriate communication channels (stdin/out redirection or
  sockets) and run the command to execute the process code.

- The code executes, periodically generating status update messages. These are
  received by the executor and re-sent to the listener. The listener responds
  appropriately, updating database fields for the data object, notifying the
  dispatcher about lifetime events or forwarding log messages to any configured
  infrastructure.

- Once the process is done, the executor will send a finalizing command to the
  listener and terminate.

- The listener will notify the dispatcher about the termination and finalize the
  database status of this data object (processing exit code, outputs).

- The dispatcher will update processing states and statistics, and re-scan the
  database for data objects which might have dependencies on the one that just
  finished and could therefore potentially be started up.
