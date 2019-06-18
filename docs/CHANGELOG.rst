##########
Change Log
##########

All notable changes to this project are documented in this file.
This project adheres to `Semantic Versioning <http://semver.org/>`_.


==========
Unreleased
==========

Changed
-------
- **BACKWARD INCOMPATIBLE:** Remove ``parents`` and ``children`` query filters
  from Data API endpoint

Added
-----
- ``/api/data/:id/parents`` and ``/api/data/:id/children`` API endpoints for
  listing parents and children Data objects of the object with given ``id``
- Add ``entity_always_create`` field to ``Process`` model

Fixed
-----
- Make sure that Elasticsearch index exists before executing a search query


===================
17.0.0 - 2019-06-17
===================

Changed
-------
- **BACKWARD INCOMPATIBLE:** Use Elasticsearch version 6.x
- **BACKWARD INCOMPATIBLE:** Bump ``Django`` requirement to version ``2.2``
- **BACKWARD INCOMPATIBLE:** Remove not used ``django-mathfilters``
  requirement

Added
-----
- Support Python 3.7
- Support forward and reverse many-to-one relations in Elasticsearch
- Add ``collection_names`` field to ``DataSerializer``
- Add test methods  to ``ProcessTestCase`` that assert directory structure and
  content: ``assertDirExists``, ``assertDir``, and ``assertDirStructure``
- Add ``upload-dir`` process


===================
16.0.1 - 2019-04-29
===================

Fixed
-----
- Pin ``django-priority-batch`` to version ``1.1`` to fix compatibility issues


===================
16.0.0 - 2019-04-16
===================

Changed
-------
- **BACKWARD INCOMPATIBLE:** Access to DataField members (in Python process
  input) changed from dict to Python objects. For example,
  ``input_field.file_field['name']`` changed to
  ``input_field.file_field.path``.
- **BACKWARD INCOMPATIBLE:** Filters that are based on ``django-filter``
  ``FilterSet`` now use dict-declaring-syntax. This requires that subclasses
  of respective filters modify their syntax too.
- Interactively save results in Python processes

Added
-----
- Add get_data_id_by_slug method to Python processes' Process class
- Python process syntax enhancements:

  - Support ``.entity_name`` in data inputs
  - Easy access to process resources through ``self.resources``
- Raise error if ViewSet receives invalid filter parameter(s)
- Report process error for exceptions in Python processes
- Report process error if spawning fails
- Automatically export files for spawned processes (in Python process syntax)
- Import files of Python process FileField inputs (usage:
  `inputs.src.import_file()`)

Fixed
-----
- Interactively write to standard output within Python processes
- Fix writing to integer and float output fields
- Allow non-required ``DataField`` as Python process input


===================
15.0.1 - 2019-03-19
===================

Fixed
-----
- Fix storage migration to use less memory


===================
15.0.0 - 2019-03-19
===================

Changed
-------
- Log plumbum commands to standard output
- Change storage data relation from many-to-one to many-to-many
- Moved ``purged`` field from ``Data`` to ``DataLocation`` model

Added
-----
- Add ``run_process`` method to ``Process`` to support triggering
  of a new process from the running Python process
- Add DataLocation model and pair it with Data model to handle data location
- Add ``entity_names`` field to ``DataSerializer``
- Support duplication of ``Data``, ``Entity`` and ``Collection``
- Support moving entities between collections
- Support relations requirement in process syntax


===================
14.4.0 - 2019-03-07
===================

Changed
-------
- Purge processes only not jet purged Data objects

Fixed
-----
- Allow references to missing Data objects in the output of finished Data
  objects, as we don't have the control over what (and when) is deleted


===================
14.3.0 - 2019-02-19
===================

Added
-----
- Add ``scheduled`` field to ``Data`` objects to store the date when object
  was dispatched to the scheduling system
- Add ``purge`` field to ``Data`` model that indicates whether ``Data`` object
  was processed by ``purge``

Fixed
-----
- Make Elasticsearch build arguments cache thread-safe and namespace cache
  keys to make sure they don't interfere
- Trigger the purge outside of the transaction, to make sure the Data object
  is commited in the database when purge worger grabs it


===================
14.2.0 - 2019-01-28
===================

Added
-----
- Add ``input`` Jinja filter to access input fields


===================
14.1.0 - 2019-01-17
===================

Added
-----
- Add ``assertFilesExist`` method to ``ProcessTestCase``
- Add ``clean_test_dir`` management command that removes files created during
  testing

Fixed
-----
- Support registration of Python processes inherited from ``process.Process``
- Skip docker image pull if image exists locally. This solves the issue
  where pull would fail if process uses an image that is only used locally.


===================
14.0.1 - 2018-12-17
===================

Fixed
-----
- Make sure that tmp dir exists in Docker executor


===================
14.0.0 - 2018-12-17
===================

Changed
-------
- **BACKWARD INCOMPATIBLE:** Run data purge in a separate worker to make sure
  that listener replies to the executor within 60 seconds
- Use batcher for spawned processes in listener
- Increase Docker's memory limit for 100MB to make sure processes are not
  killed when using all available memory and tune Docker memory limits to
  avoid OOM.

Added
-----
- Raise an exception in Docker executor if container doesn't start for 60
  seconds
- Set ``TMPDIR`` environment variable in Docker executor to ``.tmp`` dir in
  data directory to prevent filling up container's local storage

Fixed
-----
- Process SIGTERM signal in executor as expected - set the Data status to
  error and set the process_error field
- Clear cached Django settings from the manager's shared state on startup


===================
13.3.0 - 2018-11-20
===================

Changed
-------
- Switch channels_redis dependency to upstream version

Added
-----
- Python execution engine
- Support multiple entity types
- Support extending viewsets with custom filter methods
- Add `tags` attribute to ``ProcessTestCase.run_process`` method which
  adds listed tag to the created ``Data`` object
- Copy ``Data`` objects tags from parent objects for spawned ``Data``
  objects and ``Data`` objects created by workflows

Fixed
-----
- Fix manager shutdown in the test runner. If an unrecoverable exception
  occurred while running a test, and never got caught (e.g. an unpicklable
  exception in a parallel test worker), the listener would not get terminated
  properly, leading to a hang.
- Data and collection name API filters were fixed to work as expected (ngrams
  was switched to raw).


===================
13.2.0 - 2018-10-23
===================

Added
-----
- Use prioritized batcher in listener


===================
13.1.0 - 2018-10-19
===================

Added
-----
- Use batching for ES index builds

Fixed
-----
- Fix handling of M2M dependencies in ES indexer


===================
13.0.0 - 2018-10-10
===================

Changed
-------
- **BACKWARD INCOMPATIBLE:** Remove Data descriptors from Entity Elasticsearch
  index
- Support searching by ``slug`` and ``descriptor_data`` in entity viewset text
  search

Added
-----
- Add tags to collections


===================
12.0.0 - 2018-09-18
===================

Changed
-------
- **BACKWARD INCOMPATIBLE:** Switch ``Collection`` and ``Entity`` API viewsets
  to use Elasticsearch
- **BACKWARD INCOMPATIBLE:** Refactor ``Relation`` model, which includes:

  - renaming ``position`` to ``partition``
  - renaming ``label`` to ``category`` and making it required
  - adding ``unit``
  - making ``collection`` field required
  - requiring unique combination of ``collection`` and ``category``
  - renaming partition's ``position`` to ``label``
  - adding (integer) ``position`` to partition (used for sorting)
  - deleting ``Relation`` when the last ``Entity`` is removed
- **BACKWARD INCOMPATIBLE:** Remove rarely used parameters of the ``register``
  command ``--path`` and ``--schemas``.
- Omit ``current_user_permissions`` field in serialization if only a subset of
  fields is requested
- Allow slug to be null on update to enable slug auto-generation
- Retire obsolete processes. We have added the ``is_active`` field to the
  Process model. The field is read-only on the API and can only be changed
  through Django ORM. Inactive processes can not be executed. The ``register``
  command was extended with the ``--retire`` flag that removes old process
  versions which do not have associated data. Then it finds the processes that
  have been registered but do not exist in the code anymore, and:

  - If they do not have data: removes them
  - If they have data: flags them not active (``is_active=False``)

Added
-----
- Add support for URLs in ``basic:file:`` fields in Django tests
- Add ``collections`` and ``entities`` fields to Data serializer, with optional
  hydration using ``hydrate_collections`` and/or ``hydrate_entities``
- Support importing large files from Google Drive in re-import
- Add ``python3-plumbum`` package to resolwe/base:ubuntu-18.04 image

Fixed
-----
- Prevent mutation of ``input_`` parameter in ``ProcessTestCase.run_process``
- Return 400 instead of 500 error when slug already exists
- Add trailing colon to process category default
- Increase stdout buffer size in the Docker executor


===================
11.0.0 - 2018-08-13
===================

Changed
-------
- **BACKWARD INCOMPATIBLE:** Remove option to list all objects on Storage API
  endpoint
- Make the main executor non-blocking by using Python asyncio
- Debug logs are not send from executors to the listener anymore to limit the
  amount of traffic on Redis

Added
-----
- Add size to Data serializer
- Implement ``ResolweSlugRelatedField``. As a result, ``DescriptorSchema``
  objects can only be referenced by ``slug`` (instead of ``id``)
- Add options to filter by ``type`` and ``scheduling_class`` on Process API
  endpoint

Fixed
-----
- Inherit collections from ``Entity`` when adding ``Data`` object to it


===================
10.1.0 - 2018-07-16
===================

Changed
-------
- Lower the level of all ``INFO`` logs in elastic app to ``DEBUG``

Added
-----
- Add load tracking to the listener with log messages on overload
- Add job partition selection in the SLURM workload connector
- Add ``slug`` Jinja filter
- Set ``Data`` status to ``ERROR`` if executor is killed by the scheduling
  system

Fixed
-----
- Include the manager in the documentation, make sure all references work
  and tidy the content up a bit


===================
10.0.1 - 2018-07-07
===================

Changed
-------
- Convert the listener to use asyncio
- Switched to ``channels_redis_persist`` temporarily to mitigate connection
  storms

Fixed
-----
- Attempt to reconnect to Redis in the listener in case of connection
  errors


===================
10.0.0 - 2018-06-19
===================

Changed
-------
- **BACKWARD INCOMPATIBLE:** Drop support for Python 3.4 and 3.5
- **BACKWARD INCOMPATIBLE:** Start using Channels 2.x

Added
-----
- Add the options to skip creating of fresh mapping after dropping ES indices
  with ``elastic_purge`` management command
- Add ``dirname`` and ``relative_path`` Jinja filters


==================
9.0.0 - 2018-05-15
==================

Changed
-------
- Make sorting by contributor case insensitive in Elasticsearch endpoints
- Delete ES documents in post delete signal instead of pre delete one

Added
-----
- **BACKWARD INCOMPATIBLE:** Add on-register validation of default values in
  process and schemas
- **BACKWARD INCOMPATIBLE:** Validate that field names in processes and
  schemas start with a letter and only contain alpha-numeric characters
- Support Python 3.6
- Add ``range`` parameter and related validation to fields of type
  ``basic:integer:``, ``basic:decimal``, ``list:basic:integer:`` and
  ``list:basic:decimal``
- Support filtering and sorting by ``process_type`` parameter on Data API
  endpoint
- Add ``dirname`` Jinja filter
- Add ``relative_path`` Jinja filter

Fixed
-----
- Add missing ``list:basic:decimal`` type to JSON schema
- Don't crash on empty ``in`` lookup
- Fix {{ requirements.resources.* }} variables in processes to take in to
  the account overrides specified in Django settings
- Create Elasticsearch mapping even if there is no document to push


==================
8.0.0 - 2018-04-11
==================

Changed
-------
- **BACKWARD INCOMPATIBLE:** Use Elasticsearch version 5.x
- **BACKWARD INCOMPATIBLE:** Raise an error if an invalid query argument is
  used in Elasticsearch viewsets
- **BACKWARD INCOMPATIBLE:** Switch ``Data`` API viewset to use Elasticsearch
- Terminate the executor if listener response with error message
- ``verbosity`` setting is no longer propagated to the executor
- Only create Elasticsearch mappings on first push

Added
-----
- Add ``sort`` argument to ``assertFile`` and ``assertFiles`` methods in
  ``ProcessTestCase`` to sort file lines before asserting the content
- Add ``process_slug`` field to ``DataSerializer``
- Improve log messages in executor and workload connectors
- Add ``process_memory`` and ``process_cores`` fields to ``Data`` model and
  ``DataSerializer``
- Support lookup expressions (``lt``, ``lte``, ``gt``, ``gte``, ``in``,
  ``exact``) in ES viewsets
- Support for easier dynamic composition of type extensions
- Add ``elastic_mapping`` management command

Fixed
-----
- Fix Elasticsearch index rebuilding after a dependant object is deleted
- Send response to executor even if data object was already deleted
- Correctly handle reverse m2m relations when processing ES index dependencies


==================
7.0.0 - 2018-03-12
==================

Changed
-------
- **BACKWARD INCOMPATIBLE:** Remove Ubuntu 17.04 base Docker image due to end
  of lifetime
- **BACKWARD INCOMPATIBLE:** Remove support for Jinja in ``DescriptorSchema``'s
  default values
- **BACKWARD INCOMPATIBLE:** Remove ``CONTAINER_IMAGE`` configuration option
  from the Docker executor; if no container image is specified for a process
  using the Docker executor, the same pre-defined default image is used
  (currently this is ``resolwe/base:ubuntu-16.04``)
- Add mechanism to change test database name from the environment, appending a
  ``_test`` suffix to it; this replaces the static name used before

Added
-----
- Add Ubuntu 17.10 and Ubuntu 18.04 base Docker images
- Add database migration operations for process schema migrations
- Add ``delete_chunked`` method to ``Data`` objects queryset which is needed
  due to Django's extreme memory usage when deleting a large count of ``Data``
  objects
- Add ``validate_process_types`` utility function, which checks that all
  registered processes conform to their supertypes
- Add ``FLOW_CONTAINER_VALIDATE_IMAGE`` setting which can be used to validate
  container image names
- Only pull Docker images at most once per process in ``list_docker_images``
- Add ``FLOW_PROCESS_MAX_CORES`` Django setting to limit the number of CPU
  cores used by a process

Fixed
-----
- Make parallel test suite worker threads clean up after initialization
  failures
- Add mechanism to override the manager's control channel prefix from the
  environment
- Fix deletion of a ``Data`` objects which belongs to more than one ``Entity``
- Hydrate paths in ``refs`` of ``basic:file:``, ``list:basic:file:``,
  ``basic:dir:`` and ``list:basic:dir:`` fields before processing ``Data``
  object


==================
6.1.0 - 2018-02-21
==================

Changed
-------
- Remove runtime directory during general data purge instead of immediately
  after each process finishes
- Only process the Data object (and its children) for which the dispatcher's
  ``communicate()`` was triggered
- Propagate logging messages from executors to the listener
- Use process' slug instead of data id when logging errors in listener
- Improve log messages in dispatcher

Added
-----
- Add ``descriptor_completed`` field to the ``Entity`` filter
- Validate manager semaphors after each test case, to ease debugging of tests
  which execute processes

Fixed
-----
- Don't set Data object's status to error if executor is run multiple times to
  mitigate the Celery issue of tasks being run multiple times
- Make management commands respect the set verbosity level


==================
6.0.1 - 2018-01-29
==================

Fixed
-----
- Make manager more robust to ORM/database failures during data object
  processing
- Rebuild the ElasticSearch index after permission is removed from an object
- Trim ``Data.process_error``, ``Data.process_warning`` and
  ``Data.process_info`` fields before saving them
- Make sure values in ``Data.process_error``, ``Data.process_warning`` and
  ``Data.process_info`` cannot be overwritten
- Handle missing ``Data`` objects in ``hydrate_input_references`` function
- Make executor fail early when executed twice on the same data directory


==================
6.0.0 - 2018-01-17
==================

Changed
-------
- **BACKWARD INCOMPATIBLE:** ``FLOW_DOCKER_LIMIT_DEFAULTS`` has been renamed
  to ``FLOW_PROCESS_RESOURCE_DEFAULTS`` and ``FLOW_DOCKER_LIMIT_OVERRIDES``
  has been renamed to ``FLOW_PROCESS_RESOURCE_OVERRIDES``
- **BACKWARD INCOMPATIBLE:** ``Process.PERSISTENCE_TEMP`` is not used for
  execution priority anymore
- **BACKWARD INCOMPATIBLE:** There is only one available manager class, which
  includes dispatch logic; custom manager support has been removed and their
  role subsumed into the new connector system
- **BACKWARD INCOMPATIBLE:** Removed ``FLOW_DOCKER_MAPPINGS`` in favor of new
  ``FLOW_DOCKER_VOLUME_EXTRA_OPTIONS`` and ``FLOW_DOCKER_EXTRA_VOLUMES``
- Parent relations are kept even after the parent is deleted and are deleted
  when the child is deleted
- Dependency resolver in manager is sped up by use of parent relations
- Validation of ``Data`` inputs is performed on save instead of on create

Added
-----
- Support for the SLURM workload manager
- Support for dispatching ``Data`` objects to different managers
- Support for passing secrets to processes in a controlled way using a newly
  defined ``basic:secret`` input type
- ``is_testing`` test helper function, which returns ``True`` when invoked in
  tests and ``False`` otherwise
- Add ``collecttools`` Django command for collecting tools' files in single
  location defined in ``FLOW_TOOLS_ROOT`` Django setting which is used for
  mapping tools in executor when ``DEBUG`` is set to ``False`` (but not in
  tests)

Fixed
-----
- Fix ``Data`` object preparation race condition in ``communicate()``
- Set correct executor in flow manager
- Make executors more robust to unhandled failures
- Calculate ``Data.size`` by summing ``total_size`` of all file-type outputs
- Don't change slug explicitly defined by user - raise an error instead
- Objects are locked while updated over API, so concurrent operations don't
  override each other
- Make manager more robust to unhandled failures during data object processing
- Fix manager deadlock during tests
- Fix ctypes cache clear during tests
- Don't raise ``ChannelFull`` error in manager's communicate call
- Don't trim predefined slugs in ``ResolweSlugField``


==================
5.1.0 - 2017-12-12
==================

Added
-----
- Database-side JSON projections for ``Storage`` models
- Compute total size (including refs size) for file-type outputs
- Add ``size`` field to ``Data`` model and migrate all existing objects

Change
------
- Change Test Runner's test directory creation so it always creates a
  subdirectory in ``FLOW_EXECUTOR``'s ``DATA_DIR``, ``UPLOAD_DIR`` and
  ``RUNTIME_DIR`` directories

Fixed
-----
- Do not report additional failure when testing a tagged process errors or
  fails
- Fix Test Runner's ``changes-only`` mode when used together with a Git
  repository in detached ``HEAD`` state
- Fix handling of tags and test labels together in Test Runner's
  ``changes-only`` mode
- Fix parallel test execution where more test processes than databases were
  created during tests

==================
5.0.0 - 2017-11-28
==================

Changed
-------
- **BACKWARD INCOMPATIBLE:** The ``keep_data()`` method in
  ``TransactionTestCase`` is no longer supported. Use the
  ``--keep-data`` option to the test runner instead.
- **BACKWARD INCOMPATIBLE:** Convert the manager to Django Channels
- **BACKWARD INCOMPATIBLE:** Refactor executors into standalone programs
- **BACKWARD INCOMPATIBLE:** Drop Python 2 support, require Python 3.4+
- Move common test environment preparation to ``TestCaseHelpers`` mixin

Fixed
-----
- Fix parents/children filter on Data objects
- Correctly handle removed processes in the changes-only mode of the
  Resolwe test runner


==================
4.0.0 - 2017-10-25
==================

Added
-----
- **BACKWARD INCOMPATIBLE:** Add option to build only subset of
  specified queryset in Elasticsearch index builder
- ``--pull`` option to the ``list_docker_images`` management command
- Test profiling and process tagging
- New test runner, which supports partial test suite execution based
  on changed files
- Add ``all`` and ``any`` Jinja filters

Changed
-------
- **BACKWARD INCOMPATIBLE:** Bump Django requirement to version 1.11.x
- **BACKWARD INCOMPATIBLE:** Make ``ProcessTestCase`` non-transactional
- Pull Docker images after process registration is complete
- Generalize Jinja filters to accept lists of ``Data`` objects
- When new ``Data`` object is created, permissions are copied from
  collections and entity to which it belongs

Fixed
-----
- Close schema (YAML) files after ``register`` command stops using them
- Close schema files used for validating JSON schemas after they are no
  longer used
- Close stdout used to retrieve process results in executor after the
  process is finished
- Remove unrelated permissions occasionally listed among group
  permissions on ``permissions`` endpoint
- Fix ``ResolwePermissionsMixin`` to work correctly with multi-words
  model names, i.e. ``DescriptorSchema``
- Fix incorrect handling of offset/limit in Elasticsearch viewsets


==================
3.1.0 - 2017-10-05
==================

Added
-----
- ``resolwe/base`` Docker image based on Ubuntu 17.04
- Support different dependency kinds between data objects

Fixed
-----
- Serialize ``current_user_permissions`` field in a way that is
  compatible with DRF 3.6.4+
- API requests on single object endpoints are allowed to all users if
  object has appropriate public permissions


==================
3.0.1 - 2017-09-15
==================

Fixed
-----
- Correctly relabel SELinux contexts on user/group files


==================
3.0.0 - 2017-09-13
==================

Added
-----
- Add filtering by id on ``descriptor_schma`` API endpoint
- Support assigning descriptor schema by id (if set value is of type
  int) on ``Collection``, ``Data`` and ``Entity`` endpoints
- ``assertAlmostEqualGeneric`` test case helper, which enables recursive
  comparison for almost equality of floats in nested containers

Changed
-------
- **BACKWARD INCOMPATIBLE:** Run Docker containers as non-root user

Fixed
-----
- Use per-process upload dir in tests to avoid race conditions

==================
2.0.0 - 2017-08-24
==================

Added
-----
- ``descriptor`` jinja filter to get the descriptor (or part of it) in
  processes
- Ubuntu 14.04/16.04 based Docker images for Resolwe
- Add ``list_docker_images`` management command that lists all Docker
  images required by registered processes in either plain text or YAML
- Data status is set to ``ERROR`` and error message is appended to
  ``process_error`` if value of ``basic:storage:`` field is set to a
  file with invalid JSON

Changed
-------
- **BACKWARD INCOMPATIBLE:** Quote all unsafe strings when evaluating
  expressions in Bash execution engine
- **BACKWARD INCOMPATIBILE:** Rename ``permissions`` attribute on API
  endpoints to ``current_user_permissions``
- API ``permissions`` endpoint raises error if no owner is assigned to
  the object after applied changes
- ``owner`` permission cannot be assigned to a group
- Objects with public permissions are included in list API views for
  logged-in users
- Owner permission is assigned to the contributor of the processes and
  descriptor schemas in the ``register`` management command
- The base image Dockerfile is renamed to Dockerfile.fedora-26

Fixed
-----
- Add ``basic:url:link`` field to the JSON schema
- Return more descriptive error if non-existing permission is sent to
  the ``permissions`` endpoint
- Handle errors occurred while processing Elasticsearch indices and log
  them
- Return 400 error with a descriptive message if permissions on API are
  assigned to a non-existing user/group


==================
1.5.1 - 2017-07-20
==================

Changed
-------
- Add more descriptive message if user has no permission to add
  ``Data`` object to the collection when the object is created

Fixed
-----
- Set contributor of ``Data`` object to public user if it is created by
  not authenticated user
- Remove remaining references to calling ``pip`` with
  ``--process-dependency-links`` argument


==================
1.5.0 - 2017-07-04
==================

Added
-----
- Add Resolwe test framework
- Add ``with_custom_executor`` and ``with_resolwe_host`` test decorators
- Add ``isort`` linter to check order of imports
- Support basic test case based on Django's ``TransactionTestCase``
- Support ES test case based on Django's ``TransactionTestCase``
- Support process test case based on Resolwe's ``TransactionTestCase``
- Add ability to set a custom command for the Docker executor via the
  ``FLOW_DOCKER_COMMAND`` setting.
- ``get_url`` jinja filter
- When running ``register`` management command, permissions are
  automatically granted based on the permissions of previous latest
  version of the process or descriptor schema.
- Set ``parent`` relation in spawned ``Data`` objects and workflows
- Relations between entities
- Resolwe toolkit Docker images
- Archive file process
- File upload processes
- Resolwe process tests
- Add ``SET_ENV`` setting to set environment variables in executor
- Support ordering by version for descriptor schema
- Add ``NullExecutor``
- If ``choices`` are defined in JSON schema, value of field is
  validated with them
- Add cpu core, memory and network resource limits
- Add scheduling class for processes (``interactive``, ``batch``), which
  replaces the previously unused process priority field
- Add ``share_content`` flag to the collection and entity permissions
  endpoint to also share the content of the coresponding object
- Add ``delete_content`` flag to the collection and entity destroy
  method on API to also delete the content of the coresponding object

Changed
-------
- Support running tests in parallel
- Split ``flow.models`` module to multiple files
- Remove ability to set a custom executor command for any executor via
  the ``FLOW_EXECUTOR['COMMAND']`` setting.
- Rename ``RESOLWE_API_HOST`` setting and environment variable in
  executor to ``RESOLWE_HOST_URL``
- Remove ``keep_failed`` function in tests.
- Rename ``keep_all`` function to ``keep_data``.
- Manager is automatically run when new ``Data`` object is created
- Outputs of ``Data`` objects with status ``Error`` are not validated
- Superusers are no longer included in response in ``permissions``
  endpoint of resources
- Remove ``public_processes`` field from the ``Collection`` model as it
  is never used
- Public users can create new ``Data`` objects with processes and
  descriptor schemas on which they have appropriate permissions
- Add custom ``ResolweSlugField`` and use it instead of
  ``django-autoslug``

Fixed
-----
- **SECURITY:** Prevent normal users from creating new ``Processes``
  over API
- Configure parallel tests
- Isolate Elasticsearch indices for parallel tests
- Fix Docker container name for parallel tests
- Generate temporary names for upload files in tests
- Fix permissions in Elasticsearch tests
- Do not purge data in tests
- Remove primary keys before using cached schemas' in process tests
- Set appropriate SELinux labels when mounting tools in Docker
  containers
- ``Data`` objects created by the workflow inherit its permissions
- If user doesn't have permissions on the latest versions of processes
  and descriptor schemas, older ones are used or error is returned
- Support ``data:`` and ``list:data:`` types
- Set ``Data`` object status to error if worker cannot update the object
  in the database
- ``Data`` objects returned in ``CollectionViewset`` and
  ``EntityViewset`` are filtered by permissions of the user in request
- Public permissions are taken into account in elastic app
- Treat ``None`` field value as if the field is missing
- Copy parent's permissions to spawned ``Data`` objects


==================
1.4.1 - 2017-01-27
==================

Fixed
-----
- Update instructions on preparing a release to no longer build the wheel
  distribution which currently fails to install Resolwe's dependency links


==================
1.4.0 - 2017-01-26
==================

Added
-----
- Auto-process style, type tree and category index
- Support loading JSON from a file if the string passed to the ``basic:json:``
  field is a file.
- ``list:basic:integer:`` field
- Data object's checksum is automatically calculated on save
- ``get_or_create`` end point for ``Data`` objects
- ``basic:file:html:`` field for HTML files
- Helper function for comparing JSON fields in tests
- Purge directories not belonging to any data objects
- Ordering options to API endpoints
- Workflow execution engine
- ``data_by_slug`` filter for jinja expression engine
- Export ``RESOLWE_API_HOST`` environment variable in executor
- Add ``check_installed()`` test utility function
- Add support for configuring the network mode of Docker executor
- Add ``with_docker_executor`` test utility decorator
- Support for Docker image requirements
- Support version in descriptor schema YAML files
- Add ``Entity`` model that allows grouping of ``Data`` objects
- Introduce priority of Data objects
- Data objects created with processes with temporary persistence are given
  high priority.
- Add ``resolwe.elastic`` application, a framework for advanced indexing of
  Django models with ElasticSearch

Changed
-------
- Refactor linters, check PEP 8 and PEP 257
- Split expression engines into expression engines and execution engines
- Use Jinja2 instead of Django Template syntax
- Expression engine must be declared in ``requirements``
- Set Docker Compose's project name to ``resolwe`` to avoid name clashes
- Expose ``check_docker()`` test utility function
- Update versionfield to 0.5.0
- Support Django 1.10 and update filters
- Executor is no longer serialized
- Put Data objects with high priority into ``hipri`` Celery queue.

Fixed
-----
- Fix pylint warnings (PEP 8)
- Fix pydocstyle warnings (PEP 257)
- Take last version of process for spawned objects
- Use default values for descriptor fields that are not given
- Improve handling of validation errors
- Ignore file size in ``assertFields``
- Order data objects in ``CollectionViewSet``
- Fix tests for Django 1.10
- Add quotes to paths in a test process test-save-file


==================
1.3.1 - 2016-07-27
==================

Added
-----
- Sphinx extension ``autoprocess`` for automatic process documentation


==================
1.3.0 - 2016-07-27
==================

Added
-----
- Ability to pass certain information to the process running in the container
  via environment variables (currently, user's uid and gid)
- Explicitly set working directory inside the container to the mapped directory
  of the current ``Data``'s directory
- Allow overriding any ``FLOW_EXECUTOR`` setting for testing
- Support GET request on /api/<model>/<id>/permissons/ url
- Add OWNER permissions
- Validate JSON fields before saving ``Data`` object
- Add basic:dir field
- ``RESOLWE_CUSTOM_TOOLS_PATHS`` setting to support custom paths for tools
  directories
- Add test coverage and track it with Codecov
- Implement data purge
- Add ``process_fields.name`` custom tamplate tag
- Return contributor information together with objects
- Added permissions filter to determine ``Storage`` permissions based on
  referenced ``Data`` object

Changed
-------
- Move filters to separate file and systemize them
- Unify file loading in tests
- Simplify ``ProcessTestCase`` by removing the logic for handling different
  uid/gid of the user running inside the Docker container
- Upgrade to django-guardian 1.4.2
- Rename ``FLOW_EXECUTOR['DATA_PATH']`` setting to
  ``FLOW_EXECUTOR['DATA_DIR']``
- Rename ``FLOW_EXECUTOR['UPLOAD_PATH']`` setting to
  ``FLOW_EXECUTOR['UPLOAD_DIR']``
- Rename ``proc.data_path`` system variable to ``proc.data_dir``
- Rename test project's data and upload directories to ``.test_data`` and
  ``.test_upload``
- Serve permissions in new format
- Rename ``assertFiles`` method in ``ProcessTestCase`` to ``assertFile`` and
  add new ``assertFiles`` method to check ``list:basic:file`` field
- Make ``flow.tests.run_process`` function also handle file paths
- Use Travis CI to run the tests
- Include all necessary files for running the tests in source distribution
- Exclude tests from built/installed version of the package
- Put packaging tests in a separate Tox testing environment
- Put linters (pylint, pep8) into a separate Tox testing environment
- Drop django-jenkins package since we no longer use Jenkins for CI
- Move testing utilities from ``resolwe.flow.tests`` to
  ``resolwe.flow.utils.test`` and from ``resolwe.permissions.tests.base`` to
  ``resolwe.permissions.utils.test``
- Add Tox testing environment for building documentation
- Extend Reference documentation

Fixed
-----
- Spawn processors (add data to current collection)
- Set collection name to avoid warnings in test output
- Improve Python 3 compatibility
- Fix setting descriptor schema on create


==================
1.2.1 - 2016-05-15
==================

Added
-----
- Add docker-compose configuration for PostgreSQL
- Processes can be created on API
- Enable spawned processes

Changed
-------
- Move logic from ``Collection`` model to the ``BaseCollection`` abstract
  model and make it its parent
- Remove all logic for handling ``flow_collection``
- Change default database user and port in test project's settings
- Keep track of upload files created during tests and purge them afterwards

Fixed
-----
- Test processes location agnostic
- Test ignore timezone support


==================
1.2.0 - 2016-05-06
==================

Changed
-------
- Rename ``assertFileExist`` to ``assertFileExists``
- Drop ``--process-dependency-links`` from Tox's pip configuration
- Improve documentation on preparing a new release

Added
-----
- Ability to use a custom executor command by specifying the
  ``FLOW_EXECUTOR['COMMAND']`` setting
- Make workload manager configurable in settings

Fixed
-----
- Make Resolwe work with Python 3 again
- Fix tests
- Render data name again after inputs are resolved
- Ensure Tox installs the package from sdist
- Pass all Resolwe's environment variables to Tox's testing environment
- Ensure tests gracefully handle unavailability of Docker


==================
1.1.0 - 2016-04-18
==================

Changed
-------
- Rename `process_register` manage.py command to `register`
- Reference process by slug when creating new Data object
- Run manager when new Data object is created through API
- Include full DescriptorSchema object when hydrating Data and Collection
  objects
- Add `djangorestframework-filters` package instead of `django-filters`

Added
-----
- Tox tests for ensuring high-quality Python packaging
- Timezone support in executors
- Generating slugs with `django-autoslug` package
- Auto-generate Data name on creation based on template defined in Process
- Added endpoint for adding/removeing Data objects to/from Collection

Fixed
-----
- Pass all Resolwe's environment variables to Tox's testing environment
- Include all source files and supplementary package data in sdist
- Make Celery engine work
- Add all permissions to creator of `flow_collection` Colection
- Set DescriptorSchema on creating Data objects and Collections
- Loading DescriptorSchema in tests
- Handle Exceptions if input field doesn't match input schema
- Trigger ORM signals on Data status updates
- Don't set status od Data object to error status if return code of tool is 0


==================
1.0.0 - 2016-03-31
==================

Changed
-------
- Renamed Project to Collection
- Register processes from packages and custom paths
- Removed support for Python 3.3

Added
-----
- Permissions
- API for flow
- Docker executor
- Expression engine support
- Celery engine
- Purge command
- Framework for testing processors
- Processor finders
- Support for Django 1.9
- Support for Python 3.5
- Initial migrations
- Introductory documentation


==================
0.9.0 - 2015-04-09
==================

Added
-----

Initial release.
