##########
Change Log
##########

All notable changes to this project are documented in this file.
This project adheres to `Semantic Versioning <http://semver.org/>`_.

==========
Unreleased
==========

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
- Add `basic:url:link` field to the JSON schema
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
