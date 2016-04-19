##########
Change Log
##########

All notable changes to this project are documented in this file.


==========
Unreleased
==========

Added
-----
- Ability to use a custom executor command by specifying the
  ``FLOW_EXECUTOR['COMMAND']`` setting


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
