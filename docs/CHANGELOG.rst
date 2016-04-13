##########
Change Log
##########

All notable changes to this project are documented in this file.


==========
Unreleased
==========

Added
-----
- Tox tests for ensuring high-quality Python packaging

Fixed
-----
- All Resolwe's environment variables are passed to Tox's testing environment
- Included all source files and supplementary package data in sdist
- Made Celery engine work


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
