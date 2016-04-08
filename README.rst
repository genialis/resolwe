=======
Resolwe
=======

|jenkins| |docs| |pypi_version| |pypi_pyversions| |pypi_downloads|

.. |jenkins| image:: https://ci.genialis.com/buildStatus/icon?job=resolwe-master
    :target: https://ci.genialis.com/job/resolwe-master/lastCompletedBuild/testReport/
    :alt: Build Status

.. |docs| image:: https://readthedocs.org/projects/resolwe/badge/?version=latest
    :target: http://resolwe.readthedocs.org/
    :alt: Documentation Status

.. |pypi_version| image:: https://img.shields.io/pypi/v/resolwe.svg
    :target: https://pypi.python.org/pypi/resolwe
    :alt: Version on PyPI

.. |pypi_pyversions| image:: https://img.shields.io/pypi/pyversions/resolwe.svg
    :target: https://pypi.python.org/pypi/resolwe
    :alt: Supported Python versions

.. |pypi_downloads| image:: https://img.shields.io/pypi/dm/resolwe.svg
    :target: https://pypi.python.org/pypi/resolwe
    :alt: Number of downloads from PyPI

Resolwe is an open source dataflow package for `Django framework`_. We envision
Resolwe to follow the `Common Workflow Language`_ specification, but the
current implementation does not yet fully support it. Resolwe offers a complete
RESTful API to connect with external resources. A collection of bioinformatics
pipelines is available in `Resolwe Bioinformatics`_.

.. _Django framework: https://www.djangoproject.com/
.. _Common Workflow Language: https://github.com/common-workflow-language/common-workflow-language
.. _Resolwe Bioinformatics: https://github.com/genialis/resolwe-bio


Docs & Help
===========

Read about architecture, getting started, how to write `processes`, RESTful API
details, and API Reference in the documentation_.

To chat with developers or ask for help, join us on Slack_.

.. _documentation: http://resolwe.readthedocs.org/
.. _Slack: http://resolwe.slack.com/


Install
=======

Prerequisites
-------------

Make sure you have Python_ (2.7 or 3.4+) installed on your system. If you don't
have it yet, follow `these instructions
<https://docs.python.org/3/using/index.html>`__.

Resolwe requires PostgreSQL_ (9.4+). Many Linux distributions already include
the required version of PostgreSQL (e.g. Fedora 22+, Debian 8+, Ubuntu 15.04+)
and you can simply install it via distribution's package manager.
Otherwise, follow `these instructions
<https://wiki.postgresql.org/wiki/Detailed_installation_guides>`__.

Additionally, installing the ``psycopg2`` dependency from PyPI_ will require
having a C compiler (e.g. GCC_) as well as Python and PostgreSQL development
files installed on the system.

Note
^^^^

The preferred way to install the C compiler and Python and PostgreSQL
development files is to use your distribution's packages, if they exist. For
example, on a Fedora/RHEL-based system, that would mean installing ``gcc``,
``python-devel``/``python3-devel`` and ``postgresql-devel`` packages.

.. _Python: https://www.python.org/
.. _PostgreSQL: http://www.postgresql.org/
.. _PyPi: https://pypi.python.org/
.. _GCC: https://gcc.gnu.org/

From PyPI_
----------

.. code::

    pip install resolwe

From source
-----------

.. code::

   pip install https://github.com/genialis/resolwe/archive/<git-tree-ish>.tar.gz

where ``<git-tree-ish>`` can represent any commit SHA, branch name, tag name,
etc. in `Resolwe's GitHub repository`_. For example, to install the latest
Resolwe from the ``master`` branch, use:

.. code::

   pip install https://github.com/genialis/resolwe/archive/master.tar.gz

.. _`Resolwe's GitHub repository`: https://github.com/genialis/resolwe/


Contribute
==========

We welcome new contributors. To learn more, read Contributing_ section of our
documentation.

.. _Contributing: http://resolwe.readthedocs.org/en/latest/contributing.html
