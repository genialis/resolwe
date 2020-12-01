=======
Resolwe
=======

|build| |coverage| |docs| |black| |pypi_version| |pypi_pyversions| |pypi_downloads|

.. |build| image:: https://github.com/genialis/resolwe/workflows/Resolwe%20CI/badge.svg?branch=master
    :target: https://github.com/genialis/resolwe/actions?query=branch%3Amaster
    :alt: Build Status

.. |coverage| image:: https://img.shields.io/codecov/c/github/genialis/resolwe/master.svg
    :target: http://codecov.io/github/genialis/resolwe?branch=master
    :alt: Coverage Status

.. |docs| image:: https://readthedocs.org/projects/resolwe/badge/?version=latest
    :target: http://resolwe.readthedocs.io/
    :alt: Documentation Status

.. |black| image:: https://img.shields.io/badge/code%20style-black-000000.svg
    :target: https://github.com/psf/black
    :alt: Code Style Black

.. |pypi_version| image:: https://img.shields.io/pypi/v/resolwe.svg
    :target: https://pypi.org/project/resolwe
    :alt: Version on PyPI

.. |pypi_pyversions| image:: https://img.shields.io/pypi/pyversions/resolwe.svg
    :target: https://pypi.org/project/resolwe
    :alt: Supported Python versions

.. |pypi_downloads| image:: https://pepy.tech/badge/resolwe
    :target: https://pepy.tech/project/resolwe
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

.. _documentation: http://resolwe.readthedocs.io/
.. _Slack: http://resolwe.slack.com/


Install
=======

Prerequisites
-------------

Resolwe runs on Python_ 3.6 or later If you don't have it yet, follow `these
instructions <https://docs.python.org/3/using/index.html>`__.

It's easiest to run other required services in Docker containers If you don't
have it yet, you can follow the `official Docker tutorial`_ for Mac and for
Windows or install it as a distribution's package in most of standard Linux
distributions (Fedora, Ubuntu,...).

.. _Python: https://www.python.org/
.. _official Docker tutorial: https://docs.docker.com/get-started/

Using PyPI_
-----------

.. code::

    pip install resolwe

.. _PyPI: https://pypi.org/

Using source on GitHub_
-----------------------

.. code::

   pip install https://github.com/genialis/resolwe/archive/<git-tree-ish>.tar.gz

where ``<git-tree-ish>`` can represent any commit SHA, branch name, tag name,
etc. in `Resolwe's GitHub repository`_. For example, to install the latest
Resolwe from the ``master`` branch, use:

.. code::

   pip install https://github.com/genialis/resolwe/archive/master.tar.gz

.. _`Resolwe's GitHub repository`: https://github.com/genialis/resolwe/
.. _GitHub: `Resolwe's GitHub repository`_


Contribute
==========

We welcome new contributors. To learn more, read Contributing_ section of our
documentation.

.. _Contributing: http://resolwe.readthedocs.io/en/latest/contributing.html
