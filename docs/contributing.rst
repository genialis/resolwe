============
Contributing
============

Installing prerequisites
========================

Make sure you have Python_ (2.7 or 3.4+) installed on your system. If you don't
have it yet, follow `these instructions
<https://docs.python.org/3/using/index.html>`__.

Resolwe requires PostgreSQL_ (9.4+). Many Linux distributions already include
the required version of PostgreSQL (e.g. Fedora 22+, Debian 8+, Ubuntu 15.04+)
and you can simply install it via distribution's package manager.
Otherwise, follow `these instructions
<https://wiki.postgresql.org/wiki/Detailed_installation_guides>`__.

.. _Python: https://www.python.org/
.. _PostgreSQL: http://www.postgresql.org/

The pip_ tool will install all Resolwe's dependencies from PyPI_.
Installing the ``psycopg2`` dependency will require having a C compiler
(e.g. GCC_) as well as Python and PostgreSQL development files installed on
the system.

.. note::

    The preferred way to install the C compiler and Python and PostgreSQL
    development files is to use your distribution's packages, if they exist.
    For example, on a Fedora/RHEL-based system, that would mean installing
    ``gcc``, ``python-devel``/``python3-devel`` and ``postgresql-devel``
    packages.

.. _pip: https://pip.pypa.io/
.. _PyPi: https://pypi.python.org/
.. _GCC: https://gcc.gnu.org/

Preparing environment
=====================

`Fork <https://help.github.com/articles/fork-a-repo>`__ the main
`Resolwe's git repository`_.

If you don't have Git installed on your system, follow `these
instructions <http://git-scm.com/book/en/v2/Getting-Started-Installing-Git>`__.

Clone your fork (replace ``<username>`` with your GitHub account name) and
change directory::

    git clone https://github.com/<username>/resolwe.git
    cd resolwe

Prepare Resolwe for development::

    pip install -e .[docs,package,test]

.. note::

    We recommend using `virtualenv <https://virtualenv.pypa.io/>`_ (on
    Python 2.7) or `pyvenv <http://docs.python.org/3/library/venv.html>`_ (on
    Python 3.4+) to create an isolated Python environment for Resolwe.

.. _Resolwe's git repository: https://github.com/genialis/resolwe

Preparing database
==================

Create a ``resolwe`` database::

    # Remove database if exists
    dropdb resolwe

    # Create database
    createdb resolwe

Set-up database::

    cd tests
    ./manage.py migrate
    ./manage.py createsuperuser --username admin --email admin@genialis.com

Registering processes
=====================

.. code-block:: none

    cd tests
    ./manage.py process_register --path <path to packages>

Running tests
=============

To run the tests, use::

    cd tests
    ./manage.py test resolwe

To run the tests with Tox_, use::

    tox -r

.. _Tox: http://tox.testrun.org/

Buildling documentation
=======================

.. code-block:: none

    python setup.py build_sphinx

Preparing release
=================

Clean ``build`` directory::

    python setup.py clean -a

Remove previous distributions in ``dist`` directory::

    rm dist/*

Remove previous ``egg-info`` directory::

    rm -r *.egg-info

Bump project's version in ``resolwe/__about__.py`` file and update the
changelog in ``docs/CHANGELOG.rst``.

.. note::

    Use `Semantic versioning`_.

Commit changes to git::

    git commit -a -m "Prepare release <new-version>"

Test the new version with Tox_::

    tox -r

Create source distribution::

    python setup.py sdist

Build wheel::

    python setup.py bdist_wheel

Upload distribution to PyPI_::

    twine upload dist/*

Tag the new version::

    git tag <new-version>

Push changes to the main `Resolwe's git repository`_::

   git push <resolwe-upstream-name> master <new-version>

.. _Semantic versioning: https://packaging.python.org/en/latest/distributing/#semantic-versioning-preferred
