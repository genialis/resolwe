=======
Resolwe
=======

Open source dataflow engine for `Django framework`_.

We envision Resolwe to follow the `Common Workflow Language`_ specification,
but the current implementation does not yet fully support it.

.. _Django framework: https://www.djangoproject.com/
.. _Common Workflow Language: https://github.com/common-workflow-language/common-workflow-language

Install
=======

.. _install-prerequisites:

Prerequisites
-------------

Make sure you have Python_ (2.7 or 3.3+) installed on your system. If you don't
have it yet, follow `these instructions
<https://docs.python.org/3/using/index.html>`__.

Resolwe requires PostgreSQL_ (9.4+). Many Linux distributions already include
the required version of PostgreSQL (e.g. Fedora 22+, Debian 8+, Ubuntu 15.04+)
and you can simply install it via distribution's package manager.
Otherwise, follow `these instructions
<https://wiki.postgresql.org/wiki/Detailed_installation_guides>`__.

.. _Python: https://www.python.org/
.. _PostgreSQL: http://www.postgresql.org/

Installing via pip
------------------

*Resolwe is not available via PyPI yet.*

Installing from source
----------------------

This installation method will install all Resolwe's dependencies from PyPI_.
Installing the ``psycopg2`` dependency will require having a C compiler
(e.g. GCC_) as well as Python and PostgreSQL development files installed on
the system.

.. note::

    The preffered way to install the C compiler and Python and PostgreSQL
    development files is to use your distribution's packages, if they exist.
    For example, on a Fedora/RHEL-based system, that would mean installing
    ``gcc``, ``python-devel``/``python3-devel`` and ``postgresql-devel``
    packages.

Download the `latest release of Resolwe
<https://github.com/genialis/resolwe/archive/master.tar.gz>`_ and extract it.

Go to the directory with the extracted source code and install it::

    python setup.py install

.. note::

    If you want to use Python 3, substitute ``python`` with ``python3``.

.. _PyPi: https://pypi.python.org/
.. _GCC: https://gcc.gnu.org/

Develop
=======

Prepare environment
-------------------

Install prerequisites as described in *Install, Prerequisites* section.

`Fork <https://help.github.com/articles/fork-a-repo>`__ the main `Resolwe's git
repository <https://github.com/genialis/resolwe>`_.

If you don't have Git installed on your system, follow `these
instructions <http://git-scm.com/book/en/v2/Getting-Started-Installing-Git>`__.

Clone your fork (replace ``<username>`` with your GitHub account name)::

    git clone https://github.com/<username>/resolwe.git

Prepare Resolwe for development::

    python setup.py develop
    pip install -e .[docs,test]

.. note::

    We recommend using `virtualenv <https://virtualenv.pypa.io/>`_ (on
    Python 2.7) or `pyvenv <http://docs.python.org/3/library/venv.html>`_ (on
    Python 3.3+) to create an isolated Python environment for Resolwe.

Prepare database
----------------

Create a ``resolwe`` database::

    # Remove database if exists
    dropdb resolwe

    # Create database
    createdb resolwe

Set-up database::

    cd tests
    ./manage.py makemigrations
    ./manage.py migrate
    ./manage.py createsuperuser --username admin --email admin@genialis.com

Register processes
------------------

.. code-block::

    cd tests
    ./manage.py process_register --path <path to packages>

Run tests
---------

.. code-block::

    cd tests
    ./manage.py test resolwe
