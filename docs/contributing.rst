============
Contributing
============

Installing prerequisites
========================

Resolwe runs on Python_ 3.6 or later If you don't have it yet, follow `these
instructions <https://docs.python.org/3/using/index.html>`__.

It's easiest to run other required services in Docker containers, which is
assumed in this tutorial. If you don't have it yet, you can follow the
`official Docker tutorial`_ for Mac and for Windows or install it as a
distribution's package in most of standard Linux distributions (Fedora,
Ubuntu,...).

.. _Python: https://www.python.org/
.. _official Docker tutorial: https://docs.docker.com/get-started/

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

    We recommend using `pyvenv <http://docs.python.org/3/library/venv.html>`_
    to create an isolated Python environment for Resolwe.

.. _Resolwe's git repository: https://github.com/genialis/resolwe

Preparing database
==================

Start Docker containers::

    cd tests
    docker-compose up --detach

Set-up database::

    ./manage.py migrate
    ./manage.py createsuperuser --username admin --email admin@genialis.com

Registering processes
=====================

.. code-block:: none

    cd tests
    ./manage.py register

Running tests
=============

To run the tests, use::

    cd tests
    ./manage.py test resolwe --parallel=2

To run the tests with Tox_, use::

    tox -r

.. _Tox: http://tox.testrun.org/

Building documentation
======================

.. code-block:: none

    python setup.py build_sphinx

Submitting changes upstream
===========================

Signed commits are required in the Resolwe upstream repository. Generate your
personal `GPG key`_ and `configure Git to use it automatically`_.

.. _GPG key: https://www.gnupg.org/
.. _configure Git to use it automatically: https://git-scm.com/book/en/v2/Git-Tools-Signing-Your-Work

Preparing release
=================

Checkout the latest code and create a release branch::

    git checkout master
    git pull
    git checkout -b release-<new-version>

Replace the *Unreleased* heading in ``docs/CHANGELOG.rst`` with the new
version, followed by release's date (e.g. *13.2.0 - 2018-10-23*).

.. note::

    Use `Semantic versioning`_.

Commit changes to git::

    git commit -a -m "Prepare release <new-version>"

Push changes to your fork and open a pull request::

    git push --set-upstream <resolwe-fork-name> release-<new-version>

Wait for the tests to pass and the pull request to be approved. Merge the code
to master::

    git checkout master
    git merge --ff-only release-<new-version>
    git push <resolwe-upstream-name> master <new-version>

Tag the new release from the latest commit::

    git checkout master
    git tag -sm "Version <new-version>" <new-version>

.. note::

    Project's version will be automatically inferred from the git tag using
    `setuptools_scm`_.

Push the tag to the main `Resolwe's git repository`_::

    git push <resolwe-upstream-name> master <new-version>

The tagged code will we be released to PyPI automatically. Inspect Travis logs
of the Release step if errors occur.

Preparing pre-release
---------------------

When preparing a pre-release (i.e. an alpha release), one can skip the
"release" commit that updates the change log and just tag the desired commit
with a pre-release tag (e.g. *13.3.0a1*). By pushing it to GitHub, the tagged
code will be automatically tested by Travis CI and then released to PyPI.

.. _Semantic versioning: https://packaging.python.org/en/latest/distributing/#semantic-versioning-preferred
.. _setuptools_scm: https://github.com/pypa/setuptools_scm/
