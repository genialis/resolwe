=======
Resolwe
=======

Open source enterprise dataflow engine based on the `Django framework`_.

We envision Resolwe to follow the `Common Workflow Language`_ specification,
but the current implementation does not yet support it.

.. _Django framework: https://www.djangoproject.com/
.. _Common Workflow Language: https://github.com/common-workflow-language/common-workflow-language

Install::

    python setup.py install


-----------
Development
-----------

Install for Development::

    python setup.py develop
    pip install -r tests/test_requirements.txt

Install PostgreSQL_ and create a `resolwe` database::

    # Remove database if exists
    dropdb resolwe

    # Create database
    createdb resolwe

.. _PostgreSQL: https://wiki.postgresql.org/wiki/Detailed_installation_guides

Set-up database::

    cd tests
    ./manage.py makemigrations
    ./manage.py migrate
    ./manage.py createsuperuser --username admin --email admin@genialis.com

Register processors::

    cd tests
    ./manage.py tool_register --path <path to packages>

Run tests::

    cd tests
    ./manage.py test resolwe
