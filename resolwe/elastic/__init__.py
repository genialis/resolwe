""".. Ignore pydocstyle D400.

=======
Elastic
=======

Framework for advanced indexing of Django models with ElasticSearch.

To register index processor, create `elastic_indexes.py` file int your
app and put subclass of :class:`~resolwe.elastic.indices.BaseIndex` in
it. It will automatically register and index all objects specified in
it.

For building the index for the first time or manually updating it, run::

    python manage.py elastic_index

.. automodule:: resolwe.elastic.indices
.. automodule:: resolwe.elastic.viewsets
.. automodule:: resolwe.elastic.builder
.. automodule:: resolwe.elastic.pagination
.. automodule:: resolwe.elastic.utils
.. automodule:: resolwe.elastic.management.commands

"""
from elasticsearch_dsl.connections import connections

default_app_config = 'resolwe.elastic.apps.ElasticConfig'  # pylint: disable=invalid-name
