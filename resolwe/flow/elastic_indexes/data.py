"""Elastic Search indexes for Data model."""
import elasticsearch_dsl as dsl

from resolwe.elastic.fields import Name, ProcessType
from resolwe.elastic.indices import BaseIndex

from ..models import Data
from .base import BaseDocument, BaseIndexMixin


class DataDocument(BaseDocument):
    """Document for data search."""

    started = dsl.Date()
    finished = dsl.Date()
    status = dsl.Keyword()
    process = dsl.Integer()
    process_type = ProcessType()
    # Keep backward compatibility.
    type = ProcessType()
    process_name = Name()
    tags = dsl.Keyword(multi=True)

    collection = dsl.Integer()
    entity = dsl.Integer()

    class Index:
        """Meta class for data search document."""

        name = 'data'


class DataIndex(BaseIndexMixin, BaseIndex):
    """Index for data objects used in ``DataDocument``."""

    queryset = Data.objects.all().prefetch_related(
        'process',
        'contributor'
    )
    object_type = Data
    document_class = DataDocument

    mapping = {
        'process': 'process.id',
        'process_name': 'process.name',
        'process_type': 'process.type',
        'type': 'process.type',
        'entity': 'entity_id',
        'collection': 'collection_id',
    }
