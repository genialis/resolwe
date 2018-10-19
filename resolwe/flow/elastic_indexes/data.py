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
    type = ProcessType()  # pylint: disable=invalid-name
    process_name = Name()
    tags = dsl.Keyword(multi=True)

    collection = dsl.Integer(multi=True)
    parents = dsl.Integer(multi=True)
    children = dsl.Integer(multi=True)
    entity = dsl.Integer(multi=True)

    class Meta:
        """Meta class for data search document."""

        index = 'data'


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
    }

    def get_dependencies(self):
        """Return dependencies, which should trigger updates of this model."""
        # pylint: disable=no-member
        return super().get_dependencies() + [
            Data.collection_set,
            Data.entity_set,
            Data.parents,
        ]

    def get_collection_value(self, obj):
        """Extract collections this object is in."""
        return list(obj.collection_set.values_list('pk', flat=True))

    def get_parents_value(self, obj):
        """Extract parents."""
        return list(obj.parents.values_list('pk', flat=True))

    def get_children_value(self, obj):
        """Extract children."""
        return list(obj.children.values_list('pk', flat=True))

    def get_entity_value(self, obj):
        """Extract entities."""
        return list(obj.entity_set.values_list('pk', flat=True))
