"""Elastic Search indexes for Collection model."""
import elasticsearch_dsl as dsl

from resolwe.elastic.indices import BaseIndex

from ..models import Collection
from .base import BaseDocument, BaseIndexMixin


class CollectionDocument(BaseDocument):
    """Document for collection search."""

    # Data values extracted from the descriptor.
    descriptor_data = dsl.Text(multi=True)
    tags = dsl.Keyword(multi=True)

    class Meta:
        """Meta class for collection search document."""

        index = 'collection'


class CollectionIndexMixin:
    """Mixin for indices for collection objects used in ``CollectionDocument``."""

    def extract_descriptor(self, obj):
        """Extract data from the descriptor."""
        descriptor = []

        def flatten(current):
            """Flatten descriptor."""
            if isinstance(current, dict):
                for key in current:
                    flatten(current[key])
            elif isinstance(current, list):
                for val in current:
                    flatten(val)
            elif isinstance(current, (int, bool, float, str)):
                descriptor.append(str(current))

        flatten(obj.descriptor)

        return descriptor

    def get_descriptor_data_value(self, obj):
        """Extract data from the descriptors."""
        return self.extract_descriptor(obj)


class CollectionIndex(BaseIndexMixin, CollectionIndexMixin, BaseIndex):
    """Index for collection objects used in ``CollectionDocument``."""

    queryset = Collection.objects.all().prefetch_related(
        'descriptor_schema',
        'contributor'
    )
    object_type = Collection
    document_class = CollectionDocument
