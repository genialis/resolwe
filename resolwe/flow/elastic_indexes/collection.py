"""Elastic Search indexes for Collection model."""
from resolwe.elastic.fields import Name
from resolwe.elastic.indices import BaseIndex
from resolwe.flow.utils import dict_dot, iterate_schema

from ..models import Collection
from .base import BaseDocument, BaseIndexMixin


class CollectionDocument(BaseDocument):
    """Document for collection search."""

    # Data values extracted from the descriptor.
    descriptor_data = Name(multi=True)

    class Meta:
        """Meta class for collection search document."""

        index = 'collection'


class CollectionIndexMixin:
    """Mixin for indices for collection objects used in ``CollectionDocument``."""

    def extract_descriptor(self, obj):
        """Extract data from the descriptor."""
        if not obj.descriptor_schema:
            return []

        descriptor = []
        for _, _, path in iterate_schema(obj.descriptor, obj.descriptor_schema.schema):
            try:
                value = dict_dot(obj.descriptor, path)
            except KeyError:
                continue

            if not isinstance(value, list):
                value = [value]

            for item in value:
                if not isinstance(item, (int, bool, float, str)):
                    continue

                descriptor.append('{}'.format(item))

        return descriptor

    def get_descriptor_data_value(self, obj):
        """Extract data from the descriptors."""
        return [self.extract_descriptor(obj)]


class CollectionIndex(BaseIndexMixin, CollectionIndexMixin, BaseIndex):
    """Index for collection objects used in ``CollectionDocument``."""

    queryset = Collection.objects.all().prefetch_related(
        'descriptor_schema',
        'contributor'
    )
    object_type = Collection
    document_class = CollectionDocument
