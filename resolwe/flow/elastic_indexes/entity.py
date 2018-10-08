"""Elastic Search indexes for Entity model."""
import elasticsearch_dsl as dsl

from resolwe.elastic.builder import ManyToManyDependency
from resolwe.elastic.indices import BaseIndex

from ..models import Entity
from .base import BaseIndexMixin
from .collection import CollectionDocument, CollectionIndexMixin


class EntityDocument(CollectionDocument):
    """Document for entity search."""

    descriptor_completed = dsl.Boolean()
    collections = dsl.Integer(multi=True)
    type = dsl.Keyword()

    class Meta:
        """Meta class for entity search document."""

        index = 'entity'


class DataDescriptorDependency(ManyToManyDependency):
    """Dependency only on data objects with descriptor updates."""

    def filter(self, obj, update_fields=None):
        """Determine if object should be processed."""
        if update_fields is None:
            # If update fields is not passed, the parent should be updated.
            return True

        # Otherwise, the parent should only be updated on descriptor changes.
        return 'descriptor' in update_fields


class EntityIndex(BaseIndexMixin, CollectionIndexMixin, BaseIndex):
    """Index for entity objects used in ``EntityDocument``."""

    queryset = Entity.objects.all().prefetch_related(
        'descriptor_schema',
        'contributor',
    )
    object_type = Entity
    document_class = EntityDocument

    def get_dependencies(self):
        """Return dependencies, which should trigger updates of this model."""
        # pylint: disable=no-member
        return super().get_dependencies() + [
            Entity.collections,
        ]

    def get_collections_value(self, obj):
        """Extract collections."""
        return list(obj.collections.values_list('pk', flat=True))
