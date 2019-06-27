"""Resolwe entity serializer."""
from resolwe.flow.models import Entity

from .collection import CollectionSerializer


class EntitySerializer(CollectionSerializer):
    """Serializer for Entity."""

    collection = CollectionSerializer(required=False)

    class Meta(CollectionSerializer.Meta):
        """EntitySerializer Meta options."""

        model = Entity
        fields = CollectionSerializer.Meta.fields + (
            'collection',
            'descriptor_completed',
            'duplicated',
            'type',
        )
