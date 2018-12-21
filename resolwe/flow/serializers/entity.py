"""Resolwe entity serializer."""
from rest_framework import serializers

from resolwe.flow.models import Entity

from .collection import CollectionSerializer


class EntitySerializer(CollectionSerializer):
    """Serializer for Entity."""

    collections = serializers.PrimaryKeyRelatedField(many=True, read_only=True)

    class Meta(CollectionSerializer.Meta):
        """EntitySerializer Meta options."""

        model = Entity
        fields = CollectionSerializer.Meta.fields + (
            'collections',
            'descriptor_completed',
            'duplicated',
            'type',
        )

    def get_data(self, entity):
        """Return serialized list of data objects on entity that user has `view` permission on."""
        data = self._filter_queryset('view_data', entity.data.all())

        return self._serialize_data(data)
