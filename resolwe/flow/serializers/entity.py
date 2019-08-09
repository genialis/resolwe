"""Resolwe entity serializer."""
from resolwe.flow.models import Collection, Entity

from .collection import CollectionSerializer
from .fields import DictRelatedField


class EntitySerializer(CollectionSerializer):
    """Serializer for Entity."""

    collection = DictRelatedField(
        queryset=Collection.objects.all(),
        serializer=CollectionSerializer,
        allow_null=True,
        required=False,
        write_permission='add',
    )

    class Meta(CollectionSerializer.Meta):
        """EntitySerializer Meta options."""

        model = Entity
        fields = CollectionSerializer.Meta.fields + (
            'collection',
            'descriptor_completed',
            'duplicated',
            'type',
        )
