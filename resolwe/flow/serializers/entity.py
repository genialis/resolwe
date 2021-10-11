"""Resolwe entity serializer."""
from resolwe.flow.models import Collection, Entity

from .collection import BaseCollectionSerializer
from .fields import DictRelatedField


class EntitySerializer(BaseCollectionSerializer):
    """Serializer for Entity."""

    collection = DictRelatedField(
        queryset=Collection.objects.all(),
        serializer=BaseCollectionSerializer,
        allow_null=True,
        required=False,
        write_permission="edit",
    )

    class Meta(BaseCollectionSerializer.Meta):
        """EntitySerializer Meta options."""

        model = Entity
        fields = BaseCollectionSerializer.Meta.fields + (
            "collection",
            "duplicated",
            "type",
        )
