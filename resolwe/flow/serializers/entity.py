"""Resolwe entity serializer."""
from resolwe.flow.models import Collection, Entity
from resolwe.permissions.models import Permission

from .collection import BaseCollectionSerializer
from .fields import DictRelatedField


class EntitySerializer(BaseCollectionSerializer):
    """Serializer for Entity."""

    collection = DictRelatedField(
        queryset=Collection.objects.all(),
        serializer=BaseCollectionSerializer,
        allow_null=True,
        required=False,
        write_permission=Permission.EDIT,
    )

    class Meta(BaseCollectionSerializer.Meta):
        """EntitySerializer Meta options."""

        model = Entity
        fields = BaseCollectionSerializer.Meta.fields + (
            "collection",
            "duplicated",
            "type",
        )

    def update(self, instance, validated_data):
        """Update collection."""
        updated_instance = super().update(instance, validated_data)
        updated_instance.move_to_collection(instance.collection, force=True)
        return updated_instance
