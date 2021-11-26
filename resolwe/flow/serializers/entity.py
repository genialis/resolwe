"""Resolwe entity serializer."""
from django.db import transaction

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

    @transaction.atomic
    def update(self, instance, validated_data):
        """Update collection."""
        update_collection = "collection" in validated_data
        new_collection = validated_data.pop("collection", None)
        instance = super().update(instance, validated_data)
        if update_collection and new_collection != instance.collection:
            instance.move_to_collection(new_collection)
        return instance
