"""Resolwe collection serializer."""
from resolwe.flow.models import Collection, DescriptorSchema
from resolwe.rest.fields import ProjectableJSONField

from .base import ResolweBaseSerializer
from .descriptor import DescriptorSchemaSerializer
from .fields import DictRelatedField


class CollectionSerializer(ResolweBaseSerializer):
    """Serializer for Collection objects."""

    settings = ProjectableJSONField(required=False)
    descriptor = ProjectableJSONField(required=False)
    descriptor_schema = DictRelatedField(
        queryset=DescriptorSchema.objects.all(),
        serializer=DescriptorSchemaSerializer,
        allow_null=True,
        required=False,
    )

    class Meta:
        """CollectionSerializer Meta options."""

        model = Collection
        read_only_fields = (
            "created",
            "descriptor_dirty",
            "duplicated",
            "id",
            "modified",
        )
        update_protected_fields = ("contributor",)
        fields = (
            read_only_fields
            + update_protected_fields
            + (
                "description",
                "descriptor",
                "descriptor_schema",
                "name",
                "settings",
                "slug",
                "tags",
            )
        )
