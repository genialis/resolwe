"""Resolwe descriptor schema serializer."""
from resolwe.flow.models import DescriptorSchema
from resolwe.rest.fields import ProjectableJSONField

from .base import ResolweBaseSerializer


class DescriptorSchemaSerializer(ResolweBaseSerializer):
    """Serializer for DescriptorSchema objects."""

    schema = ProjectableJSONField(required=False)

    class Meta:
        """DescriptorSchemaSerializer Meta options."""

        model = DescriptorSchema
        read_only_fields = (
            "created",
            "id",
            "modified",
        )
        update_protected_fields = (
            "contributor",
            "version",
        )
        fields = (
            read_only_fields
            + update_protected_fields
            + ("description", "name", "schema", "slug",)
        )
