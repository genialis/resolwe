"""Resolwe collection serializer."""
from resolwe.flow.models import Collection
from resolwe.rest.fields import ProjectableJSONField

from .base import ResolweBaseSerializer
from .fields import NestedDescriptorSchemaSerializer


class CollectionSerializer(ResolweBaseSerializer):
    """Serializer for Collection objects."""

    settings = ProjectableJSONField(required=False)
    descriptor = ProjectableJSONField(required=False)
    descriptor_schema = NestedDescriptorSchemaSerializer(required=False)

    class Meta:
        """CollectionSerializer Meta options."""

        model = Collection
        read_only_fields = (
            'created',
            'descriptor_dirty',
            'duplicated',
            'id',
            'modified',
        )
        update_protected_fields = (
            'contributor',
        )
        fields = read_only_fields + update_protected_fields + (
            'description',
            'descriptor',
            'descriptor_schema',
            'name',
            'settings',
            'slug',
            'tags',
        )
