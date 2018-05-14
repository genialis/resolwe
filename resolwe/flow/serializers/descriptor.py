"""Resolwe descriptor schema serializer."""
from resolwe.flow.models import DescriptorSchema
from resolwe.rest.fields import ProjectableJSONField

from .base import ResolweBaseSerializer


class DescriptorSchemaSerializer(ResolweBaseSerializer):
    """Serializer for DescriptorSchema objects."""

    schema = ProjectableJSONField(required=False)

    class Meta:
        """TemplateSerializer Meta options."""

        model = DescriptorSchema
        update_protected_fields = ('contributor', )
        read_only_fields = ('id', 'created', 'modified')
        fields = ('slug', 'name', 'version', 'schema') + update_protected_fields + read_only_fields
