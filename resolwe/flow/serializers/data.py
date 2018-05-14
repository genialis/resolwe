"""Resolwe data serializer."""
from rest_framework import serializers

from resolwe.flow.models import Data, DescriptorSchema
from resolwe.rest.fields import ProjectableJSONField

from .base import ResolweBaseSerializer
from .descriptor import DescriptorSchemaSerializer


class DataSerializer(ResolweBaseSerializer):
    """Serializer for Data objects."""

    input = ProjectableJSONField(required=False)
    output = ProjectableJSONField(required=False)
    descriptor = ProjectableJSONField(required=False)
    process_slug = serializers.CharField(source='process.slug', read_only=True)
    process_name = serializers.CharField(source='process.name', read_only=True)
    process_type = serializers.CharField(source='process.type', read_only=True)
    process_input_schema = ProjectableJSONField(source='process.input_schema', read_only=True)
    process_output_schema = ProjectableJSONField(source='process.output_schema', read_only=True)

    name = serializers.CharField(read_only=False, required=False)
    slug = serializers.CharField(read_only=False, required=False)

    class Meta:
        """DataSerializer Meta options."""

        model = Data
        update_protected_fields = ('contributor', 'process',)
        read_only_fields = (
            'id', 'created', 'modified', 'started', 'finished', 'checksum', 'status', 'process_progress', 'process_rc',
            'process_info', 'process_warning', 'process_error', 'process_type', 'process_input_schema',
            'process_output_schema', 'process_slug', 'process_name', 'descriptor_dirty',
        )
        fields = (
            'slug', 'name', 'contributor', 'input', 'output', 'descriptor_schema', 'descriptor', 'tags',
            'process_memory', 'process_cores',
        ) + update_protected_fields + read_only_fields

    def get_fields(self):
        """Dynamically adapt fields based on the current request."""
        fields = super(DataSerializer, self).get_fields()

        if self.request.method == "GET":
            fields['descriptor_schema'] = DescriptorSchemaSerializer(required=False)
        else:
            fields['descriptor_schema'] = serializers.PrimaryKeyRelatedField(
                queryset=DescriptorSchema.objects.all(), required=False
            )

        return fields
