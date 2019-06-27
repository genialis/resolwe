"""Resolwe data serializer."""
from rest_framework import serializers

from resolwe.flow.models import Data, Process
from resolwe.rest.fields import ProjectableJSONField

from .base import ResolweBaseSerializer
from .collection import CollectionSerializer
from .entity import EntitySerializer
from .fields import NestedDescriptorSchemaSerializer, ResolweSlugRelatedField


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
    process = ResolweSlugRelatedField(queryset=Process.objects.all())
    descriptor_schema = NestedDescriptorSchemaSerializer(required=False)

    collection = CollectionSerializer(required=False)
    entity = EntitySerializer(required=False)

    class Meta:
        """DataSerializer Meta options."""

        model = Data
        read_only_fields = (
            'checksum',
            'created',
            'descriptor_dirty',
            'duplicated',
            'finished',
            'id',
            'modified',
            'process_cores',
            'process_error',
            'process_info',
            'process_input_schema',
            'process_memory',
            'process_name',
            'process_output_schema',
            'process_progress',
            'process_rc',
            'process_slug',
            'process_type',
            'process_warning',
            'output',
            'scheduled',
            'size',
            'started',
            'status',
        )
        update_protected_fields = (
            'contributor',
            'input',
            'process',
        )
        fields = read_only_fields + update_protected_fields + (
            'collection',
            'descriptor',
            'descriptor_schema',
            'entity',
            'name',
            'slug',
            'tags',
        )

    def validate_process(self, process):
        """Check that process is active."""
        if not process.is_active:
            raise serializers.ValidationError("Process {} is not active.".format(process))
        return process
