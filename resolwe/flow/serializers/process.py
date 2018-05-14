"""Resolwe process serializer."""
from resolwe.flow.models import Process

from .base import ResolweBaseSerializer


class ProcessSerializer(ResolweBaseSerializer):
    """Serializer for Process objects."""

    class Meta:
        """ProcessSerializer Meta options."""

        model = Process
        update_protected_fields = ('contributor', )
        read_only_fields = ('id', 'created', 'modified')
        fields = ('slug', 'name', 'data_name', 'version', 'type', 'flow_collection', 'category',
                  'persistence', 'description', 'input_schema', 'output_schema', 'requirements',
                  'run', 'scheduling_class') + update_protected_fields + read_only_fields
