"""Resolwe process serializer."""

from resolwe.flow.models import Process

from .base import ResolweBaseSerializer


class ProcessSerializer(ResolweBaseSerializer):
    """Serializer for Process objects."""

    class Meta:
        """ProcessSerializer Meta options."""

        model = Process
        read_only_fields = (
            "created",
            "id",
            "modified",
            "is_active",
        )
        update_protected_fields = (
            "category",
            "contributor",
            "data_name",
            "description",
            "entity_always_create",
            "entity_input",
            "entity_type",
            "input_schema",
            "name",
            "output_schema",
            "persistence",
            "requirements",
            "run",
            "scheduling_class",
            "slug",
            "type",
            "version",
        )
        fields = read_only_fields + update_protected_fields
