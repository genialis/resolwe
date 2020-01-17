"""Resolwe data serializer."""
from rest_framework import serializers

from resolwe.flow.models import Collection, Data, DescriptorSchema, Entity, Process
from resolwe.rest.fields import ProjectableJSONField

from .base import ResolweBaseSerializer
from .collection import CollectionSerializer
from .descriptor import DescriptorSchemaSerializer
from .entity import EntitySerializer
from .fields import DictRelatedField
from .process import ProcessSerializer


class DataSerializer(ResolweBaseSerializer):
    """Serializer for Data objects."""

    input = ProjectableJSONField(required=False)
    output = ProjectableJSONField(required=False)
    descriptor = ProjectableJSONField(required=False)
    process = DictRelatedField(
        queryset=Process.objects.all(), serializer=ProcessSerializer
    )
    descriptor_schema = DictRelatedField(
        queryset=DescriptorSchema.objects.all(),
        serializer=DescriptorSchemaSerializer,
        allow_null=True,
        required=False,
    )
    collection = DictRelatedField(
        queryset=Collection.objects.all(),
        serializer=CollectionSerializer,
        allow_null=True,
        required=False,
        write_permission="edit",
    )
    entity = DictRelatedField(
        queryset=Entity.objects.all(),
        serializer=EntitySerializer,
        allow_null=True,
        required=False,
        write_permission="edit",
    )

    class Meta:
        """DataSerializer Meta options."""

        model = Data
        read_only_fields = (
            "checksum",
            "created",
            "descriptor_dirty",
            "duplicated",
            "finished",
            "id",
            "modified",
            "process_cores",
            "process_error",
            "process_info",
            "process_memory",
            "process_progress",
            "process_rc",
            "process_warning",
            "output",
            "scheduled",
            "size",
            "started",
            "status",
        )
        update_protected_fields = (
            "contributor",
            "input",
            "process",
        )
        fields = (
            read_only_fields
            + update_protected_fields
            + (
                "collection",
                "descriptor",
                "descriptor_schema",
                "entity",
                "name",
                "slug",
                "tags",
            )
        )

    def validate_process(self, process):
        """Check that process is active."""
        if not process.is_active:
            raise serializers.ValidationError(
                "Process {} is not active.".format(process)
            )
        return process

    def validate_collection(self, collection):
        """Verify that changing collection is done in the right place."""
        if getattr(self.instance, "entity", None):
            if getattr(self.instance.entity.collection, "id", None) != getattr(
                collection, "id", None
            ):
                raise serializers.ValidationError(
                    "If Data is in entity, you can only move it to another collection "
                    "by moving entire entity."
                )
        return collection
