"""Resolwe data serializer."""
from django.db import transaction

from rest_framework import serializers

from resolwe.flow.models import Collection, Data, DescriptorSchema, Entity, Process
from resolwe.permissions.models import Permission
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
    process_resources = ProjectableJSONField(required=False)
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
        write_permission=Permission.EDIT,
    )
    entity = DictRelatedField(
        queryset=Entity.objects.all(),
        serializer=EntitySerializer,
        allow_null=True,
        required=False,
        write_permission=Permission.EDIT,
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
                "process_resources",
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
        if self.instance and self.instance.collection != collection:
            self.instance.validate_change_collection(collection)
        return collection

    @transaction.atomic
    def update(self, instance, validated_data):
        """Update."""
        update_collection = "collection" in validated_data
        new_collection = validated_data.pop("collection", None)
        instance = super().update(instance, validated_data)
        if update_collection and new_collection != instance.collection:
            instance.move_to_collection(new_collection)
        return instance
