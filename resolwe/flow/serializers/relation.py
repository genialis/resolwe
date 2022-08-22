"""Resolwe relation serializer."""
from django.db import transaction

from rest_framework import serializers

from resolwe.flow.models.collection import Collection
from resolwe.flow.models.descriptor import DescriptorSchema
from resolwe.flow.models.entity import Relation, RelationPartition, RelationType
from resolwe.flow.serializers import CollectionSerializer, DescriptorSchemaSerializer
from resolwe.flow.serializers.fields import DictRelatedField
from resolwe.permissions.models import Permission
from resolwe.rest.fields import ProjectableJSONField
from resolwe.rest.serializers import SelectiveFieldMixin

from .base import ResolweBaseSerializer


class RelationPartitionSerializer(SelectiveFieldMixin, serializers.ModelSerializer):
    """Serializer for RelationPartition objects."""

    class Meta:
        """RelationPartitionSerializer Meta options."""

        model = RelationPartition
        fields = ("id", "entity", "position", "label")


class RelationSerializer(ResolweBaseSerializer):
    """Serializer for Relation objects."""

    partitions = RelationPartitionSerializer(source="relationpartition_set", many=True)
    collection = DictRelatedField(
        queryset=Collection.objects.all(),
        serializer=CollectionSerializer,
        write_permission=Permission.EDIT,
    )
    type = serializers.SlugRelatedField(
        queryset=RelationType.objects.all(), slug_field="name"
    )
    descriptor = ProjectableJSONField(required=False)
    descriptor_schema = DictRelatedField(
        queryset=DescriptorSchema.objects.all(),
        serializer=DescriptorSchemaSerializer,
        allow_null=True,
        required=False,
    )

    class Meta:
        """RelationSerializer Meta options."""

        model = Relation
        read_only_fields = (
            "created",
            "id",
            "modified",
            "descriptor_dirty",
        )
        update_protected_fields = (
            "contributor",
            "type",
        )
        fields = (
            read_only_fields
            + update_protected_fields
            + (
                "collection",
                "category",
                "partitions",
                "unit",
                "descriptor",
                "descriptor_schema",
            )
        )

    def validate_partitions(self, partitions):
        """Raise validation error if list of partitions is empty."""
        if not partitions:
            raise serializers.ValidationError("List of partitions must not be empty.")

        return partitions

    def _create_partitions(self, instance, partitions):
        """Create partitions."""
        for partition in partitions:
            RelationPartition.objects.create(
                relation=instance,
                entity=partition["entity"],
                label=partition.get("label", None),
                position=partition.get("position", None),
            )

    def create(self, validated_data):
        """Create ``Relation`` object and add partitions of ``Entities``."""
        # `partitions` field is renamed to `relationpartition_set` based on source of nested serializer
        partitions = validated_data.pop("relationpartition_set")

        with transaction.atomic():
            instance = Relation.objects.create(**validated_data)
            self._create_partitions(instance, partitions)

        return instance

    def update(self, instance, validated_data):
        """Update ``Relation``."""
        # `partitions` field is renamed to `relationpartition_set` based on source of nested serializer
        partitions = validated_data.pop("relationpartition_set", None)

        with transaction.atomic():
            instance = super().update(instance, validated_data)

            if partitions is not None:
                # TODO: Apply the diff instead of recreating all objects.
                instance.relationpartition_set.all().delete()
                self._create_partitions(instance, partitions)

        return instance
