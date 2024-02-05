"""Resolwe annotations serializer."""

from typing import Any

from django.db import models
from django.db.models import Q

from rest_framework import serializers
from rest_framework.fields import empty

from resolwe.flow.models.annotations import NAME_LENGTH as ANNOTATION_NAME_LENGTH
from resolwe.flow.models.annotations import (
    AnnotationField,
    AnnotationGroup,
    AnnotationPreset,
    AnnotationValue,
)

from .base import ResolweBaseSerializer
from .fields import PrimaryKeyDictRelatedField


class AnnotationFieldListSerializer(serializers.ListSerializer):
    """Override list serializer to allow filtering nested relation."""

    def to_representation(self, data: "models.QuerySet[AnnotationGroup]"):
        """Filter the annotation fields within the group by the collection."""
        if (
            "request" in self.context
            and "collection_id" in self.context["request"].query_params
        ):
            collection_id = self.context["request"].query_params["collection_id"]
            data = data.filter(collections__id=collection_id)
        return super().to_representation(data)


class AnnotationGroupSerializer(ResolweBaseSerializer):
    """Serializer for AnnotationGroup objects."""

    class Meta:
        """AnnotationGroupSerializer Meta options."""

        model = AnnotationGroup
        read_only_fields = ("id", "label", "name", "sort_order")
        fields = read_only_fields


class AnnotationFieldSerializer(ResolweBaseSerializer):
    """Serializer for AnnotationField objects."""

    group = AnnotationGroupSerializer()

    class Meta:
        """AnnotationFieldSerializer Meta options."""

        model = AnnotationField
        read_only_fields = ("id",)
        fields = read_only_fields + (
            "description",
            "group",
            "label",
            "name",
            "sort_order",
            "type",
            "validator_regex",
            "vocabulary",
            "required",
        )
        list_serializer_class = AnnotationFieldListSerializer


class AnnotationFieldDictSerializer(serializers.Serializer):
    """Serializer for actions that deal with annotation fields."""

    annotation_fields = PrimaryKeyDictRelatedField(
        queryset=AnnotationField.objects.all(), many=True
    )
    confirm_action = serializers.BooleanField(default=False)


class AnnotationsByPathSerializer(serializers.Serializer):
    """Serializer that reads annotation field and its value."""

    # The field path contains the annotation group name and the annotation field name
    # separated by spaces.
    field_path = serializers.CharField(max_length=2 * ANNOTATION_NAME_LENGTH + 1)
    value = serializers.JSONField(allow_null=True)


class AnnotationPresetSerializer(ResolweBaseSerializer):
    """Serializer for AnnotationPreset objects."""

    class Meta:
        """AnnotationPresetSerializer Meta options."""

        model = AnnotationPreset
        read_only_fields = ("id",)
        fields = read_only_fields + ("name", "fields", "contributor")


class AnnotationValueListSerializer(serializers.ListSerializer):
    """Perform bulk update of annotation values to speed up requests."""

    def create(self, validated_data: Any) -> Any:
        """Perform efficient bulk create."""
        return AnnotationValue.objects.bulk_create(
            AnnotationValue(**data)
            for data in validated_data
            if data["_value"]["value"] is not None
        )

    def update(self, instance, validated_data: Any):
        """Perform efficient bulk create/update/delete."""
        # Read existing annotations in a single query.
        query = Q()
        for data in validated_data:
            query |= Q(field=data["field"], entity=data["entity"])
        existing_annotations = AnnotationValue.objects.filter(query)

        # Create a mapping between (field, entity) and the existing annotations.
        annotation_map = {
            (annotation.field, annotation.entity): annotation
            for annotation in existing_annotations
        }

        self.instance = []
        to_update = []
        to_create = []
        to_delete = []
        for data in validated_data:
            if value := annotation_map.get((data["field"], data["entity"])):
                if data["_value"]["value"] is None:
                    to_delete.append(value.pk)
                else:
                    to_update.append((value, data))
            else:
                to_create.append(data)

        # Bulk create new annotations.
        self.instance += self.create(to_create)
        # Update annotations.
        for value, data in to_update:
            self.instance.append(self.child.update(value, data))
        # Bulk delete annotations.
        AnnotationValue.objects.filter(pk__in=to_delete).delete()
        return self.instance

    def validate(self, attrs: Any) -> Any:
        """Validate list of annotation values."""
        if len(set((attr["field"], attr["entity"]) for attr in attrs)) != len(attrs):
            raise serializers.ValidationError(
                "Duplicate annotation values for the same entity and field."
            )
        return super().validate(attrs)


class AnnotationValueSerializer(ResolweBaseSerializer):
    """Serializer for AnnotationValue objects."""

    def __init__(self, instance=None, data=empty, **kwargs):
        """Rewrite value -> _value."""
        if data is not empty:
            for entry in data if isinstance(data, list) else [data]:
                if "value" in entry:
                    entry["_value"] = {"value": entry.pop("value")}
        super().__init__(instance, data, **kwargs)

    class Meta:
        """AnnotationValueSerializer Meta options."""

        model = AnnotationValue
        read_only_fields = ("label", "modified")
        update_protected_fields = ("id", "entity", "field")
        fields = read_only_fields + update_protected_fields + ("value", "_value")
        extra_kwargs = {"_value": {"write_only": True}}
        list_serializer_class = AnnotationValueListSerializer
