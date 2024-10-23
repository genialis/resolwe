"""Resolwe annotations serializer."""

from typing import Any

from django.db import models
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
from .contributor import ContributorSerializer
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
            "version",
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

        values_to_create = [AnnotationValue(**data) for data in validated_data]
        # Perform a validation on all values to create. This is necessary as bulk
        # create skips the validation.
        for value in values_to_create:
            value.validate()
        return AnnotationValue.objects.bulk_create(values_to_create)

    def save(self, **kwargs: Any) -> Any:
        """Perform efficient bulk create or delete."""
        self.instance = []
        to_create = []
        for data in self.validated_data:
            if data["_value"] is None or data["_value"]["value"] is None:
                data["_value"] = None
            to_create.append(data)

        # Create new objects and delete markers.
        created = self.create(to_create)
        # Only return created objects.
        self.instance = [entry for entry in created if entry._value is not None]
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

    contributor = ContributorSerializer()

    def __init__(self, instance=None, data=empty, **kwargs):
        """Rewrite value -> _value."""
        if data is not empty:
            for entry in data if isinstance(data, list) else [data]:
                if "value" in entry:
                    entry["_value"] = {"value": entry.pop("value")}
        super().__init__(instance, data, **kwargs)

    def get_unique_together_validators(self) -> list:
        """Return the list of validators for unique together model constraint.

        Since we are using put to create or update existing values this must be empty
        or updating existing values with put method will fail the validation.
        """
        return []

    class Meta:
        """AnnotationValueSerializer Meta options."""

        model = AnnotationValue
        read_only_fields = ("label", "created")
        update_protected_fields = ("id", "entity", "field", "contributor")
        fields = read_only_fields + update_protected_fields + ("value", "_value")
        extra_kwargs = {"_value": {"write_only": True}}
        list_serializer_class = AnnotationValueListSerializer
