"""Resolwe annotations serializer."""

from typing import Any

from django.db import models, transaction
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
from .fields import DictRelatedField, PrimaryKeyDictRelatedField


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

    group = DictRelatedField(
        queryset=AnnotationGroup.objects.all(), serializer=AnnotationGroupSerializer
    )

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

    @transaction.atomic
    def create(self, validated_data: Any) -> Any:
        """Perform bulk create."""
        # The save must be called for delete markers to be set correctly.
        values = [AnnotationValue(**data) for data in validated_data]
        for value in values:
            value.save()
        return values

    @transaction.atomic
    def save(self, **kwargs: Any) -> list[AnnotationValue]:
        """Save and return the list of annotation values."""
        for data in self.validated_data:
            data["value"] = data["_value"]["value"]
        self.instance = AnnotationValue.objects.add_annotations(self.validated_data)
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
