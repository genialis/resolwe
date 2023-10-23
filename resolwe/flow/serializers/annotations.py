"""Resolwe annotations serializer."""
from django.db import models

from rest_framework import serializers
from rest_framework.fields import empty

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


class AnnotationsSerializer(serializers.Serializer):
    """Serializer that reads annotation field and its value."""

    field = PrimaryKeyDictRelatedField(queryset=AnnotationField.objects.all())
    value = serializers.JSONField()


class AnnotationPresetSerializer(ResolweBaseSerializer):
    """Serializer for AnnotationPreset objects."""

    class Meta:
        """AnnotationPresetSerializer Meta options."""

        model = AnnotationPreset
        read_only_fields = ("id",)
        fields = read_only_fields + ("name", "fields", "contributor")


class AnnotationValueSerializer(ResolweBaseSerializer):
    """Serializer for AnnotationValue objects."""

    def __init__(self, instance=None, data=empty, **kwargs):
        """Rewrite value -> _value."""
        if data is not empty and "value" in data:
            data["_value"] = {"value": data.pop("value", None)}
        super().__init__(instance, data, **kwargs)

    class Meta:
        """AnnotationValueSerializer Meta options."""

        model = AnnotationValue
        read_only_fields = ("label",)
        update_protected_fields = ("id", "entity", "field")
        fields = read_only_fields + update_protected_fields + ("value", "_value")
        extra_kwargs = {"_value": {"write_only": True}}
