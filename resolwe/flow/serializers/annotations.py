"""Resolwe annotations serializer."""

from functools import reduce
from itertools import batched
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

    @transaction.atomic
    def create(self, validated_data: Any) -> Any:
        """Perform bulk create."""
        # The save must be called for delete markers to be set correctly.
        values = [AnnotationValue(**data) for data in validated_data]
        for value in values:
            value.save()
        return values

    @transaction.atomic
    def save(self, **kwargs: Any) -> Any:
        """Save the annotation values.

        We have to consider the possibilities:
        - the value is not a delete marker:
          - the existing value exists:
            - create a new entry and set deleted to true on the existing one if the value
              has actually changed.
          - the value does not exist yet or is deleted: create a new entry.
        - the value is a delete marker:
          - the existing value exists: set deleted to true.
        """

        def is_delete_marker(value):
            """Is the value a delete marker."""
            return (
                value is None
                or value["_value"] is None
                or value["_value"]["value"] is None
            )

        def combine_query(query: models.Q, data: dict) -> models.Q:
            """Combine the query with the data."""
            return query | models.Q(field=data["field"], entity=data["entity"])

        # The annotations are processed in batches to avoid creating too complex
        # queries. The BATCH_SIZE determines the size of the batch.
        BATCH_SIZE = 1000
        created = []
        for batch in batched(self.validated_data, BATCH_SIZE):
            to_process = {(data["field"].pk, data["entity"].pk): data for data in batch}
            # Create a mapping to easy access the existing values.
            existing_values = {
                (value.field_id, value.entity_id): value
                for value in AnnotationValue.objects.filter(
                    reduce(combine_query, batch, models.Q())
                )
            }
            # We have to:
            # - create new values
            # - create new values and delete existing ones if the value has changed
            # - delete values with None
            to_delete = []
            to_create = []
            for key, data in to_process.items():
                if existing_value := existing_values.get(key):
                    if is_delete_marker(data):
                        to_delete.append(existing_value.pk)
                    elif existing_value.value != data["_value"]["value"]:
                        to_create.append(data)
                        to_delete.append(existing_value.pk)
                elif not is_delete_marker(data):
                    to_create.append(data)

            # Create new objects.
            created.extend(self.create(to_create))
            # Mark objects as deleted.
            if to_delete:
                AnnotationValue.objects.filter(pk__in=to_delete).update(deleted=True)

        # Only return created objects.
        self.instance = created
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
