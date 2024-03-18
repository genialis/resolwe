"""Resolwe collection serializer."""

import logging
from typing import Optional

from rest_framework import serializers

from resolwe.flow.models import Collection, Data, DescriptorSchema
from resolwe.rest.fields import ProjectableJSONField

from .base import ResolweBaseSerializer
from .descriptor import DescriptorSchemaSerializer
from .fields import DictRelatedField

logger = logging.getLogger(__name__)


class BaseCollectionSerializer(ResolweBaseSerializer):
    """Base serializer for Collection objects."""

    settings = ProjectableJSONField(required=False)
    data_count = serializers.SerializerMethodField(required=False)
    status = serializers.SerializerMethodField(required=False)

    def get_data_count(self, collection: Collection) -> int:
        """Return number of data objects on the collection."""
        # Use 'data_count' attribute when available. It is created in the
        # BaseCollectionViewSet class.
        return (
            collection.data_count
            if hasattr(collection, "data_count")
            else collection.data.count()
        )

    def get_status(self, collection: Collection) -> Optional[str]:
        """Return status of the collection based on the status of data objects.

        When collection contains no data objects None is returned.
        """

        status_order = [
            Data.STATUS_ERROR,
            Data.STATUS_UPLOADING,
            Data.STATUS_PROCESSING,
            Data.STATUS_PREPARING,
            Data.STATUS_WAITING,
            Data.STATUS_RESOLVING,
            Data.STATUS_DONE,
        ]

        # Use 'data_statuses' attribute when available. It is created in the
        # BaseCollectionViewSet class. It contains all the distinct statuses of the
        # data objects in the collection.
        status_set = (
            set(collection.data_statuses)
            if hasattr(collection, "data_statuses")
            else collection.data.values_list("status", flat=True).distinct()
        )

        if not status_set:
            return None

        for status in status_order:
            if status in status_set:
                return status

        logger.warning(
            "Could not determine the status of a collection.",
            extra={"collection": collection.__dict__},
        )
        return None

    class Meta:
        """CollectionSerializer Meta options."""

        model = Collection
        read_only_fields = (
            "created",
            "duplicated",
            "id",
            "modified",
            "data_count",
            "status",
        )
        update_protected_fields = ("contributor",)
        fields = (
            read_only_fields
            + update_protected_fields
            + (
                "description",
                "name",
                "settings",
                "slug",
                "tags",
            )
        )


class CollectionSerializer(BaseCollectionSerializer):
    """Serializer for Collection objects."""

    descriptor = ProjectableJSONField(required=False)
    descriptor_schema = DictRelatedField(
        queryset=DescriptorSchema.objects.all(),
        serializer=DescriptorSchemaSerializer,
        allow_null=True,
        required=False,
    )
    entity_count = serializers.SerializerMethodField(required=False)

    def get_entity_count(self, collection: Collection) -> int:
        """Return number of entities on the collection."""
        # Use 'entity_count' attribute when available. It is created in the
        # BaseCollectionViewSet class.
        return (
            collection.entity_count
            if hasattr(collection, "entity_count")
            else collection.entity_set.count()
        )

    class Meta(BaseCollectionSerializer.Meta):
        """CollectionSerializer Meta options."""

        read_only_fields = BaseCollectionSerializer.Meta.read_only_fields + (
            "entity_count",
        )

        fields = BaseCollectionSerializer.Meta.fields + (
            "entity_count",
            "descriptor",
            "descriptor_schema",
            "descriptor_dirty",
        )
