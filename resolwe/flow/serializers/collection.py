"""Resolwe collection serializer."""
import logging

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
    descriptor = ProjectableJSONField(required=False)
    descriptor_schema = DictRelatedField(
        queryset=DescriptorSchema.objects.all(),
        serializer=DescriptorSchemaSerializer,
        allow_null=True,
        required=False,
    )
    data_count = serializers.SerializerMethodField(required=False)
    status = serializers.SerializerMethodField(required=False)

    def get_data_count(self, collection):
        """Return number of data objects on the collection."""
        return collection.data.count()

    def get_status(self, collection):
        """Return status of the collection based on the status of data objects."""
        status_set = set([data.status for data in collection.data.all()])

        if not status_set:
            return None

        status_order = [
            Data.STATUS_ERROR,
            Data.STATUS_UPLOADING,
            Data.STATUS_PROCESSING,
            Data.STATUS_PREPARING,
            Data.STATUS_WAITING,
            Data.STATUS_RESOLVING,
            Data.STATUS_DONE,
        ]

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
            "descriptor_dirty",
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
                "descriptor",
                "descriptor_schema",
                "name",
                "settings",
                "slug",
                "tags",
            )
        )


class CollectionSerializer(BaseCollectionSerializer):
    """Serializer for Collection objects."""

    entity_count = serializers.SerializerMethodField(required=False)

    def get_entity_count(self, collection):
        """Return number of entities on the collection."""
        return collection.entity_set.count()

    class Meta(BaseCollectionSerializer.Meta):
        """CollectionSerializer Meta options."""

        read_only_fields = BaseCollectionSerializer.Meta.read_only_fields + (
            "entity_count",
        )

        fields = BaseCollectionSerializer.Meta.fields + ("entity_count",)
