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
    data_count = serializers.IntegerField(required=False)
    status = serializers.SerializerMethodField(required=False)

    def get_status(self, collection):
        """Return status of the collection based on the status of data objects."""
        if not hasattr(collection, "data_count"):
            return None
        if collection.data_count == 0:
            return None

        if collection.data_error_count:
            return Data.STATUS_ERROR
        if collection.data_uploading_count:
            return Data.STATUS_UPLOADING
        if collection.data_processing_count:
            return Data.STATUS_PROCESSING
        if collection.data_preparing_count:
            return Data.STATUS_PREPARING
        if collection.data_waiting_count:
            return Data.STATUS_WAITING
        if collection.data_resolving_count:
            return Data.STATUS_RESOLVING
        if collection.data_done_count == collection.data_count:
            return Data.STATUS_DONE

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

    entity_count = serializers.IntegerField(required=False)

    class Meta(BaseCollectionSerializer.Meta):
        """CollectionSerializer Meta options."""

        read_only_fields = BaseCollectionSerializer.Meta.read_only_fields + (
            "entity_count",
        )

        fields = BaseCollectionSerializer.Meta.fields + ("entity_count",)
