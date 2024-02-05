"""Resolwe storage serializer."""

from resolwe.flow.models import Storage
from resolwe.rest.fields import ProjectableJSONField

from .base import ResolweBaseSerializer


class StorageSerializer(ResolweBaseSerializer):
    """Serializer for Storage objects."""

    json = ProjectableJSONField()

    class Meta:
        """StorageSerializer Meta options."""

        model = Storage
        read_only_fields = (
            "created",
            "id",
            "modified",
        )
        update_protected_fields = ("contributor",)
        fields = (
            read_only_fields
            + update_protected_fields
            + (
                "data",
                "json",
                "name",
                "slug",
            )
        )
