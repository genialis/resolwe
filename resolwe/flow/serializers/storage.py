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
        update_protected_fields = ('contributor', )
        read_only_fields = ('id', 'created', 'modified')
        fields = ('slug', 'name', 'data', 'json') + update_protected_fields + read_only_fields
