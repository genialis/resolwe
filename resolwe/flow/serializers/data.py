"""Resolwe data serializer."""
from django.contrib.auth.models import AnonymousUser

from rest_framework import serializers

from resolwe.flow.models import Data, Process
from resolwe.rest.fields import ProjectableJSONField

from .base import ResolweBaseSerializer
from .fields import NestedDescriptorSchemaSerializer, ResolweSlugRelatedField


class WithoutDataSerializerMixin:
    """Mixin which removes the `data` field from serializers.

    This is used to avoid infinite recursion with hydrated items.
    """

    def get_fields(self):
        """Dynamically adapt fields based on the current request."""
        fields = super().get_fields()
        del fields['data']
        return fields


class DataSerializer(ResolweBaseSerializer):
    """Serializer for Data objects."""

    input = ProjectableJSONField(required=False)
    output = ProjectableJSONField(required=False)
    descriptor = ProjectableJSONField(required=False)
    process_slug = serializers.CharField(source='process.slug', read_only=True)
    process_name = serializers.CharField(source='process.name', read_only=True)
    process_type = serializers.CharField(source='process.type', read_only=True)
    process_input_schema = ProjectableJSONField(source='process.input_schema', read_only=True)
    process_output_schema = ProjectableJSONField(source='process.output_schema', read_only=True)
    process = ResolweSlugRelatedField(queryset=Process.objects.all())
    descriptor_schema = NestedDescriptorSchemaSerializer(required=False)
    entity_name = serializers.SerializerMethodField()
    collection_name = serializers.SerializerMethodField()

    collection = serializers.SerializerMethodField()
    entity = serializers.SerializerMethodField()

    class Meta:
        """DataSerializer Meta options."""

        model = Data
        read_only_fields = (
            'checksum',
            'created',
            'descriptor_dirty',
            'duplicated',
            'finished',
            'id',
            'modified',
            'process_cores',
            'process_error',
            'process_info',
            'process_input_schema',
            'process_memory',
            'process_name',
            'process_output_schema',
            'process_progress',
            'process_rc',
            'process_slug',
            'process_type',
            'process_warning',
            'output',
            'scheduled',
            'size',
            'started',
            'status',
            'collection_name',
            'entity_name',
        )
        update_protected_fields = (
            'contributor',
            'input',
            'process',
            'collection',
            'entity',
        )
        fields = read_only_fields + update_protected_fields + (
            'descriptor',
            'descriptor_schema',
            'name',
            'slug',
            'tags',
        )

    def _serialize_item(self, serializer, kind, item):
        """Return serialized item or id, depending on `hydrate_XXX` query param."""
        if self.request and self.request.query_params.get('hydrate_{}'.format(kind), False):
            serializer = serializer(item, read_only=True)
            serializer.bind(kind, self)
            return serializer.data
        else:
            return item.id

    @property
    def user(self):
        """Get user."""
        return self.request.user if self.request else AnonymousUser()

    def get_collection_name(self, data):
        """Return collection name of data that user has `view` permission on."""
        if self.user.has_perm('view_collection', data.collection):
            return getattr(data.collection, 'name', '')

    def get_entity_name(self, data):
        """Return entity name of data that user has `view` permission on."""
        if self.user.has_perm('view_entity', data.entity):
            return getattr(data.entity, 'name', '')

    def get_collection(self, data):
        """Return serialized collection object of data that user has `view` permission on."""
        if not self.user.has_perm('view_collection', data.collection):
            return

        from .collection import CollectionSerializer

        class CollectionWithoutDataSerializer(WithoutDataSerializerMixin, CollectionSerializer):
            """Collection without data field serializer."""

        return self._serialize_item(CollectionWithoutDataSerializer, 'collection', data.collection)

    def get_entity(self, data):
        """Return serialized list of entity objects on data that user has `view` permission on."""
        if not self.user.has_perm('view_entity', data.entity):
            return

        from .entity import EntitySerializer

        class EntityWithoutDataSerializer(WithoutDataSerializerMixin, EntitySerializer):
            """Entity without data field serializer."""

        return self._serialize_item(EntityWithoutDataSerializer, 'entity', data.entity)

    def get_fields(self):
        """Dynamically adapt fields based on the current request."""
        fields = super(DataSerializer, self).get_fields()

        # Hide collection/entity fields on list views as fetching them may be expensive.
        if self.parent is not None:
            del fields['collection']
            del fields['entity']

        return fields

    def validate_process(self, process):
        """Check that process is active."""
        if not process.is_active:
            raise serializers.ValidationError("Process {} is not active.".format(process))
        return process
