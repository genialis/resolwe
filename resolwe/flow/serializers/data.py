"""Resolwe data serializer."""
from django.contrib.auth.models import AnonymousUser

from rest_framework import serializers

from resolwe.flow.models import Data, Process
from resolwe.permissions.shortcuts import get_objects_for_user
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
    entity_names = serializers.SerializerMethodField()
    collection_names = serializers.SerializerMethodField()

    collections = serializers.SerializerMethodField()
    entities = serializers.SerializerMethodField()

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
            'collection_names',
            'entity_names',
        )
        update_protected_fields = (
            'contributor',
            'input',
            'process',
            'collections',
            'entities',
        )
        fields = read_only_fields + update_protected_fields + (
            'descriptor',
            'descriptor_schema',
            'name',
            'slug',
            'tags',
        )

    def _serialize_items(self, serializer, kind, items):
        """Return serialized items or list of ids, depending on `hydrate_XXX` query param."""
        if self.request and self.request.query_params.get('hydrate_{}'.format(kind), False):
            serializer = serializer(items, many=True, read_only=True)
            serializer.bind(kind, self)
            return serializer.data
        else:
            return [item.id for item in items]

    def _filter_queryset(self, perms, queryset):
        """Filter object objects by permissions of user in request."""
        user = self.request.user if self.request else AnonymousUser()
        return get_objects_for_user(user, perms, queryset)

    def get_collection_names(self, data):
        """Return serialized list of collection names on data that user has `view` permission on."""
        collections = self._filter_queryset('view_collection', data.collection_set.all())
        return list(collections.values_list('name', flat=True))

    def get_entity_names(self, data):
        """Return serialized list of entity names on data that user has `view` permission on."""
        entities = self._filter_queryset('view_entity', data.entity_set.all())
        return list(entities.values_list('name', flat=True))

    def get_collections(self, data):
        """Return serialized list of collection objects on data that user has `view` permission on."""
        collections = self._filter_queryset('view_collection', data.collection_set.all())

        from .collection import CollectionSerializer

        class CollectionWithoutDataSerializer(WithoutDataSerializerMixin, CollectionSerializer):
            """Collection without data field serializer."""

        return self._serialize_items(CollectionWithoutDataSerializer, 'collections', collections)

    def get_entities(self, data):
        """Return serialized list of entity objects on data that user has `view` permission on."""
        entities = self._filter_queryset('view_entity', data.entity_set.all())

        from .entity import EntitySerializer

        class EntityWithoutDataSerializer(WithoutDataSerializerMixin, EntitySerializer):
            """Entity without data field serializer."""

        return self._serialize_items(EntityWithoutDataSerializer, 'entities', entities)

    def get_fields(self):
        """Dynamically adapt fields based on the current request."""
        fields = super(DataSerializer, self).get_fields()

        # Hide collections/entities fields on list views as fetching them may be expensive.
        if self.parent is not None:
            del fields['collections']
            del fields['entities']

        return fields
