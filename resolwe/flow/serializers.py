""".. Ignore pydocstyle D400.

================
Flow Serializers
================

"""
from __future__ import absolute_import, division, print_function, unicode_literals

from django.contrib import auth
from django.contrib.auth.models import AnonymousUser
from django.db import transaction

from rest_framework import serializers, status
from rest_framework.exceptions import APIException, ValidationError
from rest_framework.fields import empty

from resolwe.flow.models import Collection, Data, DescriptorSchema, Entity, Process, Relation, RelationType, Storage
from resolwe.flow.models.entity import PositionInRelation
from resolwe.permissions.shortcuts import get_objects_for_user
from resolwe.rest.fields import ProjectableJSONField
from resolwe.rest.serializers import SelectiveFieldMixin


class NoContentError(APIException):
    """Content has not changed exception."""

    status_code = status.HTTP_204_NO_CONTENT
    detail = 'The content has not changed'


class ContributorSerializer(SelectiveFieldMixin, serializers.ModelSerializer):
    """Serializer for contributor User objects."""

    class Meta:
        """Serializer configuration."""

        # The model needs to be determined when instantiating the serializer class as
        # the applications may not yet be ready at this point.
        model = None
        fields = ('id', 'username', 'first_name', 'last_name')

    def __init__(self, instance=None, data=empty, **kwargs):
        """Initialize attributes."""
        # Use the correct User model.
        if self.Meta.model is None:
            self.Meta.model = auth.get_user_model()

        super(ContributorSerializer, self).__init__(instance, data, **kwargs)

    def to_internal_value(self, data):
        """Format the internal value."""
        # When setting the contributor, it may be passed as an integer.
        if isinstance(data, dict) and isinstance(data.get('id', None), int):
            data = data['id']
        elif isinstance(data, int):
            pass
        else:
            raise ValidationError("Contributor must be an integer or a dictionary with key 'id'")

        return self.Meta.model.objects.get(pk=data)


class ResolweBaseSerializer(SelectiveFieldMixin, serializers.ModelSerializer):
    """Base serializer for all `Resolwe` objects.

    This class is inherited from `django_rest_framework`'s
    `ModelSerialzer` class. The difference is that
    `update_protected_fields` are removed from `data` dict when update
    is performed.

    To check whether the class is called to create an instance or
    to update an existing one, it checks its value. If the value is
    `None`, a new instance is being created.
    The `update_protected_fields` tuple can be defined in the `Meta`
    class of child class.

    `NoContentError` is raised if no data would be changed, so we
    prevent changing `modified` field.

    """

    contributor = ContributorSerializer()

    def __init__(self, instance=None, data=empty, **kwargs):
        """Initialize attributes."""
        if (instance is not None and data is not empty and
                hasattr(self.Meta, 'update_protected_fields')):  # pylint: disable=no-member
            for field in self.Meta.update_protected_fields:  # pylint: disable=no-member
                if field in data:
                    data.pop(field)  # pylint: disable=no-member

            # prevent changing `modified` field if no field would be changed
            if set(data.keys()).issubset(set(self.Meta.read_only_fields)):  # pylint: disable=no-member
                raise NoContentError()

        super(ResolweBaseSerializer, self).__init__(instance, data, **kwargs)

    @property
    def request(self):
        """Extract request object from serializer context."""
        return self.context.get('request', None)


class ProcessSerializer(ResolweBaseSerializer):
    """Serializer for Process objects."""

    class Meta:
        """ProcessSerializer Meta options."""

        model = Process
        update_protected_fields = ('contributor', )
        read_only_fields = ('id', 'created', 'modified')
        fields = ('slug', 'name', 'data_name', 'version', 'type', 'flow_collection', 'category',
                  'persistence', 'description', 'input_schema', 'output_schema', 'requirements',
                  'run', 'scheduling_class') + update_protected_fields + read_only_fields


class DescriptorSchemaSerializer(ResolweBaseSerializer):
    """Serializer for DescriptorSchema objects."""

    schema = ProjectableJSONField(required=False)

    class Meta:
        """TemplateSerializer Meta options."""

        model = DescriptorSchema
        update_protected_fields = ('contributor', )
        read_only_fields = ('id', 'created', 'modified')
        fields = ('slug', 'name', 'version', 'schema') + update_protected_fields + read_only_fields


class DataSerializer(ResolweBaseSerializer):
    """Serializer for Data objects."""

    input = ProjectableJSONField(required=False)
    output = ProjectableJSONField(required=False)
    descriptor = ProjectableJSONField(required=False)
    process_name = serializers.CharField(source='process.name', read_only=True)
    process_type = serializers.CharField(source='process.type', read_only=True)
    process_input_schema = ProjectableJSONField(source='process.input_schema', read_only=True)
    process_output_schema = ProjectableJSONField(source='process.output_schema', read_only=True)

    name = serializers.CharField(read_only=False, required=False)
    slug = serializers.CharField(read_only=False, required=False)

    class Meta:
        """DataSerializer Meta options."""

        model = Data
        update_protected_fields = ('contributor', 'process',)
        read_only_fields = ('id', 'created', 'modified', 'started', 'finished', 'checksum',
                            'status', 'process_progress', 'process_rc', 'process_info',
                            'process_warning', 'process_error', 'process_type',
                            'process_input_schema', 'process_output_schema',
                            'process_name', 'descriptor_dirty')
        fields = ('slug', 'name', 'contributor', 'input', 'output', 'descriptor_schema',
                  'descriptor', 'tags') + update_protected_fields + read_only_fields

    def get_fields(self):
        """Dynamically adapt fields based on the current request."""
        fields = super(DataSerializer, self).get_fields()

        if self.request.method == "GET":
            fields['descriptor_schema'] = DescriptorSchemaSerializer(required=False)
        else:
            fields['descriptor_schema'] = serializers.PrimaryKeyRelatedField(
                queryset=DescriptorSchema.objects.all(), required=False
            )

        return fields


class CollectionSerializer(ResolweBaseSerializer):
    """Serializer for Collection objects."""

    slug = serializers.CharField(read_only=False, required=False)
    settings = ProjectableJSONField(required=False)
    descriptor = ProjectableJSONField(required=False)

    class Meta:
        """CollectionSerializer Meta options."""

        model = Collection
        update_protected_fields = ('contributor',)
        read_only_fields = ('id', 'created', 'modified', 'descriptor_dirty')
        fields = ('slug', 'name', 'description', 'settings', 'descriptor_schema', 'descriptor',
                  'data') + update_protected_fields + read_only_fields

    def _serialize_data(self, data):
        """Return serialized data or list of ids, depending on `hydrate_data` query param."""
        if self.request and self.request.query_params.get('hydrate_data', False):
            serializer = DataSerializer(data, many=True, read_only=True)
            serializer.bind('data', self)
            return serializer.data
        else:
            return [d.id for d in data]

    def _filter_queryset(self, perms, queryset):
        """Filter object objects by permissions of user in request."""
        user = self.request.user if self.request else AnonymousUser()
        return get_objects_for_user(user, perms, queryset)

    def get_data(self, collection):
        """Return serialized list of data objects on collection that user has `view` permission on."""
        data = self._filter_queryset('view_data', collection.data.all())

        return self._serialize_data(data)

    def get_fields(self):
        """Dynamically adapt fields based on the current request."""
        fields = super(CollectionSerializer, self).get_fields()

        if self.request.method == "GET":
            fields['data'] = serializers.SerializerMethodField()
        else:
            fields['data'] = serializers.PrimaryKeyRelatedField(many=True, read_only=True)

        if self.request.method == "GET":
            fields['descriptor_schema'] = DescriptorSchemaSerializer(required=False)
        else:
            fields['descriptor_schema'] = serializers.PrimaryKeyRelatedField(
                queryset=DescriptorSchema.objects.all(), required=False
            )

        return fields


class EntitySerializer(CollectionSerializer):
    """Serializer for Entity."""

    collections = serializers.PrimaryKeyRelatedField(many=True, read_only=True)

    class Meta(CollectionSerializer.Meta):
        """Serializer configuration."""

        model = Entity
        fields = CollectionSerializer.Meta.fields + ('collections', 'descriptor_completed', 'tags')

    def get_data(self, entity):
        """Return serialized list of data objects on entity that user has `view` permission on."""
        data = self._filter_queryset('view_data', entity.data.all())

        return self._serialize_data(data)


class StorageSerializer(ResolweBaseSerializer):
    """Serializer for Storage objects."""

    json = ProjectableJSONField()

    class Meta:
        """StorageSerializer Meta options."""

        model = Storage
        update_protected_fields = ('contributor', )
        read_only_fields = ('id', 'created', 'modified')
        fields = ('slug', 'name', 'data', 'json') + update_protected_fields + read_only_fields


class PositionInRelationSerializer(SelectiveFieldMixin, serializers.ModelSerializer):
    """Serializer for PositionInRelation objects."""

    position = ProjectableJSONField(allow_null=True, required=False)

    class Meta:
        """PositionInRelationSerializer Meta options."""

        model = PositionInRelation
        fields = ('entity', 'position')


class RelationSerializer(ResolweBaseSerializer):
    """Serializer for Relation objects."""

    entities = PositionInRelationSerializer(source='positioninrelation_set', many=True)
    collection = serializers.PrimaryKeyRelatedField(queryset=Collection.objects.all(), required=True)

    class Meta:
        """RelationSerializer Meta options."""

        model = Relation
        update_protected_fields = ('contributor', 'entities', 'type')
        read_only_fields = ('id', 'created', 'modified')
        fields = ('collection', 'entities', 'label') + update_protected_fields + read_only_fields

    def get_fields(self):
        """Dynamically adapt fields based on the current request."""
        fields = super(RelationSerializer, self).get_fields()

        if self.request.method == "GET":
            fields['type'] = serializers.CharField(source='type.name')
        else:
            fields['type'] = serializers.PrimaryKeyRelatedField(queryset=RelationType.objects.all())

        return fields

    def create(self, validated_data):
        """Create ``Relation`` object and add ``Entities``."""
        # `entities` field is renamed to `positioninrelation_set` based on source of nested serializer

        entities = validated_data.pop('positioninrelation_set', {})

        # Prevent "Failed to populate slug Relation.slug from name" output
        validated_data['name'] = 'Relation'

        with transaction.atomic():
            instance = Relation.objects.create(**validated_data)
            for entity in entities:
                PositionInRelation.objects.create(
                    relation=instance,
                    entity=entity['entity'],
                    position=entity.get('position', None)
                )

        return instance
