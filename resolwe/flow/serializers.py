"""
================
Flow Serializers
================

"""
from __future__ import absolute_import, division, print_function, unicode_literals

from rest_framework import serializers, status
from rest_framework.exceptions import APIException
from rest_framework.fields import empty
from resolwe.flow.models import Process, Collection, Data, DescriptorSchema, Trigger, Storage


class NoContentError(APIException):
    status_code = status.HTTP_204_NO_CONTENT
    detail = 'The content has not changed'


class ResolweBaseSerializer(serializers.ModelSerializer):
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
    def __init__(self, instance=None, data=empty, **kwargs):
        if (instance is not None and data is not empty and
                hasattr(self.Meta, 'update_protected_fields')):
            for field in self.Meta.update_protected_fields:
                if field in data:
                    data.pop(field)

            # prevent changing `modified` field if no field would be changed
            if set(data.keys()).issubset(set(self.Meta.read_only_fields)):
                raise NoContentError()

        super(ResolweBaseSerializer, self).__init__(instance, data, **kwargs)


class ProcessSerializer(ResolweBaseSerializer):

    """Serializer for Process objects."""

    class Meta:
        """ProcessSerializer Meta options."""
        model = Process
        update_protected_fields = ('contributor', )
        read_only_fields = ('id', 'created', 'modified')
        fields = ('slug', 'name', 'data_name', 'version', 'type',
                  'flow_collection', 'category', 'persistence',
                  'priority', 'description', 'input_schema',
                  'output_schema', 'run') + update_protected_fields + read_only_fields


class DescriptorSchemaSerializer(ResolweBaseSerializer):

    """Serializer for DescriptorSchema objects."""

    class Meta:
        """TemplateSerializer Meta options."""
        model = DescriptorSchema
        update_protected_fields = ('contributor', )
        read_only_fields = ('id', 'created', 'modified')
        fields = ('slug', 'name', 'version', 'schema') + update_protected_fields + read_only_fields


class DataSerializer(ResolweBaseSerializer):

    """Serializer for Data objects."""

    process_name = serializers.CharField(source='process.name', read_only=True)
    process_type = serializers.CharField(source='process.type', read_only=True)
    process_input_schema = serializers.JSONField(source='process.input_schema', read_only=True)
    process_output_schema = serializers.JSONField(source='process.output_schema', read_only=True)
    descriptor_schema = DescriptorSchemaSerializer(required=False, read_only=True)

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
                            'process_name')
        fields = ('slug', 'name', 'contributor', 'input', 'output', 'descriptor_schema',
                  'descriptor') + update_protected_fields + read_only_fields


class CollectionSerializer(ResolweBaseSerializer):

    """Serializer for Collection objects."""

    descriptor_schema = DescriptorSchemaSerializer(required=False)

    slug = serializers.CharField(read_only=False, required=False)

    class Meta:
        """CollectionSerializer Meta options."""
        model = Collection
        update_protected_fields = ('contributor',)
        read_only_fields = ('id', 'created', 'modified')
        fields = ('slug', 'name', 'description', 'settings', 'descriptor_schema', 'descriptor',
                  'data') + update_protected_fields + read_only_fields

    def __init__(self, *args, **kwargs):
        super(CollectionSerializer, self).__init__(*args, **kwargs)

        request = kwargs.get('context', {}).get('request', None)

        if request and request.query_params.get('hydrate_data', False):
            self.fields['data'] = DataSerializer(many=True, read_only=True)
        else:
            self.fields['data'] = serializers.PrimaryKeyRelatedField(many=True, read_only=True)


class TriggerSerializer(ResolweBaseSerializer):

    """Serializer for Trigger objects."""

    class Meta:
        """TriggerSerializer Meta options."""
        model = Trigger
        update_protected_fields = ('contributor', )
        read_only_fields = ('id', 'created', 'modified')
        fields = ('slug', 'name', 'trigger', 'trigger_input', 'process', 'input', 'collection',
                  'autorun') + update_protected_fields + read_only_fields


class StorageSerializer(ResolweBaseSerializer):

    """Serializer for Storage objects."""

    class Meta:
        """StorageSerializer Meta options."""
        model = Storage
        update_protected_fields = ('contributor', )
        read_only_fields = ('id', 'created', 'modified')
        fields = ('slug', 'name', 'data', 'json') + update_protected_fields + read_only_fields
