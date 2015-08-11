"""
================
Flow Serializers
================

"""
from __future__ import absolute_import, division, print_function, unicode_literals

from rest_framework import serializers, status
from rest_framework.exceptions import APIException
from rest_framework.fields import empty
from resolwe.flow.models import Project, Tool, Data, AnnotationSchema, Trigger, Storage


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
        if (instance is not None and data is not empty
                and hasattr(self.Meta, 'update_protected_fields')):
            for field in self.Meta.update_protected_fields:
                if field in data:
                    data.pop(field)

            # prevent changing `modified` field if no field would be changed
            if set(data.keys()).issubset(set(self.Meta.read_only_fields)):
                raise NoContentError()

        return super(ResolweBaseSerializer, self).__init__(instance, data, **kwargs)


class ProjectSerializer(ResolweBaseSerializer):

    """Serializer for Project objects."""

    class Meta:
        """ProjectSerializer Meta options."""
        model = Project
        update_protected_fields = ('contributor',)
        read_only_fields = ('id', 'created', 'modified')
        fields = ('slug', 'name', 'description', 'settings') + update_protected_fields + read_only_fields


class ToolSerializer(ResolweBaseSerializer):

    """Serializer for Tool objects."""

    class Meta:
        """ToolSerializer Meta options."""
        model = Tool
        update_protected_fields = ('contributor', )
        read_only_fields = ('slug', 'name', 'created', 'modified', 'version', 'type', 'category',
                            'persistence', 'description', 'input_schema', 'output_schema',
                            'adapter', )
        fields = update_protected_fields + read_only_fields


class DataSerializer(ResolweBaseSerializer):

    """Serializer for Data objects."""

    class Meta:
        """DataSerializer Meta options."""
        model = Data
        update_protected_fields = ('contributor', 'tool')
        read_only_fields = ('id', 'created', 'modified', 'started', 'finished', 'checksum',
                            'status', 'tool_progress', 'tool_rc', 'tool_info', 'tool_warning',
                            'tool_error')
        fields = ('slug', 'name', 'contributor', 'input', 'output', 'annotation_schema',
                  'annotation') + update_protected_fields + read_only_fields


class AnnotationSchemaSerializer(ResolweBaseSerializer):

    """Serializer for AnnotationSchema objects."""

    class Meta:
        """TemplateSerializer Meta options."""
        model = AnnotationSchema
        update_protected_fields = ('contributor', )
        read_only_fields = ('created', 'modified')
        fields = ('slug', 'name', 'version', 'schema') + update_protected_fields + read_only_fields


class TriggerSerializer(ResolweBaseSerializer):

    """Serializer for Trigger objects."""

    class Meta:
        """TriggerSerializer Meta options."""
        model = Trigger
        update_protected_fields = ('contributor', )
        read_only_fields = ('created', 'modified')
        fields = ('slug', 'name', 'trigger', 'trigger_input', 'tool', 'input', 'project',
                  'autorun') + update_protected_fields + read_only_fields


class StorageSerializer(ResolweBaseSerializer):

    """Serializer for Storage objects."""

    class Meta:
        """StorageSerializer Meta options."""
        model = Storage
        update_protected_fields = ('contributor', )
        read_only_fields = ('created', 'modified')
        fields = ('slug', 'name', 'data', 'json') + update_protected_fields + read_only_fields
