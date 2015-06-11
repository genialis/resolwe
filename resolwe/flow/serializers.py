"""
================
Flow Serializers
================

"""
from __future__ import absolute_import, division, print_function, unicode_literals

from rest_framework import serializers

from resolwe.flow.models import Project, Tool, Data, AnnotationSchema, Trigger, Storage


class ProjectSerializer(serializers.ModelSerializer):

    """Serializer for Project objects."""

    class Meta:
        """ProjectSerializer Meta options."""
        model = Project
        fields = ('slug', 'name', 'created', 'modified', 'contributor', 'description', 'settings',
                  'data')
        readonly_fields = ('created_by')

    def update(self, instance, validated_data):
        instance.created = instance.created
        instance.modified = instance.modified
        instance.created_by = instance.created_by
        instance.save()
        return instance


class ToolSerializer(serializers.ModelSerializer):

    """Serializer for Tool objects."""

    class Meta:
        """ToolSerializer Meta options."""
        model = Tool
        fields = ('slug', 'name', 'created', 'modified', 'contributor', 'version', 'type',
                  'category', 'persistence', 'description', 'input_schema', 'output_schema',
                  'static_schema', 'run', )


class DataSerializer(serializers.ModelSerializer):

    """Serializer for Data objects."""

    class Meta:
        """DataSerializer Meta options."""
        model = Data
        fields = ('slug', 'name', 'created', 'modified', 'contributor', 'started', 'finished',
                  'checksum', 'status', 'persistence', 'tool', 'template', 'input', 'output',
                  'static', 'var')


class AnnotationSchemaSerializer(serializers.ModelSerializer):

    """Serializer for AnnotationSchema objects."""

    class Meta:
        """TemplateSerializer Meta options."""
        model = AnnotationSchema
        fields = ('slug', 'name', 'created', 'modified', 'contributor', 'version', 'schema')


class TriggerSerializer(serializers.ModelSerializer):

    """Serializer for Trigger objects."""

    class Meta:
        """TriggerSerializer Meta options."""
        model = Trigger
        fields = ('slug', 'name', 'created', 'modified', 'contributor', 'trugger', 'trigger_input',
                  'tool', 'input', 'project', 'autorun')


class StorageSerializer(serializers.ModelSerializer):

    """Serializer for Storage objects."""

    class Meta:
        """StorageSerializer Meta options."""
        model = Storage
        fields = ('slug', 'name', 'created', 'modified', 'contributor', 'data', 'json')
