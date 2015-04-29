"""
================
Flow Serializers
================

"""
from rest_framework import serializers

from .models import Project, Tool, Data, Template, Trigger, Storage


class ProjectSerializer(serializers.ModelSerializer):

    """Serializer for Project objects."""

    class Meta:
        """ProjectSerializer Meta options."""
        model = Project
        fields = ('slug', 'title', 'created', 'modified', 'created_by', 'description', 'settings',
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
        fields = ('slug', 'title', 'created', 'modified', 'created_by', 'version', 'type',
                  'category', 'persistence', 'description', 'input_schema', 'output_schema',
                  'static_schema', 'run', )


class DataSerializer(serializers.ModelSerializer):

    """Serializer for Data objects."""

    class Meta:
        """DataSerializer Meta options."""
        model = Data
        fields = ('slug', 'title', 'created', 'modified', 'created_by', 'started', 'finished',
                  'checksum', 'status', 'persistence', 'tool', 'template', 'input', 'output',
                  'static', 'var')


class TemplateSerializer(serializers.ModelSerializer):

    """Serializer for Template objects."""

    class Meta:
        """TemplateSerializer Meta options."""
        model = Template
        fields = ('slug', 'title', 'created', 'modified', 'created_by', 'version', 'schema')


class TriggerSerializer(serializers.ModelSerializer):

    """Serializer for Trigger objects."""

    class Meta:
        """TriggerSerializer Meta options."""
        model = Trigger
        fields = ('slug', 'title', 'created', 'modified', 'created_by', 'trugger', 'trigger_input',
                  'tool', 'input', 'project', 'autorun')


class StorageSerializer(serializers.ModelSerializer):

    """Serializer for Storage objects."""

    class Meta:
        """StorageSerializer Meta options."""
        model = Storage
        fields = ('slug', 'title', 'created', 'modified', 'created_by', 'data', 'json')
