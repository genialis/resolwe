"""
================
Apps Serializers
================

"""
from rest_framework import serializers

from .models import Package, App


class PackageSerializer(serializers.ModelSerializer):

    """Serializer for Package objects."""

    class Meta:
        """PackageSerializer Meta options."""
        model = Package
        fields = ('slug', 'title', 'created', 'modified', 'created_by', 'version', 'modules', 'index')


class AppSerializer(serializers.ModelSerializer):

    """Serializer for Apps objects."""

    class Meta:
        """AppSerializer Meta options."""
        model = App
        fields = ('slug', 'title', 'created', 'modified', 'created_by', 'package', 'modules', 'projects',
                  'default_project', 'description')
