"""
================
Apps Serializers
================

"""
from __future__ import absolute_import, division, print_function, unicode_literals

from ..flow.serializers import ResolweBaseSerializer
from .models import Package, App


class PackageSerializer(ResolweBaseSerializer):

    """Serializer for Package objects."""

    class Meta:
        """PackageSerializer Meta options."""
        model = Package
        update_protected_fields = ('contributor',)
        read_only_fields = ('id', 'created', 'modified', 'version', 'index', 'modules')
        fields = ('slug', 'title') + update_protected_fields + read_only_fields


class AppSerializer(ResolweBaseSerializer):

    """Serializer for Apps objects."""

    class Meta:
        """AppSerializer Meta options."""
        model = App
        update_protected_fields = ('contributor',)
        read_only_fields = ('id', 'created', 'modified')
        fields = ('slug', 'title', 'package', 'modules', 'collections', 'default_collection',
                  'description') + update_protected_fields + read_only_fields
