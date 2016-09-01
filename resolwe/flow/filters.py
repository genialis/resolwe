""".. Ignore pydocstyle D400.

============
Flow Filters
============

"""
from __future__ import absolute_import, division, print_function, unicode_literals

import rest_framework_filters as filters

from .models import Collection, Data, DescriptorSchema, Process


class DescriptorSchemaFilter(filters.FilterSet):
    """Filter the DescriptorSchema endpoint."""

    class Meta:
        """Filter configuration."""

        model = DescriptorSchema
        fields = {
            'slug': '__all__',
            'id': '__all__',
        }


class CollectionFilter(filters.FilterSet):
    """Filter the Collection endpoint."""

    data = filters.ModelChoiceFilter(queryset=Data.objects.all())
    descriptor_schema = filters.RelatedFilter(DescriptorSchemaFilter, name='descriptor_schema')

    class Meta:
        """Filter configuration."""

        model = Collection
        fields = {
            'contributor': ['exact', ],
            'created': '__all__',
            'data': ['exact', ],
            'description': '__all__',
            'descriptor_schema': ['exact', ],
            'id': '__all__',
            'modified': '__all__',
            'name': '__all__',
            'slug': '__all__',
        }


class DataFilter(filters.FilterSet):
    """Filter the Data endpoint."""

    collection = filters.ModelChoiceFilter(queryset=Collection.objects.all())
    type = filters.CharFilter(name='process__type', lookup_type='startswith')

    class Meta:
        """Filter configuration."""

        model = Data
        fields = {
            'collection': ['exact', ],
            'contributor': ['exact', ],
            'created': '__all__',
            'finished': '__all__',
            'id': '__all__',
            'modified': '__all__',
            'name': '__all__',
            'process': ['exact', ],
            'slug': '__all__',
            'started': '__all__',
            'status': '__all__',
            'type': ['exact', ],
        }


class ProcessFilter(filters.FilterSet):
    """Filter the Process endpoint."""

    category = filters.CharFilter(name='category', lookup_type='startswith')

    class Meta:
        """Filter configuration."""

        model = Process
        fields = {
            'category': ['exact', ],
            'contributor': ['exact', ],
            'created': '__all__',
            'id': '__all__',
            'modified': '__all__',
            'name': '__all__',
            'slug': '__all__',
        }
