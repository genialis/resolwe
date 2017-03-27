""".. Ignore pydocstyle D400.

============
Flow Filters
============

"""
from __future__ import absolute_import, division, print_function, unicode_literals

import rest_framework_filters as filters
from django_filters.filters import BaseCSVFilter

from .models import Collection, Data, DescriptorSchema, Entity, Process


class BaseResolweFilter(filters.FilterSet):
    """Base filter for Resolwe's endpoints."""

    id = filters.AllLookupsFilter()  # pylint: disable=invalid-name
    slug = filters.AllLookupsFilter()
    name = filters.AllLookupsFilter()
    contributor = filters.NumberFilter()
    created = filters.AllLookupsFilter()
    modified = filters.AllLookupsFilter()


class DescriptorSchemaFilter(BaseResolweFilter):
    """Filter the DescriptorSchema endpoint."""

    class Meta:
        """Filter configuration."""

        model = DescriptorSchema


class CollectionFilter(BaseResolweFilter):
    """Filter the Collection endpoint."""

    data = filters.ModelChoiceFilter(queryset=Data.objects.all())
    descriptor_schema = filters.RelatedFilter(DescriptorSchemaFilter)
    entity = filters.ModelChoiceFilter(queryset=Entity.objects.all())
    descriptor_schema = filters.RelatedFilter(DescriptorSchemaFilter, name='descriptor_schema')
    description = filters.AllLookupsFilter()

    class Meta:
        """Filter configuration."""

        model = Collection


class TagsFilter(BaseCSVFilter, filters.CharFilter):
    """Filter for tags."""

    def __init__(self, *args, **kwargs):
        """Construct tags filter."""
        kwargs.setdefault('lookup_type', 'contains')
        super(TagsFilter, self).__init__(*args, **kwargs)


class EntityFilter(CollectionFilter):
    """Filter the Entity endpoint."""

    collection = filters.ModelChoiceFilter(queryset=Collection.objects.all())
    tags = TagsFilter()

    class Meta(CollectionFilter.Meta):
        """Filter configuration."""

        model = Entity


class ProcessFilter(BaseResolweFilter):
    """Filter the Process endpoint."""

    category = filters.CharFilter(name='category', lookup_type='startswith')

    class Meta:
        """Filter configuration."""

        model = Process


class DataFilter(BaseResolweFilter):
    """Filter the Data endpoint."""

    collection = filters.RelatedFilter(CollectionFilter)
    entity = filters.ModelChoiceFilter(queryset=Entity.objects.all())
    type = filters.CharFilter(name='process__type', lookup_type='startswith')
    status = filters.CharFilter(lookup_expr='iexact')
    finished = filters.AllLookupsFilter()
    started = filters.AllLookupsFilter()
    process = filters.RelatedFilter(ProcessFilter)
    tags = TagsFilter()

    class Meta:
        """Filter configuration."""

        model = Data
