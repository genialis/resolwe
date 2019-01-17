""".. Ignore pydocstyle D400.

============
Flow Filters
============

"""
import rest_framework_filters as filters
from django_filters.filters import BaseCSVFilter

from .models import Collection, Data, DescriptorSchema, Entity, Process, Relation


class BaseResolweFilter(filters.FilterSet):
    """Base filter for Resolwe's endpoints."""

    id = filters.AutoFilter(lookups='__all__')  # pylint: disable=invalid-name
    slug = filters.AutoFilter(lookups='__all__')
    name = filters.AutoFilter(lookups='__all__')
    contributor = filters.NumberFilter()
    created = filters.AutoFilter(lookups='__all__')
    modified = filters.AutoFilter(lookups='__all__')

    class Meta:
        """Filter configuration."""

        fields = ['id', 'slug', 'name', 'contributor', 'created', 'modified']


class DescriptorSchemaFilter(BaseResolweFilter):
    """Filter the DescriptorSchema endpoint."""

    class Meta(BaseResolweFilter.Meta):
        """Filter configuration."""

        model = DescriptorSchema


class CollectionFilter(BaseResolweFilter):
    """Filter the Collection endpoint."""

    data = filters.ModelChoiceFilter(queryset=Data.objects.all())
    entity = filters.ModelChoiceFilter(queryset=Entity.objects.all())
    descriptor_schema = filters.RelatedFilter(
        DescriptorSchemaFilter, field_name='descriptor_schema', queryset=DescriptorSchema.objects.all()
    )
    description = filters.AutoFilter(lookups='__all__')

    class Meta(BaseResolweFilter.Meta):
        """Filter configuration."""

        model = Collection
        fields = BaseResolweFilter.Meta.fields + ['data', 'entity', 'descriptor_schema', 'description']


class TagsFilter(BaseCSVFilter, filters.CharFilter):
    """Filter for tags."""

    def __init__(self, *args, **kwargs):
        """Construct tags filter."""
        kwargs.setdefault('lookup_expr', 'contains')
        super().__init__(*args, **kwargs)


class EntityFilter(CollectionFilter):
    """Filter the Entity endpoint."""

    collections = filters.ModelChoiceFilter(queryset=Collection.objects.all())
    descriptor_completed = filters.BooleanFilter()
    tags = TagsFilter()

    class Meta(CollectionFilter.Meta):
        """Filter configuration."""

        model = Entity
        fields = BaseResolweFilter.Meta.fields + ['collections', 'descriptor_completed', 'tags']


class ProcessFilter(BaseResolweFilter):
    """Filter the Process endpoint."""

    category = filters.CharFilter(field_name='category', lookup_expr='startswith')
    type = filters.CharFilter(field_name='type', lookup_expr='startswith')
    scheduling_class = filters.AutoFilter(lookups='__all__')
    is_active = filters.BooleanFilter()

    class Meta(BaseResolweFilter.Meta):
        """Filter configuration."""

        model = Process
        fields = BaseResolweFilter.Meta.fields + ['category', 'type', 'scheduling_class', 'is_active']


class DataFilter(BaseResolweFilter):
    """Filter the Data endpoint."""

    collection = filters.RelatedFilter(CollectionFilter, queryset=Collection.objects.all())
    entity = filters.ModelChoiceFilter(queryset=Entity.objects.all())
    type = filters.CharFilter(field_name='process__type', lookup_expr='startswith')
    status = filters.CharFilter(lookup_expr='iexact')
    finished = filters.AutoFilter(lookups='__all__')
    started = filters.AutoFilter(lookups='__all__')
    process = filters.RelatedFilter(ProcessFilter, queryset=Process.objects.all())
    tags = TagsFilter()
    parents = filters.ModelChoiceFilter(queryset=Data.objects.all())
    children = filters.ModelChoiceFilter(queryset=Data.objects.all())

    class Meta(BaseResolweFilter.Meta):
        """Filter configuration."""

        model = Data
        fields = BaseResolweFilter.Meta.fields + [
            'collection', 'entity', 'type', 'status', 'finished', 'started', 'process', 'tags',
            'parents', 'children'
        ]


class RelationFilter(BaseResolweFilter):
    """Filter the Relation endpoint."""

    category = filters.CharFilter(lookup_expr='iexact')
    collection = filters.ModelChoiceFilter(queryset=Collection.objects.all())
    type = filters.CharFilter(field_name='type__name')

    class Meta(BaseResolweFilter.Meta):
        """Filter configuration."""

        model = Relation
        fields = BaseResolweFilter.Meta.fields + [
            'category', 'collection', 'type'
        ]
