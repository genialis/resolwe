""".. Ignore pydocstyle D400.

============
Flow Filters
============

"""
import django_filters as filters

from .models import Collection, Data, DescriptorSchema, Entity, Process, Relation

NUMBER_LOOKUPS = [
    'exact',
    'in',
    'gt', 'gte', 'lt', 'lte',
    'isnull',
]
TEXT_LOOKUPS = [
    'exact', 'iexact',
    'contains', 'icontains',
    'in',
    'startswith', 'istartswith',
    'endswith', 'iendswith',
    'isnull',
]
DATE_LOOKUPS = [
    'exact',
    'gt', 'gte', 'lt', 'lte',
    'year', 'year__gt', 'year__gte', 'year__lt', 'year__lte',
    'month', 'month__gt', 'month__gte', 'month__lt', 'month__lte',
    'day', 'day__gt', 'day__gte', 'day__lt', 'day__lte',
    'isnull',

]
DATETIME_LOOKUPS = DATE_LOOKUPS + [
    'date',
    'time',
    'hour', 'hour__gt', 'hour__gte', 'hour__lt', 'hour__lte',
    'minute', 'minute__gt', 'minute__gte', 'minute__lt', 'minute__lte',
    'second', 'second__gt', 'second__gte', 'second__lt', 'second__lte',
]


class BaseResolweFilter(filters.FilterSet):
    """Base filter for Resolwe's endpoints."""

    class Meta:
        """Filter configuration."""

        fields = {
            'id': NUMBER_LOOKUPS[:],
            'slug': TEXT_LOOKUPS[:],
            'name': TEXT_LOOKUPS[:],
            'contributor': ['exact', 'in'],
            'created': DATETIME_LOOKUPS[:],
            'modified': DATETIME_LOOKUPS[:],
        }


class DescriptorSchemaFilter(BaseResolweFilter):
    """Filter the DescriptorSchema endpoint."""

    class Meta(BaseResolweFilter.Meta):
        """Filter configuration."""

        model = DescriptorSchema


class CollectionFilter(BaseResolweFilter):
    """Filter the Collection endpoint."""

    data = filters.ModelChoiceFilter(queryset=Data.objects.all())
    entity = filters.ModelChoiceFilter(queryset=Entity.objects.all())

    class Meta(BaseResolweFilter.Meta):
        """Filter configuration."""

        model = Collection
        fields = {**BaseResolweFilter.Meta.fields, **{
            'description': TEXT_LOOKUPS[:],
            'descriptor_schema': ['exact'],
        }}


class TagsFilter(filters.filters.BaseCSVFilter, filters.CharFilter):
    """Filter for tags."""

    def __init__(self, *args, **kwargs):
        """Construct tags filter."""
        kwargs.setdefault('lookup_expr', 'contains')
        super().__init__(*args, **kwargs)


class EntityFilter(CollectionFilter):
    """Filter the Entity endpoint."""

    collection = filters.ModelChoiceFilter(field_name='collections', queryset=Collection.objects.all())
    descriptor_completed = filters.rest_framework.filters.BooleanFilter(field_name='descriptor_completed')
    tags = TagsFilter()

    class Meta(CollectionFilter.Meta):
        """Filter configuration."""

        model = Entity


class ProcessFilter(BaseResolweFilter):
    """Filter the Process endpoint."""

    category = filters.CharFilter(field_name='category', lookup_expr='startswith')
    type = filters.CharFilter(field_name='type', lookup_expr='startswith')
    is_active = filters.rest_framework.filters.BooleanFilter(field_name='is_active')

    class Meta(BaseResolweFilter.Meta):
        """Filter configuration."""

        model = Process
        fields = {**BaseResolweFilter.Meta.fields, **{
            'scheduling_class': ['exact'],
        }}


class CharInFilter(filters.BaseInFilter, filters.CharFilter):
    """Helper class for creation of CharFilter with "in" lookup."""

    pass


class DataFilter(BaseResolweFilter):
    """Filter the Data endpoint."""

    collection = filters.ModelChoiceFilter(queryset=Collection.objects.all())
    collection__slug = filters.CharFilter(field_name='collection__slug', lookup_expr='exact')

    entity = filters.ModelChoiceFilter(queryset=Entity.objects.all())

    parents = filters.ModelChoiceFilter(queryset=Data.objects.all())
    children = filters.ModelChoiceFilter(queryset=Data.objects.all())

    type = filters.CharFilter(field_name='process__type', lookup_expr='startswith')
    status = filters.CharFilter(lookup_expr='iexact')
    status__in = CharInFilter(field_name='status', lookup_expr='in')

    tags = TagsFilter()

    class Meta(BaseResolweFilter.Meta):
        """Filter configuration."""

        model = Data
        fields = {**BaseResolweFilter.Meta.fields, **{
            'process': ['exact'],
            'process__slug': ['exact'],
            'finished': DATETIME_LOOKUPS[:],
            'started': DATETIME_LOOKUPS[:],
        }}


class RelationFilter(BaseResolweFilter):
    """Filter the Relation endpoint."""

    category = filters.CharFilter(lookup_expr='iexact')
    collection = filters.ModelChoiceFilter(queryset=Collection.objects.all())
    type = filters.CharFilter(field_name='type__name')

    class Meta(BaseResolweFilter.Meta):
        """Filter configuration."""

        model = Relation
        fields = BaseResolweFilter.Meta.fields
