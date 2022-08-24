""".. Ignore pydocstyle D400.

============
Flow Filters
============

"""
from functools import partial

from django_filters import rest_framework as filters
from django_filters.filterset import FilterSetMetaclass
from versionfield import VersionField

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.contrib.postgres.search import SearchQuery, SearchRank
from django.db.models import Count, F, ForeignKey, Q, Subquery

from rest_framework import exceptions
from rest_framework.filters import OrderingFilter as DrfOrderingFilter

from resolwe.composer import composer
from resolwe.permissions.models import Permission, get_anonymous_user

from .models import (
    AnnotationField,
    AnnotationPreset,
    AnnotationValue,
    Collection,
    Data,
    DescriptorSchema,
    Entity,
    Process,
    Relation,
)

RELATED_LOOKUPS = [
    "exact",
    "in",
    "isnull",
]
NUMBER_LOOKUPS = [
    "exact",
    "in",
    "gt",
    "gte",
    "lt",
    "lte",
    "isnull",
]
TEXT_LOOKUPS = [
    "exact",
    "iexact",
    "contains",
    "icontains",
    "in",
    "startswith",
    "istartswith",
    "isnull",
]
DATE_LOOKUPS = [
    "exact",
    "gt",
    "gte",
    "lt",
    "lte",
    "isnull",
]
DATETIME_LOOKUPS = DATE_LOOKUPS + [
    "date",
    "time",
]
SLUG_LOOKUPS = [
    "exact",
    "in",
]


user_model = get_user_model()


class CheckQueryParamsMixin:
    """Custom query params validation."""

    def get_always_allowed_arguments(self):
        """Get always allowed query arguments."""
        return (
            "fields",
            "format",
            "limit",
            "offset",
            "ordering",
        )

    def validate_query_params(self):
        """Ensure no unsupported query params were used."""
        allowed_params = set(self.get_filters().keys())
        allowed_params.update(self.get_always_allowed_arguments())

        unallowed = set(self.request.query_params.keys()) - allowed_params

        if unallowed:
            msg = (
                "Unsupported parameter(s): {}. Please use a combination of: {}.".format(
                    ", ".join(unallowed), ", ".join(allowed_params)
                )
            )
            self.form.add_error(field=None, error=msg)

    def is_valid(self):
        """Validate filterset."""
        self.validate_query_params()
        return super().is_valid()


class TextFilterMixin:
    """Mixin for full-text filtering."""

    def filter_text(self, queryset, name, value):
        """Full-text search."""
        query = SearchQuery(value, config="simple")
        return (
            queryset.filter(**{name: query})
            # This assumes that field is already a TextSearch vector and thus
            # doesn't need to be transformed. To achieve that F function is
            # required.
            .annotate(rank=SearchRank(F(name), query)).order_by("-rank")
        )


class UserFilterMixin:
    """Mixin for filtering by contributors and owners."""

    @staticmethod
    def _get_user_subquery(value):
        user_subquery = user_model.objects.all()
        for key in value.split():
            user_subquery = user_subquery.extra(
                where=["CONCAT_WS(' ', first_name, last_name, username) ILIKE %s"],
                params=("%{}%".format(key),),
            )

        return user_subquery

    def filter_owners(self, queryset, name, value):
        """Filter queryset by owner's id."""
        try:
            user = user_model.objects.get(pk=value)
        except user_model.DoesNotExist:
            return queryset.none()

        return queryset.filter_for_user(
            user, self.owner_permission, with_superuser=False
        )

    def filter_owners_name(self, queryset, name, value):
        """Filter queryset by owner's name."""
        result = queryset.model.objects.none()
        user_subquery = self._get_user_subquery(value)
        for user in user_subquery:
            result = result.union(
                queryset.filter_for_user(
                    user, self.owner_permission, with_superuser=False
                )
            )

        # Union can no longer be filtered, so we have to create a new queryset
        # for following filters.
        return result.model.objects.filter(pk__in=Subquery(result.values("pk")))

    def filter_contributor_name(self, queryset, name, value):
        """Filter queryset by owner's name."""
        return queryset.filter(contributor__in=self._get_user_subquery(value))

    def filter_for_user(self, queryset, name, value):
        """Filter queryset by permissions."""
        user = self.request.user
        return queryset.filter_for_user(user, Permission.from_name(value))

    def filter_for_group(self, queryset, name, value):
        """Filter queryset by group."""
        return queryset.filter(permission_group__permissions__group=value)

    def filter_private(self, queryset, name, value):
        """Return only elements that are not public (explicitly shared with me)."""

        public_filter = Q(permission_group__permissions__user=get_anonymous_user())

        # Exclude public elements if filter value is True
        if value:
            return queryset.exclude(public_filter)
        else:
            return queryset.filter(public_filter)


class TagsFilter(filters.BaseCSVFilter, filters.CharFilter):
    """Filter for tags."""

    def __init__(self, *args, **kwargs):
        """Construct tags filter."""
        kwargs.setdefault("lookup_expr", "contains")
        super().__init__(*args, **kwargs)


class ResolweFilterMetaclass(FilterSetMetaclass):
    """Support extending filter by third-party packages.

    .. note::

        This injects filters in an undocumented way and can thus break in
        future releases.

    For example, to add a filter by ``my_field``, define an extension and add
    it to the composer:

    .. code-block:: python

        from resolwe.composer import composer


        class ExtendedFilter:

            my_field = filters.CharFilter()

        composer.add_extension("path.to.my.Filter", ExtendedFilter)

    """

    def __new__(mcs, name, bases, namespace):
        """Inject extensions into the filter."""
        class_path = "{}.{}".format(namespace["__module__"], name)
        for extension in composer.get_extensions(class_path):
            for name in dir(extension):
                if name.startswith("__"):
                    continue
                namespace[name] = getattr(extension, name)

        return super().__new__(mcs, name, bases, namespace)


class BaseResolweFilter(
    CheckQueryParamsMixin, filters.FilterSet, metaclass=ResolweFilterMetaclass
):
    """Base filter for Resolwe's endpoints."""

    owner_permission = Permission.OWNER

    class Meta:
        """Filter configuration."""

        fields = {
            "contributor": ["exact", "in"],
            "created": DATETIME_LOOKUPS[:],
            "id": NUMBER_LOOKUPS[:],
            "name": TEXT_LOOKUPS[:],
            "modified": DATETIME_LOOKUPS[:],
            "slug": SLUG_LOOKUPS[:],
            "version": NUMBER_LOOKUPS[:],
        }

        filter_overrides = {
            VersionField: {"filter_class": filters.CharFilter},
            ForeignKey: {"filter_class": filters.Filter},
        }


class DescriptorSchemaFilter(BaseResolweFilter):
    """Filter the DescriptorSchema endpoint."""

    class Meta(BaseResolweFilter.Meta):
        """Filter configuration."""

        model = DescriptorSchema


class BaseCollectionFilter(TextFilterMixin, UserFilterMixin, BaseResolweFilter):
    """Base filter for Collection and Entity endpoints."""

    contributor_name = filters.CharFilter(method="filter_contributor_name")
    owners = filters.CharFilter(method="filter_owners")
    owners_name = filters.CharFilter(method="filter_owners_name")
    shared_with_me = filters.BooleanFilter(method="filter_private")
    group = filters.ModelChoiceFilter(
        method="filter_for_group", queryset=Group.objects.all()
    )
    permission = filters.CharFilter(method="filter_for_user")
    tags = TagsFilter()
    text = filters.CharFilter(field_name="search", method="filter_text")

    class Meta(BaseResolweFilter.Meta):
        """Filter configuration."""

        fields = {
            **BaseResolweFilter.Meta.fields,
            **{
                "description": TEXT_LOOKUPS[:],
                "descriptor_schema": ["exact"],
            },
        }


class CollectionFilter(BaseCollectionFilter):
    """Filter the Collection endpoint."""

    entity_count = filters.NumberFilter(
        method="count_entities", field_name="entity__count"
    )
    entity_count__gt = filters.NumberFilter(
        method="count_entities", field_name="entity__count__gt"
    )
    entity_count__lt = filters.NumberFilter(
        method="count_entities", field_name="entity__count__lt"
    )
    entity_count__gte = filters.NumberFilter(
        method="count_entities", field_name="entity__count__gte"
    )
    entity_count__lte = filters.NumberFilter(
        method="count_entities", field_name="entity__count__lte"
    )

    class Meta(BaseCollectionFilter.Meta):
        """Filter configuration."""

        model = Collection

    def count_entities(self, queryset, name, value):
        """Filter by the number of associated entities."""

        return queryset.annotate(Count("entity")).filter(**{name: value})


class EntityFilter(BaseCollectionFilter):
    """Filter the Entity endpoint."""

    relation_id = filters.NumberFilter(field_name="relation__id")

    class Meta(BaseCollectionFilter.Meta):
        """Filter configuration."""

        model = Entity
        fields = {
            **BaseCollectionFilter.Meta.fields,
            **{
                "collection": RELATED_LOOKUPS[:],
                "collection__name": TEXT_LOOKUPS[:],
                "collection__slug": SLUG_LOOKUPS[:],
                "type": ["exact"],
            },
        }


class ProcessFilter(BaseResolweFilter):
    """Filter the Process endpoint."""

    category = filters.CharFilter(field_name="category", lookup_expr="startswith")
    type = filters.CharFilter(field_name="type", lookup_expr="startswith")

    class Meta(BaseResolweFilter.Meta):
        """Filter configuration."""

        model = Process
        fields = {
            **BaseResolweFilter.Meta.fields,
            **{
                "is_active": ["exact"],
                "scheduling_class": ["exact"],
            },
        }


class CharInFilter(filters.BaseInFilter, filters.CharFilter):
    """Helper class for creation of CharFilter with "in" lookup."""


class DataFilter(TextFilterMixin, UserFilterMixin, BaseResolweFilter):
    """Filter the Data endpoint."""

    contributor_name = filters.CharFilter(method="filter_contributor_name")
    owners = filters.CharFilter(method="filter_owners")
    owners_name = filters.CharFilter(method="filter_owners_name")
    tags = TagsFilter()
    text = filters.CharFilter(field_name="search", method="filter_text")
    type = filters.CharFilter(field_name="process__type", lookup_expr="startswith")
    type__exact = filters.CharFilter(field_name="process__type", lookup_expr="exact")
    relation_id = filters.NumberFilter(
        field_name="entity__relationpartition__relation_id"
    )
    shared_with_me = filters.BooleanFilter(method="filter_private")
    group = filters.ModelChoiceFilter(
        method="filter_for_group", queryset=Group.objects.all()
    )
    permission = filters.CharFilter(method="filter_for_user")

    class Meta(BaseResolweFilter.Meta):
        """Filter configuration."""

        model = Data
        fields = {
            **BaseResolweFilter.Meta.fields,
            **{
                "collection": RELATED_LOOKUPS[:],
                "collection__name": TEXT_LOOKUPS[:],
                "collection__slug": SLUG_LOOKUPS[:],
                "entity": RELATED_LOOKUPS[:],
                "entity__name": TEXT_LOOKUPS[:],
                "entity__slug": SLUG_LOOKUPS[:],
                "process": RELATED_LOOKUPS[:],
                "process__name": TEXT_LOOKUPS[:],
                "process__slug": SLUG_LOOKUPS[:],
                "finished": DATETIME_LOOKUPS[:],
                "started": DATETIME_LOOKUPS[:],
                "status": ["exact", "in"],
            },
        }


class RelationFilter(BaseResolweFilter):
    """Filter the Relation endpoint."""

    category = filters.CharFilter(lookup_expr="iexact")
    collection = filters.ModelChoiceFilter(queryset=Collection.objects.all())
    type = filters.CharFilter(field_name="type__name")

    class Meta(BaseResolweFilter.Meta):
        """Filter configuration."""

        model = Relation
        fields = {
            **BaseResolweFilter.Meta.fields,
            **{
                "descriptor_schema": ["exact"],
            },
        }

    def get_always_allowed_arguments(self):
        """Get always allowed query arguments."""
        return super().get_always_allowed_arguments() + (
            "entity",
            "label",
            "position",
        )


class OrderingFilter(DrfOrderingFilter):
    """Order results by field specified in request.

    If no ordering is given, default ordering is used. Default ordering
    is skipped for views filtered by full-text search as it already
    applies ordering by ranking.
    """

    def get_ordering(self, request, queryset, view):
        """Return name of the filed by which results should be ordered.

        Ordering is skipped if ``None`` is returned.
        """
        params = request.query_params.get(self.ordering_param)
        if params:
            fields = [param.strip() for param in params.split(",")]
            ordering = self.remove_invalid_fields(queryset, fields, view, request)
            if ordering:
                return ordering

        # Skip default ordering as full-text search applies ordering by rank.
        if request.query_params.get("text"):
            return None

        return self.get_default_ordering(view)


class AnnotationFieldFilter(BaseResolweFilter):
    """Filter the AnnotationField endpoint."""

    @classmethod
    def filter_for_field(cls, field, field_name, lookup_expr=None):
        """Add permission check for collections lookups.

        The old filter method is moved to the original_filter and replaced with the
        filter that checks the permissions on the collections.
        """

        def collection_permission_filter(self, qs, value):
            """Check if user has access the collections.

            The collection filer is defined in the parent filter.

            :raises PermissionDenied: if user does not have the permissions to access any
                of the collections defined by the filter.
            """

            if value:
                field_name, lookup_expression = self.field_name, self.lookup_expr
                if field_name == "collection":
                    field_name += "__id"

                collection_field = field_name.split("__", maxsplit=1)[1]
                collection_filter = f"{collection_field}__{lookup_expression}"
                filtered_collections = Collection.objects.filter(
                    **{collection_filter: value}
                )
                user_visible_collections = filtered_collections.filter_for_user(
                    self.parent.request.user
                )
                if filtered_collections.count() != user_visible_collections.count():
                    raise exceptions.PermissionDenied()
            return self.original_filter(qs, value)

        base_filter = filters.FilterSet.filter_for_field(field, field_name, lookup_expr)
        # No need to overwrite the exact filter.
        if field_name.startswith("collection__"):
            base_filter.original_filter = base_filter.filter
            base_filter.filter = partial(collection_permission_filter, base_filter)
        return base_filter

    class Meta(BaseResolweFilter.Meta):
        """Filter configuration."""

        model = AnnotationField
        fields = {
            **{
                "name": TEXT_LOOKUPS,
                "label": TEXT_LOOKUPS,
                "type": TEXT_LOOKUPS,
                "description": TEXT_LOOKUPS,
                "group__name": TEXT_LOOKUPS,
                "collection": ["exact"],
                "collection__name": TEXT_LOOKUPS[:],
                "collection__slug": SLUG_LOOKUPS[:],
            },
        }


class AnnotationPresetFilter(BaseResolweFilter):
    """Filter the AnnotationPreset endpoint."""

    class Meta(BaseResolweFilter.Meta):
        """Filter configuration."""

        model = AnnotationPreset
        fields = {
            **{
                "name": TEXT_LOOKUPS,
            },
        }


class AnnotationValueFilter(BaseResolweFilter):
    """Filter the AnnotationValue endpoint."""

    entity_ids = filters.CharFilter(method="filter_by_entities", required=True)
    field_ids = filters.CharFilter(method="filter_by_fields")
    value = filters.CharFilter(method="filter_by_value")

    def filter_by_entities(self, queryset, name, entity_ids):
        """Filter by the collection id.

        The filtering must also take permissions on the collections in account.
        """
        entities = Entity.objects.filter(pk__in=entity_ids).filter_for_user(
            self.request.user
        )
        return queryset.filter(entity__in=entities)

    def filter_by_fields(self, queryset, name, field_ids):
        """Filter by field ids."""
        return queryset.filter(field_id__in=field_ids)

    def filter_by_value(self, queryset, name, value):
        """Filter by value."""
        return queryset.filter(_value__value__icontains=value)

    class Meta(BaseResolweFilter.Meta):
        """Filter configuration."""

        model = AnnotationValue
        fields = {
            **{"field__name": TEXT_LOOKUPS},
            **{"field__label": TEXT_LOOKUPS},
        }
