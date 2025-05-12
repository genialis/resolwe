""".. Ignore pydocstyle D400.

============
Flow Filters
============

"""

import re
import types
from copy import deepcopy
from dataclasses import dataclass
from functools import partial, reduce
from typing import Any, Optional, Union

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.contrib.postgres.search import SearchQuery, SearchRank
from django.core.exceptions import FieldDoesNotExist, ValidationError
from django.db import connection
from django.db.models import Count, F, ForeignKey, Model, OuterRef, Q, Subquery
from django.db.models.expressions import RawSQL
from django.db.models.query import QuerySet
from django_filters import rest_framework as filters
from django_filters.constants import EMPTY_VALUES
from django_filters.filterset import FilterSetMetaclass
from rest_framework import fields
from rest_framework.filters import OrderingFilter as DrfOrderingFilter

from resolwe.composer import composer
from resolwe.flow.models.fields import VersionField
from resolwe.permissions.models import (
    Permission,
    PermissionInterface,
    get_anonymous_user,
)

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
from .models.annotations import AnnotationType

# The key used in the full-text search filter.
FULL_TEXT_SEARCH_KEY = "text"
# The actual database fields used for full-text search for every supported entity.
FULL_TEXT_SEARCH_FIELD = {"Entity": "search", "Data": "search", "Collection": "search"}

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
            # Used for full-text search.
            "text",
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


class UserFilterMixin:
    """Mixin for filtering by contributors and owners."""

    @staticmethod
    def _get_user_subquery(value: str):
        user_subquery = user_model.objects.all()
        for key in value.split():
            user_subquery = user_subquery.extra(
                where=["CONCAT_WS(' ', first_name, last_name, username) ILIKE %s"],
                params=("%{}%".format(key),),
            )

        return user_subquery

    def filter_owners(self, queryset: QuerySet, name: str, value: int):
        """Filter queryset by owner's id."""
        try:
            user = user_model.objects.get(pk=value)
        except user_model.DoesNotExist:
            return queryset.none()

        return queryset.filter_for_user(
            user, self.owner_permission, with_superuser=False
        )

    def filter_owners_name(self, queryset: QuerySet, name: str, value: str):
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

    def filter_contributor_name(self, queryset: QuerySet, name: str, value: str):
        """Filter queryset by owner's name."""
        return queryset.filter(contributor__in=self._get_user_subquery(value))

    def filter_for_user(
        self, queryset: QuerySet[PermissionInterface], name: str, value: str
    ) -> QuerySet[PermissionInterface]:
        """Filter queryset by permissions."""
        user = self.request.user
        return queryset.filter_for_user(user, Permission.from_name(value))

    def filter_for_group(self, queryset: QuerySet, name: str, value: Group):
        """Filter queryset by group."""
        return queryset.filter(permission_group__permissions__group=value)

    def filter_private(self, queryset: QuerySet, name: str, value: bool):
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

    def __new__(
        mcs: type[object],
        name: str,
        bases: tuple[type[object]],
        namespace: dict[str, Any],
    ):
        """Inject filters."""

        def inject_composer_extensions(namespace: dict[str, Any], name: str):
            """Inject filters from composer extensions."""
            class_path = "{}.{}".format(namespace["__module__"], name)
            for extension in composer.get_extensions(class_path):
                for name in dir(extension):
                    if name.startswith("__"):
                        continue
                    namespace[name] = getattr(extension, name)

        def get_related_model_from_path(related_path: str) -> type[Model]:
            """Get the related model from the path."""
            return reduce(
                lambda Model, part: Model._meta.get_field(part).related_model,
                related_path.split("__"),
                namespace["Meta"].model,
            )

        def add_filters_with_permissions(
            prefix: str, related_path: str, base_filter: filters.FilterSet
        ):
            """Add filters on related objects with permissions.

            All the filters belonging to the BaseFilter are added with the given prefix.
            """

            def filter_permissions(
                self,
                qs: QuerySet,
                value: str,
                original_filter: filters.Filter,
                original_model: Model,
            ):
                """Apply the filter and respect permissions."""

                # Do not filter when value is empty. At least one of the values must be
                # non-empty since form in the AnnotationValueFilter class requires it.
                if value in EMPTY_VALUES:
                    return qs

                visible_objects = list(
                    original_filter.filter(original_model.objects.all(), value)
                    .filter_for_user(self.parent.request.user)
                    .values_list("pk", flat=True)
                )
                return qs.filter(**{f"{related_path}__in": visible_objects})

            # Add all filters from EntityFilter to namespaces before creating class.
            BaseModel = get_related_model_from_path(related_path)
            for filter_name, filter in base_filter.get_filters().items():
                new_filter_name = f"{prefix}__{filter_name}"
                if filter_name == "id" or filter_name.startswith("id__"):
                    new_filter_name = f"{prefix}{filter_name[2:]}"

                filter_copy = deepcopy(filter)
                filter_copy.field_name = f"{prefix}__{filter_copy.field_name}"
                filter_method = partial(
                    filter_permissions, original_filter=filter, original_model=BaseModel
                )
                # Bind the new_filter to filter instance and set it as new filter.
                filter_copy.filter = types.MethodType(filter_method, filter_copy)
                namespace[new_filter_name] = filter_copy
                # If filter uses a method, add it to the namespace as well.
                if filter_copy.method is not None:
                    namespace[filter_copy.method] = deepcopy(
                        getattr(base_filter, filter_copy.method)
                    )

        def inject_related_permissions(namespace: dict):
            """Parse the namespace and add filters with permissions."""
            for prefix, value in list(namespace.items()):
                if isinstance(value, FilterRelatedWithPermissions):
                    namespace.pop(prefix)
                    add_filters_with_permissions(
                        prefix, value.related_path or prefix, value.BaseFilter
                    )

        inject_related_permissions(namespace)
        inject_composer_extensions(namespace, name)
        return super().__new__(mcs, name, bases, namespace)


@dataclass
class FilterRelatedWithPermissions:
    """Base class for filters with permissions.

    The class using this must have FilterRelatedWithPermissionsMeta as its meta class.
    The filter is then defined as:

    prefix = FilterRelatedWithPermissions(BaseFilter, related_path="related__path")

    All the filters from the BaseFilter (with the given prefix) are added and
    permissions on the related objects are respected.

    The optional parameter `related__path` is used to specify the path of the related
    object (if different from the prefix).
    """

    BaseFilter: type[filters.FilterSet]
    related_path: Optional[str] = None


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


class CharInFilter(filters.BaseInFilter, filters.CharFilter):
    """Basic filter for CharField with 'in' lookup."""


class BaseCollectionFilter(UserFilterMixin, BaseResolweFilter):
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
    status = filters.CharFilter(field_name="status")
    status__in = CharInFilter(field_name="status", lookup_expr="in")

    class Meta(BaseResolweFilter.Meta):
        """Filter configuration."""

        fields = {
            **BaseResolweFilter.Meta.fields,
            **{
                "description": TEXT_LOOKUPS[:],
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
        fields = {
            **BaseCollectionFilter.Meta.fields,
            **{
                "descriptor_schema": ["exact"],
            },
        }

    def count_entities(self, queryset: QuerySet, name: str, value: str):
        """Filter by the number of associated entities."""

        return queryset.annotate(Count("entity")).filter(**{name: value})


class EntityFilter(BaseCollectionFilter):
    """Filter the Entity endpoint."""

    relation_id = filters.NumberFilter(field_name="relation__id")
    annotations = filters.CharFilter(method="filter_annotations")

    def filter_annotations(self, queryset: QuerySet, name: str, value: str):
        """Filter entities by annotations.

        The value must be in the following format: field_id__lookup_type:value
        """
        type_to_field = {
            AnnotationType.DATE.value: fields.DateField(),
            AnnotationType.STRING.value: fields.CharField(),
            AnnotationType.INTEGER.value: fields.IntegerField(),
            AnnotationType.DECIMAL.value: fields.FloatField(),
        }
        allowed_lookups = {
            AnnotationType.DATE.value: DATE_LOOKUPS,
            AnnotationType.STRING.value: TEXT_LOOKUPS,
            AnnotationType.INTEGER.value: NUMBER_LOOKUPS,
            AnnotationType.DECIMAL.value: NUMBER_LOOKUPS,
        }
        post_processing_map = {
            AnnotationType.DATE.value: lambda value: value.isoformat()
        }

        match = re.match(
            r"^(?P<field_id>\d+)(__(?P<lookup>\w+))?:(?P<value>.+)$", value
        )
        if not match:
            raise ValidationError(f"Invalid annotation filter format '{value}'.")

        field_id = match.group("field_id")
        lookup_type = match.group("lookup") or "exact"
        string_value = match.group("value")
        field_type = (
            AnnotationField.objects.filter(id=field_id)
            .values_list("type", flat=True)
            .get()
        )
        field = type_to_field[field_type]

        if lookup_type not in allowed_lookups[field_type]:
            raise ValueError(
                f"Lookup '{lookup_type}' not supported for the field '{field_id}'."
            )

        processing = field.run_validation
        if post_processing := post_processing_map.get(field_type, None):
            processing = lambda value: post_processing(field.run_validation(value))

        if lookup_type == "in":
            validated_value = map(processing, string_value.split(","))
        else:
            validated_value = processing(string_value)

        q_filter = {
            f"annotations___value__value__{lookup_type}": validated_value,
            f"annotations___value__label__{lookup_type}": validated_value,
        }
        value_query = Q()
        for key, value in q_filter.items():
            value_query |= Q(**{key: value})
        field_query = Q(annotations__field_id=field_id)
        return queryset.filter().filter(field_query & value_query)

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


class DataFilter(UserFilterMixin, BaseResolweFilter):
    """Filter the Data endpoint."""

    contributor_name = filters.CharFilter(method="filter_contributor_name")
    owners = filters.CharFilter(method="filter_owners")
    owners_name = filters.CharFilter(method="filter_owners_name")
    tags = TagsFilter()
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
    tags = TagsFilter(field_name="collection__tags")

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

    @property
    def _annotation_handlers(self):
        """Return a list of custom annotation handlers."""
        return (self._annotate_annotations,)

    def _annotate_annotations(self, queryset, ordering):
        """Annotate queryset with annotations.

        The annotations are only applied if ordering by annotations is requested.
        """
        if not issubclass(queryset.model, Entity):
            return queryset

        regex = re.compile(r"-?annotations_order_(?P<field_id>\d+)")
        for field in ordering:
            if not isinstance(field, str):
                continue
            if match := regex.match(field):
                field_id = match.group("field_id")
                # Always filter by user-visible attribute annotation_label.
                annotation_value = AnnotationValue.objects.filter(
                    entity_id=OuterRef("pk"), field_id=field_id
                ).values("_value__label")
                annotate = {f"annotations_order_{field_id}": Subquery(annotation_value)}
                queryset = queryset.annotate(**annotate)
        return queryset

    def _annotate_queryset(self, queryset, ordering):
        """Annotate queryset with ordering fields.

        Some ordering field require additional annotations to be present.
        """
        for handler in self._annotation_handlers:
            queryset = handler(queryset, ordering)
        return queryset

    def filter_queryset(self, request, queryset, view):
        """Return ordered queryset."""
        if ordering := self.get_ordering(request, queryset, view):
            queryset = self._annotate_queryset(queryset, ordering).order_by(*ordering)
        return queryset

    def _remove_invalid_annotation_field(self, queryset, field, view, request):
        """Check if the given sorting field is annotations field.

        The format of the ordering field is annotations__field_id, possibly prefixed by
        a minus sign. The minus sign indicates descending order.
        """
        if re.match(r"-?annotations__(?P<field_id>\d+)", field):
            return field.replace("__", "_order_")

    def _remove_invalid_json_field(self, queryset, field, view, request):
        """Remove invalid JSON field.

        Either return None or a ordering expression.
        """
        # When a dot is present in the order field, treat it as a JSON path.
        if "." not in field:
            return None

        path = field.split(".")
        base_field = path[0]
        if len(path) < 2:
            return None
        if not super().remove_invalid_fields(queryset, [base_field], view, request):
            return None

        reverse = False
        if base_field[0] == "-":
            base_field = base_field[1:]
            reverse = True

        # Resolve base field.
        try:
            quote_name = connection.ops.quote_name
            model_meta = queryset.model._meta
            base_field = "{}.{}".format(
                quote_name(model_meta.db_table),
                quote_name(model_meta.get_field(base_field).column),
            )
        except FieldDoesNotExist:
            return None

        placeholders = "->".join(["%s"] * (len(path) - 2))
        # The last placeholder should be accessed via the ->> operator.
        if placeholders:
            placeholders = "->{}->>%s".format(placeholders)
        else:
            placeholders = "->>%s"

        expression = RawSQL(
            # We can use base_field here directly because we've resolved it via Django ORM.
            "{}{}".format(base_field, placeholders),
            params=path[1:],
        )
        if reverse:
            expression = expression.desc()

        return expression

    @property
    def _invalid_field_handlers(self):
        """Return a list of custom invalid field handlers."""
        return (
            self._remove_invalid_annotation_field,
            self._remove_invalid_json_field,
        )

    def _remove_invalid_field(self, queryset, field, view, request):
        """Remove invalid fields from the ordering.

        Returns the ordering expression if the field is valid, otherwise None.
        """
        arguments = (queryset, field, view, request)
        for handler in self._invalid_field_handlers:
            if field := handler(*arguments):
                return field
        return None

    def remove_invalid_fields(self, queryset, fields, view, request):
        """Remove invalid fields and preserve sort order.

        First check for custom ordering fields and then for the default ones.
        """
        order_fields = []
        base_order_fields = super().remove_invalid_fields(
            queryset, fields, view, request
        )
        for field in fields:
            if field in base_order_fields:
                order_fields.append(field)
            elif expression := self._remove_invalid_field(
                queryset, field, view, request
            ):
                order_fields.append(expression)
            else:
                raise ValidationError(
                    "Ordering by field '{}' is not supported.".format(field)
                )
        return order_fields


class FullTextSearchFilter(DrfOrderingFilter):
    """Full text search filter.

    Filter and order the queryset by the full text search rank.
    """

    def filter_queryset(self, request, queryset, view):
        """Return ordered queryset.

        If full text search is used and ordering by some other field is requested, raise
        an error.
        """
        if FULL_TEXT_SEARCH_KEY in view.request.query_params:

            order_fields = request.query_params.get(self.ordering_param)
            if FULL_TEXT_SEARCH_KEY in view.request.query_params and order_fields:
                raise ValidationError(
                    f"Ordering by full text search rank and other fields ({order_fields}) "
                    f"is not supported."
                )

            model_name = queryset.model.__name__
            field_name = FULL_TEXT_SEARCH_FIELD.get(model_name)
            if field_name is None:
                raise ValidationError(
                    f"Full text search is not supported for the model {model_name}."
                )
            value = view.request.query_params[FULL_TEXT_SEARCH_KEY]
            query = SearchQuery(value, config="simple_unaccent")
            return (
                queryset.filter(**{field_name: query})
                .annotate(rank=SearchRank(F(field_name), query))
                .order_by("-rank")
            )
        return queryset


class AnnotationFieldFilter(BaseResolweFilter):
    """Filter the AnnotationField endpoint."""

    entity = FilterRelatedWithPermissions(EntityFilter, related_path="values__entity")

    @classmethod
    def filter_for_field(cls, field, field_name, lookup_expr=None):
        """Add permission check for collections lookups.

        The old filter method is moved to the original_filter and replaced with the
        filter that checks the permissions on the collections.
        """

        def collection_permission_filter(
            self, qs: QuerySet, value: Union[str, list[Collection]]
        ):
            """Check if user has access the collections.

            The collection filer is defined in the parent filter.

            :raises PermissionDenied: if user does not have the permissions to access any
                of the collections defined by the filter.
            """
            qs = self._original_filter(qs, value)
            if not value:
                return qs

            field_name = self.field_name
            exact_filter = field_name == "collection"
            if exact_filter:
                field_name += "__id"

            collection_field = field_name.split("__", maxsplit=1)[1]
            collection_filter = f"{collection_field}__{self.lookup_expr}"
            # Exact filter is special: value is a list of collections.
            filter_value = value[0].pk if exact_filter else value
            visible_collections = Collection.objects.filter(
                **{collection_filter: filter_value}
            ).filter_for_user(self.parent.request.user)
            qs = qs.filter(collection__in=visible_collections)
            return qs

        base_filter = filters.FilterSet.filter_for_field(field, field_name, lookup_expr)
        # When collection filter is used replace it with the one that respects
        # permissions on collections.
        if field_name.startswith("collection"):
            base_filter._original_filter = base_filter.filter
            base_filter.filter = partial(collection_permission_filter, base_filter)
        return base_filter

    class Meta(BaseResolweFilter.Meta):
        """Filter configuration."""

        model = AnnotationField
        fields = {
            **{
                "id": NUMBER_LOOKUPS[:],
                "name": TEXT_LOOKUPS[:],
                "label": TEXT_LOOKUPS[:],
                "type": TEXT_LOOKUPS[:],
                "description": TEXT_LOOKUPS[:],
                "group__name": TEXT_LOOKUPS[:],
                "collection": ["exact"],
                "collection__name": TEXT_LOOKUPS[:],
                "collection__slug": SLUG_LOOKUPS[:],
                "required": ["exact"],
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

    entity = FilterRelatedWithPermissions(EntityFilter)
    label = filters.CharFilter(method="filter_by_label")

    def filter_by_label(self, queryset: QuerySet, name: str, value: str):
        """Filter by label."""
        return queryset.filter(_value__label__icontains=value)

    class Meta(BaseResolweFilter.Meta):
        """Filter configuration."""

        model = AnnotationValue
        fields = {
            **{
                "id": NUMBER_LOOKUPS[:],
                "field__id": NUMBER_LOOKUPS[:],
                "field__name": TEXT_LOOKUPS[:],
                "field__label": TEXT_LOOKUPS[:],
                "field__group__name": TEXT_LOOKUPS[:],
                "created": DATE_LOOKUPS[:],
                "contributor": ["exact", "in"],
            },
        }
