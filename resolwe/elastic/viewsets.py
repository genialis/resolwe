""".. Ignore pydocstyle D400.

================
Elastic Viewsets
================

.. autoclass:: resolwe.elastic.viewsets.ElasticSearchMixin
    :members:

"""
from types import MethodType

from elasticsearch_dsl.query import Q

from django.db.models import Case, IntegerField, Value, When

from guardian.utils import get_anonymous_user
from rest_framework.exceptions import APIException, ParseError
from rest_framework.response import Response
from rest_framework.viewsets import GenericViewSet

from .composer import composer
from .lookup import QueryBuilder
from .pagination import LimitOffsetPostPagination

__all__ = (
    'ElasticSearchMixin',
    'PaginationMixin',
    'ElasticSearchBaseViewSet',
)

ELASTICSEARCH_SIZE = 10000  # maximum number of results returned by ElasticSearch


class TooManyResults(APIException):
    """Exception when elastic query returns more than ``ELASTICSEARCH_SIZE`` results."""

    status_code = 400
    default_detail = 'Query returned too many results. Please, add more filters or use pagination.'
    default_code = 'bad_request'


class ElasticSearchMixin:
    """Mixin to use Django REST Framework with ElasticSearch based querysets.

    This mixin adds following methods:
      * :func:`~ElasticSearchMixin.order_search`
      * :func:`~ElasticSearchMixin.filter_search`
      * :func:`~ElasticSearchMixin.filter_permissions`

    """

    filtering_fields = []
    filtering_map = {}
    ordering_fields = []
    ordering_map = {}
    ordering = None

    def __init__(self, *args, **kwargs):
        """Construct viewset."""
        # Add registered viewset extensions. We take care not to modify the original
        # class-derived attributes.
        self.ordering_map = self.ordering_map.copy()

        for extension in composer.get_extensions(self):
            filtering_fields = getattr(extension, 'filtering_fields', [])
            filtering_map = getattr(extension, 'filtering_map', {})
            ordering_fields = getattr(extension, 'ordering_fields', [])
            ordering_map = getattr(extension, 'ordering_map', {})

            self.filtering_fields = self.filtering_fields + tuple(filtering_fields)
            self.filtering_map.update(filtering_map)
            self.ordering_fields = self.ordering_fields + tuple(ordering_fields)
            self.ordering_map.update(ordering_map)

            for field_name in filtering_fields:
                custom_filter_name = 'custom_filter_{}'.format(field_name)
                custom_filter = getattr(extension, custom_filter_name, None)
                if custom_filter:
                    setattr(self, custom_filter_name, MethodType(custom_filter, self))

        super().__init__(*args, **kwargs)

    def get_query_param(self, key, default=None):
        """Get query parameter uniformly for GET and POST requests."""
        value = self.request.query_params.get(key, None)
        if value is None:
            value = self.request.data.get(key, None)
        if value is None:
            value = default
        return value

    def get_query_params(self):
        """Get combined query parameters (GET and POST)."""
        params = self.request.query_params.copy()
        params.update(self.request.data)
        return params

    def order_search(self, search):
        """Order given search by the ordering parameter given in request.

        :param search: ElasticSearch query object

        """
        ordering = self.get_query_param('ordering', self.ordering)
        if not ordering:
            return search

        sort_fields = []
        for raw_ordering in ordering.split(','):
            ordering_field = raw_ordering.lstrip('-')
            if ordering_field not in self.ordering_fields:
                raise ParseError('Ordering by `{}` is not supported.'.format(ordering_field))

            ordering_field = self.ordering_map.get(ordering_field, ordering_field)
            direction = '-' if raw_ordering[0] == '-' else ''
            sort_fields.append('{}{}'.format(direction, ordering_field))

        return search.sort(*sort_fields)

    def get_always_allowed_arguments(self):
        """Return query arguments which are always allowed."""
        return [
            'ordering',
            'offset',
            'limit',
            'format',
            'fields',
        ]

    def filter_search(self, search):
        """Filter given search by the filter parameter given in request.

        :param search: ElasticSearch query object

        """
        builder = QueryBuilder(
            self.filtering_fields,
            self.filtering_map,
            self
        )
        search, unmatched = builder.build(search, self.get_query_params())

        # Ensure that no unsupported arguments were used.
        for argument in self.get_always_allowed_arguments():
            unmatched.pop(argument, None)

        if unmatched:
            raise ParseError('Unsupported filter arguments: {}'.format(', '.join(unmatched.keys())))

        return search

    def filter_permissions(self, search):
        """Filter given query based on permissions of the user in the request.

        :param search: ElasticSearch query object

        """
        user = self.request.user
        if user.is_superuser:
            return search
        if user.is_anonymous:
            user = get_anonymous_user()

        filters = [Q('match', users_with_permissions=user.pk)]
        filters.extend([
            Q('match', groups_with_permissions=group.pk) for group in user.groups.all()
        ])
        filters.append(Q('match', public_permission=True))

        # `minimum_should_match` is set to 1 by default
        return search.query('bool', should=filters)


class PaginationMixin:
    """Mixin for making paginated response in case pagination parameters are provided."""

    def paginate_response(self, queryset, serializers_kwargs={}):
        """Optionally return paginated response.

        If pagination parameters are provided in the request, then paginated response
        is returned, otherwise response is not paginated.

        """
        page = self.paginate_queryset(queryset)
        if page is not None:
            serializer = self.get_serializer(page, many=True, **serializers_kwargs)
            return self.get_paginated_response(serializer.data)

        serializer = self.get_serializer(queryset, many=True, **serializers_kwargs)
        return Response(serializer.data)


class ElasticSearchBaseViewSet(PaginationMixin, ElasticSearchMixin, GenericViewSet):
    """Base ViewSet for ElasticSearch based views.

    This ViewSet creates search based on ``document_class`` parameter, specified
    in subclass and:

      * filter it by query parameters
      * apply ordering
      * filter permissions
      * apply pagination

    .. IMPORTANT::

        Both ``POST`` and ``GET`` requests are supported.
    """

    document_class = None

    pagination_class = LimitOffsetPostPagination

    def custom_filter(self, search):
        """Perform custom search filtering.

        This method is intended to be overriden in subclasses.
        """
        return search

    def search(self):
        """Handle the search request."""
        search = self.document_class().search()  # pylint: disable=not-callable

        search = self.custom_filter(search)

        search = self.filter_search(search)
        search = self.order_search(search)
        search = self.filter_permissions(search)

        if search.count() > ELASTICSEARCH_SIZE:
            limit = self.paginator.get_limit(self.request)

            if not limit or limit > ELASTICSEARCH_SIZE:
                raise TooManyResults()

        search = search.extra(size=ELASTICSEARCH_SIZE)
        return search

    def list_with_post(self, request):
        """Endpoint handler."""
        search = self.search()
        return self.paginate_response(search)

    def list(self, request):
        """Endpoint handler."""
        return self.list_with_post(request)


class ElasticSearchCombinedViewSet(ElasticSearchBaseViewSet):
    """ViewSet, which combines database and ES queries.

    To use this viewset, you should mixin the viewset that handles
    the database part, for example:

    .. code:: python

        class MyCombinedViewSet(ElasticSearchCombinedViewSet, MyViewSet):
            # ...

    Whenever the ``is_search_request`` method returns ``True`` (by default
    it always does), the search part is used. Otherwise, only the database
    part is used. In both cases, search results are only used to get the
    primary keys to filter the database query.

    In order for this to work, your index must store the primary key
    in a field (by default it should be called ``id``). You can change
    this by setting ``primary_key_field``.
    """

    primary_key_field = 'id'

    def is_search_request(self):
        """Check if current request is a search request."""
        return True

    def list_with_post(self, request):
        """Endpoint handler."""
        if self.is_search_request():
            search = self.search()

            page = self.paginate_queryset(search)
            if page is None:
                items = search
            else:
                items = page

            try:
                primary_keys = []
                order_map_cases = []
                for order, item in enumerate(items):
                    pk = item[self.primary_key_field]
                    primary_keys.append(pk)
                    order_map_cases.append(When(pk=pk, then=Value(order)))

                queryset = self.get_queryset().filter(
                    pk__in=primary_keys
                ).order_by(
                    Case(*order_map_cases, output_field=IntegerField()).asc()
                )
            except KeyError:
                raise KeyError("Combined viewset requires that your index contains a field with "
                               "the primary key. By default this field is called 'id', but you "
                               "can change it by setting primary_key_field.")

            # Pagination must be handled differently.
            serializer = self.get_serializer(queryset, many=True)
            if page is not None:
                return self.get_paginated_response(serializer.data)

            return Response(serializer.data)
        else:
            queryset = self.filter_queryset(self.get_queryset())
            return self.paginate_response(queryset)

    def list(self, request):
        """Endpoint handler."""
        return self.list_with_post(request)
