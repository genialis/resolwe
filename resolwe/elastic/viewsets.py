""".. Ignore pydocstyle D400.

================
Elastic Viewsets
================

.. autoclass:: resolwe.elastic.viewsets.ElasticSearchMixin
    :members:

"""
from __future__ import absolute_import, division, print_function, unicode_literals

from elasticsearch_dsl.query import Q

from django.conf import settings
from django.contrib.auth import get_user_model
from django.db.models import Case, IntegerField, Value, When

from rest_framework.exceptions import APIException
from rest_framework.response import Response
from rest_framework.viewsets import GenericViewSet

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


class ElasticSearchMixin(object):
    """Mixin to use Django REST Framework with ElasticSearch based querysets.

    This mixin adds following methods:
      * :func:`~ElasticSearchMixin.order_search`
      * :func:`~ElasticSearchMixin.filter_search`
      * :func:`~ElasticSearchMixin.filter_permissions`

    """

    filtering_fields = []
    ordering_fields = []
    ordering_map = {}
    ordering = None

    def get_query_param(self, key, default=None):
        """Get query parameter uniformly for GET and POST requests."""
        value = self.request.query_params.get(key, None)
        if value is None:
            value = self.request.data.get(key, None)
        if value is None:
            value = default
        return value

    def order_search(self, search):
        """Order given search by the ordering parameter given in request.

        :param search: ElasticSearch query object

        """
        ordering = self.get_query_param('ordering', self.ordering)
        if not ordering:
            return search

        ordering_field = ordering.lstrip('-')
        if ordering_field not in self.ordering_fields:
            raise KeyError('Ordering by `{}` is not supported.'.format(ordering_field))

        ordering_field = self.ordering_map.get(ordering_field, ordering_field)
        direction = '-' if ordering[0] == '-' else ''

        return search.sort('{}{}'.format(direction, ordering_field))

    def filter_search(self, search):
        """Filter given search by the filter parameter given in request.

        :param search: ElasticSearch query object

        """
        for field in self.filtering_fields:
            value = self.get_query_param(field, None)
            if value:
                custom_filter = getattr(self, 'custom_filter_{}'.format(field), None)
                if custom_filter is not None:
                    search = custom_filter(value, search)
                elif isinstance(value, list):
                    # Default is 'should' between matches. If you need anything else,
                    # a custom filter for this field should be implemented.
                    filters = [Q('match', **{field: item}) for item in value]
                    search = search.query('bool', should=filters)
                else:
                    search = search.query('match', **{field: value})

        return search

    def filter_permissions(self, search):
        """Filter given query based on permissions of the user in the request.

        :param search: ElasticSearch query object

        """
        user = self.request.user
        if user.is_superuser:
            return search
        if user.is_anonymous():
            user_model = get_user_model()
            user = user_model.objects.get(**{user_model.USERNAME_FIELD: settings.ANONYMOUS_USER_NAME})

        filters = [Q('match', users_with_permissions=user.pk)]
        filters.extend([
            Q('match', groups_with_permissions=group.pk) for group in user.groups.all()
        ])
        filters.append(Q('match', public_permission=True))

        # `minimum_should_match` is set to 1 by default
        return search.query('bool', should=filters)


class PaginationMixin(object):
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
            limit = self.get_query_param('limit')
            offset = self.get_query_param('offset')

            if not limit or not offset or limit > ELASTICSEARCH_SIZE:
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

    Whenever any of the filters declared in ``filtering_fields`` are
    used, the search part is used. Otherwise, only the database part is
    used. In both cases, search results are only used to get the primary
    keys to filter the database query.

    In order for this to work, your index must store the primary key
    in a field (by default it should be called ``id``). You can change
    this by setting ``primary_key_field``.
    """

    primary_key_field = 'id'

    def is_search_request(self):
        """Check if current request is a search request."""
        return any([
            self.get_query_param(field, None) is not None
            for field in self.filtering_fields
        ])

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
