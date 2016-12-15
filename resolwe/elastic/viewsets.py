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


__all__ = ('ElasticSearchMixin',)


class ElasticSearchMixin(object):
    """Mixin to use Django REST Framework with ElasticSearch based querysets.

    This mixin adds following methods:
      * :func:`~ElasticSearchMixin.order_search`
      * :func:`~ElasticSearchMixin.filter_search`
      * :func:`~ElasticSearchMixin.filter_permissions`

    """

    filtering_fields = []
    ordering_fields = []
    ordering = None

    def order_search(self, search):
        """Order given search by the ordering parameter given in request.

        :param search: ElasticSearch query object

        """
        ordering = self.request.query_params.get('ordering', self.ordering)

        ordering_field = ordering.lstrip('-')
        if ordering_field not in self.ordering_fields:
            raise KeyError('Ordering by `{}` is not supported.'.format(ordering_field))

        return search.sort(ordering)

    def filter_search(self, search):
        """Filter given search by the filter parameter given in request.

        :param search: ElasticSearch query object

        """
        for field in self.filtering_fields:
            value = self.request.query_params.get(field, None)
            if value:
                search = search.query('wildcard', **{field: value})

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

        # `minimum_should_match` is set to 1 by default
        return search.query('bool', should=filters)
