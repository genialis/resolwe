""".. Ignore pydocstyle D400.

==================
Permissions Filter
==================

"""
from rest_framework.filters import BaseFilterBackend

from resolwe.permissions.shortcuts import get_objects_for_user


class ResolwePermissionsFilter(BaseFilterBackend):
    """Permissions filter."""

    def filter_queryset(self, request, queryset, view):
        """Filter permissions queryset."""
        user = request.user

        app_label = queryset.model._meta.app_label  # pylint: disable=protected-access
        model_name = queryset.model._meta.model_name  # pylint: disable=protected-access

        kwargs = {}
        if model_name == 'storage':
            model_name = 'data'
            kwargs['perms_filter'] = 'data__pk__in'

        if model_name == 'relation':
            model_name = 'collection'
            kwargs['perms_filter'] = 'collection__pk__in'

        permission = '{}.view_{}'.format(app_label, model_name)

        return get_objects_for_user(user, permission, queryset, **kwargs)
