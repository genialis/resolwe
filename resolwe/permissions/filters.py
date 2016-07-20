from __future__ import absolute_import, division, print_function, unicode_literals

from rest_framework.filters import BaseFilterBackend

from resolwe.permissions.shortcuts import get_objects_for_user


class ResolwePermissionsFilter(BaseFilterBackend):

    def filter_queryset(self, request, queryset, view):
        user = request.user

        app_label = queryset.model._meta.app_label
        model_name = queryset.model._meta.model_name

        kwargs = {}
        if model_name == 'storage':
            model_name = 'data'
            kwargs['perms_filter'] = 'data__pk__in'

        permission = '{}.view_{}'.format(app_label, model_name)

        return get_objects_for_user(user, permission, queryset, **kwargs)
