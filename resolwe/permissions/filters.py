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

        app_label = queryset.model._meta.app_label
        permission = f"{app_label}.view"
        return get_objects_for_user(user, permission, queryset)
