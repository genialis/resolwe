""".. Ignore pydocstyle D400.

==================
Permissions Filter
==================

"""
from rest_framework.filters import BaseFilterBackend

from resolwe.permissions.utils import model_has_permissions


class ResolwePermissionsFilter(BaseFilterBackend):
    """Permissions filter."""

    def filter_queryset(self, request, queryset, view):
        """Filter permissions queryset.

        When model has no permission group defined return the entire queryset.
        """
        if model_has_permissions(queryset.model):
            return queryset.filter_for_user(request.user)
        else:
            return queryset
