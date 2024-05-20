""".. Ignore pydocstyle D400.

==================
Permissions Filter
==================

"""

from rest_framework.filters import BaseFilterBackend
from rest_framework.request import Request

from resolwe.permissions.models import PermissionQuerySet
from resolwe.permissions.utils import model_has_permissions


class ResolwePermissionsFilter(BaseFilterBackend):
    """Permissions filter."""

    def filter_queryset(
        self, request: Request, queryset: PermissionQuerySet, view
    ) -> PermissionQuerySet:
        """Filter permissions queryset.

        When model has no permission group defined return the entire queryset.
        """
        if model_has_permissions(queryset.model):
            queryset = queryset.filter_for_user(request.user)

        return queryset
