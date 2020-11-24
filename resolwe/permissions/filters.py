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
        model_name = queryset.model._meta.model_name
        original_model_name = model_name

        kwargs = {}
        if model_name == "storage":
            model_name = "data"
            kwargs["perms_filter"] = "data__pk__in"

        if model_name == "relation":
            model_name = "collection"
            kwargs["perms_filter"] = "collection__pk__in"

        permission = "{}.view_{}".format(app_label, model_name)

        filtered_objects = get_objects_for_user(user, permission, queryset, **kwargs)

        # The storage endpoint supports only retrieval of single object by id.
        # When storage object S in the queryset is linked with more than one
        # Data object the permission check returns the queryset consisting of
        # multiple copies of S: one copy for each linked Data object.
        #
        # Returning a queryset containing more than one object causes crash in
        # the REST framework since at most one object is expected. In such case
        # the original queryset (the one containing only one copy of object S)
        # is returned.
        if original_model_name == "storage" and filtered_objects.exists():
            return queryset

        return filtered_objects
