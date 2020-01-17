"""Custom permissions for Flow API."""
from django.contrib.auth.models import AnonymousUser
from django.http import Http404

from rest_framework import permissions


class ResolwePermissions(permissions.DjangoObjectPermissions):
    """Resolwe Permissions."""

    perms_map = {
        "GET": ["%(app_label)s.view_%(model_name)s"],
        "OPTIONS": ["%(app_label)s.view_%(model_name)s"],
        "HEAD": ["%(app_label)s.view_%(model_name)s"],
        "POST": ["%(app_label)s.edit_%(model_name)s"],
        "PUT": ["%(app_label)s.edit_%(model_name)s"],
        "PATCH": ["%(app_label)s.edit_%(model_name)s"],
        "DELETE": ["%(app_label)s.edit_%(model_name)s"],
    }

    def has_permission(self, request, view):
        """Return `True` as we don't use model level perms."""
        return True

    def has_object_permission(self, request, view, obj):
        """Check object permissions."""
        # admins can do anything
        if request.user.is_superuser:
            return True

        # `share` permission is required for editing permissions
        if "permissions" in view.action:
            self.perms_map["POST"] = ["%(app_label)s.share_%(model_name)s"]

        if view.action in ["add_data", "remove_data"]:
            self.perms_map["POST"] = ["%(app_label)s.add_%(model_name)s"]

        if hasattr(view, "get_queryset"):
            queryset = view.get_queryset()
        else:
            queryset = getattr(view, "queryset", None)

        assert queryset is not None, (
            "Cannot apply DjangoObjectPermissions on a view that "
            "does not set `.queryset` or have a `.get_queryset()` method."
        )

        model_cls = queryset.model
        user = request.user

        perms = self.get_required_object_permissions(request.method, model_cls)

        if not user.has_perms(perms, obj) and not AnonymousUser().has_perms(perms, obj):
            # If the user does not have permissions we need to determine if
            # they have read permissions to see 403, or not, and simply see
            # a 404 response.

            if request.method in permissions.SAFE_METHODS:
                # Read permissions already checked and failed, no need
                # to make another lookup.
                raise Http404

            read_perms = self.get_required_object_permissions("GET", model_cls)
            if not user.has_perms(read_perms, obj):
                raise Http404

            # Has read permissions.
            return False

        return True
