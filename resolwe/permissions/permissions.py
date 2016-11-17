"""Custom permissions for Flow API."""
from __future__ import absolute_import, division, print_function, unicode_literals

from rest_framework import permissions


class ResolwePermissions(permissions.DjangoObjectPermissions):
    """Resolwe Permissions."""

    perms_map = {
        'GET': ['%(app_label)s.view_%(model_name)s'],
        'OPTIONS': ['%(app_label)s.view_%(model_name)s'],
        'HEAD': ['%(app_label)s.view_%(model_name)s'],
        'POST': ['%(app_label)s.edit_%(model_name)s'],
        'PUT': ['%(app_label)s.edit_%(model_name)s'],
        'PATCH': ['%(app_label)s.edit_%(model_name)s'],
        'DELETE': ['%(app_label)s.edit_%(model_name)s'],
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
        if 'permissions' in view.action:
            self.perms_map['POST'] = ['%(app_label)s.share_%(model_name)s']

        if view.action in ['add_data', 'remove_data']:
            self.perms_map['POST'] = ['%(app_label)s.add_%(model_name)s']

        return super(ResolwePermissions, self).has_object_permission(request, view, obj)
