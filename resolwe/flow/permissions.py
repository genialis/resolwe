"""
Custom permissions for Django REST Framework API for Flow models.

"""

from rest_framework import permissions


class ResolwePermissions(permissions.DjangoObjectPermissions):
    """

    """
    perms_map = {
        'GET': ['%(app_label)s.view_%(model_name)s'],
        'OPTIONS': ['%(app_label)s.view_%(model_name)s'],
        'HEAD': ['%(app_label)s.view_%(model_name)s'],
        'POST': ['%(app_label)s.edit_%(model_name)s'],
        'PUT': ['%(app_label)s.edit_%(model_name)s'],
        'PATCH': ['%(app_label)s.edit_%(model_name)s'],
        'DELETE': ['%(app_label)s.edit_%(model_name)s'],
    }
