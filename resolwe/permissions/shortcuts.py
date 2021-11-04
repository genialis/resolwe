""".. Ignore pydocstyle D400.

=====================
Permissions shortcuts
=====================

.. autofunction:: _group_groups
.. autofunction:: get_object_perms

"""
from typing import Dict, List, Optional, Tuple

from django.contrib.auth.models import Group, User
from django.db import models

from resolwe.permissions.models import (
    Permission,
    PermissionList,
    UserOrGroup,
    get_anonymous_user,
)
from resolwe.permissions.utils import get_identity, model_has_permissions


def get_user_group_perms(
    user_or_group: UserOrGroup, obj: models.Model
) -> Tuple[PermissionList, List[Tuple[int, str, PermissionList]]]:
    """Get permissions for the user/group on the given model.

    This method is only used in Resolwe views and expects permissions to be
    prefetched by the view. To avoid additional database hits, the filtering is
    done in Python.

    If the given user_or_group is user with superuser privileges, only directly
    attached permissions are returned.

    :returns: the tuple cosisting of permissions for the given user and
    a list of permissions for every group the user belongs to.
    """
    if not model_has_permissions(obj):
        return [], []

    entity, entity_name = get_identity(user_or_group)
    user_perms = []
    groups_perms = []
    if entity_name == "user":
        if not entity.is_active:
            return [], []

        for permission_model in obj.permission_group.permissions.all():
            if permission_model.user_id == entity.id:
                user_perms = permission_model.permissions

    # The correct group permissions are pre-fetched, iterate over them.
    for permission_model in obj.permission_group.permissions.all():
        if permission_model.group_id is not None:
            groups_perms.append(
                (
                    permission_model.group_id,
                    permission_model.group.name,
                    permission_model.permissions,
                )
            )
    return user_perms, groups_perms


def _get_users_with_perms(obj: models.Model) -> Dict[User, PermissionList]:
    """Get users with permissions on the given object.

    Only permissions attached to users directly are considered.

    The users with administrative privileges without permissions attached
    directly to the given object are not returned, even though they have
    implicit 'owner' permissions.

    When user with administrative privileges has permissions attached to a
    given object the permission in the database is returned (and not the
    owner permission).

    This method is only used in Resolwe views and expects permissions to be
    prefetched by the view. To avoid additional database hits, the filtering is
    done in Python.
    """
    return {
        permission_model.user: permission_model.permissions
        for permission_model in obj.permission_group.permissions.all()
        if permission_model.user_id is not None and permission_model.value > 0
    }


def _get_groups_with_perms(obj: models.Model) -> Dict[Group, PermissionList]:
    """Get groups with permissions on the given object.

    This method is only used in Resolwe views and expects permissions to be
    prefetched by the view. To avoid additional database hits, the filtering is
    done in Python.
    """
    return {
        permission_model.group: permission_model.permissions
        for permission_model in obj.permission_group.permissions.all()
        if permission_model.group_id is not None and permission_model.value > 0
    }


def get_object_perms(
    obj: models.Model,
    user: Optional[User] = None,
    mock_superuser_permissions: bool = False,
) -> List[Dict]:
    """Return permissions for given object in Resolwe specific format.

    Function returns permissions for given object ``obj`` in the following
    format::

       {
           "type": "group"/"user"/"public",
           "id": <group_or_user_id>,
           "name": <group_or_user_name>,
           "permissions": [<first_permission>, <second_permission>,...]
       }

    For ``public`` type ``id`` and ``name`` keys are omitted.

    If ``user`` parameter is given, permissions are limited only to
    given user, groups he belongs to and public permissions.

    This function should be only used from Resolwe views: since permissions
    for the current user (users when user has share permission on the given
    object) are prefetched, we only iterate through objects here and filter
    them in Python. Using filter method would result in a new database query.

    :param obj: Resolwe's DB model's instance
    :param user: Django user
    :param mock_superuser_permissions: when True return all permissions for
        users that are superusers
    :return: list of permissions object in described format

    """
    perms_list = []
    public_permissions = []
    anonymous_user = get_anonymous_user()

    # Request only permissions for the given user.
    if user is not None:
        if user.is_authenticated:
            user_permissions, groups_permissions = get_user_group_perms(user, obj)
            if mock_superuser_permissions and user.is_superuser:
                user_permissions = list(Permission.highest())
        else:
            user_permissions, groups_permissions = [], []

        if user_permissions:
            perms_list.append(
                {
                    "type": "user",
                    "id": user.pk,
                    "name": user.get_full_name() or user.username,
                    "username": user.username,
                    "permissions": [str(permission) for permission in user_permissions],
                }
            )

        if groups_permissions:
            for group_id, group_name, permissions in groups_permissions:
                perms_list.append(
                    {
                        "type": "group",
                        "id": group_id,
                        "name": group_name,
                        "permissions": [str(permission) for permission in permissions],
                    }
                )
    else:
        for user, permissions in _get_users_with_perms(obj).items():
            if user == anonymous_user:
                # public user permissions are considered bellow
                public_permissions = permissions
                continue
            perms_list.append(
                {
                    "type": "user",
                    "id": user.pk,
                    "name": user.get_full_name() or user.username,
                    "username": user.username,
                    "permissions": [str(permission) for permission in permissions],
                }
            )

        for group, permissions in _get_groups_with_perms(obj).items():
            perms_list.append(
                {
                    "type": "group",
                    "id": group.pk,
                    "name": group.name,
                    "permissions": [str(permission) for permission in permissions],
                }
            )
    # Retrieve public permissions only if they are not set.
    # if not public_permissions:
    public_permissions = get_user_group_perms(anonymous_user, obj)[0]

    if public_permissions:
        perms_list.append(
            {
                "type": "public",
                "permissions": [str(permission) for permission in public_permissions],
            }
        )

    return perms_list
