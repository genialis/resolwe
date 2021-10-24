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
    PermissionList,
    PermissionModel,
    UserOrGroup,
    get_anonymous_user,
)
from resolwe.permissions.utils import get_identity, model_has_permissions


def get_user_group_perms(
    user_or_group: UserOrGroup, obj: models.Model
) -> Tuple[PermissionList, List[Tuple[int, str, PermissionList]]]:
    """Get permissions for the user/group on the given model.

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
        user_perms = obj.get_permissions(entity)
        groups = entity.groups.all()
    else:
        groups = [entity]

    # Get group permissions, iterate through queryset to lower the numbers of
    # produced queries.
    for permission_model in PermissionModel.objects.filter(
        group__in=groups, permission_group=obj.permission_group
    ).order_by("group__pk"):
        groups_perms.append(
            (
                permission_model.group_id,
                permission_model.group.name,
                permission_model.permissions,
            )
        )

    return user_perms, groups_perms


def get_users_with_perms(obj: models.Model) -> Dict[User, PermissionList]:
    """Get users with permissions on the given object.

    Only permissions attached to user directly are consider, permissions
    attached to the user groups are ignored.
    """
    return {
        permission_model.user: permission_model.permissions
        for permission_model in PermissionModel.objects.filter(
            permission_group=obj.permission_group, user__isnull=False
        )
    } or {}


def get_groups_with_perms(obj: models.Model) -> Dict[Group, PermissionList]:
    """Get groups with permissions on the given object."""
    return {
        permission_model.group: permission_model.permissions
        for permission_model in PermissionModel.objects.filter(
            permission_group=obj.permission_group, group__isnull=False
        )
    } or {}


def get_object_perms(obj: models.Model, user: Optional[User] = None) -> Dict:
    """Return permissions for given object in Resolwe specific format.

    Function returns permissions for given object ``obj`` in following
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

    :param obj: Resolwe's DB model's instance
    :type obj: a subclass of :class:`~resolwe.flow.models.base.BaseModel`
    :param user: Django user
    :type user: :class:`~django.contrib.auth.models.User` or :data:`None`
    :return: list of permissions object in described format
    :rtype: list

    """
    perms_list = []
    public_permissions = []
    anonymous_user = get_anonymous_user()

    if user is not None:
        if user.is_authenticated:
            user_perms, groups_perms = get_user_group_perms(user, obj)
        else:
            user_perms, groups_perms = [], []

        if user_perms:
            perms_list.append(
                {
                    "type": "user",
                    "id": user.pk,
                    "name": user.get_full_name() or user.username,
                    "username": user.username,
                    "permissions": user_perms,
                }
            )

        if groups_perms:
            for group_id, group_name, permissions in groups_perms:
                perms_list.append(
                    {
                        "type": "group",
                        "id": group_id,
                        "name": group_name,
                        "permissions": permissions,
                    }
                )
    else:
        for user, permissions in get_users_with_perms(obj).items():
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
                    "permissions": permissions,
                }
            )

        for group, permissions in get_groups_with_perms(obj).items():
            perms_list.append(
                {
                    "type": "group",
                    "id": group.pk,
                    "name": group.name,
                    "permissions": permissions,
                }
            )
    # Retrieve public permissions only if they are not set.
    if not public_permissions:
        public_permissions = get_user_group_perms(anonymous_user, obj)[0]

    if public_permissions:
        perms_list.append({"type": "public", "permissions": public_permissions})

    return perms_list
