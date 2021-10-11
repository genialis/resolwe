""".. Ignore pydocstyle D400.

=====================
Permissions shortcuts
=====================

.. autofunction:: _group_groups
.. autofunction:: get_object_perms

"""
from typing import Dict, List, Optional, Tuple, Union

from django.conf import settings
from django.contrib.auth.models import Group, User
from django.db import models

from resolwe.permissions.models import Permission, PermissionModel
from resolwe.permissions.utils import get_anonymous_user, get_identity, user_model_check


def get_user_group_perms_qset(
    user_or_group: Union[User, Group], qset: models.QuerySet
) -> Tuple[List[str], List[Tuple[int, str, List[str]]]]:
    """Get permissions for every object in the queryset."""
    user, groups = get_identity(user_or_group)

    assert user is not None, "User must not be none"

    if groups is None and user is not None:
        groups = user.groups.all()

    assert groups is not None

    filters = models.Q(permission_group__permissions__users=user) | models.Q(
        permission_group__permissions__groups__in=groups.all()
    )

    anonymous_user = get_anonymous_user()
    if user != anonymous_user:
        filters = filters | models.Q(
            permission_group__permissions__users__username=anonymous_user.username
        )

    return (
        qset.filter(filters)
        .annotate(
            entityid=models.functions.Coalesce(
                "permission_group__permissions__users",
                "permission_group__permissions__groups",
            )
        )
        .annotate(
            entityname=models.functions.Coalesce(
                "permission_group__permissions__users__username",
                "permission_group__permissions__groups__name",
            )
        )
        .annotate(
            is_user=models.functions.Coalesce(
                models.functions.Cast(
                    "permission_group__permissions__users",
                    output_field=models.BooleanField(),
                ),
                False,
            )
        )
        .annotate(permission=models.F("permission_group__permissions__permission"))
    ).values("pk", "entityid", "entityname", "is_user", "permission")


def get_user_group_perms(
    user_or_group: Union[User, Group], obj: models.Model
) -> Tuple[List[str], List[Tuple[int, str, List[str]]]]:
    """Get permissins for user groups.

    Based on guardian.core.ObjectPermissionChecker.
    """
    user, groups = get_identity(user_or_group)

    # Inactive users shoud have no permissions.
    if user is not None and not user.is_active:
        return [], []

    user_perms = []
    groups_perms = []

    if user is not None:
        if not user.is_active:
            return [], []
        if user.is_superuser:
            user_perms = Permission.highest_permission().all_permissions()
        else:
            # Get user permissions on obj.
            should_continue, user, obj = user_model_check(user, obj)
            if not should_continue:
                return [], []
            permission = PermissionModel.objects.filter(
                users=user, permission_group=obj.permission_group
            ).values_list("permission", flat=True)
            if permission:
                user_perms = Permission(permission[0]).all_permissions()

        groups = user.groups.all()
    else:
        groups = [groups]

    # Get group permissions for every group
    for group in groups:
        group_qset = PermissionModel.objects.filter(
            groups=group, permission_group=obj.permission_group
        ).values_list("permission", flat=True)
        if group_qset:
            group_permissions = Permission(group_qset[0]).all_permissions()
            groups_perms.append((group.pk, group.name, group_permissions))

    return user_perms, groups_perms


def get_users_with_perms(obj: models.Model) -> Dict[User, List[str]]:
    """Get users with permissions on the given object.

    Only permissions attached to user directly are consider, permissions
    attached to the user groups are ignored.

    :returns: a dict where users are keys and list of permissions are values.

    TODO: Sanity check if object has permission group?
    """
    permissions = {}
    for permission_model in PermissionModel.objects.filter(
        permission_group=obj.permission_group
    ):
        all_permissions = Permission(permission_model.permission).all_permissions()
        for user in permission_model.users.all():
            permissions[user] = all_permissions
    return permissions


def get_groups_with_perms(obj: models.Model) -> Dict[Group, List[str]]:
    """Get groups with permissions on the given object.

    :returns: a dict where groups are keys and list of permissions are values.

    TODO: Sanity check if object has permission group?
    """
    permissions = {}
    for permission_model in PermissionModel.objects.filter(
        permission_group=obj.permission_group
    ):
        all_permissions = Permission(permission_model.permission).all_permissions()
        for group in permission_model.groups.all():
            permissions[group] = all_permissions
    return permissions


def get_object_perms(obj: models.Model, user: Optional[User] = None):
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
            if user.username == settings.ANONYMOUS_USER_NAME:
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
        public_permissions = get_user_group_perms(get_anonymous_user(), obj)[0]
    if public_permissions:
        perms_list.append({"type": "public", "permissions": public_permissions})

    return perms_list


# based on guardian.shortcuts.get_objects_for_user
def get_objects_for_user(
    user: User,
    perms: Union[List[str], str],
    queryset: models.QuerySet,
    use_groups=True,
    any_perm=False,  # <-- in testing
    with_superuser=True,  # <--
    accept_global_perms=True,
    perms_filter="pk__in",  # <-- REMOVE, no longer needed
) -> models.QuerySet:
    """Return queryset with required permissions."""
    if isinstance(perms, str):
        perms = [perms]

    if not perms:
        return queryset.none()

    app_label = queryset.model._meta.app_label
    codenames = set()

    # Compute codenames set and ctype if possible
    for perm in perms:
        if "." in perm:
            new_app_label, codename = perm.split(".", 1)
            if app_label is not None and app_label != new_app_label:
                raise RuntimeError(
                    "Given perms must have same app label "
                    "({} != {})".format(app_label, new_app_label)
                )
            else:
                app_label = new_app_label
        else:
            codename = perm
        codenames.add(codename)

    # First check if user is superuser and if so, return queryset immediately
    if with_superuser and user.is_superuser:
        return queryset

    # Get anonymous user instance when user in not logged in.
    if user.is_anonymous:
        user = get_anonymous_user()

    # When any permission is sufficient take the bottom, else top one.
    if not any_perm:
        needed_permission = max(Permission.from_name(perm) for perm in codenames).value
    else:
        needed_permission = min(Permission.from_name(perm) for perm in codenames).value

    # Handle special case of Storage and Relation.
    filters_prefix = ""
    if queryset.model._meta.label == "flow.Storage":
        filters_prefix = "data__"
    if queryset.model._meta.label == "flow.Relation":
        filters_prefix = "collection__"

    user_filters = {
        f"{filters_prefix}permission_group__permissions__users": user,
        f"{filters_prefix}permission_group__permissions__permission__gte": needed_permission,
    }
    anonymous_filters = {
        f"{filters_prefix}permission_group__permissions__users": get_anonymous_user(),  # TODO: can this be more efficient?
        f"{filters_prefix}permission_group__permissions__permission__gte": needed_permission,
    }

    if not use_groups:
        ids = (
            queryset.filter(models.Q(**user_filters) | models.Q(**anonymous_filters))
            .distinct()
            .values_list("id", flat=True)
        )
    else:
        group_filters = {
            f"{filters_prefix}permission_group__permissions__groups__in": user.groups.all().values_list(
                "pk", flat=True
            ),
            f"{filters_prefix}permission_group__permissions__permission__gte": needed_permission,
        }

        # Return the original queryset without additional joins.
        # So first filter out all the necessary ids and then firter the
        # original queryset based on the set of ids.

        # Warning: do not remove list here otherwise the query bellow may fail
        # when using ExpressionLateralJoin.
        ids = list(
            queryset.filter(
                models.Q(**user_filters)
                | models.Q(**anonymous_filters)
                | models.Q(**group_filters)
            )
            .distinct()
            .values_list("id", flat=True)
        )

    return queryset.filter(id__in=ids)
