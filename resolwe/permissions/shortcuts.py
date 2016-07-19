"""
=====================
Permissions shortcuts
=====================

.. autofunction:: _group_groups
.. autofunction:: get_object_perms

"""

from __future__ import unicode_literals

from itertools import chain, groupby
import six

from django.contrib.auth.models import AnonymousUser, Group, Permission
from django.contrib.contenttypes.models import ContentType
from django.conf import settings

from guardian.utils import get_group_obj_perms_model, get_identity, get_user_obj_perms_model
from guardian.shortcuts import get_users_with_perms, get_groups_with_perms, get_perms
from guardian.compat import get_user_model


def _group_groups(perm_list):
    """Group permissions by group

    Input is list of tuples of length 3, where each tuple is in
    following format::

        (<group_id>, <group_name>, <single_permission>)

    Permissions are regrouped and returned in such way that there is
    only one tuple for each group::

        (<group_id>, <group_name>, [<first_permission>, <second_permission>,...])

    :param list perm_list: list of touples of length 3
    :return: list tuples with grouped permissions
    :rtype: list

    """
    perm_list = sorted(perm_list, key=lambda tup: tup[0])

    grouped_perms = []
    for key, group in groupby(perm_list, lambda tup: (tup[0], tup[1])):
        grouped_perms.append((key[0], key[1], [g[2] for g in group]))

    return grouped_perms


# Based on guardian.core.ObjectPermissionChecker.
def get_user_group_perms(user_or_group, obj):
    user, group = get_identity(user_or_group)

    if user and not user.is_active:
        return [], []
    User = get_user_model()
    ctype = ContentType.objects.get_for_model(obj)

    group_model = get_group_obj_perms_model(obj)
    group_rel_name = group_model.permission.field.related_query_name()  # pylint: disable=no-member
    if user:
        user_rel_name = User.groups.field.related_query_name()  # pylint: disable=no-member
        group_filters = {user_rel_name: user}
    else:
        group_filters = {'pk': group.pk}
    if group_model.objects.is_generic():
        group_filters.update({
            '{}__content_type'.format(group_rel_name): ctype,
            '{}__object_pk'.format(group_rel_name): obj.pk,
        })
    else:
        group_filters['{}__content_object'.format(group_rel_name)] = obj

    user_perms, group_perms = [], []

    if user:
        perms_qs = Permission.objects.filter(content_type=ctype)
        if user.is_superuser:
            user_perms = list(chain(perms_qs.values_list("codename", flat=True)))
        else:
            model = get_user_obj_perms_model(obj)
            related_name = model.permission.field.related_query_name()  # pylint: disable=no-member
            user_filters = {'{}__user'.format(related_name): user}
            if model.objects.is_generic():
                user_filters.update({
                    '{}__content_type'.format(related_name): ctype,
                    '{}__object_pk'.format(related_name): obj.pk,
                })
            else:
                user_filters['{}__content_object'.format(related_name)] = obj

            user_perms_qs = perms_qs.filter(**user_filters)
            user_perms = list(chain(user_perms_qs.values_list("codename", flat=True)))

    group_perms_qs = Group.objects.filter(**group_filters)
    group_perms = list(chain(group_perms_qs.order_by("pk").values_list(
        "pk", "name", "{}__permission__codename".format(group_rel_name))))

    group_perms = _group_groups(group_perms)

    return user_perms, group_perms


def get_object_perms(obj, user=None):
    """Return permissions for given object in Resolwe specific format

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
    :type obj: a subclass of :class:`~resolwe.flow.models.BaseModel`
    :param user: Django user
    :type user: :class:`~django.contrib.auth.models.User` or :data:`None`
    :return: list of permissions object in described format
    :rtype: list

    """
    def format_permissions(perms):
        """Remove model name from permission"""
        ctype = ContentType.objects.get_for_model(obj)
        return [perm.replace('_{}'.format(ctype.name), '') for perm in perms]

    perms_list = []

    if user:
        if user.is_authenticated():
            user_perms, group_perms = get_user_group_perms(user, obj)
        else:
            user_perms, group_perms = [], []

        if user_perms != []:
            perms_list.append({
                'type': 'user',
                'id': user.pk,
                'name': user.get_full_name() or user.username,
                'permissions': format_permissions(user_perms),
            })

        if group_perms != []:
            for group_id, group_name, perms in group_perms:
                perms_list.append({
                    'type': 'group',
                    'id': group_id,
                    'name': group_name,
                    'permissions': format_permissions(perms),
                })
    else:
        user_options = {
            'attach_perms': True,
            'with_superusers': True,
            'with_group_users': False
        }
        for user, perms in six.iteritems(get_users_with_perms(obj, **user_options)):
            if user.username == settings.ANONYMOUS_USER_NAME:
                # public user is treated separately
                continue
            perms_list.append({
                'type': 'user',
                'id': user.pk,
                'name': user.get_full_name() or user.username,
                'permissions': format_permissions(perms),
            })

        group_options = {
            'attach_perms': True,
        }
        for group, perms in six.iteritems(get_groups_with_perms(obj, **group_options)):
            perms_list.append({
                'type': 'group',
                'id': group.pk,
                'name': group.name,
                'permissions': format_permissions(perms),
            })

    public_perms = get_perms(AnonymousUser(), obj)
    if public_perms != []:
        perms_list.append({
            'type': 'public',
            'permissions': format_permissions(public_perms),
        })

    return perms_list
