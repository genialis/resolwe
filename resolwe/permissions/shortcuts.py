""".. Ignore pydocstyle D400.

=====================
Permissions shortcuts
=====================

.. autofunction:: _group_groups
.. autofunction:: get_object_perms

"""
from collections import defaultdict
from itertools import chain, groupby

from django.conf import settings
from django.contrib.auth.models import AnonymousUser, Group, Permission
from django.contrib.contenttypes.models import ContentType
from django.db.models import Count, Q
from django.shortcuts import _get_queryset

from guardian.compat import get_user_model
from guardian.ctypes import get_content_type
from guardian.exceptions import MixedContentTypeError, WrongAppError
from guardian.models import GroupObjectPermission
from guardian.shortcuts import get_perms, get_users_with_perms
from guardian.utils import get_anonymous_user, get_group_obj_perms_model, get_identity, get_user_obj_perms_model


# XXX: This is a copy of guardian.shortcuts.get_groups_with_perms with a fixed bug
#      Use the original version once the following fix is merged and released:
#      https://github.com/django-guardian/django-guardian/pull/529
def get_groups_with_perms(obj, attach_perms=False):
    """Return queryset of all ``Group`` objects with *any* object permissions for the given ``obj``."""
    ctype = get_content_type(obj)
    group_model = get_group_obj_perms_model(obj)

    if not attach_perms:
        # It's much easier without attached perms so we do it first if that is the case
        group_rel_name = group_model.group.field.related_query_name()
        if group_model.objects.is_generic():
            group_filters = {
                '%s__content_type' % group_rel_name: ctype,
                '%s__object_pk' % group_rel_name: obj.pk,
            }
        else:
            group_filters = {'%s__content_object' % group_rel_name: obj}
        return Group.objects.filter(**group_filters).distinct()
    else:
        group_perms_mapping = defaultdict(list)
        groups_with_perms = get_groups_with_perms(obj)
        queryset = group_model.objects.filter(group__in=groups_with_perms).prefetch_related('group', 'permission')
        if group_model is GroupObjectPermission:
            queryset = queryset.filter(object_pk=obj.pk, content_type=ctype)
        else:
            queryset = queryset.filter(content_object_id=obj.pk)

        for group_perm in queryset:
            group_perms_mapping[group_perm.group].append(group_perm.permission.codename)
        return dict(group_perms_mapping)


def _group_groups(perm_list):
    """Group permissions by group.

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


def get_user_group_perms(user_or_group, obj):
    """Get permissins for user groups.

    Based on guardian.core.ObjectPermissionChecker.

    """
    user, group = get_identity(user_or_group)

    if user and not user.is_active:
        return [], []
    user_model = get_user_model()
    ctype = ContentType.objects.get_for_model(obj)

    group_model = get_group_obj_perms_model(obj)
    group_rel_name = group_model.permission.field.related_query_name()
    if user:
        user_rel_name = user_model.groups.field.related_query_name()
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
            related_name = model.permission.field.related_query_name()
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
    def format_permissions(perms):
        """Remove model name from permission."""
        ctype = ContentType.objects.get_for_model(obj)
        return [perm.replace('_{}'.format(ctype.name), '') for perm in perms]

    perms_list = []

    if user:
        if user.is_authenticated:
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
            'with_group_users': False
        }
        for user, perms in get_users_with_perms(obj, **user_options).items():
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
        for group, perms in get_groups_with_perms(obj, **group_options).items():
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


# based on guardian.shortcuts.get_objects_for_user
def get_objects_for_user(user, perms, klass=None, use_groups=True, any_perm=False,
                         with_superuser=True, accept_global_perms=True, perms_filter='pk__in'):
    """Return queryset with required permissions."""
    if isinstance(perms, str):
        perms = [perms]

    ctype = None
    app_label = None
    codenames = set()

    # Compute codenames set and ctype if possible
    for perm in perms:
        if '.' in perm:
            new_app_label, codename = perm.split('.', 1)
            if app_label is not None and app_label != new_app_label:
                raise MixedContentTypeError(
                    "Given perms must have same app label "
                    "({} != {})".format(app_label, new_app_label))
            else:
                app_label = new_app_label
        else:
            codename = perm
        codenames.add(codename)

        if app_label is not None:
            new_ctype = ContentType.objects.get(app_label=app_label,
                                                permission__codename=codename)
            if ctype is not None and ctype != new_ctype:
                raise MixedContentTypeError(
                    "ContentType was once computed to be {} and another "
                    "one {}".format(ctype, new_ctype))
            else:
                ctype = new_ctype

    # Compute queryset and ctype if still missing
    if ctype is None and klass is not None:
        queryset = _get_queryset(klass)
        ctype = ContentType.objects.get_for_model(queryset.model)
    elif ctype is not None and klass is None:
        queryset = _get_queryset(ctype.model_class())
    elif klass is None:
        raise WrongAppError("Cannot determine content type")
    else:
        queryset = _get_queryset(klass)
        if ctype.model_class() != queryset.model and perms_filter == 'pk__in':
            raise MixedContentTypeError("Content type for given perms and "
                                        "klass differs")

    # At this point, we should have both ctype and queryset and they should
    # match which means: ctype.model_class() == queryset.model
    # we should also have `codenames` list

    # First check if user is superuser and if so, return queryset immediately
    if with_superuser and user.is_superuser:
        return queryset

    # Check if the user is anonymous. The
    # django.contrib.auth.models.AnonymousUser object doesn't work for queries
    # and it's nice to be able to pass in request.user blindly.
    if user.is_anonymous:
        user = get_anonymous_user()

    global_perms = set()
    has_global_perms = False
    # a superuser has by default assigned global perms for any
    if accept_global_perms and with_superuser:
        for code in codenames:
            if user.has_perm(ctype.app_label + '.' + code):
                global_perms.add(code)
        for code in global_perms:
            codenames.remove(code)
        # prerequisite: there must be elements in global_perms otherwise just
        # follow the procedure for object based permissions only AND
        # 1. codenames is empty, which means that permissions are ONLY set
        # globally, therefore return the full queryset.
        # OR
        # 2. any_perm is True, then the global permission beats the object
        # based permission anyway, therefore return full queryset
        if global_perms and (not codenames or any_perm):
            return queryset
        # if we have global perms and still some object based perms differing
        # from global perms and any_perm is set to false, then we have to flag
        # that global perms exist in order to merge object based permissions by
        # user and by group correctly. Scenario: global perm change_xx and
        # object based perm delete_xx on object A for user, and object based
        # permission delete_xx  on object B for group, to which user is
        # assigned.
        # get_objects_for_user(user, [change_xx, delete_xx], use_groups=True,
        # any_perm=False, accept_global_perms=True) must retrieve object A and
        # B.
        elif global_perms and codenames:
            has_global_perms = True

    # Now we should extract list of pk values for which we would filter
    # queryset
    user_model = get_user_obj_perms_model(queryset.model)
    user_obj_perms_queryset = (user_model.objects
                               .filter(Q(user=user) | Q(user=get_anonymous_user()))
                               .filter(permission__content_type=ctype))

    if codenames:
        user_obj_perms_queryset = user_obj_perms_queryset.filter(
            permission__codename__in=codenames)
    direct_fields = ['content_object__pk', 'permission__codename']
    generic_fields = ['object_pk', 'permission__codename']
    if user_model.objects.is_generic():
        user_fields = generic_fields
    else:
        user_fields = direct_fields

    if use_groups:
        group_model = get_group_obj_perms_model(queryset.model)
        group_filters = {
            'permission__content_type': ctype,
            'group__{}'.format(get_user_model().groups.field.related_query_name()): user,
        }
        if codenames:
            group_filters.update({
                'permission__codename__in': codenames,
            })
        groups_obj_perms_queryset = group_model.objects.filter(**group_filters)
        if group_model.objects.is_generic():
            group_fields = generic_fields
        else:
            group_fields = direct_fields
        if not any_perm and codenames and not has_global_perms:
            user_obj_perms = user_obj_perms_queryset.values_list(*user_fields)
            groups_obj_perms = groups_obj_perms_queryset.values_list(*group_fields)
            data = list(user_obj_perms) + list(groups_obj_perms)
            # sorting/grouping by pk (first in result tuple)
            data = sorted(data, key=lambda t: t[0])
            pk_list = []
            for pk, group in groupby(data, lambda t: t[0]):
                obj_codenames = set((e[1] for e in group))
                if codenames.issubset(obj_codenames):
                    pk_list.append(pk)
            objects = queryset.filter(**{perms_filter: pk_list})
            return objects

    if not any_perm and len(codenames) > 1:
        counts = user_obj_perms_queryset.values(
            user_fields[0]).annotate(object_pk_count=Count(user_fields[0]))
        user_obj_perms_queryset = counts.filter(
            object_pk_count__gte=len(codenames))

    values = user_obj_perms_queryset.values_list(user_fields[0], flat=True)
    if user_model.objects.is_generic():
        values = list(values)
    query = Q(**{perms_filter: values})
    if use_groups:
        values = groups_obj_perms_queryset.values_list(group_fields[0], flat=True)
        if group_model.objects.is_generic():
            values = list(values)
        query |= Q(**{perms_filter: values})

    return queryset.filter(query)


def get_users_with_permission(obj, permission):
    """Return users with specific permission on object.

    :param obj: Object to return users for
    :param permission: Permission codename
    """
    user_model = get_user_model()
    return user_model.objects.filter(
        userobjectpermission__object_pk=obj.pk,
        userobjectpermission__permission__codename=permission,
    ).distinct()
