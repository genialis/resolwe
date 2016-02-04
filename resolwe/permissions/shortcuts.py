from __future__ import unicode_literals

from itertools import chain

from django.contrib.auth.models import Permission
from django.contrib.contenttypes.models import ContentType

from guardian.utils import get_identity
from guardian.utils import get_user_obj_perms_model
from guardian.utils import get_group_obj_perms_model
from guardian.compat import get_user_model


# Based on guardian.core.ObjectPermissionChecker.
def get_user_group_perms(user_or_group, obj):
    user, group = get_identity(user_or_group)

    if user and not user.is_active:
        return [], []
    User = get_user_model()
    ctype = ContentType.objects.get_for_model(obj)

    group_model = get_group_obj_perms_model(obj)
    group_rel_name = group_model.permission.field.related_query_name()
    if user:
        fieldname = '%s__group__%s' % (
            group_rel_name,
            User.groups.field.related_query_name(),
        )
        group_filters = {fieldname: user}
    else:
        group_filters = {'%s__group' % group_rel_name: group}
    if group_model.objects.is_generic():
        group_filters.update({
            '%s__content_type' % group_rel_name: ctype,
            '%s__object_pk' % group_rel_name: obj.pk,
        })
    else:
        group_filters['%s__content_object' % group_rel_name] = obj

    user_perms = []
    group_perms = []

    if user and user.is_superuser:
        user_perms = list(chain(*Permission.objects
                                .filter(content_type=ctype)
                                .values_list("codename")))
    elif user:
        model = get_user_obj_perms_model(obj)
        related_name = model.permission.field.related_query_name()
        user_filters = {'%s__user' % related_name: user}
        if model.objects.is_generic():
            user_filters.update({
                '%s__content_type' % related_name: ctype,
                '%s__object_pk' % related_name: obj.pk,
            })
        else:
            user_filters['%s__content_object' % related_name] = obj
        perms_qs = Permission.objects.filter(content_type=ctype)
        user_perms_qs = perms_qs.filter(**user_filters)
        user_perms = user_perms_qs.values_list("codename", flat=True)
        group_perms_qs = perms_qs.filter(**group_filters)
        group_perms = group_perms_qs.values_list("codename", flat=True)
    else:
        user_perms = list(set(chain(*Permission.objects
                                    .filter(content_type=ctype)
                                    .filter(**group_filters)
                                    .values_list("codename"))))

    return user_perms, group_perms
