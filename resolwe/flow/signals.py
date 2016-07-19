from django.db.models.signals import post_delete, post_save
from django.dispatch import receiver

from guardian.models import GroupObjectPermission, UserObjectPermission
from guardian.shortcuts import assign_perm, remove_perm

from resolwe.flow.models import Data, Storage
from resolwe.flow.managers import manager


@receiver(post_save, sender=Data)
def manager_post_save_handler(sender, instance, **kwargs):
    if instance.status == Data.STATUS_DONE or instance.status == Data.STATUS_ERROR:
        manager.communicate()


def _process_storage_perm(perm, user_or_group, data, perm_func):
    if not isinstance(data, Data):
        return

    if perm.codename.startswith('download'):
        return  # `Storage` doesn't have `download` permission

    perm = perm.codename.replace(perm.content_type.name, 'storage')

    for storage in Storage.objects.filter(data=data):
        perm_func(perm, user_or_group, storage)


@receiver(post_save, sender=GroupObjectPermission)
def storage_perms_group_create(sender, instance, **kwargs):
    if kwargs['created']:
        _process_storage_perm(instance.permission, instance.group, instance.content_object, assign_perm)
    else:
        pass  # TODO: sync perms


@receiver(post_save, sender=UserObjectPermission)
def storage_perms_user_create(sender, instance, **kwargs):
    if kwargs['created']:
        _process_storage_perm(instance.permission, instance.user, instance.content_object, assign_perm)


@receiver(post_delete, sender=GroupObjectPermission)
def storage_permis_group_delete(sender, instance, **kwargs):
    _process_storage_perm(instance.permission, instance.group, instance.content_object, remove_perm)


@receiver(post_delete, sender=UserObjectPermission)
def storage_perms_user_delete(sender, instance, **kwargs):
    _process_storage_perm(instance.permission, instance.user, instance.content_object, remove_perm)
