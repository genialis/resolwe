""".. Ignore pydocstyle D400.

===============
Signal Handlers
===============

"""
from django.db.models.signals import post_save, pre_delete
from django.dispatch import receiver

from resolwe.flow.models import Data, Entity
from resolwe.flow.managers import manager


@receiver(post_save, sender=Data)
def manager_post_save_handler(sender, instance, **kwargs):
    """Run newly created (spawned) processes."""
    if instance.status == Data.STATUS_DONE or instance.status == Data.STATUS_ERROR:
        manager.communicate(verbosity=0)


@receiver(pre_delete, sender=Data)
def delete_entity(sender, instance, **kwargs):
    """Delete Entity when last Data object is deleted."""
    try:
        entity = Entity.objects.get(data=instance.pk)
    except Entity.DoesNotExist:  # pylint: disable=no-member
        return

    if entity.data.count() == 1:  # last Data object will be just deleted
        entity.delete()
