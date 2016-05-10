from django.db.models.signals import post_save
from django.dispatch import receiver

from resolwe.flow.models import Data
from resolwe.flow.managers import manager


@receiver(post_save, sender=Data)
def manager_post_save_handler(sender, instance, **kwargs):
    if instance.status == Data.STATUS_DONE or instance.status == Data.STATUS_ERROR:
        manager.communicate()
