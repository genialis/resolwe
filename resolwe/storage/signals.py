""".. Ignore pydocstyle D400.

===============
Signal Handlers
===============

"""
import logging

from asgiref.sync import async_to_sync
from channels.layers import ChannelFull, get_channel_layer

from django.db.models.signals import post_delete
from django.dispatch import receiver

from resolwe.flow.models import Data
from resolwe.storage.protocol import (
    CHANNEL_STORAGE_CLEANUP_WORKER,
    TYPE_STORAGE_CLEANUP_RUN,
)

logger = logging.getLogger(__name__)


@receiver(post_delete, sender=Data)
def data_post_delete_handler(sender, instance: Data, using, **kwargs):
    """Remove files referenced by deleted object."""
    channel_layer = get_channel_layer()
    channel_data = {"type": TYPE_STORAGE_CLEANUP_RUN}
    try:
        if instance.location_id is not None:
            channel_data["file_storage_id"] = instance.location_id
        async_to_sync(channel_layer.send)(CHANNEL_STORAGE_CLEANUP_WORKER, channel_data)
    except ChannelFull:
        logger.warning("Cannot trigger storage manager run because channel is full.")
