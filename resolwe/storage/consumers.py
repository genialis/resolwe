"""Channels consumers for Storage application."""
import logging

from channels.consumer import SyncConsumer

from resolwe.storage.manager import Manager

logger = logging.getLogger(__name__)


class StorageManagerConsumer(SyncConsumer):
    """Start Storage Manager when triggered."""

    def storagemanager_run(self, event):
        """Start the manager run."""
        manager = Manager()
        try:
            manager.process()
        except Exception:
            logger.exception("Error while running manager.")
