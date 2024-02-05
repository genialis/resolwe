"""Channels consumers for Storage application."""

import logging

from channels.consumer import SyncConsumer

from resolwe.storage.cleanup import Cleaner
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


class StorageCleanupConsumer(SyncConsumer):
    """Start Storage Cleanup when triggered.

    Optionally id of the FileStorage object to clean can be sent with the
    event.
    """

    def storagecleanup_run(self, event):
        """Start the cleanup run."""
        cleaner = Cleaner()
        try:
            file_storage_id = event.get("file_storage_id")
            cleaner.process(file_storage_id)
        except Exception:
            logger.exception("Error while running cleanup.")
