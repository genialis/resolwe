"""Constants used in Django Channels."""

# Channel used for notifying storage manager to start.
CHANNEL_STORAGE_MANAGER_WORKER = "storage.manager"
# Channel used for notifying cleanup manager to start.
CHANNEL_STORAGE_CLEANUP_WORKER = "storage.cleanup"
# Message type for starting the storage manager.
TYPE_STORAGE_MANAGER_RUN = "storagemanager.run"
# Message type for starting the storage cleanup.
TYPE_STORAGE_CLEANUP_RUN = "storagecleanup.run"
