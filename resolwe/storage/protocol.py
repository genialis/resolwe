"""Constants used in Django Channels."""

# Channel used for notifying storage manager to start processing data.
CHANNEL_STORAGE_MANAGER_WORKER = "storage.manager"
# Message type for starting the storage manager.
TYPE_STORAGE_MANAGER_RUN = "storagemanager.run"
