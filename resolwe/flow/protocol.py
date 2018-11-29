"""Constants used in Django Channels."""

# Channel used for notifying workers from ORM signals.
CHANNEL_PURGE_WORKER = 'flow.purge'
# Message type for starting the Data purge.
TYPE_PURGE_RUN = 'purge.run'
