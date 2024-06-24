"""Worker data model representing the state of the executor."""

import logging

from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer
from django.db import models

logger = logging.getLogger(__name__)


class Worker(models.Model):
    """Stores information about worker."""

    STATUS_PREPARING = "PP"
    STATUS_FINISHED_PREPARING = "FP"
    STATUS_PROCESSING = "PR"
    STATUS_NONRESPONDING = "NR"
    STATUS_COMPLETED = "CM"
    STATUS_ERROR_PREPARING = "EP"

    FINAL_STATUSES = (
        STATUS_COMPLETED,
        STATUS_ERROR_PREPARING,
        STATUS_NONRESPONDING,
    )

    STATUS_CHOICES = (
        (STATUS_PROCESSING, "Processing data"),
        (STATUS_NONRESPONDING, "Unresponsive"),
        (STATUS_PREPARING, "Preparing data"),
        (STATUS_COMPLETED, "Finished"),
    )

    #: worker started date and time
    started = models.DateTimeField(auto_now=True)

    #: worker finioshed date and time
    finished = models.DateTimeField(null=True)

    #: reference to the data object
    data = models.OneToOneField(
        "flow.Data", on_delete=models.CASCADE, related_name="worker"
    )

    #: status of the worker object
    status = models.CharField(max_length=2, choices=STATUS_CHOICES)

    #: client private key to communicate with the listener
    private_key = models.BinaryField(max_length=40)

    # cliend public key to communicate with the listener
    public_key = models.BinaryField(max_length=40)

    def terminate(self):
        """Terminate the running worker."""

        # Can only terminate running processes.
        if self.status != self.STATUS_PROCESSING:
            logger.info(
                "Worker has status %s, no terminate signal is sent.", self.status
            )
            return

        packet = {"type": "terminate", "identity": str(self.data.id).encode()}
        from resolwe.flow.managers.state import LISTENER_CONTROL_CHANNEL

        logger.debug("Sending the terminate signal for data '%d'.", self.data_id)
        async_to_sync(get_channel_layer().send)(LISTENER_CONTROL_CHANNEL, packet)

    def __str__(self):
        """Return the string representation."""
        return f"Worker(status={self.status}, data={self.data}, status={self.status})"
