"""Worker data model representing the state of the executor."""
from django.db import models


class Worker(models.Model):
    """Stores information about worker."""

    STATUS_PREPARING = "PP"
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

    started = models.DateTimeField(auto_now=True)
    finished = models.DateTimeField(null=True)
    data = models.OneToOneField(
        "flow.Data", on_delete=models.CASCADE, related_name="worker"
    )
    status = models.CharField(max_length=2, choices=STATUS_CHOICES)

    def __str__(self):
        """Return the string representation."""
        return f"Worker(status={self.status}, data={self.data}, status={self.status})"
