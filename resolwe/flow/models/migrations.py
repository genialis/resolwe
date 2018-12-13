"""Resolwe migration history models."""
from django.contrib.postgres.fields import JSONField
from django.db import models


class MigrationHistoryBase(models.Model):
    """Abstract model for storing migration history."""

    class Meta:
        """Model meta."""

        abstract = True

    #: migration identifier
    migration = models.CharField(max_length=255, db_index=True)

    #: creation date and time
    created = models.DateTimeField(auto_now_add=True)

    #: migration-specific metadata
    metadata = JSONField(default=dict)

    def __str__(self):
        """Format model name."""
        return self.migration


class ProcessMigrationHistory(MigrationHistoryBase):
    """Model for storing process migration history."""

    process = models.ForeignKey('Process', related_name='migration_history', on_delete=models.CASCADE)


class DataMigrationHistory(MigrationHistoryBase):
    """Model for storing data migration history."""

    data = models.ForeignKey('Data', related_name='migration_history', on_delete=models.CASCADE)
