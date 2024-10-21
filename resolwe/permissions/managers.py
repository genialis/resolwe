"""Custom object managers."""

import datetime

from django.db import models, transaction


# NOTE: This method is used in migrations.
def delete_chunked(queryset, chunk_size=500):
    """Chunked delete, which should be used if deleting many objects.

    The reason why this method is needed is that deleting a lot of Data objects
    requires Django to fetch all of them into memory (fast path is not used) and
    this causes huge memory usage (and possibly OOM).

    :param chunk_size: Optional chunk size
    """
    while True:
        # Discover primary key to limit the current chunk. This is required because delete
        # cannot be called on a sliced queryset due to ordering requirement.
        with transaction.atomic():
            # Get offset of last item (needed because it may be less than the chunk size).
            offset = queryset.order_by("pk")[:chunk_size].count()
            if not offset:
                break

            # Fetch primary key of last item and use it to delete the chunk.
            last_instance = queryset.order_by("pk")[offset - 1]
            queryset.filter(pk__lte=last_instance.pk).delete()


class BaseQuerySet(models.QuerySet):
    """Base query set for Resolwe's ORM objects."""

    def delete_chunked(self, chunk_size=500):
        """Chunked delete, which should be used if deleting many objects.

        The reason why this method is needed is that deleting a lot of Data objects
        requires Django to fetch all of them into memory (fast path is not used) and
        this causes huge memory usage (and possibly OOM).

        :param chunk_size: Optional chunk size
        """
        return delete_chunked(self, chunk_size=chunk_size)


class BaseManager[M: models.Model, Q: models.QuerySet](models.Manager[M]):
    """Base query set for Resolwe's ORM objects."""

    # The data is partitioned into groups with the same partition fields value. The
    # object with the greatest version inside the partition is considered the latest.
    partition_fields: list[str] = ["slug"]

    # The field to use as version. The latest version is determined by sorting the
    # data by version field in the ascending order and taking the first entry.
    version_field: str = "version"

    # The created field is used to determine the version at certain point in time.
    created_field: str = "created"

    # The queryset to use.
    QuerySet: type[Q] = BaseQuerySet

    # Should versioning be enabled. When set to True, only latest version of object is
    # returned.
    enable_versioning: bool = True

    def _only_latest(self, queryset: Q) -> Q:
        """Return only latest values from the queryset.

        The queryset is partitioned by the partition fields and the latest version from
        each partition is returned.
        """
        latest_entries = queryset.annotate(
            rank=models.Window(
                expression=models.functions.DenseRank(),
                partition_by=[models.F(field) for field in self.partition_fields],
                order_by=models.F(self.version_field).desc(),
            ),
        ).filter(rank=1)
        return latest_entries

    def get_queryset(self) -> Q:
        """Return only the latest version for every field.

        Make sure the queryset is lazily evaluated, since the get_queryset method can
        be called many times (for instance, when filtering the queryset).
        """
        queryset = self.QuerySet(model=self.model, using=self._db)
        return self._only_latest(queryset) if self.enable_versioning else queryset

    def history(self, timestamp: datetime.datetime) -> Q:
        """Get values at certain point in time"""
        created_filter = {f"{self.created_field}__le": timestamp}
        filtered_queryset: Q = self.filter(**created_filter)  # type: ignore
        return self._only_latest(filtered_queryset)

    def delete_chunked(self, chunk_size=500):
        """Chunked delete, which should be used if deleting many objects.

        The reason why this method is needed is that deleting a lot of Data objects
        requires Django to fetch all of them into memory (fast path is not used) and
        this causes huge memory usage (and possibly OOM).

        :param chunk_size: Optional chunk size
        """
        return delete_chunked(self, chunk_size=chunk_size)
