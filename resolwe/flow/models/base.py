"""Base model for all Resolwe models."""

import datetime
from typing import TypeVar

from django.conf import settings
from django.db import IntegrityError, models, transaction
from versionfield import VersionField

from resolwe.auditlog.models import AuditModel
from resolwe.flow.models.fields import ResolweSlugField
from resolwe.permissions.models import PermissionManager, PermissionQuerySet

VERSION_NUMBER_BITS = (8, 10, 14)

# Maximum number of slug-generation retries.
MAX_SLUG_RETRIES = 10
MAX_SLUG_LENGTH = 100
MAX_NAME_LENGTH = 100

M = TypeVar("M", bound=models.Model)
Q = TypeVar("Q", bound=models.QuerySet)


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


class BaseQuerySet(PermissionQuerySet):
    """Base query set for Resolwe's ORM objects."""

    def delete_chunked(self, chunk_size=500):
        """Chunked delete, which should be used if deleting many objects.

        The reason why this method is needed is that deleting a lot of Data objects
        requires Django to fetch all of them into memory (fast path is not used) and
        this causes huge memory usage (and possibly OOM).

        :param chunk_size: Optional chunk size
        """
        return delete_chunked(self, chunk_size=chunk_size)


class BaseManager(PermissionManager[M, Q]):
    """Base manager set for Resolwe's ORM objects."""

    # The data is partitioned into groups with the same partition fields value. The
    # object with the greatest version inside the partition is considered the latest.
    partition_fields: list[str] = ["slug"]

    # The field to use as version. The latest version is determined by sorting the
    # data by version field in the ascending order and taking the first entry.
    version_field: str = "version"

    # The created field is used to determine the version at certain point in time.
    created_field: str = "created"

    # Versioning is enabled by default.
    enable_versioning: bool = True

    # Use BaseQuerySet by default.
    QuerySet = BaseQuerySet

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
        queryset = super().get_queryset()
        return self._only_latest(queryset) if self.enable_versioning else queryset

    def history(self, timestamp: datetime.datetime) -> Q:
        """Get values at certain point in time."""
        created_filter = {f"{self.created_field}__le": timestamp}
        filtered_queryset: Q = self.filter(**created_filter)  # type: ignore
        return self._only_latest(filtered_queryset)


class BaseManagerWithoutVersion(BaseManager[M, Q]):
    """Base manager for Resolwe's ORM objects without versioning."""

    # Disable object versioning.
    enable_versioning: bool = False


class ModifiedField(models.DateTimeField):
    """Custom datetime field.

    When attribute 'skip_auto_now' is set to True, the field is not updated in the
    pre_save method even if auto_now is set to True. The 'skip_auto_now' is reset
    on every save to the default value 'False'.
    """

    def pre_save(self, model_instance, add):
        """Prepare field for saving."""
        # When skip_auto_now is set do not generate the new value even is auto_now is
        # set to True.
        if getattr(model_instance, "skip_auto_now", False) is True:
            return getattr(model_instance, self.attname)
        else:
            return super().pre_save(model_instance, add)


class UniqueSlugError(IntegrityError):
    """The error indicates slug collision."""


class BaseModel(AuditModel):
    """Abstract model that includes common fields for other models."""

    class Meta:
        """BaseModel Meta options."""

        abstract = True
        unique_together = ("slug", "version")
        default_permissions = ()
        get_latest_by = "version"

    #: URL slug
    slug = ResolweSlugField(
        populate_from="name", unique_with="version", max_length=MAX_SLUG_LENGTH
    )

    #: process version
    version = VersionField(number_bits=VERSION_NUMBER_BITS, default="0.0.0")

    #: object name
    name = models.CharField(max_length=MAX_NAME_LENGTH)

    #: creation date and time
    created = models.DateTimeField(auto_now_add=True, db_index=True)

    #: modified date and time
    modified = ModifiedField(auto_now=True, db_index=True)

    #: user that created the entry
    contributor = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.PROTECT)

    #: the latest version of the objects
    objects = BaseManager()

    #: all abjects
    all_objects = models.Manager()

    def __str__(self):
        """Format model name."""
        return self.name

    def save(self, *args, **kwargs):
        """Save the model."""
        name_max_len = self._meta.get_field("name").max_length
        if len(self.name) > name_max_len:
            self.name = self.name[: (name_max_len - 3)] + "..."

        for _ in range(MAX_SLUG_RETRIES):
            try:
                # Attempt to save the model. It may fail due to slug conflict.
                with transaction.atomic():
                    super().save(*args, **kwargs)
                    break
            except IntegrityError as error:
                # Retry in case of slug conflicts.
                if "{}_slug".format(self._meta.db_table) in error.args[0]:
                    self.slug = None
                    continue

                raise
        else:
            raise UniqueSlugError(
                f"Unable to generate unique slug after {MAX_SLUG_RETRIES} retries."
            )
