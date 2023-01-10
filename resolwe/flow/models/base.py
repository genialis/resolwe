"""Base model for all Resolwe models."""
from versionfield import VersionField

from django.conf import settings
from django.db import IntegrityError, models, transaction

from resolwe.auditlog.models import AuditModel

from .fields import ResolweSlugField

VERSION_NUMBER_BITS = (8, 10, 14)

# Maximum number of slug-generation retries.
MAX_SLUG_RETRIES = 10


class UniqueSlugError(IntegrityError):
    """The error indicates slug collision."""


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


class BaseModel(AuditModel):
    """Abstract model that includes common fields for other models."""

    class Meta:
        """BaseModel Meta options."""

        abstract = True
        unique_together = ("slug", "version")
        default_permissions = ()
        get_latest_by = "version"

    #: URL slug
    slug = ResolweSlugField(populate_from="name", unique_with="version", max_length=100)

    #: process version
    version = VersionField(number_bits=VERSION_NUMBER_BITS, default="0.0.0")

    #: object name
    name = models.CharField(max_length=100)

    #: creation date and time
    created = models.DateTimeField(auto_now_add=True, db_index=True)

    #: modified date and time
    modified = models.DateTimeField(auto_now=True, db_index=True)

    #: user that created the entry
    contributor = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.PROTECT)

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
