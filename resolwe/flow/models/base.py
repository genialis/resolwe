"""Base model for all Resolwe models."""
from versionfield import VersionField

from django.conf import settings
from django.db import IntegrityError, models, transaction

from .fields import ResolweSlugField

VERSION_NUMBER_BITS = (8, 10, 14)

# Maximum number of slug-generation retries.
MAX_SLUG_RETRIES = 10


class BaseModel(models.Model):
    """Abstract model that includes common fields for other models."""

    class Meta:
        """BaseModel Meta options."""

        abstract = True
        unique_together = ('slug', 'version')
        default_permissions = ()
        get_latest_by = 'version'

    #: URL slug
    slug = ResolweSlugField(populate_from='name', unique_with='version', max_length=100)

    #: process version
    version = VersionField(number_bits=VERSION_NUMBER_BITS, default='0.0.0')

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
        for _ in range(MAX_SLUG_RETRIES):
            try:
                # Attempt to save the model. It may fail due to slug conflict.
                with transaction.atomic():
                    super().save(*args, **kwargs)
                    break
            except IntegrityError as error:
                # Retry in case of slug conflicts.
                if '{}_slug'.format(self._meta.db_table) in error.args[0]:
                    self.slug = None
                    continue

                raise
        else:
            raise IntegrityError("Maximum number of retries exceeded during slug generation")
