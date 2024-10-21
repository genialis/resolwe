"""Base model for all Resolwe models."""

from django.conf import settings
from django.db import IntegrityError, models, transaction
from versionfield import VersionField

from resolwe.auditlog.models import AuditModel
from resolwe.flow.models.fields import ResolweSlugField
from resolwe.permissions.managers import BaseManager

VERSION_NUMBER_BITS = (8, 10, 14)

# Maximum number of slug-generation retries.
MAX_SLUG_RETRIES = 10
MAX_SLUG_LENGTH = 100
MAX_NAME_LENGTH = 100


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
