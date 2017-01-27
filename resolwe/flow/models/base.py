"""Base model for all Resolwe models."""


from __future__ import absolute_import, division, print_function, unicode_literals

from django.db import models
from django.conf import settings

from versionfield import VersionField
from autoslug import AutoSlugField


VERSION_NUMBER_BITS = (8, 10, 14)


class BaseModel(models.Model):
    """Abstract model that includes common fields for other models."""

    class Meta:
        """BaseModel Meta options."""

        abstract = True
        unique_together = ('slug', 'version')
        default_permissions = ()

    #: URL slug
    slug = AutoSlugField(populate_from='name', unique_with='version', editable=True, max_length=100)

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
