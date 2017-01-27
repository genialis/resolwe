"""Resolwe collection model."""
from __future__ import absolute_import, division, print_function, unicode_literals

from django.contrib.postgres.fields import JSONField
from django.db import models

from .base import BaseModel


class BaseCollection(BaseModel):
    """Template for Postgres model for storing a collection."""

    class Meta(BaseModel.Meta):
        """BaseCollection Meta options."""

        abstract = True

    #: detailed description
    description = models.TextField(blank=True)

    settings = JSONField(default=dict)

    public_processes = models.ManyToManyField('flow.Process')

    data = models.ManyToManyField('flow.Data')

    #: collection descriptor schema
    descriptor_schema = models.ForeignKey('flow.DescriptorSchema', blank=True, null=True, on_delete=models.PROTECT)

    #: collection descriptor
    descriptor = JSONField(default=dict)


class Collection(BaseCollection):
    """Postgres model for storing a collection."""

    class Meta(BaseCollection.Meta):
        """Collection Meta options."""

        permissions = (
            ("view_collection", "Can view collection"),
            ("edit_collection", "Can edit collection"),
            ("share_collection", "Can share collection"),
            ("download_collection", "Can download files from collection"),
            ("add_collection", "Can add data objects to collection"),
            ("owner_collection", "Is owner of the collection"),
        )
