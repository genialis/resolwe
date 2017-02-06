"""Resolwe entity model."""
from __future__ import absolute_import, division, print_function, unicode_literals

from django.db import models

from .collection import BaseCollection


class Entity(BaseCollection):
    """Postgres model for storing entities."""

    class Meta(BaseCollection.Meta):
        """Entity Meta options."""

        permissions = (
            ("view_entity", "Can view entity"),
            ("edit_entity", "Can edit entity"),
            ("share_entity", "Can share entity"),
            ("download_entity", "Can download files from entity"),
            ("add_entity", "Can add data objects to entity"),
            ("owner_entity", "Is owner of the entity"),
        )

    #: list of collections to which entity belongs
    collections = models.ManyToManyField('Collection')

    #: indicate whether `descriptor` is completed (set by user)
    descriptor_completed = models.BooleanField(default=False)
