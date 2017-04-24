"""Resolwe storage model."""
from __future__ import absolute_import, division, print_function, unicode_literals

from django.contrib.postgres.fields import JSONField
from django.db import models

from .base import BaseModel


class Storage(BaseModel):
    """Postgres model for storing storages."""

    #: corresponding data object
    data = models.ForeignKey('Data')

    #: actual JSON stored
    json = JSONField()


class LazyStorageJSON(object):
    """Lazy load `json` attribute of `Storage` object."""

    def __init__(self, **kwargs):
        """Initialize private attributes."""
        self._kwargs = kwargs
        self._json = None

    def _get_storage(self):
        """Load `json` field from `Storage` object."""
        if self._json is None:
            self._json = Storage.objects.get(**self._kwargs).json

    def __getitem__(self, key):
        """Access by key."""
        self._get_storage()
        return self._json[key]

    def __repr__(self):
        """Format the object representation."""
        self._get_storage()
        return self._json.__repr__()
