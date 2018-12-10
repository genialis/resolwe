"""Resolwe storage model."""
from django.contrib.postgres.fields import JSONField
from django.db import models

from .base import BaseModel
from .functions import JsonGetPath
from .utils import json_path_components


class StorageManager(models.Manager):
    """Manager for Storage objects."""

    def with_json_path(self, path, field=None):
        """Annotate Storage objects with a specific JSON path.

        :param path: Path to get inside the stored object, which can be
            either a list of path components or a comma-separated
            string
        :param field: Optional output field name
        """
        if field is None:
            field = '_'.join(['json'] + json_path_components(path))

        kwargs = {field: JsonGetPath('json', path)}
        return self.defer('json').annotate(**kwargs)

    def get_json_path(self, path):
        """Return only a specific JSON path of Storage objects.

        :param path: Path to get inside the stored object, which can be
            either a list of path components or a comma-separated
            string
        """
        return self.with_json_path(path, field='result').values_list('result', flat=True)


class Storage(BaseModel):
    """Postgres model for storing storages."""

    #: corresponding data objects
    data = models.ManyToManyField('Data', related_name='storages')

    #: actual JSON stored
    json = JSONField()

    #: storage manager
    objects = StorageManager()


class LazyStorageJSON:
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
