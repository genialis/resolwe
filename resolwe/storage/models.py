"""Resolwe storage model."""
import logging
import os
from pathlib import PurePath
from typing import TYPE_CHECKING, Iterable, List, Optional, Union

from django.db import models

from resolwe.storage.connectors import DEFAULT_CONNECTOR_PRIORITY, connectors
from resolwe.storage.connectors.transfer import Transfer
from resolwe.storage.connectors.utils import paralelize

if TYPE_CHECKING:
    from resolwe.storage.connectors.baseconnector import BaseStorageConnector

logger = logging.getLogger(__name__)


class FileStorage(models.Model):
    """Proxy between Data object and StorageLocation objects."""

    #: creation date and time
    created = models.DateTimeField(auto_now_add=True)

    def has_storage_location(self, connector_name: str) -> bool:
        """Return if StorageLocation object with given connector name exist.

        Only return StorageLocation objects with status set to OK.
        """
        return self.storage_locations.filter(
            connector_name=connector_name, status=StorageLocation.STATUS_DONE
        ).exists()

    @property
    def default_storage_location(self) -> Optional["StorageLocation"]:
        """Return default storage location for this instance.

        This is a storage location using connector with the lowest priority.
        When multiple storage locations have same priority one of them is
        returned.

        Usually only StorageLocation objects with status OK are considered.
        When no such objects exist one of remaining objects is returned (this
        should only happen when data object is still processing). When no
        StorageLocation objects exist None is returned.

        When StorageLocation has ceonnector that is not defined in settings the
        default priority of 100 is applied to it.

        :returns: instance of StorageLocation class or None.
        :rtype: Optional[StorageLocation]
        """
        whens = [
            models.When(
                connector_name=connector_name, then=connectors[connector_name].priority,
            )
            for connector_name in connectors
        ]
        q_set_all = self.storage_locations.annotate(
            priority=models.Case(
                *whens,
                default=DEFAULT_CONNECTOR_PRIORITY,
                output_field=models.IntegerField(),
            )
        ).order_by("priority")
        q_set_done = q_set_all.filter(status=StorageLocation.STATUS_DONE)
        return q_set_done.first() or q_set_all.first()

    def get_path(self, prefix: str = None, filename: str = None) -> str:
        """Return path of the default storage location."""
        return self.default_storage_location.get_path(prefix, filename)

    @property
    def subpath(self) -> str:
        """Return URL of the default storage location."""
        return self.default_storage_location.url

    @property
    def files(self) -> models.QuerySet:
        """Get referenced path objects in default storage location."""
        return self.default_storage_location.files

    @property
    def urls(self) -> List[str]:
        """Get a list of URLs of objects stored in default storage location."""
        return self.default_storage_location.urls


class LocationsDoneManager(models.Manager):
    """Return only StorageLocation objects with status set to OK."""

    def get_queryset(self) -> models.QuerySet:
        """Override default queryset."""
        return super().get_queryset().filter(status="OK")


class AllLocationsManager(models.Manager):
    """Return all StorageLocation objects."""

    def unreferenced_locations(self) -> models.QuerySet:
        """Get a queryset containing all unreferenced locations.

        The location must also be unlocked (has no accesslogs with finished
        date set to null) to be included in the list.
        """
        return (
            StorageLocation.all_objects.filter(file_storage__data__isnull=True)
            .annotate(
                logs_num=models.Count(
                    "access_logs", filter=models.Q(access_logs__finished__isnull=True)
                )
            )
            .filter(logs_num=0)
        )


class StorageLocation(models.Model):
    """Stores path to where the actual data is stored."""

    # Access all objects through all_objects storage manager.
    # This is also _default_object_manager since it is defined first.
    all_objects = AllLocationsManager()
    # By default iterate only through locations with status set to OK.
    objects = LocationsDoneManager()

    #: data upload is preparing
    STATUS_PREPARING = "PR"
    #: data is uploading
    STATUS_UPLOADING = "UP"
    #: data object is ready
    STATUS_DONE = "OK"
    #: data object is deleting
    STATUS_DELETING = "DE"
    STATUS_CHOICES = (
        (STATUS_PREPARING, "Preparing"),
        (STATUS_UPLOADING, "Uploading"),
        (STATUS_DONE, "Done"),
        (STATUS_DELETING, "Deleting"),
    )

    #: file storage object
    file_storage = models.ForeignKey(
        "FileStorage", on_delete=models.PROTECT, related_name="storage_locations"
    )

    #: url to where data is stored
    url = models.CharField(max_length=60, null=False, blank=False)

    #: name of the connector that manages this location
    connector_name = models.CharField(max_length=30, null=False, blank=False)

    #: date and time of the last update
    last_update = models.DateTimeField(auto_now=True)

    status = models.CharField(
        max_length=2, choices=STATUS_CHOICES, default=STATUS_PREPARING
    )
    """
    :class:`StorageLocation` status

    It can be one of the following:

    - :attr:`STATUS_UPLOADING`
    - :attr:`STATUS_PREPARING`
    - :attr:`STATUS_DONE`
    - :attr:`STATUS_DELETING`
    """

    class Meta:
        """Add unique constaint."""

        unique_together = ("url", "connector_name")

    def __str__(self) -> str:
        """Stringify StorageLocation object."""
        return "StorageLocation(pk={}, connector={}, url={})".format(
            self.pk, self.connector_name, self.url
        )

    def get_path(
        self,
        prefix: Optional[Union[str, PurePath]] = None,
        filename: Optional[Union[str, PurePath]] = None,
    ) -> str:
        """Get the path for this storage location."""
        prefix = prefix or self.connector.base_path
        path = PurePath(prefix) / self.url

        if filename:
            path = path / filename

        return os.fspath(path)

    @property
    def connector(self) -> "BaseStorageConnector":
        """Get the connector for this storage location."""
        return connectors[self.connector_name]

    @property
    def data(self):
        """Return the FileStorage data object."""
        return self.file_storage.data

    @property
    def subpath(self) -> str:
        """Return URL of the default storage location object.

        Exists for backwards compatibility.
        """
        return self.url

    @property
    def locked(self) -> bool:
        """Is this storage location locked."""
        return self.access_logs.filter(finished__isnull=True).exists()

    @property
    def urls(self) -> List[str]:
        """Get a list of URLs of stored files and directories."""
        return [file_.path for file_ in self.files.all()]

    def delete_data(self):
        """Delete all data for this storage location."""
        self.connector.delete(self.url, self.urls)

    def delete(self, *args, **kwargs):
        """Delete StorageLocation object.

        Set status to DELETING, remove data and finally location object.
        """
        self.status = StorageLocation.STATUS_DELETING
        self.save()
        self.delete_data()
        super().delete(*args, **kwargs)
        # Remove referenced paths if they are not connected to another storage
        # location.
        ReferencedPath.objects.filter(storage_locations__isnull=True).delete()

    def transfer_data(self, destination: "StorageLocation"):
        """Transfer data to another storage location."""
        t = Transfer(self.connector, destination.connector)
        transfered_files = t.transfer_objects(self.url, list(self.files.values()))
        if transfered_files is None:
            destination.files.add(*self.files.all())
        else:
            referenced_paths = [ReferencedPath(**e) for e in transfered_files]
            ReferencedPath.objects.bulk_create(referenced_paths)
            destination.files.add(*referenced_paths)

    def verify_data(self) -> bool:
        """Verify data stored in this location.

        Verify hashes and sizes of all files. This operation could be slow
        and cause network traffic, use with care.

        :returns: True if data is OK, False otherwise. In case of failure
        additional information about error is logged.
        """

        def worker(referenced_paths: Iterable[ReferencedPath]) -> bool:
            """Check given referenced paths."""
            hash_types = ["md5", "crc32c"]
            connector = self.connector.duplicate()
            for referenced_path in referenced_paths:
                url = self.get_path(filename=referenced_path.path, prefix=PurePath(""))

                # Connector check failed: log error and return False.
                if not connector.check_url(url):
                    logger.error(
                        "Connector {} check for URL {} failed".format(
                            connector.name, url
                        )
                    )
                    return False

                connector_hashes = connector.get_hashes(url, hash_types)
                # Could not retrieve hashes: log error and return False.
                if connector_hashes is None:
                    logger.error(
                        "Connector {} could not retrieve hashes for {}".format(
                            connector.name, url
                        )
                    )
                    return False

                for hash_name, hash_value in connector_hashes.items():
                    referenced_path_hash = getattr(referenced_path, hash_name)
                    if not referenced_path_hash:
                        logger.warning(
                            "ReferencedPath with id {} has no {} hash".format(
                                referenced_path.id, hash_name
                            )
                        )
                        continue

                    # Hashes differ: log error and return False.
                    if referenced_path_hash != hash_value:
                        logger.error(
                            "ReferencedPath with id {} has wrong {} hash: {} instead of {}".format(
                                referenced_path.id,
                                hash_name,
                                referenced_path_hash,
                                hash_value,
                            )
                        )
                        return False
            return True

        max_threads = max(1, min((self.files.count() // 5), 20))
        futures = paralelize(
            self.files.all().exclude(path__endswith="/"),
            worker,
            max_threads=max_threads,
        )
        return all(future.result() for future in futures)


class AccessLog(models.Model):
    """Class that holds access log to storage locations."""

    #: when accessing storage location data started
    started = models.DateTimeField(auto_now_add=True)

    #: when accessing storage location data finished
    finished = models.DateTimeField(null=True, blank=True)

    #: storage location object
    storage_location = models.ForeignKey(
        "StorageLocation", on_delete=models.CASCADE, related_name="access_logs"
    )

    #: human readable reason for access
    reason = models.CharField(max_length=120, null=False, blank=False)


class ReferencedPath(models.Model):
    """Stores reference to a single object (file or directory).

    All references are relative according to the subpath stored in the
    FileStorage object. Directories are stored with '/' at the end and are not
    supported on all conectors.
    """

    #: refers to a file or directory using '/' as separator
    path = models.TextField(db_index=True)

    #: size of the file (-1 undefined)
    size = models.BigIntegerField(default=-1)

    #: StorageLocation objects
    storage_locations = models.ManyToManyField("StorageLocation", related_name="files")

    #: MD5 hexadecimal hash
    md5 = models.CharField(max_length=32, null=False, blank=False)

    #: CRC32C hexadecimal hash
    crc32c = models.CharField(max_length=8, null=False, blank=False)

    #: CRC32C hexadecimal hash
    awss3etag = models.CharField(max_length=50, null=False, blank=False)

    @property
    def file_storage(self) -> Optional[FileStorage]:
        """Get the file storage object that holds this object.

        When no such FileStorage exists None is returned.
        """
        storage_location = self.storage_locations.first()
        if storage_location is None:
            return None

        return storage_location.file_storage
