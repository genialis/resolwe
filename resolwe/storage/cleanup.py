"""Storage cleanup."""
import logging
from typing import Optional

from django.db import transaction

from resolwe.storage.models import FileStorage, StorageLocation
from resolwe.utils import BraceMessage as __

logger = logging.getLogger(__name__)


class Cleaner:
    """Remove unreferenced data.

    Remove data from StorageLocation objects that are no longer referenced
    by any Data object.
    """

    def _cleanup(self, storage_location: StorageLocation):
        """Delete data from StorageLocation object."""
        # Make sure this will get writen to the database.
        # Problem is we are inside transaction so what happens if the
        # connection to the database fails or database crashed or...
        connector = storage_location.connector
        if connector is None:
            logger.error(
                __(
                    "Unable to cleanup StorageLocation {}: connector not found.",
                    storage_location.id,
                )
            )
            return
        try:
            # Delete the storage location(files will be also removed).
            logger.info(__("Deleting StorageLocation {}.", storage_location.pk))
            storage_location.delete()

        except Exception:
            logger.exception(
                __(
                    "Exception deleting StorageLocation {}.",
                    storage_location.id,
                )
            )

    def _process_file_storage(self, file_storage: FileStorage):
        """Delete all data from FileStorage object."""
        # Do not remove locked StorageLocation.
        logger.info(__("Processing FileStorage {}.", file_storage.pk))
        for storage_location in file_storage.storage_locations.filter(
            status=StorageLocation.STATUS_DELETING
        ):
            self._cleanup(storage_location)

        if file_storage.storage_locations.count() == 0:
            logger.info(__("Deleting FileStorage {}.", file_storage.pk))
            file_storage.delete()

    def process(self, file_storage_id: Optional[int] = None):
        """Process objects to clean.

        When file_storage is not None process only that object.
        """

        qset = FileStorage.objects.all()
        if file_storage_id is not None:
            logger.info(__("Starting processing for FileStorage {}.", file_storage_id))
            qset = qset.filter(pk=file_storage_id)
        else:
            logger.info("Starting processing")
        for file_storage in qset.filter(data__isnull=True).iterator():
            # Set applicable storage locations to deleting.
            StorageLocation.all_objects.unreferenced_locations().filter(
                file_storage=file_storage
            ).update(status=StorageLocation.STATUS_DELETING)
            with transaction.atomic():
                q_set = FileStorage.objects.filter(
                    id=file_storage.id
                ).select_for_update(skip_locked=True)
                # The FileStorage object is locked or deleted, skip processing.
                if not q_set.exists():
                    logger.debug(
                        "Skipping locked or deleted FileStorage {}.", file_storage.id
                    )
                    continue
                self._process_file_storage(q_set.first())

        logger.info("Finished processing")
