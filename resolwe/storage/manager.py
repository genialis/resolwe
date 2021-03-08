"""Storage manager."""
import logging
from typing import Optional

from django.db import transaction
from django.utils.timezone import now

from resolwe.storage.connectors import connectors
from resolwe.storage.models import AccessLog, FileStorage, StorageLocation
from resolwe.utils import BraceMessage as __

logger = logging.getLogger(__name__)


class Manager:
    """Storage manager."""

    def _lock_file_storage(self, file_storage_id: int) -> Optional[FileStorage]:
        """Lock file storage for processing and return it.

        :returns: the file storage object or None if locking fails.
        """
        return (
            FileStorage.objects.filter(id=file_storage_id)
            .select_for_update(skip_locked=True)
            .first()
        )

    def delete_single_location(self, file_storage: FileStorage, connector_name: str):
        """Delete given storage location."""
        delete_location = file_storage.storage_locations.filter(
            connector_name=connector_name
        ).first()
        # Some other storage manager might have already processed this storage
        # location before we did.
        if not delete_location:
            return
        logger.info(__("Deleting {} ({}).", file_storage, connector_name))
        delete_location.delete()

    def process_delete(self):
        """Delete storage locations."""
        for connector_name in connectors:
            logger.info(__("Deleting locations from {}.", connector_name))
            for file_storage in StorageLocation.objects.to_delete(
                connector_name
            ).iterator():
                with transaction.atomic():
                    file_storage = self._lock_file_storage(file_storage.id)
                    if file_storage is None:
                        continue
                    try:
                        self.delete_single_location(file_storage, connector_name)
                    except Exception:
                        logger.exception(
                            "Error deleting data from StorageLocation instance",
                        )

    def copy_single_location(self, file_storage: FileStorage, connector_name: str):
        """Copy given location to a given connector."""

        logger.info(__("Copying {} to {}.", file_storage, connector_name))
        storage_location = file_storage.default_storage_location
        assert storage_location is not None
        from_connector = connectors[storage_location.connector_name]

        access_log = None
        try:
            new_storage_location = StorageLocation.all_objects.get_or_create(
                file_storage=file_storage,
                url=storage_location.url,
                connector_name=connector_name,
                defaults={"status": StorageLocation.STATUS_UPLOADING},
            )[0]
            # Some other storage manager might have already processed this storage
            # location before we did.
            if new_storage_location.status == StorageLocation.STATUS_DONE:
                return

            access_log = AccessLog.objects.create(
                storage_location=storage_location,
                reason="Manager data transfer",
            )
            storage_location.transfer_data(new_storage_location)
            new_storage_location.status = StorageLocation.STATUS_DONE
            new_storage_location.save()
        except Exception:
            logger.exception(
                "Error transfering data",
                extra={
                    "file_storage_id": file_storage.pk,
                    "from_connector": from_connector.name,
                    "to_connector": connector_name,
                },
            )
        finally:
            if access_log is not None:
                access_log.finished = now()
                access_log.save()

    def process_copy(self):
        """Copy location to all applicable connectors."""
        for connector_name in connectors:
            logger.info(__("Copying locations to {}", connector_name))
            for file_storage in StorageLocation.objects.to_copy(
                connector_name
            ).iterator():
                with transaction.atomic():
                    file_storage = self._lock_file_storage(file_storage.id)
                    if file_storage is None:
                        continue
                    self.copy_single_location(file_storage, connector_name)

    def process(self):
        """Process all FileStorage objects."""
        logger.info("Starting storage manager copy run.")
        self.process_copy()
        logger.info("Starting storage manager delete run.")
        self.process_delete()
        logger.info("Storage manager run completed.")
