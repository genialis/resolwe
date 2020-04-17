"""Storage manager."""
import copy
import logging
from datetime import timedelta
from typing import List, Optional

from django.db import transaction
from django.utils.timezone import now

from resolwe.storage.connectors import Transfer, connectors
from resolwe.storage.models import AccessLog, FileStorage, StorageLocation
from resolwe.storage.settings import STORAGE_CONNECTORS

logger = logging.getLogger(__name__)


class DecisionMaker:
    """Decide which data should be copied to another location or deleted."""

    def __init__(self, file_storage: FileStorage):
        """Initialization."""
        self.file_storage = file_storage
        data_slug = None
        process_type = None
        data = file_storage.data.first()
        if data is not None:
            process_type = data.process.type
            data_slug = data.slug
        # Later element override previous ones.
        self.override_priorities = [
            ("process_type", process_type),
            ("data_slug", data_slug),
        ]

    def _get_rules(self, connector_name: str, _type: str) -> dict:
        """Get rules for the given connector name and _type.

        Known rule types are 'copy' and 'delete'.
        """
        location_settings = copy.deepcopy(STORAGE_CONNECTORS.get(connector_name, {}))
        rules: dict = location_settings.get("config", {}).get(_type, {})
        for override, name in self.override_priorities:
            override_rules = rules.pop(override, {})
            if name in override_rules:
                rules.update(override_rules[name])
        return rules

    def copy(self) -> List[str]:
        """Get a list of connector names where data must be copied to."""
        return [
            connector_name
            for connector_name in STORAGE_CONNECTORS
            if self._should_copy_to(connector_name)
        ]

    def _should_copy_to(self, connector_name: str) -> bool:
        """Get if data should be copied to the given location.

        :param connector_name: name of the storage connector.
        :type connector_name: str

        :return: True when data should be copied to the location managed by
            connector connector_name.
        """
        copy_rules = self._get_rules(connector_name, "copy")

        rule_results = {}

        rule_results["has_rules"] = len(copy_rules) > 0

        rule_results["copy_not_present"] = not self.file_storage.has_storage_location(
            connector_name
        )

        if "delay" in copy_rules:
            move_delay = timedelta(days=copy_rules["delay"])
            current_delay = now() - self.file_storage.created
            rule_results["delay"] = current_delay >= move_delay
        return all(rule_results.values())

    def delete(self) -> Optional[StorageLocation]:
        """Get StorageLocation object that can be deleted.

        When multiple StorageLocation objects can be deleted the one with
        lowest priority is returned. When no StorageLocation objects can be
        deleted None is returned.

        :return: storage location that can be safely removed or None.
        :rtype: Optional[StorageLocation]
        """
        can_delete = [
            storage_location
            for storage_location in self.file_storage.storage_locations.all()
            if self._should_delete(storage_location) and not storage_location.locked
        ]
        if can_delete:
            return sorted(
                can_delete, key=lambda e: connectors[e.connector_name].priority
            )[-1]

    def _should_delete(self, storage_location: StorageLocation) -> bool:
        """Get if the given StorageLocation should be deleted.

        :return: True if storage_location should be deleted, False otherwise.
        :rtype: bool
        """
        rules = self._get_rules(storage_location.connector_name, "delete")
        rule_results = {}
        rule_results["has_rules"] = len(rules) > 0
        rule_results["not_locked"] = not storage_location.locked

        # Never remove the last storage location.
        min_other_copies = rules.get("min_other_copies", 1)
        current_copies = self.file_storage.storage_locations.count()
        rule_results["has_copies"] = min_other_copies < current_copies

        # Never remove the location with "highest" priority that is
        # default_storage_location on FileStorage object.
        default_location = storage_location.file_storage.default_storage_location
        rule_results["highest_priority"] = storage_location != default_location

        if "delay" in rules:
            delete_delay = timedelta(days=rules["delay"])
            current_delay = now() - storage_location.last_update
            rule_results["delay"] = current_delay >= delete_delay

        return all(rule_results.values())


class Manager:
    """Storage manager."""

    def _process_file_storage(self, file_storage: FileStorage):
        """Process single FileStorage object."""
        logger.debug("Processing FileStorage object with id {}".format(file_storage.id))
        decide = DecisionMaker(file_storage)
        storage_location = file_storage.default_storage_location
        from_connector = connectors[storage_location.connector_name]
        copy_locations = decide.copy()

        # Copy data to new StorageLocations.
        for connector_name in copy_locations:
            to_connector = connectors[connector_name]
            logger.debug("Copying to location {}".format(connector_name))
            access_log = None
            try:
                access_log = AccessLog.objects.create(
                    storage_location=storage_location, reason="Manager data transfer"
                )
                transfer = Transfer(from_connector, to_connector)
                new_storage_location = StorageLocation.objects.create(
                    file_storage=file_storage,
                    url=storage_location.url,
                    connector_name=connector_name,
                    status=StorageLocation.STATUS_UPLOADING,
                )
                transfer.transfer_rec(storage_location.url)
                new_storage_location.status = StorageLocation.STATUS_DONE
                new_storage_location.save()
            except Exception:
                logger.exception(
                    "Error transfering data",
                    extra={
                        "file_storage_id": file_storage.id,
                        "from_connector": from_connector.name,
                        "to_connector": to_connector.name,
                    },
                )
                new_storage_location.delete()
            finally:
                if access_log is not None:
                    access_log.finished = now()
                    access_log.save()

        delete_location = decide.delete()
        while delete_location:
            delete_connector = delete_location.connector
            logger.debug(
                "Delete from location {}".format(delete_location.connector_name)
            )
            # This is a problem, since delete is NOT atomic and can even be
            # long running on cloud (as in couple of days).
            # Delete the StorageLocation first (otherwise we have inconsistent
            # state) and let the healthcare operation remove stale files.
            # TODO: healthcare
            filenames = [
                delete_location.get_path(e) for e in delete_location.files.all()
            ]
            delete_location.delete()
            delete_connector.delete(filenames)
            delete_location = decide.delete()

    def process(self):
        """Process all FileStorage objects."""
        logger.debug("Starting storage manager run")
        for file_storage in FileStorage.objects.all():
            with transaction.atomic():
                q_set = FileStorage.objects.filter(
                    id=file_storage.id
                ).select_for_update(skip_locked=True)
                # The FileStorage object is locked or deleted, skip processing.
                if not q_set.exists():
                    continue
                file_storage = q_set.first()
                self._process_file_storage(file_storage)
        logger.debug("Finished storage manager run")
