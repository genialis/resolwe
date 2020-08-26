"""Storage manager."""
import copy
import logging
from datetime import timedelta
from typing import List, Optional

from django.db import transaction
from django.utils.timezone import now

from resolwe.flow.models import Data
from resolwe.storage.connectors import connectors
from resolwe.storage.models import AccessLog, FileStorage, StorageLocation
from resolwe.storage.settings import STORAGE_CONNECTORS

logger = logging.getLogger(__name__)


class DecisionMaker:
    """Decide which data should be copied to another location or deleted."""

    def __init__(self, file_storage: FileStorage):
        """Initialization."""
        self.file_storage = file_storage
        self.process_type = ""
        self.data_slug = ""
        data = file_storage.data.first()
        if data is not None:
            self.process_type = data.process.type
            self.data_slug = data.slug

    def _get_data_slug_overrides(self, override_rules: dict) -> dict:
        """Get a matching override rule for data slug."""
        return override_rules.get(self.data_slug, dict())

    def _get_process_type_overrides(self, override_rules: dict) -> dict:
        """Get a matching override rule for process type."""
        rule = dict()
        matching_keys = []
        for key in override_rules.keys():
            modified_key = key if key.endswith(":") else key + ":"
            if self.process_type.startswith(modified_key):
                matching_keys.append(key)
        matching_keys.sort(key=len)
        for matching_key in matching_keys:
            rule.update(override_rules[matching_key])
        return rule

    def _get_rules(self, connector_name: str, rule_type: str) -> dict:
        """Get rules for the given connector name and _type.

        Known rule types are 'copy' and 'delete'.
        """
        location_settings = copy.deepcopy(STORAGE_CONNECTORS.get(connector_name, {}))
        rules: dict = location_settings.get("config", {}).get(rule_type, {})
        override_rules = []
        override_rules.append(
            self._get_process_type_overrides(rules.pop("process_type", {}))
        )
        override_rules.append(self._get_data_slug_overrides(rules.pop("data_slug", {})))
        for override_rule in override_rules:
            rules.update(override_rule)
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

        rule_results["data_object_ok"] = self.file_storage.data.filter(
            status=Data.STATUS_DONE
        ).exists()

        # Do not copy objects that have no StorageLocation.
        default_location = self.file_storage.default_storage_location
        rule_results["location_present"] = (
            default_location is not None
            and default_location.status == StorageLocation.STATUS_DONE
        )

        rule_results["copy_not_present"] = not self.file_storage.has_storage_location(
            connector_name
        )

        if "delay" in copy_rules:
            # Do not copy if delay is negative.
            rule_results["delay_non_negative"] = copy_rules["delay"] >= 0

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
            if self._should_delete(storage_location)
        ]
        if can_delete:
            return sorted(
                can_delete, key=lambda e: connectors[e.connector_name].priority
            )[-1]
        return None

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
        current_copies = self.file_storage.storage_locations.filter(
            status=StorageLocation.STATUS_DONE
        ).count()
        rule_results["has_copies"] = min_other_copies < current_copies

        # Never remove the location with "highest" priority that is
        # default_storage_location on FileStorage object.
        default_location = storage_location.file_storage.default_storage_location
        rule_results["highest_priority"] = storage_location != default_location

        # Do not delete location with status STATUS_DELETING. Probably error
        # occured while removing it and we do not want to fall into endless
        # loop of trying to remove same location over and over again.
        # The Cleanup Manager will remove it eventually.
        rule_results["status_ok"] = (
            storage_location.status == StorageLocation.STATUS_DONE
        )

        if "delay" in rules:
            # Do not remove if delay is negative.
            rule_results["delay_non_negative"] = rules["delay"] >= 0

            delete_delay = timedelta(days=rules["delay"])
            current_delay = now() - storage_location.last_update
            rule_results["delay"] = current_delay >= delete_delay

            last_access_log = storage_location.access_logs.order_by("started").last()
            if last_access_log is not None:
                access_log_delay = now() - last_access_log.started
                rule_results["access_log_delay"] = access_log_delay >= delete_delay

        return all(rule_results.values())


class Manager:
    """Storage manager."""

    def _process_file_storage(self, file_storage: FileStorage):
        """Process single FileStorage object."""
        logger.debug("Processing FileStorage object with id {}".format(file_storage.id))
        decide = DecisionMaker(file_storage)
        storage_location = file_storage.default_storage_location
        assert storage_location is not None

        from_connector = connectors[storage_location.connector_name]
        copy_locations = decide.copy()

        # Copy data to new StorageLocations.
        for connector_name in copy_locations:
            logger.debug("Copying to location {}".format(connector_name))
            access_log = None
            try:
                access_log = AccessLog.objects.create(
                    storage_location=storage_location, reason="Manager data transfer"
                )
                new_storage_location, _ = StorageLocation.all_objects.get_or_create(
                    file_storage=file_storage,
                    url=storage_location.url,
                    connector_name=connector_name,
                    status=StorageLocation.STATUS_UPLOADING,
                )
                storage_location.transfer_data(new_storage_location)
                new_storage_location.status = StorageLocation.STATUS_DONE
                new_storage_location.save()
            except Exception:
                logger.exception(
                    "Error transfering data",
                    extra={
                        "file_storage_id": file_storage.id,
                        "from_connector": from_connector.name,
                        "to_connector": connector_name,
                    },
                )
            finally:
                if access_log is not None:
                    access_log.finished = now()
                    access_log.save()

        processed_locations = set()
        delete_location = decide.delete()
        while delete_location:
            if delete_location.pk in processed_locations:
                logger.error(
                    "Location {} already processed, aborting delete".format(
                        storage_location
                    )
                )
                break

            logger.debug(
                "Deleting data from location {}".format(delete_location.connector_name)
            )
            processed_locations.add(delete_location.pk)
            # This is a problem, since delete is NOT atomic and can even be
            # long running on cloud (as in couple of days).
            # Delete the StorageLocation first (otherwise we have inconsistent
            # state) and let the healthcare operation remove stale files.
            # TODO: healthcare
            try:
                # Update status to DELETING so DecisionManager will not pick
                # it again in case of error while deleting data.
                delete_location.delete()
            except Exception:
                logger.exception(
                    "Error deleting data from StorageLocation instance",
                    extra={"storage_location_id": delete_location.id},
                )
            delete_location = decide.delete()

    def process(self):
        """Process all FileStorage objects."""
        logger.debug("Starting storage manager run")
        for file_storage in FileStorage.objects.iterator():
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
