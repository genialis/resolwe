"""Basic listener commands."""
import logging
from typing import TYPE_CHECKING, Any, Dict, List, Union

from django.conf import settings
from django.db import transaction
from django.db.models import Sum
from django.db.models.functions import Coalesce
from django.urls import reverse
from django.utils.timezone import now

from resolwe.flow.executors.socket_utils import Message, Response
from resolwe.flow.managers.protocol import ExecutorProtocol
from resolwe.flow.models import Data, DataDependency, Process
from resolwe.flow.models.utils import referenced_files, validate_data_object
from resolwe.flow.utils import dict_dot, iterate_fields
from resolwe.storage.connectors import connectors
from resolwe.storage.connectors.hasher import StreamHasher
from resolwe.storage.models import ReferencedPath, StorageLocation
from resolwe.utils import BraceMessage as __

from .plugin import ListenerPlugin

if TYPE_CHECKING:
    from resolwe.flow.managers.listener.listener import Processor

logger = logging.getLogger(__name__)


class BasicCommands(ListenerPlugin):
    """Basic listener handlers."""

    def handle_run(
        self, message: Message[dict], manager: "Processor"
    ) -> Response[List[int]]:
        """Handle spawning new data object.

        The response is the id of the created object.
        """
        export_files_mapper = message.message_data["export_files_mapper"]
        manager.data.refresh_from_db()

        try:
            data = message.message_data["data"]
            logger.debug(__("Spawning new data object from dict: {}", data))

            data["contributor"] = manager.data.contributor
            data["process"] = Process.objects.filter(slug=data["process"]).latest()
            data["tags"] = manager.data.tags
            data["collection"] = manager.data.collection
            data["subprocess_parent"] = manager.data

            with transaction.atomic():
                for field_schema, fields in iterate_fields(
                    data.get("input", {}), data["process"].input_schema
                ):
                    type_ = field_schema["type"]
                    name = field_schema["name"]
                    value = fields[name]

                    if type_ == "basic:file:":
                        fields[name] = self.hydrate_spawned_files(
                            export_files_mapper, value
                        )
                    elif type_ == "list:basic:file:":
                        fields[name] = [
                            self.hydrate_spawned_files(export_files_mapper, fn)
                            for fn in value
                        ]
                created_object = Data.objects.create(**data)
        except Exception:
            manager._log_exception(
                f"Error while preparing spawned Data objects for process '{manager.data.process.slug}'"
            )
            return message.respond_error([])
        else:
            return message.respond_ok(created_object.id)

    def handle_update_rc(
        self, message: Message[dict], manager: "Processor"
    ) -> Response[str]:
        """Update return code.

        When return code is chaned from zero to non-zero value the Data object
        status is set to ERROR.

        :args: data contains at least key rc and possibly key error.
        """
        return_code = message.message_data["rc"]
        if return_code not in (0, manager.return_code):
            changes: dict = {"process_rc": manager.return_code}
            if manager.return_code == 0:
                error_details = message.message_data.get("error", "Unspecified error")
                changes["status"] = Data.STATUS_ERROR
                changes["process_error"] = manager.data.process_error
                changes["process_error"].append(error_details)
            manager.return_code = return_code
            manager._update_data(changes)
        return message.respond_ok("OK")

    def handle_get_output_files_dirs(
        self, message: Message[str], manager: "Processor"
    ) -> Response[dict]:
        """Get the output for file and dir fields.

        The sent dictionary has field names as its keys and tuple filed_type,
        field_value for its values.
        """

        def is_file_or_dir(field_type: str) -> bool:
            """Is file or directory."""
            return "basic:file" in field_type or "basic:dir" in field_type

        output = dict()
        for field_schema, fields in iterate_fields(
            manager.data.output, manager.data.process.output_schema
        ):
            if is_file_or_dir(field_schema["type"]):
                name = field_schema["name"]
                output[name] = (field_schema["type"], fields[name])

        return message.respond_ok(output)

    def handle_finish(
        self, message: Message[Dict], manager: "Processor"
    ) -> Response[str]:
        """Handle an incoming ``Data`` finished processing request."""
        process_rc = int(message.message_data.get("rc", 1))
        changeset = {
            "process_progress": 100,
            "finished": now(),
        }
        if process_rc != 0:
            changeset["process_rc"] = process_rc
            if manager.data.status != Data.STATUS_ERROR:
                changeset["status"] = Data.STATUS_ERROR
                manager.data.process_error.append(
                    message.message_data.get("error", "Process return code is not 0")
                )
                changeset["process_error"] = manager.data.process_error
        elif manager.data.status != Data.STATUS_ERROR:
            changeset["status"] = Data.STATUS_DONE

        changeset["size"] = manager.data.location.files.aggregate(
            size=Coalesce(Sum("size"), 0)
        ).get("size")
        manager._update_data(changeset)

        local_location = manager.data.location.default_storage_location
        local_location.status = StorageLocation.STATUS_DONE
        local_location.save()

        # Only validate objects with DONE status. Validating objects in ERROR
        # status will only cause unnecessary errors to be displayed.
        if manager.data.status == Data.STATUS_DONE:
            validate_data_object(manager.data)
        return message.respond_ok("OK")

    def handle_get_referenced_files(
        self, message: Message, manager: "Processor"
    ) -> Response[List]:
        """Get a list of files referenced by the data object.

        The list also depends on the Data object itself, more specifically on
        its output field. So the method is not idempotent.
        """
        return message.respond_ok(referenced_files(manager.data))

    def handle_missing_data_locations(
        self, message: Message, manager: "Processor"
    ) -> Response[Union[str, Dict[str, Dict[str, Any]]]]:
        """Handle an incoming request to get missing data locations."""
        storage_name = "data"
        filesystem_connector = None
        filesystem_connectors = [
            connector
            for connector in connectors.for_storage(storage_name)
            if connector.mountable
        ]

        if filesystem_connectors:
            filesystem_connector = filesystem_connectors[0]

        missing_data = dict()
        dependencies = (
            Data.objects.filter(
                children_dependency__child=manager.data,
                children_dependency__kind=DataDependency.KIND_IO,
            )
            .exclude(location__isnull=True)
            .exclude(pk=manager.data.id)
            .distinct()
        )

        for parent in dependencies:
            file_storage = parent.location
            # Is location available on some local connector?
            if any(
                file_storage.has_storage_location(filesystem_connector.name)
                for filesystem_connector in filesystem_connectors
            ):
                continue

            from_location = file_storage.default_storage_location
            if from_location is None:
                manager._log_exception(
                    "No storage location exists (handle_get_missing_data_locations).",
                    extra={"file_storage_id": file_storage.id},
                )
                return message.respond_error("No storage location exists")

            # When there exists at least one filesystem connector for the data
            # storage download inputs to the shared storage.
            missing_data_item = {
                "data_id": parent.pk,
                "from_connector": from_location.connector_name,
            }
            if filesystem_connector:
                to_location = StorageLocation.all_objects.get_or_create(
                    file_storage=file_storage,
                    url=from_location.url,
                    connector_name=filesystem_connector.name,
                )[0]
                missing_data_item["from_storage_location_id"] = from_location.id
                missing_data_item["to_storage_location_id"] = to_location.id
                missing_data_item["to_connector"] = filesystem_connector.name
            else:
                missing_data_item["files"] = list(
                    ReferencedPath.objects.filter(
                        storage_locations=from_location
                    ).values()
                )

            missing_data[from_location.url] = missing_data_item
            # Set last modified time so it does not get deleted.
            from_location.last_update = now()
            from_location.save()
        return message.respond_ok(missing_data)

    def handle_referenced_files(
        self, message: Message[List[dict]], manager: "Processor"
    ) -> Response[str]:
        """Store  a list of files and directories produced by the worker.

        The files are a list of dictionaries with keys 'path', 'size' and
        hashes.
        """
        update_fields = ["size"] + StreamHasher.KNOWN_HASH_TYPES
        storage_location = manager.data.location.default_storage_location
        referenced_files = message.message_data
        paths = {
            referenced_file["path"]: referenced_file
            for referenced_file in referenced_files
        }
        updated_paths = set()
        with transaction.atomic():
            # Bulk update existing paths.
            database_paths = ReferencedPath.objects.filter(
                storage_locations=storage_location, path__in=paths.keys()
            )
            for database_path in database_paths:
                updated_paths.add(database_path.path)
                for key, value in paths[database_path.path].items():
                    setattr(database_path, key, value)
            ReferencedPath.objects.bulk_update(database_paths, update_fields)
            logger.debug("Updated %s.", database_paths.values())

            # Bulk insert new paths.
            created_paths = [
                ReferencedPath(**referenced_file)
                for referenced_file in referenced_files
                if referenced_file["path"] not in updated_paths
            ]
            ReferencedPath.objects.bulk_create(created_paths)
            storage_location.files.add(*created_paths)
            logger.debug("Created %s.", [e.path for e in created_paths])

        return message.respond_ok("OK")

    def hydrate_spawned_files(
        self,
        exported_files_mapper: Dict[str, str],
        filename: str,
    ):
        """Pop the given file's map from the exported files mapping.

        :param exported_files_mapper: The dict of file mappings this
            process produced.
        :param filename: The filename to format and remove from the
            mapping.
        :return: The formatted mapping between the filename and
            temporary file path.
        :rtype: dict
        """
        if filename not in exported_files_mapper:
            raise KeyError(
                "Use 're-export' to prepare the file for spawned process: {}".format(
                    filename
                )
            )
        return {"file_temp": exported_files_mapper.pop(filename), "file": filename}

    def handle_update_status(
        self, message: Message[str], manager: "Processor"
    ) -> Response[str]:
        """Update data status."""
        new_status = manager._choose_worst_status(
            message.message_data, manager.data.status
        )
        if new_status != manager.data.status:
            manager._update_data({"status": new_status})
            if new_status == Data.STATUS_ERROR:
                logger.error(
                    __(
                        "Error occured while running process '{}' (handle_update).",
                        manager.data.process.slug,
                    ),
                    extra={
                        "data_id": manager.data.id,
                        "api_url": "{}{}".format(
                            getattr(settings, "RESOLWE_HOST_URL", ""),
                            reverse(
                                "resolwe-api:data-detail",
                                kwargs={"pk": manager.data.id},
                            ),
                        ),
                    },
                )
        return message.respond_ok(new_status)

    def handle_get_data_by_slug(
        self, message: Message[str], manager: "Processor"
    ) -> Response[int]:
        """Get data id by slug."""
        return message.respond_ok(Data.objects.get(slug=message.message_data).id)

    def handle_set_data_size(
        self, message: Message[int], manager: "Processor"
    ) -> Response[str]:
        """Set size of data object."""
        assert isinstance(message.message_data, int)
        manager._update_data({"size": message.message_data})
        return message.respond_ok("OK")

    def handle_update_output(
        self, message: Message[Dict[str, Any]], manager: "Processor"
    ) -> Response[str]:
        """Update data output."""
        for key, val in message.message_data.items():
            if key not in manager.storage_fields:
                dict_dot(manager.data.output, key, val)
            else:
                manager.save_storage(key, val)
        with transaction.atomic():
            manager._update_data({"output": manager.data.output})
        return message.respond_ok("OK")

    def handle_get_files_to_download(
        self, message: Message[int], manager: "Processor"
    ) -> Response[Union[str, List[dict]]]:
        """Get a list of files belonging to a given storage location object."""
        return message.respond_ok(
            list(
                ReferencedPath.objects.filter(
                    storage_locations=message.message_data
                ).values()
            )
        )

    def handle_download_started(
        self, message: Message[dict], manager: "Processor"
    ) -> Response[str]:
        """Handle an incoming request to start downloading data.

        We have to check if the download for given StorageLocation object has
        already started.
        """
        storage_location_id = message.message_data[ExecutorProtocol.STORAGE_LOCATION_ID]
        lock = message.message_data.get(ExecutorProtocol.DOWNLOAD_STARTED_LOCK, False)
        with transaction.atomic():
            query = StorageLocation.all_objects.select_for_update().filter(
                pk=storage_location_id
            )
            location_status = query.values_list("status", flat=True).get()
            return_status = {
                StorageLocation.STATUS_PREPARING: ExecutorProtocol.DOWNLOAD_STARTED,
                StorageLocation.STATUS_UPLOADING: ExecutorProtocol.DOWNLOAD_IN_PROGRESS,
                StorageLocation.STATUS_DONE: ExecutorProtocol.DOWNLOAD_FINISHED,
            }[location_status]

            if location_status == StorageLocation.STATUS_PREPARING and lock:
                query.update(status=StorageLocation.STATUS_UPLOADING)

        return message.respond_ok(return_status)

    def handle_download_aborted(
        self, message: Message[int], manager: "Processor"
    ) -> Response[str]:
        """Handle an incoming download aborted request.

        Since download is aborted it does not matter if the location does
        exist or not so we do an update with only one SQL query.
        """
        StorageLocation.all_objects.filter(pk=message.message_data).update(
            status=StorageLocation.STATUS_PREPARING
        )
        return message.respond_ok("OK")

    def handle_download_finished(
        self, message: Message[int], manager: "Processor"
    ) -> Response[str]:
        """Handle an incoming download finished request."""
        storage_location = StorageLocation.all_objects.get(pk=message.message_data)
        # Add files to the downloaded storage location.
        default_storage_location = (
            storage_location.file_storage.default_storage_location
        )
        with transaction.atomic():
            storage_location.files.add(*default_storage_location.files.all())
            storage_location.status = StorageLocation.STATUS_DONE
            storage_location.save()
        return message.respond_ok("OK")

    def handle_annotate(
        self, message: Message[dict], manager: "Processor"
    ) -> Response[str]:
        """Handle an incoming ``Data`` object annotate request."""

        if manager.data.entity is None:
            raise RuntimeError(
                f"No entity to annotate for process '{manager.data.process.slug}'"
            )

        for key, val in message.message_data.items():
            dict_dot(manager.data.entity.descriptor, key, val)

        manager.data.entity.save()
        return message.respond_ok("OK")

    def handle_process_log(
        self, message: Message[dict], manager: "Processor"
    ) -> Response[str]:
        """Handle an process log request."""
        changeset = dict()
        for key, values in message.message_data.items():
            data_key = f"process_{key}"
            max_length = Data._meta.get_field(data_key).base_field.max_length
            changeset[data_key] = getattr(manager.data, data_key)
            if isinstance(values, str):
                values = [values]
            for i, entry in enumerate(values):
                if len(entry) > max_length:
                    values[i] = entry[: max_length - 3] + "..."
            changeset[data_key].extend(values)
        if "process_error" in changeset:
            changeset["status"] = Data.STATUS_ERROR

        manager._update_data(changeset)
        return message.respond_ok("OK")

    def handle_progress(
        self, message: Message[float], manager: "Processor"
    ) -> Response[str]:
        """Handle a progress change request."""
        manager._update_data({"process_progress": message.message_data})
        return message.respond_ok("OK")

    def handle_get_script(
        self, message: Message[str], manager: "Processor"
    ) -> Response[str]:
        """Return script for the current Data object."""
        return message.respond_ok(manager._listener.get_program(manager.data))
