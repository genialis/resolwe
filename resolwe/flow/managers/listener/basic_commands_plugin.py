"""Basic listener commands."""

import logging
import re
from collections import defaultdict
from contextlib import suppress
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Union

from django.conf import settings
from django.db import transaction
from django.db.models import Sum
from django.db.models.functions import Coalesce
from django.urls import reverse
from django.utils.timezone import now

from resolwe.flow.executors.socket_utils import Message, Response
from resolwe.flow.managers.protocol import ExecutorProtocol
from resolwe.flow.models import Data, DataDependency, Process, Worker
from resolwe.flow.models.utils import validate_data_object
from resolwe.flow.utils import dict_dot, iterate_fields, iterate_schema
from resolwe.storage.connectors import connectors
from resolwe.storage.connectors.hasher import StreamHasher
from resolwe.storage.models import ReferencedPath, StorageLocation
from resolwe.utils import BraceMessage as __

from .plugin import ListenerPlugin, listener_plugin_manager

if TYPE_CHECKING:
    from resolwe.flow.managers.listener.listener import Processor

logger = logging.getLogger(__name__)


class BasicCommands(ListenerPlugin):
    """Basic listener handlers."""

    plugin_manager = listener_plugin_manager

    def handle_run(
        self, data_id: int, message: Message[dict], manager: "Processor"
    ) -> Response[int]:
        """Handle spawning new data object.

        The response is the id of the created object or -1 when object can not
        be created.
        """
        export_files_mapper = message.message_data["export_files_mapper"]
        processing_data: Data = Data.objects.get(pk=data_id)

        try:
            data = message.message_data["data"]
            logger.debug(__("Spawning new data object from dict: {}", data))

            data["contributor"] = processing_data.contributor
            data["process"] = (
                Process.objects.filter(slug=data["process"])
                .filter_for_user(processing_data.contributor)
                .get()
            )
            data["tags"] = processing_data.tags
            data["collection"] = processing_data.collection
            data["subprocess_parent"] = processing_data

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
                processing_data,
                f"Error while preparing spawned Data objects for process '{processing_data.process.slug}'",
            )
            return message.respond_error(-1)
        else:
            return message.respond_ok(created_object.id)

    def handle_update_rc(
        self, data_id: int, message: Message[dict], manager: "Processor"
    ) -> Response[str]:
        """Update return code.

        When return code is chaned from zero to non-zero value the Data object
        status is set to ERROR.

        :args: data contains at least key rc and possibly key error.
        """
        data = manager.data(data_id)
        new_return_code = message.message_data["rc"]
        if new_return_code not in (0, data.process_rc):
            changes = ["process_rc"]
            if data.process_rc is None or data.process_rc == 0:
                max_length = Data._meta.get_field("process_error").base_field.max_length
                error_details = message.message_data.get("error", "Unspecified error")
                if len(error_details) > max_length:
                    error_details = error_details[: max_length - 3] + "..."
                # Avoid duplicate entries in process_error.
                if error_details not in data.process_error:
                    data.process_error.append(error_details)
                data.status = Data.STATUS_ERROR
                changes += ["process_error", "status"]
            data.process_rc = new_return_code
            manager._save_data(data, changes)
        return message.respond_ok("OK")

    def handle_finish(
        self, data_id: int, message: Message[Dict], manager: "Processor"
    ) -> Response[str]:
        """Handle an incoming ``Data`` finished processing request."""
        data = manager.data(data_id)
        process_rc = int(message.message_data.get("rc", 1))
        data.process_progress = 100
        data.finished = now()
        changes = ["process_progress", "finished", "size"]
        if process_rc != 0:
            data.process_rc = process_rc
            changes.append("process_rc")
            if data.status != Data.STATUS_ERROR:
                data.status = Data.STATUS_ERROR
                changes.append("status")
                error_message = message.message_data.get(
                    "error", "Process return code is not 0."
                )
                if error_message not in data.process_error:
                    data.process_error.append(error_message)
                    changes.append("process_error")
        elif data.status != Data.STATUS_ERROR:
            data.status = Data.STATUS_DONE
            changes.append("status")

        data.size = data.location.files.aggregate(size=Coalesce(Sum("size"), 0)).get(
            "size"
        )
        with transaction.atomic():
            manager._save_data(data, changes)
            manager._update_worker(data_id, changes={"status": Worker.STATUS_COMPLETED})
            default_location = data.location.default_storage_location
            default_location.status = StorageLocation.STATUS_DONE
            default_location.save(update_fields=["status"])

        # Only validate objects with DONE status. Validating objects in ERROR
        # status will only cause unnecessary errors to be displayed.
        if data.status == Data.STATUS_DONE:
            validate_data_object(data)
        return message.respond_ok("OK")

    def handle_missing_data_locations(
        self, data_id: int, message: Message, manager: "Processor"
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
                children_dependency__child_id=data_id,
                children_dependency__kind=DataDependency.KIND_IO,
            )
            .exclude(location__isnull=True)
            .exclude(pk=data_id)
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
                data = manager.data(data_id)
                manager._log_exception(
                    data,
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
        self, data_id: int, message: Message[List[dict]], manager: "Processor"
    ) -> Response[str]:
        """Store  a list of files and directories produced by the worker.

        The files are a list of dictionaries with keys 'path', 'size' and
        hashes.
        """
        data = Data.objects.get(pk=data_id)
        update_fields = ["size"] + StreamHasher.KNOWN_HASH_TYPES
        storage_location = data.location.default_storage_location
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

        # Extract directories from paths and make sure they are also stored in the
        # database.
        directories: set[str] = set()
        for path in paths:
            directories.update(f"{directory}/" for directory in Path(path).parents)
        directories.remove("./")
        directories = directories - paths.keys()
        if directories:
            existing_directories = set(
                ReferencedPath.objects.filter(
                    storage_locations=storage_location, path__in=directories
                ).values_list("path", flat=True)
            )
            created_directories = ReferencedPath.objects.bulk_create(
                ReferencedPath(path=path, size=0)
                for path in directories - existing_directories
            )
            storage_location.files.add(*created_directories)
        return message.respond_ok("OK")

    def handle_resolve_url(
        self, data_id: int, message: Message[str], manager: "Processor"
    ) -> Response[str]:
        """Resolve the download link.

        The download link is of the format protocol://link.
        Currently we support the protocol "s3".
        """

        def handle_s3(url: str) -> str:
            """Handle the S3 url.

            The URL is of the form s3://bucket-name/key_name.
            """
            # Avoid optional boto3 requirement on startup.
            from resolwe.storage.connectors.s3connector import AwsS3Connector

            allowed_characters = r"[1-9a-z\-\.]{3,63}"
            regexp = rf"^s3://(?P<bucket>{allowed_characters})/(?P<key>(.*){{1,1024}})$"
            with suppress(Exception):
                match = re.match(regexp, url)
                bucket, key = match["bucket"], match["key"]
                for connector in connectors.values():
                    if isinstance(connector, AwsS3Connector):
                        if connector.bucket_name == bucket:
                            # The generated URL can expire quite fast, since the
                            # download is triggered immediatelly.
                            presigned = connector.presigned_url(key, expiration=300)
                            if presigned is not None:
                                return presigned
            return url

        handle_default = lambda url: url
        url = message.message_data
        handlers: Dict[str, Callable[[str], str]] = defaultdict(lambda: handle_default)
        handlers["s3"] = handle_s3
        protocol_regex = r"^(?P<protocol>[a-zA-Z1-9]+)://"
        with suppress(Exception):
            protocol = re.match(protocol_regex, url)["protocol"].lower()
            return message.respond_ok(handlers[protocol](url))

        return message.respond_ok(url)

    def hydrate_spawned_files(
        self, exported_files_mapper: Dict[str, str], filename: str
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
        self, data_id: int, message: Message[str], manager: "Processor"
    ) -> Response[str]:
        """Update data status."""

        data_status = manager.get_data_fields(data_id, "status")
        new_status = manager._choose_worst_status(message.message_data, data_status)
        if new_status != data_status:
            data = manager.data(data_id)
            data.status = new_status

            with transaction.atomic():
                manager._save_data(data, ["status"])
                if new_status == Data.STATUS_PREPARING:
                    manager._update_worker(
                        data_id, changes={"status": Worker.STATUS_PREPARING}
                    )
                elif new_status == Data.STATUS_PROCESSING:
                    manager._update_worker(
                        data_id, changes={"status": Worker.STATUS_PROCESSING}
                    )

            if new_status == Data.STATUS_ERROR:
                logger.error(
                    __(
                        "Error occured while processing data object '{}' (handle_update).",
                        data_id,
                    ),
                    extra={
                        "data_id": data_id,
                        "api_url": "{}{}".format(
                            getattr(settings, "RESOLWE_HOST_URL", ""),
                            reverse(
                                "resolwe-api:data-detail",
                                kwargs={"pk": data_id},
                            ),
                        ),
                    },
                )
        return message.respond_ok(new_status)

    def handle_get_data_by_slug(
        self, data_id: int, message: Message[str], manager: "Processor"
    ) -> Response[int]:
        """Get data id by slug."""
        return message.respond_ok(
            Data.objects.filter(slug=message.message_data)
            .values_list("id", flat=True)
            .get()
        )

    def handle_set_data_size(
        self, data_id: int, message: Message[int], manager: "Processor"
    ) -> Response[str]:
        """Set size of data object.

        Note: the signals on data object are not triggered on size change.
        """
        assert isinstance(message.message_data, int)
        manager._update_data(data_id, {"size": message.message_data})
        return message.respond_ok("OK")

    def handle_update_output(
        self, data_id: int, message: Message[Dict[str, Any]], manager: "Processor"
    ) -> Response[str]:
        """Update data output."""
        data = manager.data(data_id)
        output_schema = manager.get_data_fields(data_id, ("process__output_schema"))
        storage_fields = {
            field_name
            for schema, _, field_name in iterate_schema(data.output, output_schema)
            if schema["type"].startswith("basic:json:")
        }

        with transaction.atomic():
            for key, val in message.message_data.items():
                if key in storage_fields:
                    val = manager.save_storage(key, val, data).pk
                dict_dot(data.output, key, val)
            manager._save_data(data, ["output"])
        return message.respond_ok("OK")

    def handle_get_files_to_download(
        self, data_id: int, message: Message[int], manager: "Processor"
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
        self, data_id: int, message: Message[dict], manager: "Processor"
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
        self, data_id: int, message: Message[int], manager: "Processor"
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
        self, data_id: int, message: Message[int], manager: "Processor"
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
        self, data_id: int, message: Message[dict], manager: "Processor"
    ) -> Response[str]:
        """Handle an incoming ``Data`` object annotate request."""
        (entity_id,) = manager.get_data_fields(data_id, ["entity_id"])

        if entity_id is None:
            raise RuntimeError(f"No entity to annotate for object '{data_id}'")

        handler = self.plugin_manager.get_handler("set_entity_annotations")
        assert handler is not None, "Handler for 'set_entity_annotations' not found"

        message.message_data = [entity_id, message.message_data, True]
        return handler(data_id, message, manager)

    def handle_process_log(
        self, data_id, message: Message[dict], manager: "Processor"
    ) -> Response[str]:
        """Handle an process log request."""
        data = manager.data(data_id)
        changes = []
        for key, values in message.message_data.items():
            data_key = f"process_{key}"
            changes.append(data_key)
            max_length = Data._meta.get_field(data_key).base_field.max_length
            if isinstance(values, str):
                values = [values]
            for i, entry in enumerate(values):
                if len(entry) > max_length:
                    values[i] = entry[: max_length - 3] + "..."
                # Avoid duplicates.
                log_list = getattr(data, data_key)
                if values[i] not in log_list:
                    log_list.append(values[i])
        if "process_error" in changes:
            data.status = Data.STATUS_ERROR
            changes.append("status")
        manager._save_data(data, changes)
        return message.respond_ok("OK")

    def handle_progress(
        self, data_id: int, message: Message[float], manager: "Processor"
    ) -> Response[str]:
        """Handle a progress change request.

        Do not trigger the signals when only progress changes.
        """
        data = manager.data(data_id)
        manager._save_data(data, {"process_progress": message.message_data})
        return message.respond_ok("OK")
