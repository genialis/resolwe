""".. Ignore pydocstyle D400.

========
Listener
========

Standalone client used as a contact point for executors. It uses zeromq
socket for communication.

.. autoclass:: resolwe.flow.managers.listener.ExecutorListener
    :members:

"""

import asyncio
import json
import logging
import os
import shlex
import time
from contextlib import suppress
from importlib import import_module
from typing import Any, Dict, List, MutableMapping, Optional, Tuple, Union

import zmq
import zmq.asyncio
from asgiref.sync import async_to_sync
from channels.db import database_sync_to_async

from django.apps import apps
from django.conf import settings
from django.contrib.postgres.fields.jsonb import JSONField
from django.core.exceptions import ValidationError
from django.db import transaction
from django.urls import reverse
from django.utils.timezone import now

from django_priority_batch import PrioritizedBatcher
from guardian.shortcuts import get_objects_for_user

from resolwe.flow.engine import BaseEngine, InvalidEngineError, load_engines
from resolwe.flow.executors.socket_utils import (
    BaseProtocol,
    Message,
    PeerIdentity,
    PeerStatus,
    Response,
    ResponseStatus,
)
from resolwe.flow.executors.zeromq_utils import ZMQCommunicator
from resolwe.flow.models import (
    Collection,
    Data,
    DataDependency,
    Process,
    Storage,
    Worker,
)
from resolwe.flow.models.utils import (
    referenced_files,
    serialize_collection_relations,
    validate_data_object,
)
from resolwe.flow.utils import dict_dot, iterate_fields
from resolwe.storage.models import AccessLog, ReferencedPath, StorageLocation
from resolwe.storage.settings import STORAGE_LOCAL_CONNECTOR
from resolwe.utils import BraceMessage as __

from . import consumer
from .protocol import ExecutorProtocol, WorkerProtocol

logger = logging.getLogger(__name__)


class PythonProcessMixin:
    """Handler methods for Python processes."""

    ALLOWED_MODELS_C = ["Collection", "Entity", "Data"]
    ALLOWED_MODELS_RW = ["Data", "Process", "Entity", "Collection", "Storage"]
    WRITE_FIELDS = {"Data.name", "Data.output", "Data.descriptor"}

    def _has_permission(self, model, model_pk: int, permission_name: str):
        """Check if contributor has requested permissions.

        :raises RuntimeError: with detailed explanation when check fails.
        """
        object_ = model.objects.filter(pk=model_pk)
        filtered_object = get_objects_for_user(
            self.contributor,
            [permission_name],
            object_,
        )
        if not filtered_object:
            if object_:
                raise RuntimeError(
                    f"No permissions: {model._meta.model_name} with id {model_pk}."
                )
            else:
                raise RuntimeError(
                    f"Object {model._meta.model_name} with id {model_pk} not found."
                )

    def _can_modify_data(self, **model_data: Dict[str, Any]):
        """Check if user has the permission to create a Data object.

        User can always create a data object but also need edit permission
        to the collection for example.

        :raises RuntimeError: witd detailed description when user can not
            create Data object.
        """
        can_set = {
            "process_id",
            "output",
            "input",
            "tags",
            "entity_id",
            "collection_id",
        }
        not_allowed_keys = set(model_data.keys()) - can_set
        if not_allowed_keys:
            message = f"Not allowed to set {','.join(not_allowed_keys)} not allowed."
            raise RuntimeError(message)

        # Check process permissions.
        self._has_permission(Process, model_data["process_id"], "view_process")

        if "entity_id" in model_data:
            # Check entity permissions.
            self._has_permission(Process, model_data["entity_id"], "edit_entity")

        if "collection_id" in model_data:
            # Check collection permissions.
            self._has_permission(
                Collection, model_data["collection_id"], "edit_collection"
            )

    def handle_create_object(
        self, message: Message[Tuple[str, Dict[str, Any]]]
    ) -> Response[int]:
        """Create and return the new object id."""
        model_name, model_data = message.message_data
        assert (
            model_name in self.ALLOWED_MODELS_C
        ), f"Not allowed to create model {model_name}."

        model = apps.get_model("flow", model_name)
        if model_name == "Data":
            self._can_modify_data(**model_data)
        model_data["contributor"] = self.contributor
        created = model.objects.create(**model_data)
        return message.respond_ok(created.id)

    def handle_filter_objects(
        self, message: Message[Tuple[str, Dict[str, Any]]]
    ) -> Response[List[int]]:
        """Create and return the new object id."""
        model_name, filters = message.message_data
        assert (
            model_name in self.ALLOWED_MODELS_RW
        ), f"Not allowed to access {model_name}."

        # TODO: storages!!!
        model = apps.get_model("flow", model_name)
        permission_name = f"{model_name.lower()}_view"
        model_instances = get_objects_for_user(
            self.contributor,
            [permission_name],
            model.objects.filter(**filters),
        )
        return message.respond_ok(list(model_instances.values_list("id", flat=True)))

    def handle_update_model_fields(
        self, message: Message[Tuple[str, int, Dict[str, Any]]]
    ) -> Response[str]:
        """Update the value for the given fields.

        The received message format is
        (model name, primary key, mapping from field names to field values).

        Field name can be given in dot notation for JSON fields.
        """
        model_name, model_pk, mapping = message.message_data
        assert (
            model_name in self.ALLOWED_MODELS_RW
        ), f"Access to the model {model_name} not allowed."
        permission_name = f"{model_name.lower()}_edit"
        if model_name == "Data" and model_pk == self.data_id:
            model_instance = self.data
            model = Data
        else:
            model = apps.get_model("flow", model_name)
            if model_name == "Storage":
                model_instance = Storage.objects.get(pk=model_pk)
                data_objects = get_objects_for_user(
                    self.contributor, ["data_edit"], model_instance.data
                )
                if not data_objects.exists():
                    raise RuntimeError(f"Access to Storage with id {model_pk} denied.")
            else:

                model_instance = get_objects_for_user(
                    self.contributor,
                    [permission_name],
                    model.objects.filter(pk=model_pk),
                ).get()
        for field_name, field_value in mapping.items():
            logger.debug(
                __(
                    "Updating field: {} with value: {}",
                    model._meta.get_field(field_name),
                    field_value,
                )
            )
            if isinstance(model._meta.get_field(field_name), JSONField):
                instance_value = getattr(model_instance, field_name)
                for key, value in field_value.items():
                    dict_dot(instance_value, key, value)
            else:
                setattr(model_instance, field_name, field_value)
        model_instance.save(update_fields=mapping.keys())
        return message.respond_ok("OK")

    def handle_get_model_fields_details(
        self, message: Message[str]
    ) -> Response[Dict[str, Tuple[str, Optional[str]]]]:
        """Get the field names and types for the given model.

        The response is a dictionary which maps names to types.
        """
        model = apps.get_model("flow", message.message_data)
        response = {}
        for field in model._meta.get_fields():
            related_model = None
            if field.is_relation:
                related_model = field.related_model._meta.label.split(".")[1]
            response[field.name] = (
                field.get_internal_type(),
                not field.null,
                related_model,
            )
        return message.respond_ok(response)

    def handle_get_relations(self, message: Message[int]) -> Response[List[dict]]:
        """Get relations for the given collection object."""
        collection = get_objects_for_user(
            self.contributor,
            "view_collection",
            Collection.objects.filter(id=message.message_data),
        ).get()

        return message.respond_ok(serialize_collection_relations(collection))

    def handle_get_model_fields(
        self, message: Message[Tuple[str, int, List[str]]]
    ) -> Response[Dict[str, Any]]:
        """Return the value of the given model for the given fields.

        The received message format is
        (model_name, primary_key, list_of_fields).

        In case of JSON field the field name can contain underscores to get
        only the part of the JSON we are interested in.
        """
        model_name, model_pk, field_names = message.message_data
        assert model_name in self.ALLOWED_MODELS_RW
        permission_name = f"{model_name.lower()}_view"
        model = apps.get_model("flow", model_name)

        filtered_object = get_objects_for_user(
            self.contributor, [permission_name], model.objects.filter(pk=model_pk)
        )
        values = filtered_object.values(*field_names)
        # NOTE: non JSON serializable fields are NOT supported. If such field
        # is requested the exception will be handled one level above and
        # response with status error will be returned.
        if values.count() == 1:
            values = values.get()
        return message.respond_ok(values)

    def handle_get_process_requirements(self, message: Message[int]) -> Response[dict]:
        """Return the requirements for the process with the given id."""
        process_id = message.message_data
        filtered_process = get_objects_for_user(
            self.contributor, ["process_view"], Process.objects.filter(pk=process_id)
        )
        process_requirements, process_slug = filtered_process.values_list(
            "requirements", "slug"
        ).get()
        resources = process_requirements.get("resources", {})

        # Get limit defaults and overrides.
        limit_defaults = getattr(settings, "FLOW_PROCESS_RESOURCE_DEFAULTS", {})
        limit_overrides = getattr(settings, "FLOW_PROCESS_RESOURCE_OVERRIDES", {})

        limits = {}
        limits["cores"] = int(resources.get("cores", 1))
        max_cores = getattr(settings, "FLOW_PROCESS_MAX_CORES", None)
        if max_cores:
            limits["cores"] = min(limits["cores"], max_cores)

        memory = limit_overrides.get("memory", {}).get(process_slug, None)
        if memory is None:
            memory = int(
                resources.get(
                    "memory",
                    # If no memory resource is configured, check settings.
                    limit_defaults.get("memory", 4096),
                )
            )
        limits["memory"] = memory
        process_requirements["resources"] = limits
        return message.respond_ok(process_requirements)


class Processor(PythonProcessMixin):
    """Class respresents instance of one connected worker."""

    def __init__(
        self,
        peer_identity: PeerIdentity,
        starting_sequence_number: int,
        listener: "ListenerProtocol",
    ):
        """Initialize.

        Assumption: first_message is checked to be of the expected type.

        :raises ValueError: when identity is not in the correct format.
        :raises Data.DoesNotExist: when Data object is not found.
        """
        self.data_id = int(peer_identity)
        self._data: Optional[Data] = None
        self._worker: Optional[Worker] = None
        self._listener = listener

        self.expected_sequence_number = starting_sequence_number
        self.return_code = 0

    @property
    def contributor(self) -> settings.AUTH_USER_MODEL:
        """Get the user that created the Data object we are processing.

        This user is used when checking permissions.
        """
        return self.data.contributor

    @property
    def data(self) -> Data:
        """Get the data object.

        The evaluation is done when needed in order not to make potentially
        failing SQL query in the constructor.

        This method uses Django ORM.
        """
        if self._data is None:
            self._data = Data.objects.get(pk=self.data_id)
        return self._data

    @property
    def worker(self) -> Worker:
        """Get the worker object.

        The evaluation is done when needed in order not to make potentially
        failing SQL query in the constructor.

        This method uses Django ORM.
        """
        if self._worker is None:
            self._worker = Worker.objects.get(data=self.data)
        return self._worker

    def _unlock_all_inputs(self):
        """Unlock all data objects that were locked by the given data.

        If exception occurs during unlocking we can not do much but ignore it.
        The method is called when dispatcher is nudged.
        """
        with suppress(Exception):
            query = AccessLog.objects.filter(cause_id=self.data_id)
            query.update(finished=now())

    def _choose_worst_status(self, status1: str, status2: str) -> str:
        """Get more problematic status of the two.

        Assumption: statuses in Data.STATUS_CHOICES are ordered from least
            to most problematic.

        :raises AssertionError: when status1 or status2 do not represent
            possible status of Data object.

        """
        statuses = [status[0] for status in Data.STATUS_CHOICES]
        assert status1 in statuses
        assert status2 in statuses
        return max([status1, status2], key=lambda status: statuses.index(status))

    def _save_error(self, error: str):
        """Log error to Data object, ignore possible exceptions.

        The status of Data object is changed to ``Data.STATUS_ERROR``.
        """
        with suppress(Exception):
            self.data.refresh_from_db()
        self.data.process_error.append(error)
        self.data.status = Data.STATUS_ERROR
        with suppress(Exception):
            self.data.save(update_fields=["process_error", "status"])

    def _log_exception(
        self, error: str, extra: Optional[dict] = None, save_to_data_object: bool = True
    ):
        """Log current exception and optionally store it to the data object.

        When argument ``save_to_data_object`` is false the status of the
        data object is not changed.
        """
        if extra is None:
            extra = dict()
        extra.update({"data_id": self.data.id})
        logger.exception(error, extra=extra)
        if save_to_data_object:
            self._save_error(error)

    def _log_error(
        self, error: str, extra: dict = {}, save_to_data_object: bool = True
    ):
        """Log error and optionally store it to the data object.

        When argument ``save_to_data_object`` is false the status of the
        data object is not changed.
        """
        extra.update({"data_id": self.data.id})
        logger.error(error, extra=extra)
        if save_to_data_object:
            self._save_error(error)

    def _abort_processing(self):
        """Abort processing.

        Use this call with caution. It also notifier the dispatcher to abort
        processing which means closing listener connection while testing.

        A sensible default is to call this method only when peer becomes
        unresponsive.
        """
        self.notify_dispatcher_abort()
        self._listener.remove_peer(str(self.data_id).encode())

    async def _finish_processing(self):
        """Finish processing.

        This method notifies the dispatcher that the worker has finished
        processing the data object.
        """
        self.notify_dispatcher_finish()

    def _update_data(self, changes: Dict[str, Any], refresh: bool = False):
        """Update the data object.

        Changes are dictionary mapping from field names to field values.

        :param changes: dictionary with chenges to be saved.
        :param refresh: if True refresh data object before applying changes.

        :raises: exception when data object cannot be saved.
        """
        if "process_rc" in changes and changes["process_rc"] < 0:
            changes["process_rc"] = abs(changes["process_rc"])
        self._update_object(changes, self.data)

    def _update_worker(self, changes: Dict[str, Any]):
        """Update the worker object.

        :raises: exception when data object cannot be saved.
        """
        self._update_object(changes, self.worker)

    def _update_object(self, changes: Dict[str, Any], obj: Union[Worker, Data]):
        """Update object properties and save them to database."""
        for key, value in changes.items():
            setattr(obj, key, value)
        obj.save(update_fields=list(changes.keys()))

    async def terminate(self):
        """Send the terminate command to the worker.

        Peer should terminate by itself and send finish message back to us.
        """
        await self._listener.communicator.send_command(
            Message.command("terminate", ""),
            peer_identity=str(self.data_id).encode(),
        )

    async def peer_not_responding(self):
        """Peer is not responding, abort the processing.

        TODO: what to do with unresponsive peer which wakes up later and
        starts sending commands to the listener? Currently the situation
        is handled by sending "terminate" command to it and ignore its
        commands.
        """
        logger.debug(__("Peer with id={} is not responding.", self.data_id))
        await database_sync_to_async(self._log_error)("Worker is not responding")
        await database_sync_to_async(self._update_worker)(
            {"status": Worker.STATUS_NONRESPONDING}
        )
        await self.notify_dispatcher_abort_async()
        self._listener.remove_peer(str(self.data_id).encode())

    def process_command(self, message: Message) -> Response:
        """Process a single command from the peer.

        This command is run in the database_sync_to_async so it is safe to
        perform Django ORM operations inside.

        Exceptions will be handler one level up and error response will be
        sent in this case.
        """
        # This worker must be in status processing or preparing.
        # All messages from workers not in this status will be discarted and
        # error will be returned.
        if self.worker.status not in [
            Worker.STATUS_PROCESSING,
            Worker.STATUS_PREPARING,
        ]:
            self._log_error(
                f"Wrong worker status: {self.worker.status} for peer with id {self.data_id}."
            )
            return message.respond_error(f"Wrong worker status: {self.worker.status}")

        command_name = message.command_name
        handler_name = f"handle_{command_name}"
        handler = getattr(self, handler_name, None)
        if not handler:
            error = f"No command handler for '{command_name}'."
            self._log_error(error, save_to_data_object=False)
            return message.respond_error(error)

        # Read sequence number and refresh data object if it differs.
        if self.expected_sequence_number != message.sequence_number:
            try:
                self.data.refresh_from_db()
                self.worker.refresh_from_db()
            except:
                self._log_exception("Unable to refresh data object")
                return message.respond_error("Unable to refresh the data object")

        if self.worker.status != Worker.STATUS_PROCESSING:
            self.worker.status = Worker.STATUS_PROCESSING
            self.worker.save(update_fields=["status"])

        if self.data.started is None:
            self.data.started = now()
            self.data.save(update_fields=["started"])

        self.expected_sequence_number = message.sequence_number + 1
        try:
            with PrioritizedBatcher.global_instance():
                result = handler(message)
                # Set status of the response to ERROR when data object status
                # is Data.STATUS_ERROR. Such response will trigger terminate
                # procedure in the processing container and stop processing.
                if self.data.status == Data.STATUS_ERROR:
                    result.type_data = ResponseStatus.ERROR.value
                return result
        except ValidationError as err:
            error = (
                f"Validation error when saving Data object of process "
                f"'{self.data.process.slug}' ({handler_name}): "
                f"{err}"
            )
            self._log_exception(error)
            return message.respond_error("Validation error")
        except Exception as err:
            error = f"Error in command handler '{handler_name}': {err}"
            self._log_exception(error)
            return message.respond_error(f"Error in command handler '{handler_name}'")

    def notify_dispatcher_abort(self):
        """Notify dispatcher that processing was aborted.

        .. IMPORTANT::

            This only makes manager's state consistent and doesn't
            affect Data object in any way.

        :param obj: The Channels message object. Command object format:

            .. code-block:: none

                {
                    'command': 'abort',
                    'data_id': [id of the :class:`~resolwe.flow.models.Data` object
                               this command was triggered by],
                }
        """
        async_to_sync(self.notify_dispatcher_abort_async)()

    async def notify_dispatcher_abort_async(self):
        """Notify dispatcher that processing was aborted.

        :param obj: The Channels message object. Command object format:

            .. code-block:: none

                {
                    'command': 'abort',
                    'data_id': [id of the :class:`~resolwe.flow.models.Data` object
                               this command was triggered by],
                }
        """
        await database_sync_to_async(self._unlock_all_inputs)()
        await consumer.send_event(
            {
                WorkerProtocol.COMMAND: WorkerProtocol.ABORT,
                WorkerProtocol.DATA_ID: self.data.id,
                WorkerProtocol.FINISH_COMMUNICATE_EXTRA: {
                    "executor": getattr(settings, "FLOW_EXECUTOR", {}).get(
                        "NAME", "resolwe.flow.executors.local"
                    ),
                },
            }
        )
        logger.debug("notify_dispatcher_abort: consumer event sent")

    def notify_dispatcher_finish(self):
        """Notify dispatcher that the processing is finished.

        See ``notify_dispatcher_abort`` for message format.
        """
        async_to_sync(self.notify_dispatcher_finish_async)()

    async def notify_dispatcher_finish_async(self):
        """Notify dispatcher that the processing is finished.

        See ``notify_dispatcher_abort`` for message format.
        """
        await database_sync_to_async(self._unlock_all_inputs)()
        await consumer.send_event(
            {
                WorkerProtocol.COMMAND: WorkerProtocol.FINISH,
                WorkerProtocol.DATA_ID: self.data.id,
                WorkerProtocol.FINISH_COMMUNICATE_EXTRA: {
                    "executor": getattr(settings, "FLOW_EXECUTOR", {}).get(
                        "NAME", "resolwe.flow.executors.local"
                    ),
                },
            }
        )

    def handle_run(self, message: Message[dict]) -> Response[List[int]]:
        """Handle spawning new data object.

        The response is the id of the created object.
        """
        export_files_mapper = message.message_data["export_files_mapper"]
        # TODO: is this refresh needed??
        self.data.refresh_from_db()

        try:
            data = message.message_data["data"]
            logger.debug(__("Spawning new data object from dict: {}", data))

            data["contributor"] = self.data.contributor
            data["process"] = Process.objects.filter(slug=data["process"]).latest()
            data["tags"] = self.data.tags
            data["collection"] = self.data.collection
            data["subprocess_parent"] = self.data

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
            self._log_exception(
                f"Error while preparing spawned Data objects for process '{self.data.process.slug}'"
            )
            return message.respond_error("Error creating new Data object.")
        else:
            return message.respond_ok(created_object.id)

    def handle_update_rc(self, message: Message[dict]) -> Response[str]:
        """Update return code.

        When return code is chaned from zero to non-zero value the Data object
        status is set to ERROR.

        :args: data contains at least key rc and possibly key error.
        """
        return_code = message.message_data["rc"]
        if return_code not in (0, self.return_code):
            changes: dict = {"process_rc": self.return_code}
            if self.return_code == 0:
                error_details = message.message_data.get("error", "Unspecified error")
                changes["status"] = Data.STATUS_ERROR
                changes["process_error"] = self.data.process_error
                changes["process_error"].append(error_details)
            self.return_code = return_code
            self._update_data(changes)
        return message.respond_ok("OK")

    def handle_get_output_files_dirs(self, message: Message[str]) -> Response[dict]:
        """Get the output for file and dir fields.

        The sent dictionary has field names as its keys and tuple filed_type,
        field_value for its values.
        """

        def is_file_or_dir(field_type: str) -> bool:
            """Is file or directory."""
            return "basic:file" in field_type or "basic:dir" in field_type

        output = dict()
        for field_schema, fields in iterate_fields(
            self.data.output, self.data.process.output_schema
        ):
            if is_file_or_dir(field_schema["type"]):
                name = field_schema["name"]
                output[name] = (field_schema["type"], fields[name])

        return message.respond_ok(output)

    def handle_finish(self, message: Message[Dict]) -> Response[str]:
        """Handle an incoming ``Data`` finished processing request."""
        # with transaction.atomic():
        # TODO: relavant?
        # Data wrap up happens last, so that any triggered signals
        # already see the spawned children. What the children themselves
        # see is guaranteed by the transaction we're in.

        # The worker is finished with processing.
        process_rc = int(message.message_data.get("rc", 1))
        changeset = {
            "process_progress": 100,
            "finished": now(),
        }
        if process_rc != 0:
            changeset["process_rc"] = process_rc
            if self.data.status != Data.STATUS_ERROR:
                changeset["status"] = Data.STATUS_ERROR
                self.data.process_error.append(
                    message.message_data.get("error", "Process return code is not 0")
                )
                changeset["process_error"] = self.data.process_error
        elif self.data.status != Data.STATUS_ERROR:
            changeset["status"] = Data.STATUS_DONE

        self._update_data(changeset)

        # Only validate objects with DONE status. Validating objects in ERROR
        # status will only cause unnecessary errors to be displayed.
        if self.data.status == Data.STATUS_DONE:
            validate_data_object(self.data)
        return message.respond_ok("OK")

    def handle_get_referenced_files(self, message: Message) -> Response[List]:
        """Get a list of files referenced by the data object.

        The list also depends on the Data object itself, more specifically on
        its output field. So the method is not idempotent.
        """
        return message.respond_ok(referenced_files(self.data))

    def handle_missing_data_locations(
        self, message: Message
    ) -> Response[Union[str, List[Dict[str, Any]]]]:
        """Handle an incoming request to get missing data locations."""
        missing_data = []
        dependencies = (
            Data.objects.filter(
                children_dependency__child=self.data,
                children_dependency__kind=DataDependency.KIND_IO,
            )
            .exclude(location__isnull=True)
            .exclude(pk=self.data.id)
            .distinct()
        )

        for parent in dependencies:
            file_storage = parent.location
            if not file_storage.has_storage_location(STORAGE_LOCAL_CONNECTOR):
                from_location = file_storage.default_storage_location
                if from_location is None:
                    self._log_exception(
                        "No storage location exists (handle_get_missing_data_locations).",
                        extra={"file_storage_id": file_storage.id},
                    )
                    return message.respond_error("No storage location exists")

                to_location = StorageLocation.all_objects.get_or_create(
                    file_storage=file_storage,
                    url=from_location.url,
                    connector_name=STORAGE_LOCAL_CONNECTOR,
                )[0]
                missing_data.append(
                    {
                        "connector_name": from_location.connector_name,
                        "url": from_location.url,
                        "data_id": self.data.id,
                        "to_storage_location_id": to_location.id,
                        "from_storage_location_id": from_location.id,
                    }
                )
                # Set last modified time so it does not get deleted.
                from_location.last_update = now()
                from_location.save()
        return message.respond_ok(missing_data)

    def handle_referenced_files(self, message: Message[List[dict]]) -> Response[str]:
        """Store  a list of files and directories produced by the worker."""
        file_storage = self.data.location
        # At this point default_storage_location will always be local.
        local_location = file_storage.default_storage_location
        with transaction.atomic():
            try:
                referenced_paths = [
                    ReferencedPath(**object_) for object_ in message.message_data
                ]
                ReferencedPath.objects.filter(storage_locations=local_location).delete()
                ReferencedPath.objects.bulk_create(referenced_paths)
                local_location.files.add(*referenced_paths)
                local_location.status = StorageLocation.STATUS_DONE
                local_location.save()
            except Exception:
                self._log_exception("Error saving referenced files")
                self._abort_processing()
                return message.respond_error("Error saving referenced files")

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

    def handle_update_status(self, message: Message[str]) -> Response[str]:
        """Update data status."""
        new_status = self._choose_worst_status(message.message_data, self.data.status)
        if new_status != self.data.status:
            self._update_data({"status": new_status})
            if new_status == Data.STATUS_ERROR:
                # TODO: should we log this?
                logger.error(
                    __(
                        "Error occured while running process '{}' (handle_update).",
                        self.data.process.slug,
                    ),
                    extra={
                        "data_id": self.data.id,
                        "api_url": "{}{}".format(
                            getattr(settings, "RESOLWE_HOST_URL", ""),
                            reverse(
                                "resolwe-api:data-detail", kwargs={"pk": self.data.id}
                            ),
                        ),
                    },
                )
        return message.respond_ok(new_status)

    def handle_get_data_by_slug(self, message: Message[str]) -> Response[int]:
        """Get data id by slug."""
        return message.respond_ok(Data.objects.get(slug=message.message_data).id)

    def handle_set_data_size(self, message: Message[int]) -> Response[str]:
        """Set size of data object."""
        assert isinstance(message.message_data, int)
        self._update_data({"size": message.message_data})
        return message.respond_ok("OK")

    def handle_update_output(self, message: Message[Dict[str, Any]]) -> Response[str]:
        """Update data output."""
        for key, val in message.message_data.items():
            dict_dot(self.data.output, key, val)
        with transaction.atomic():
            self._update_data({"output": self.data.output})
            storage_location = self.data.location.default_storage_location
            ReferencedPath.objects.filter(storage_locations=storage_location).delete()
            referenced_paths = [
                ReferencedPath(path=path)
                for path in referenced_files(self.data, include_descriptor=False)
            ]
            ReferencedPath.objects.bulk_create(referenced_paths)
            storage_location.files.add(*referenced_paths)

        return message.respond_ok("OK")

    def handle_get_files_to_download(
        self, message: Message[int]
    ) -> Response[Union[str, List[dict]]]:
        """Get a list of files belonging to a given storage location object."""
        return message.respond_ok(
            list(
                ReferencedPath.objects.filter(
                    storage_locations=message.message_data
                ).values()
            )
        )

    def handle_download_started(self, message: Message[dict]) -> Response[str]:
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

    def handle_download_aborted(self, message: Message[int]) -> Response[str]:
        """Handle an incoming download aborted request.

        Since download is aborted it does not matter if the location does
        exist or not so we do an update with only one SQL query.
        """
        StorageLocation.all_objects.filter(pk=message.message_data).update(
            status=StorageLocation.STATUS_PREPARING
        )
        return message.respond_ok("OK")

    def handle_download_finished(self, message: Message[int]) -> Response[str]:
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

    def handle_annotate(self, message: Message[dict]) -> Response[str]:
        """Handle an incoming ``Data`` object annotate request."""

        if self.data.entity is None:
            raise RuntimeError(
                f"No entity to annotate for process '{self.data.process.slug}'"
            )

        for key, val in message.message_data.items():
            dict_dot(self.data.entity.descriptor, key, val)

        self.data.entity.save()
        return message.respond_ok("OK")

    def handle_process_log(self, message: Message[dict]) -> Response[str]:
        """Handle an process log request."""
        changeset = dict()
        for key, values in message.message_data.items():
            data_key = f"process_{key}"
            max_length = Data._meta.get_field(data_key).base_field.max_length
            changeset[data_key] = getattr(self.data, data_key)
            if isinstance(values, str):
                values = [values]
            for i, entry in enumerate(values):
                if len(entry) > max_length:
                    values[i] = entry[: max_length - 3] + "..."
            changeset[data_key].extend(values)
        if "process_error" in changeset:
            changeset["status"] = Data.STATUS_ERROR

        self._update_data(changeset)
        return message.respond_ok("OK")

    def handle_progress(self, message: Message[float]) -> Response[str]:
        """Handle a progress change request."""
        self._update_data({"process_progress": message.message_data})
        return message.respond_ok("OK")

    def handle_get_script(self, message: Message[str]) -> Response[str]:
        """Return script for the current Data object."""
        return message.respond_ok(self._listener.get_program(self.data))


class ListenerProtocol(BaseProtocol):
    """Listener protocol implementation."""

    def __init__(
        self,
        hosts: List[str],
        port: int,
        protocol: str,
        zmq_socket: Optional[zmq.asyncio.Socket] = None,
    ):
        """Initialization."""
        if zmq_socket is None:
            zmq_context: zmq.asyncio.Context = zmq.asyncio.Context.instance()
            zmq_socket = zmq_context.socket(zmq.ROUTER)
            for host in hosts:
                zmq_socket.bind(f"{protocol}://{host}:{port}")

        super().__init__(
            ZMQCommunicator(
                zmq_socket, "listener <-> workers", logger, self.peer_status_changed
            ),
            logger,
        )

        # Mapping from the data ID to the Worker instance. Each data object is
        # processed by (at most) one Worker so the map is one-to-one.
        self.peers: MutableMapping[PeerIdentity, Processor] = dict()

        # The code above is used when preparing script for the worker when it
        # is requested. Maybe there is a better way?
        self._execution_engines = self._load_execution_engines()
        logger.info(
            __(
                "Found {} execution engines: {}",
                len(self._execution_engines),
                ", ".join(self._execution_engines.keys()),
            )
        )
        self._expression_engines = self._load_expression_engines()
        logger.info(
            __(
                "Found {} expression engines: {}",
                len(self._expression_engines),
                ", ".join(self._expression_engines.keys()),
            )
        )
        self._executor_preparer = self._load_executor_preparer()

    def handle_log(
        self, message: Message[Union[str, bytes, bytearray]]
    ) -> Response[str]:
        """Handle an incoming log processing request.

        :param obj: The Channels message object. Command object format:

            .. code-block:: none

                {
                    'command': 'log',
                    'message': [log message]
                }
        """
        record_dict = json.loads(message.message_data)
        record_dict["msg"] = record_dict["msg"]

        executors_dir = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "executors"
        )
        record_dict["pathname"] = os.path.join(executors_dir, record_dict["pathname"])
        logger.handle(logging.makeLogRecord(record_dict))
        return message.respond_ok("OK")

    async def peer_status_changed(
        self, peer_identity: PeerIdentity, status: PeerStatus
    ):
        """Check the new peer status and invoke the necessary handlers.

        Remove the unresponsive peer from the list of peers and update its
        status to UNRESPONSIVE.
        """
        logger.debug(__("Peer {} status changed: {}.", peer_identity, status))
        if status == PeerStatus.UNRESPONSIVE:
            peer = self.remove_peer(peer_identity)
            if peer is not None:
                await peer.peer_not_responding()

    def remove_peer(self, peer_identity: PeerIdentity) -> Optional[Processor]:
        """Remove peer from list of known peers."""
        peer = self.peers.pop(peer_identity, None)
        if peer is None:
            logger.error("Peer %s is not known, can not remove.", peer_identity)
        return peer

    async def _get_response(
        self, peer_identity: PeerIdentity, message: Message
    ) -> Response:
        """Handle command received over 0MQ."""
        # Logging is handled separately. The worker might have non-numeric peer identifier.
        if message.command_name == "log":
            try:
                return self.handle_log(message)
            except:
                logger.exception("Error in handle_log method.")
                return message.respond("Error writing logs.", ResponseStatus.ERROR)

        if peer_identity not in self.peers:
            try:
                peer = Processor(
                    peer_identity,
                    message.sequence_number,
                    self,
                )
                self.peers[peer_identity] = peer
            except:
                logger.exception("Error creating Processor instance.")
                return message.respond(
                    "Error creating Processor instance", ResponseStatus.ERROR
                )

        peer = self.peers[peer_identity]
        try:
            response = await database_sync_to_async(peer.process_command)(message)
        except:
            logger.exception("Error in process_command method.")
            return message.respond("Error processing command", ResponseStatus.ERROR)
        else:
            return response

    async def _process_command(
        self, peer_identity: PeerIdentity, received_message: Message
    ):
        """Process command."""
        response = await self._get_response(peer_identity, received_message)
        try:
            assert received_message.sent_timestamp is not None
            response_time = time.time() - received_message.sent_timestamp
            self.logger.debug(__("Response time: {}", response_time))
            await self.communicator.send_response(response, peer_identity)

        except RuntimeError:
            # When unable to send message just log the error. Peer will be
            # removed by the watchdog later on.
            self.logger.exception(
                "Protocol: error sending response to {received_message}."
            )
        # Remove peer and nudge manager on "finish" command.
        if received_message.command_name == "finish":
            peer = self.peers[peer_identity]
            self.remove_peer(peer_identity)

            # Worker status must be updated in the dispatcher to avoid
            # synchronization issues while testing (if status is updated here
            # another message might raise runtime barrier before our message
            # reaches the dispatcher.
            with suppress(Data.DoesNotExist):
                await peer.notify_dispatcher_finish_async()

    async def process_command(
        self, peer_identity: PeerIdentity, received_message: Message
    ):
        """Override process command."""
        # Make it run in the background so another message can be processed.
        asyncio.ensure_future(self._process_command(peer_identity, received_message))

    def get_program(self, data: Data) -> str:
        """Get a program for given data object."""
        execution_engine_name = data.process.run.get("language", None)
        program = [self._get_execution_engine(execution_engine_name).evaluate(data)]
        # TODO: should executor be changed? Definitely for tests (from case to case).
        # Should I use file prepared by the Dispatcher (that takes care of that)?
        env_vars = self.get_executor().get_environment_variables()
        settings_env_vars = {
            "RESOLWE_HOST_URL": getattr(settings, "RESOLWE_HOST_URL", "localhost"),
        }
        additional_env_vars = getattr(settings, "FLOW_EXECUTOR", {}).get("SET_ENV", {})
        env_vars.update(settings_env_vars)
        env_vars.update(additional_env_vars)
        export_commands = [
            "export {}={}".format(key, shlex.quote(value))
            for key, value in env_vars.items()
        ]

        # Paths for Resolwe tools.
        tools_num = len(self._executor_preparer.get_tools_paths())
        tools_paths = ":".join(f"/usr/local/bin/resolwe/{i}" for i in range(tools_num))
        export_commands += [f"export PATH=$PATH:{tools_paths}"]

        # Disable brace expansion and set echo.
        echo_commands = ["set -x +B"]

        commands = echo_commands + export_commands + program
        return os.linesep.join(commands)

    def _get_execution_engine(self, name):
        """Return an execution engine instance."""
        try:
            return self._execution_engines[name]
        except KeyError:
            raise InvalidEngineError("Unsupported execution engine: {}".format(name))

    def _load_execution_engines(self) -> Dict[str, BaseEngine]:
        """Load execution engines."""
        execution_engines = getattr(
            settings, "FLOW_EXECUTION_ENGINES", ["resolwe.flow.execution_engines.bash"]
        )
        return load_engines(
            self, "ExecutionEngine", "execution_engines", execution_engines
        )

    def _load_expression_engines(self) -> Dict[str, BaseEngine]:
        """Load expression engines."""
        expression_engines = getattr(
            settings,
            "FLOW_EXPRESSION_ENGINES",
            ["resolwe.flow.expression_engines.jinja"],
        )
        return load_engines(
            self, "ExpressionEngine", "expression_engines", expression_engines
        )

    def _load_executor_preparer(self) -> Any:
        """Load and return the executor preparer class."""
        executor_name = (
            getattr(settings, "FLOW_EXECUTOR", {}).get(
                "NAME", "resolwe.flow.executors.docker"
            )
            + ".prepare"
        )
        return import_module(executor_name).FlowExecutorPreparer()

    def get_executor(self) -> Any:
        """Get the executor preparer class."""
        return self._executor_preparer

    def get_expression_engine(self, name: str):
        """Return an expression engine instance."""
        try:
            return self._expression_engines[name]
        except KeyError:
            raise InvalidEngineError("Unsupported expression engine: {}".format(name))


def handle_exceptions(loop, context):
    """Log uncaught exceptions in asyncio."""
    msg = context.get("exception", context["message"])
    name = context.get("future").get_coro().__name__
    logger.error(f"Caught exception from {name}: {msg}")


class ExecutorListener:
    """The contact point implementation for executors."""

    def __init__(
        self,
        *args,
        **kwargs,
    ):
        """Initialization."""
        # Read socket parameters.
        self._hosts = list(
            kwargs.get(
                "hosts",
                getattr(settings, "LISTENER_CONNECTION", {}).get(
                    "hosts", {"local": "127.0.0.1"}
                ),
            ).values()
        )

        self.port = kwargs.get(
            "port", getattr(settings, "LISTENER_CONNECTION", {}).get("port", 53892)
        )
        self.protocol = kwargs.get(
            "protocol",
            getattr(settings, "LISTENER_CONNECTION", {}).get("protocol", "tcp"),
        )

        # When zmq_socket kwarg is not None, use this one instead of creating
        # a new one.
        self.zmq_socket = kwargs.get("zmq_socket")

        self._listener_protocol: Optional[ListenerProtocol] = None

        # Running coordination.
        self._should_stop: Optional[asyncio.Event] = None
        self._runner_future: Optional[asyncio.Future] = None
        self._communicating_future: Optional[asyncio.Future] = None

    @property
    def should_stop(self):
        """Return should stop event.

        Use lazy instantiation to avoid creation event loop on init.
        """
        if self._should_stop is None:
            self._should_stop = asyncio.Event()
        return self._should_stop

    @property
    def hosts(self):
        """Get hosts to bind on."""
        return self._hosts

    @hosts.setter
    def hosts(self, hosts: dict):
        """Set the hosts to bind on."""
        self._hosts = list(hosts.values())

    @property
    def listener_protocol(self):
        """Return the listener protocol object.

        Used to lazy create object when property is accessed.
        """
        if self._listener_protocol is None:
            self._listener_protocol = ListenerProtocol(
                self.hosts, self.port, self.protocol, self.zmq_socket
            )
        return self._listener_protocol

    async def terminate_worker(self, identity: PeerIdentity):
        """Terminate the worker."""
        peer = self.listener_protocol.peers.get(identity)
        if peer is not None:
            await peer.terminate()

    async def __aenter__(self):
        """On entering a context, start the listener thread."""
        loop = asyncio.get_event_loop()
        loop.set_exception_handler(handle_exceptions)
        self._should_stop = None
        self._runner_future = asyncio.ensure_future(self.run())
        return self

    async def __aexit__(self, typ, value, trace):
        """On exiting a context, kill the listener and wait for it.

        .. note::

            Exceptions are all propagated.
        """
        assert self._runner_future is not None
        self.terminate()
        await asyncio.gather(self._runner_future)
        self._listener_protocol = None

    def terminate(self):
        """Stop the standalone manager."""
        logger.info(__("Terminating Resolwe listener."))
        self.should_stop.set()

    async def run(self):
        """Run the main listener run loop.

        Doesn't return until :meth:`terminate` is called.
        """
        logger.info(
            __(
                f"Starting Resolwe listener on  '{self.protocol}://{self.hosts}:{self.port}'."
            )
        )
        communicator_future = asyncio.ensure_future(
            self.listener_protocol.communicate()
        )
        await self.should_stop.wait()
        self.listener_protocol.stop_communicate()
        await communicator_future
        logger.info(
            __(
                f"Stoping Resolwe listener on  '{self.protocol}://{self.hosts}:{self.port}'."
            )
        )
