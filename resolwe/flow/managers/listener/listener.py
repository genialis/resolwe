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
import copy
import json
import logging
import os
import shlex
import threading
import time
from collections import defaultdict
from contextlib import suppress
from importlib import import_module
from pathlib import Path
from typing import Any, Dict, List, MutableMapping, Optional, Set, Tuple, Union

import zmq
import zmq.asyncio
from asgiref.sync import async_to_sync
from channels.db import database_sync_to_async

from django.conf import settings
from django.core.exceptions import PermissionDenied, ValidationError
from django.forms.models import model_to_dict
from django.utils.timezone import now

from django_priority_batch import PrioritizedBatcher

from resolwe.flow.engine import BaseEngine, InvalidEngineError, load_engines
from resolwe.flow.executors import constants
from resolwe.flow.executors.socket_utils import (
    BaseProtocol,
    Message,
    PeerIdentity,
    PeerStatus,
    Response,
    ResponseStatus,
)
from resolwe.flow.executors.zeromq_utils import ZMQCommunicator
from resolwe.flow.managers import consumer
from resolwe.flow.managers.protocol import ExecutorFiles, WorkerProtocol
from resolwe.flow.models import Data, Process, Storage, Worker
from resolwe.flow.utils import iterate_schema
from resolwe.storage import settings as storage_settings
from resolwe.storage.models import AccessLog
from resolwe.test.utils import is_testing
from resolwe.utils import BraceMessage as __

# Register plugins by importing them.
from .basic_commands_plugin import BasicCommands  # noqa: F401
from .init_container_plugin import InitContainerPlugin  # noqa: F401
from .plugin import plugin_manager
from .python_process_plugin import PythonProcess  # noqa: F401

logger = logging.getLogger(__name__)


class Processor:
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
        self.peer_identity = peer_identity
        self.data_id = int(peer_identity)
        self._data: Optional[Data] = None
        self._worker: Optional[Worker] = None
        self._storage_fields: Optional[Set[str]] = None
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

    @property
    def storage_fields(self) -> Set[str]:
        """Get the names of storage fields in schema."""
        if self._storage_fields is None:
            self._storage_fields = {
                field_name
                for schema, _, field_name in iterate_schema(
                    self.data.output, self.data.process.output_schema
                )
                if schema["type"].startswith("basic:json:")
            }
        return self._storage_fields

    def save_storage(self, key: str, content: str, data: Optional[Data] = None):
        """Save storage to data object.

        It is the responsibility of the caller to save the changes to the Data
        object.
        """
        data = data or self.data
        if key in data.output:
            storage = Storage.objects.get(pk=data.output[key])
            storage.json = content
            storage.save()
        else:
            storage = Storage.objects.create(
                json=content,
                name="Storage for data id {}".format(data.pk),
                contributor=data.contributor,
            )
            storage.data.add(data)
            data.output[key] = storage.pk

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
        await database_sync_to_async(self._save_error, thread_sensitive=False)(
            "Processing was cancelled."
        )
        # Ignore the possible timeout.
        with suppress(RuntimeError):
            await self._listener.communicator.send_command(
                Message.command("terminate", ""),
                peer_identity=str(self.data_id).encode(),
                response_timeout=1,
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
        handler = plugin_manager.get_handler(command_name)
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
                result = handler(message, self)
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


class ListenerProtocol(BaseProtocol):
    """Listener protocol implementation."""

    def __init__(
        self,
        hosts: List[str],
        port: int,
        protocol: str,
        zmq_socket: Optional[zmq.asyncio.Socket] = None,
    ):
        """Initialize."""
        if zmq_socket is None:
            zmq_context: zmq.asyncio.Context = zmq.asyncio.Context.instance()
            zmq_socket = zmq_context.socket(zmq.ROUTER)
            zmq_socket.setsockopt(zmq.ROUTER_HANDOVER, 1)
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

        self._parallel_commands_counter = 0
        self._max_parallel_commands = 10
        self._parallel_commands_semaphore: Optional[asyncio.Semaphore] = None
        self._get_program_lock = threading.Lock()

        self._bootstrap_cache: Dict[str, Any] = defaultdict(dict)

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
        record_dict["args"] = tuple(record_dict["args"])

        executors_dir = Path(__file__).parents[1] / "executors"
        record_dict["pathname"] = os.fspath(executors_dir / record_dict["pathname"])
        logger.handle(logging.makeLogRecord(record_dict))
        return message.respond_ok("OK")

    def _bootstrap_prepare_static_cache(self):
        """Prepare cache for bootstrap."""

        def marshal_settings() -> dict:
            """Marshal Django settings into a serializable object.

            :return: The serialized settings.
            :rtype: dict
            """
            result = {}
            for key in dir(settings):
                if any(
                    map(
                        key.startswith,
                        [
                            "FLOW_",
                            "RESOLWE_",
                            "CELERY_",
                            "KUBERNETES_",
                        ],
                    )
                ):
                    result[key] = getattr(settings, key)
            result.update(
                {
                    "USE_TZ": settings.USE_TZ,
                    "FLOW_EXECUTOR_TOOLS_PATHS": self._executor_preparer.get_tools_paths(),
                    "FLOW_STORAGE": storage_settings.FLOW_STORAGE,
                }
            )
            # TODO: this is q&d solution for serializing Path objects.
            return json.loads(json.dumps(result, default=str))

        # Prepare Django settings.
        if "settings" not in self._bootstrap_cache:
            self._bootstrap_cache["settings"] = marshal_settings()
            connectors_settings = copy.deepcopy(storage_settings.STORAGE_CONNECTORS)
            for connector_settings in connectors_settings.values():
                # Fix class name for inclusion in the executor.
                klass = connector_settings["connector"]
                klass = "executors." + klass.rsplit(".storage.")[-1]
                connector_settings["connector"] = klass
                connector_config = connector_settings["config"]
                # Prepare credentials for executor.
                if "credentials" in connector_config:
                    src_credentials = connector_config["credentials"]
                    base_credentials_name = os.path.basename(src_credentials)

                    self._bootstrap_cache["connector_secrets"][
                        base_credentials_name
                    ] = ""
                    if os.path.isfile(src_credentials):
                        with open(src_credentials, "r") as f:
                            self._bootstrap_cache["connector_secrets"][
                                base_credentials_name
                            ] = f.read()
                    connector_config["credentials"] = os.fspath(
                        constants.SECRETS_VOLUME / base_credentials_name
                    )
            self._bootstrap_cache["settings"][
                "STORAGE_CONNECTORS"
            ] = connectors_settings
            self._bootstrap_cache["settings"][
                "FLOW_VOLUMES"
            ] = storage_settings.FLOW_VOLUMES

            # Prepare process meta data.
            self._bootstrap_cache["process_meta"] = {
                k: getattr(Process, k)
                for k in dir(Process)
                if k.startswith("SCHEDULING_CLASS_")
                and isinstance(getattr(Process, k), str)
            }

            self._bootstrap_cache["process"] = dict()

    def bootstrap_prepare_process_cache(self, data: Data):
        """Prepare cache for process with the given id."""
        if data.process_id not in self._bootstrap_cache["process"]:
            self._bootstrap_cache["process"][data.process_id] = model_to_dict(
                data.process
            )
            self._bootstrap_cache["process"][data.process_id][
                "resource_limits"
            ] = data.process.get_resource_limits()

    def handle_bootstrap(self, message: Message[Tuple[int, str]]) -> Response[Dict]:
        """Handle bootstrap request.

        :raises RuntimeError: when settings name is not known.
        """
        data_id, settings_name = message.message_data
        data = Data.objects.get(pk=data_id)
        if is_testing():
            self._bootstrap_cache = defaultdict(dict)
        self._bootstrap_prepare_static_cache()
        response: Dict[str, Any] = dict()
        self.bootstrap_prepare_process_cache(data)

        if settings_name == "executor":
            execution_engine_name = data.process.run.get("language", None)
            execution_engine = self._get_execution_engine(execution_engine_name)
            volume_maps = execution_engine.prepare_volumes()

            response[ExecutorFiles.EXECUTOR_SETTINGS] = {
                "DATA_DIR": data.location.get_path()
            }
            response[ExecutorFiles.LOCATION_SUBPATH] = data.location.subpath
            response[ExecutorFiles.DJANGO_SETTINGS] = self._bootstrap_cache["settings"]
            response[ExecutorFiles.DJANGO_SETTINGS]["RUNTIME_VOLUME_MAPS"] = volume_maps
            response[ExecutorFiles.PROCESS_META] = self._bootstrap_cache["process_meta"]
            response[ExecutorFiles.PROCESS] = self._bootstrap_cache["process"][
                data.process.id
            ]

        elif settings_name == "init":
            response[ExecutorFiles.DJANGO_SETTINGS] = {
                "STORAGE_CONNECTORS": self._bootstrap_cache["settings"][
                    "STORAGE_CONNECTORS"
                ],
                "FLOW_STORAGE": storage_settings.FLOW_STORAGE,
                "FLOW_VOLUMES": storage_settings.FLOW_VOLUMES,
            }
            if hasattr(settings, constants.INPUTS_VOLUME_NAME):
                response[ExecutorFiles.DJANGO_SETTINGS][
                    constants.INPUTS_VOLUME_NAME
                ] = self._bootstrap_cache["settings"][constants.INPUTS_VOLUME_NAME]
            response[ExecutorFiles.SECRETS_DIR] = self._bootstrap_cache[
                "connector_secrets"
            ]
            try:
                response[ExecutorFiles.SECRETS_DIR].update(data.resolve_secrets())
            except PermissionDenied as e:
                data.process_error.append(str(e))
                data.save(update_fields=["process_error"])
                raise
            response[ExecutorFiles.LOCATION_SUBPATH] = data.location.subpath
        elif settings_name == "communication":
            response[ExecutorFiles.DJANGO_SETTINGS] = {
                "STORAGE_CONNECTORS": self._bootstrap_cache["settings"][
                    "STORAGE_CONNECTORS"
                ],
                "FLOW_STORAGE": storage_settings.FLOW_STORAGE,
            }
            response[ExecutorFiles.LOCATION_SUBPATH] = data.location.subpath
        else:
            raise RuntimeError(
                f"Settings {settings_name} sent by peer with id {data_id} unknown."
            )
        return message.respond_ok(response)

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
        # Logging and bootstraping are handled separately.
        if message.command_name in ["log", "bootstrap"]:
            try:
                handler = getattr(self, f"handle_{message.command_name}")
                return await database_sync_to_async(handler, thread_sensitive=False)(
                    message
                )
            except Exception:
                logger.exception(
                    __(
                        "Error handling command {} for peer {}.",
                        message.command_name,
                        peer_identity,
                    )
                )
                return message.respond("Error in handler.", ResponseStatus.ERROR)

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
            response = await database_sync_to_async(
                peer.process_command, thread_sensitive=False
            )(message)
        except:
            logger.exception("Error in process_command method.")
            return message.respond("Error processing command", ResponseStatus.ERROR)
        else:
            return response

    async def _process_command(
        self, peer_identity: PeerIdentity, received_message: Message
    ):
        """Process command."""
        # Executor uses separate identity of the form f"e_{data.id}".
        response_identity = peer_identity
        if b"_" in peer_identity:
            peer_identity = peer_identity.split(b"_")[1]

        if self._parallel_commands_semaphore is None:
            self._parallel_commands_semaphore = asyncio.Semaphore(
                self._max_parallel_commands
            )

        self.logger.debug(
            __("Internal semaphore count: {}", self._parallel_commands_semaphore._value)
        )
        async with self._parallel_commands_semaphore:
            self._parallel_commands_counter += 1
            self.logger.debug(
                __("Processing {} commands", self._parallel_commands_counter)
            )
            response = await self._get_response(peer_identity, received_message)
            try:
                assert received_message.sent_timestamp is not None
                response_time = time.time() - received_message.sent_timestamp
                self.logger.debug(__("Response time: {}", response_time))
                await self.communicator.send_response(response, response_identity)

            except RuntimeError:
                # When unable to send message just log the error. Peer will be
                # removed by the watchdog later on.
                self.logger.exception(
                    __("Protocol: error sending response to {}.", received_message)
                )
            try:
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
            except:
                logger.exception(
                    __("Error processing 'finish' for peer {}", peer_identity)
                )
            finally:
                self._parallel_commands_counter -= 1
                logger.debug(
                    __(
                        "Finished processing command, {} remaining.",
                        self._parallel_commands_counter,
                    )
                )

    async def process_command(
        self, peer_identity: PeerIdentity, received_message: Message
    ):
        """Override process command in socket_utils."""
        # Make it run in the background so another message can be processed.
        asyncio.ensure_future(self._process_command(peer_identity, received_message))

    def get_program(self, data: Data) -> str:
        """Get a program for given data object."""
        # When multiple get_script commands run in parallel the string
        # escaping will be fragile at best. Process this command here
        # without offloading it to the listener.
        with self._get_program_lock:
            execution_engine_name = data.process.run.get("language", None)
            program = [self._get_execution_engine(execution_engine_name).evaluate(data)]
            # TODO: should executor be changed? Definitely for tests (from case to case).
            # Should I use file prepared by the Dispatcher (that takes care of that)?
            env_vars = self.get_executor().get_environment_variables()
            settings_env_vars = {
                "RESOLWE_HOST_URL": getattr(settings, "RESOLWE_HOST_URL", "localhost"),
            }
            additional_env_vars = getattr(settings, "FLOW_EXECUTOR", {}).get(
                "SET_ENV", {}
            )
            env_vars.update(settings_env_vars)
            env_vars.update(additional_env_vars)
            export_commands = [
                "export {}={}".format(key, shlex.quote(value))
                for key, value in env_vars.items()
            ]

            # Paths for Resolwe tools.
            tools_num = len(self._executor_preparer.get_tools_paths())
            tools_paths = ":".join(
                f"/usr/local/bin/resolwe/{i}" for i in range(tools_num)
            )
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
        """Initialize."""
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
