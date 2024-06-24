""".. Ignore pydocstyle D400.

==========
Dispatcher
==========

"""

import asyncio
import json
import logging
import os
import shlex
import shutil
from contextlib import suppress
from importlib import import_module
from pathlib import Path
from typing import List, Optional, Tuple, Union

from channels.db import database_sync_to_async
from channels.exceptions import ChannelFull
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured, PermissionDenied
from django.db import IntegrityError, connection, models, transaction
from django.db.models import OuterRef, Q, Subquery
from django.utils.timezone import now
from zmq import curve_keypair

from resolwe.flow.engine import InvalidEngineError, load_engines
from resolwe.flow.execution_engines import ExecutionError
from resolwe.flow.executors.constants import PROCESSING_VOLUME_NAME
from resolwe.flow.models import Data, DataDependency, Process, Worker
from resolwe.flow.models.utils import referenced_files
from resolwe.storage import settings as storage_settings
from resolwe.storage.connectors import DEFAULT_CONNECTOR_PRIORITY, connectors
from resolwe.storage.models import (
    AccessLog,
    FileStorage,
    ReferencedPath,
    StorageLocation,
)
from resolwe.utils import BraceMessage as __

from . import consumer, state
from .protocol import WorkerProtocol

logger = logging.getLogger(__name__)

DEFAULT_CONNECTOR = "resolwe.flow.managers.workload_connectors.local"


class SettingsJSONifier(json.JSONEncoder):
    """Customized JSON encoder, coercing all unknown types into strings.

    Needed due to the class hierarchy coming out of the database,
    which can't be serialized using the vanilla json encoder.
    """

    def default(self, o):
        """Try default; otherwise, coerce the object into a string."""
        try:
            return super().default(o)
        except TypeError:
            return str(o)


class Manager:
    """The manager handles process job dispatching.

    Each :class:`~resolwe.flow.models.Data` object that's still waiting
    to be resolved is dispatched to a concrete workload management
    system (such as Celery or SLURM). The specific manager for that
    system (descended from
    :class:`~resolwe.flow.managers.workload_connectors.base.BaseConnector`)
    then handles actual job setup and submission. The job itself is an
    executor invocation; the executor then in turn sets up a safe and
    well-defined environment within the workload manager's task in which
    the process is finally run.
    """

    def discover_engines(self):
        """Discover configured engines.

        :param executor: Optional executor module override
        """
        executor = getattr(settings, "FLOW_EXECUTOR", {}).get(
            "NAME", "resolwe.flow.executors.local"
        )
        self.executor = self.load_executor(executor)
        logger.info(
            __(
                "Loaded '{}' executor.",
                str(self.executor.__class__.__module__).replace(".prepare", ""),
            )
        )

        expression_engines = getattr(
            settings,
            "FLOW_EXPRESSION_ENGINES",
            ["resolwe.flow.expression_engines.jinja"],
        )
        self.expression_engines = self.load_expression_engines(expression_engines)
        logger.info(
            __(
                "Found {} expression engines: {}",
                len(self.expression_engines),
                ", ".join(self.expression_engines.keys()),
            )
        )

        execution_engines = getattr(
            settings, "FLOW_EXECUTION_ENGINES", ["resolwe.flow.execution_engines.bash"]
        )
        self.execution_engines = self.load_execution_engines(execution_engines)
        logger.info(
            __(
                "Found {} execution engines: {}",
                len(self.execution_engines),
                ", ".join(self.execution_engines.keys()),
            )
        )

    async def drain_messages(self):
        """Drain Django Channel messages."""
        await consumer.run_consumer(timeout=1)

    def __init__(self, *args, **kwargs):
        """Initialize arguments."""
        self.discover_engines()
        # Value is the indicator whether we are running in sync mode.
        self._sync_finished_event = None
        # The couter for number of messages that are being processed.
        # Used with sync mode to know when to remove the runtime
        # barrier.
        self._messages_processing = 0

        # Ensure there is only one manager instance per process. This
        # is required as other parts of the code access the global
        # manager instance.
        with suppress(ImportError):
            from resolwe.flow import managers

            assert not hasattr(managers, "manager")

        self.scheduling_class_map = dict(Process.SCHEDULING_CLASS_CHOICES)

        # Check settings for consistency.
        flow_manager = getattr(settings, "FLOW_MANAGER", {})
        if "DISPATCHER_MAPPING" in flow_manager and "NAME" in flow_manager:
            raise ImproperlyConfigured(
                "Key 'DISPATCHER_MAPPING' conflicts with key 'NAME' in FLOW_MANAGER settings."
            )

        if "DISPATCHER_MAPPING" in flow_manager:
            mapping = flow_manager["DISPATCHER_MAPPING"]

            scheduling_classes = set(self.scheduling_class_map.values())
            map_keys = set(mapping.keys())
            class_difference = scheduling_classes - map_keys
            if class_difference:
                raise ImproperlyConfigured(
                    "Dispatcher manager mapping settings incomplete, missing {}.".format(
                        class_difference
                    )
                )
            connector_list = [mapping[klass] for klass in scheduling_classes]
        else:
            connector_list = [flow_manager.get("NAME", DEFAULT_CONNECTOR)]

        # Store the whitelist and blacklist for later use.
        self._processes_allow = getattr(settings, "FLOW_PROCESSES_ALLOW_LIST", None)
        self._processes_ignore = getattr(settings, "FLOW_PROCESSES_IGNORE_LIST", None)

        # Pre-load all needed connectors.
        self.connectors = {}
        for module_name in connector_list:
            connector_module = import_module(module_name)
            self.connectors[module_name] = connector_module.Connector()

        logger.info(
            __(
                "Found {} workload connectors: {}",
                len(self.connectors),
                ", ".join(self.connectors.keys()),
            )
        )

        super().__init__(*args, **kwargs)

    def _include_environment_variables(self, program: str, executor_vars: dict) -> str:
        """Include environment variables in program."""
        env_vars = {
            "RESOLWE_HOST_URL": getattr(settings, "RESOLWE_HOST_URL", "localhost"),
        }
        set_env = getattr(settings, "FLOW_EXECUTOR", {}).get("SET_ENV", {})
        env_vars.update(executor_vars)
        env_vars.update(set_env)

        export_commands = [
            "export {}={}".format(key, shlex.quote(value))
            for key, value in env_vars.items()
        ]
        return os.linesep.join(export_commands) + os.linesep + program

    def run(self, data: Data, argv: List):
        """Select a concrete connector and run the process through it.

        :param data: The :class:`~resolwe.flow.models.Data` object that
            is to be run.
        :param argv: The argument vector used to spawn the executor.
        """
        process_scheduling = self.scheduling_class_map[data.process.scheduling_class]
        if "DISPATCHER_MAPPING" in getattr(settings, "FLOW_MANAGER", {}):
            class_name = settings.FLOW_MANAGER["DISPATCHER_MAPPING"][process_scheduling]
        else:
            class_name = getattr(settings, "FLOW_MANAGER", {}).get(
                "NAME", DEFAULT_CONNECTOR
            )

        data.scheduled = now()
        data.save(update_fields=["scheduled"])

        workload_class = class_name.rsplit(".", maxsplit=1)[1]
        host, port, protocol = self._get_listener_settings(data, workload_class)
        argv[-1] += " {} {} {}".format(host, port, protocol)

        return self.connectors[class_name].submit(data, argv)

    def _get_data_connector_name(self) -> str:
        """Return storage connector that will be used for new data object.

        The current implementation returns the connector with the lowest
        priority.
        """
        return min(
            (connector.priority, connector.name) for connector in connectors.values()
        )[1]

    def _prepare_data_dir(self, data: Data):
        """Prepare destination directory where the data will live.

        :param data: The :class:`~resolwe.flow.models.Data` object for
            which to prepare the private execution directory.
        :return: The prepared data directory path.
        :rtype: str
        """
        logger.debug(__("Preparing data directory for Data with id {}.", data.id))
        connector_name = self._get_data_connector_name()
        with transaction.atomic():
            # Create Worker object and set its status to preparing if needed.
            if not Worker.objects.filter(data=data).exists():
                public_key, private_key = curve_keypair()
                Worker.objects.get_or_create(
                    data=data,
                    status=Worker.STATUS_PREPARING,
                    public_key=public_key,
                    private_key=private_key,
                )

            file_storage = FileStorage.objects.create()
            # Data produced by the processing container will be uploaded to the
            # created location.
            data_location = StorageLocation.objects.create(
                file_storage=file_storage,
                url=str(file_storage.id),
                status=StorageLocation.STATUS_PREPARING,
                connector_name=connector_name,
            )
            file_storage.data.add(data)

            # Reference 'special' files.
            for file_ in referenced_files(data, include_descriptor=False):
                referenced_path = ReferencedPath.objects.create(path=file_)
                referenced_path.storage_locations.add(data_location)

        dir_mode = getattr(settings, "FLOW_EXECUTOR", {}).get("DATA_DIR_MODE", 0o755)
        connectors[connector_name].prepare_url(data_location.url, dir_mode=dir_mode)

    def _get_listener_settings(
        self, data: Data, workload_class_name: str
    ) -> Tuple[str, int, str]:
        """Return the listener address, port and protocol."""
        listener_settings = getattr(settings, "FLOW_EXECUTOR", {}).get(
            "LISTENER_CONNECTION", {}
        )
        container_listener_connection = getattr(
            settings,
            "COMMUNICATION_CONTAINER_LISTENER_CONNECTION",
            {"local": "172.17.0.1"},
        )

        # Return address associated with the key workload_class_name or first
        # one if the key is not present.
        if workload_class_name in container_listener_connection:
            host = container_listener_connection[workload_class_name]
        else:
            host = next(iter(container_listener_connection.values()))
        return (
            host,
            listener_settings.get("port", 53893),
            listener_settings.get("protocol", "tcp"),
        )

    async def handle_control_event(self, message: dict):
        """Handle the control event.

        The method is called from the channels layer when there is nome change
        either in the state of the Data object of the executors have finished
        with processing.

        When running in sync state check that all database objects are in
        final state before raising the execution_barrier.

        Channels layer callback, do not call directly.
        """

        def workers_finished():
            unfinished_workers = Worker.objects.exclude(
                status__in=Worker.FINAL_STATUSES
            )
            return not (unfinished_workers.exists())

        cmd = message[WorkerProtocol.COMMAND]
        logger.debug(__("Manager worker got channel command '{}'.", message))

        try:
            if cmd == WorkerProtocol.COMMUNICATE:
                await database_sync_to_async(self._data_scan, thread_sensitive=False)(
                    **message[WorkerProtocol.COMMUNICATE_EXTRA]
                )

            def purge_secrets_and_local_data(data_id: int) -> Data:
                """Purge secrets and return the Data object.

                :raises Data.DoesNotExist: when Data object does not exist.
                :raises FileStorage.DoesNotExist: when FileStorage object does not
                    exist.

                """
                data = Data.objects.get(pk=data_id)
                with suppress(Worker.DoesNotExist):
                    worker = Worker.objects.get(data=data)
                    worker.status = Worker.STATUS_COMPLETED
                    worker.save()

                subpath = FileStorage.objects.get(data__id=data_id).subpath
                volume = storage_settings.FLOW_VOLUMES[PROCESSING_VOLUME_NAME]
                if volume["type"] == "host_path":
                    processing_dir = Path(volume["config"]["path"]) / subpath
                    if processing_dir.is_dir():
                        shutil.rmtree(processing_dir)

                return data

            if cmd in [WorkerProtocol.FINISH, WorkerProtocol.ABORT]:
                self._messages_processing += 1
                data_id = message.get(WorkerProtocol.DATA_ID)
                if data_id is not None:
                    with suppress(
                        Data.DoesNotExist,
                        Process.DoesNotExist,
                        FileStorage.DoesNotExist,
                        AttributeError,
                    ):
                        await database_sync_to_async(
                            purge_secrets_and_local_data, thread_sensitive=False
                        )(data_id)
        except Exception:
            logger.exception(
                "Unknown error occured while processing communicate control command."
            )
            raise
        finally:
            self._messages_processing -= 1
            # No additional messages are being procesed.
            # Check if workers are finished with the processing.
            logger.debug(__("Dispatcher checking final condition"))
            logger.debug(__("Message counter: {}", self._messages_processing))

            if self._sync_finished_event is not None and self._messages_processing == 0:
                if await database_sync_to_async(
                    workers_finished, thread_sensitive=False
                )():
                    self._sync_finished_event.set()

    async def execution_barrier(self):
        """Wait for executors to finish.

        At least one must finish after this point to avoid a deadlock.
        """
        consumer_future = asyncio.ensure_future(consumer.run_consumer())
        assert self._sync_finished_event is not None
        await self._sync_finished_event.wait()
        self._sync_finished_event = None
        await consumer.exit_consumer()
        await consumer_future

    async def communicate(self, data_id=None, run_sync=False):
        """Scan database for resolving Data objects and process them.

        This is submitted as a task to the manager's channel workers.

        :param data_id: Optional id of Data object which (+ its
            children) should be processes. If it is not given, all
            resolving objects are processed.
        :param run_sync: If ``True``, wait until all processes spawned
            from this point on have finished processing.
        """
        first_sync_call = False
        if run_sync and self._sync_finished_event is None:
            first_sync_call = True
            self._sync_finished_event = asyncio.Event()

        self._messages_processing += 1
        logger.debug(
            __(
                "Manager sending communicate command on '{}' triggered by Data with id {}.",
                state.MANAGER_CONTROL_CHANNEL,
                data_id,
            )
        )
        try:
            await consumer.send_event(
                {
                    WorkerProtocol.COMMAND: WorkerProtocol.COMMUNICATE,
                    WorkerProtocol.COMMUNICATE_EXTRA: {
                        "data_id": data_id,
                    },
                }
            )
        except ChannelFull:
            logger.exception(
                "ChannelFull error occurred while sending communicate message."
            )
        except:
            logger.exception(
                "Unknown error occurred while sending communicate message."
            )

        if first_sync_call:
            logger.debug(
                __(
                    "Manager on channel '{}' entering synchronization block.",
                    state.MANAGER_CONTROL_CHANNEL,
                )
            )
            await self.execution_barrier()
            logger.debug(
                __(
                    "Manager on channel '{}' exiting synchronization block.",
                    state.MANAGER_CONTROL_CHANNEL,
                )
            )

    def _lock_inputs_local_storage_locations(self, data: Data):
        """Lock storage locations for inputs.

        Lock storage locations of inputs so they are not deleted while data
        object is processing.
        """
        data_connectors = connectors.for_storage("data")
        mountable_data_connectors = [
            connector for connector in data_connectors if connector.mountable
        ]
        priority_range = data_connectors[-1].priority - data_connectors[0].priority + 1

        connector_priorities = {
            connector.name: connector.priority for connector in data_connectors
        }

        # Prefer mountable locations but keep their relations intact.
        for connector in mountable_data_connectors:
            connector_priorities[connector.name] -= priority_range

        whens = [
            models.When(connector_name=connector_name, then=priority)
            for connector_name, priority in connector_priorities.items()
        ]

        storage_location_subquery = (
            StorageLocation.objects.filter(file_storage_id=OuterRef("file_storage_id"))
            .annotate(
                priority=models.Case(
                    *whens,
                    default=DEFAULT_CONNECTOR_PRIORITY,
                    output_field=models.IntegerField(),
                )
            )
            .order_by("priority")
            .values_list("id", flat=True)[:1]
        )

        file_storages = (
            DataDependency.objects.filter(child=data, kind=DataDependency.KIND_IO)
            .values_list("parent__location", flat=True)
            .distinct()
        )

        storage_locations = (
            StorageLocation.objects.filter(file_storage__in=file_storages)
            .filter(pk__in=Subquery(storage_location_subquery))
            .values_list("id", flat=True)
        )

        AccessLog.objects.bulk_create(
            [
                AccessLog(
                    storage_location_id=storage_location,
                    reason="Input for data with id {}".format(data.id),
                    cause=data,
                )
                for storage_location in storage_locations
            ]
        )

    def _data_execute(self, data: Data):
        """Execute the Data object.

        The activities carried out here include target directory
        preparation, executor copying, setting serialization and actual
        execution of the object.

        :param data: The :class:`~resolwe.flow.models.Data` object to
            execute.
        """
        logger.debug(__("Manager preparing Data with id {} for processing.", data.id))

        # Prepare the executor's environment.
        try:
            self._prepare_data_dir(data)

            executor_module = ".{}".format(
                getattr(settings, "FLOW_EXECUTOR", {})
                .get("NAME", "resolwe.flow.executors.local")
                .rpartition(".executors.")[-1]
            )
            self._lock_inputs_local_storage_locations(data)

            argv = [
                "/bin/sh",
                "-c",
                getattr(settings, "FLOW_EXECUTOR", {}).get(
                    "PYTHON", "/usr/bin/env python"
                )
                + " -m executors "
                + executor_module
                + " {}".format(data.pk),
            ]
            self.executor.prepare_for_execution(data)
        except PermissionDenied as error:
            data.status = Data.STATUS_ERROR
            data.process_error.append("Permission denied for process: {}".format(error))
            data.save()
            if hasattr(data, "worker"):
                data.worker.status = Worker.STATUS_ERROR_PREPARING
                data.worker.save()
            return
        except OSError as err:
            logger.exception(
                __(
                    "OSError occurred while preparing data {} (will skip): {}",
                    data.id,
                    err,
                )
            )
            if hasattr(data, "worker"):
                data.worker.status = Worker.STATUS_ERROR_PREPARING
                data.worker.save()
            return

        # Hand off to the run() method for execution.
        logger.info(__("Running executor for data with id {}", data.pk))
        self.run(data, argv)

    def _data_scan(self, data_id: Optional[int] = None, **kwargs):
        """Scan for new Data objects and execute them.

        :param data_id: Optional id of Data object which (+ its
            children) should be scanned. If it is not given, all
            resolving objects are processed.
        :param executor: The fully qualified name of the executor to use
            for all :class:`~resolwe.flow.models.Data` objects
            discovered in this pass.
        """

        def process_data_object(data: Data):
            """Process a single data object."""
            # Lock for update. Note that we want this transaction to be as short as possible in
            # order to reduce contention and avoid deadlocks. This is why we do not lock all
            # resolving objects for update, but instead only lock one object at a time. This
            # allows managers running in parallel to process different objects.
            data = Data.objects.select_for_update().get(pk=data.pk)
            if data.status != Data.STATUS_RESOLVING:
                # The object might have already been processed while waiting for the lock to be
                # obtained. In this case, skip the object.
                return

            dep_status = data.dependency_status()

            if dep_status == Data.STATUS_ERROR:
                data.status = Data.STATUS_ERROR
                data.process_error.append("One or more inputs have status ERROR")
                data.process_rc = 1
                data.save()
                if hasattr(data, "worker"):
                    data.worker.status = Worker.STATUS_ERROR_PREPARING
                    data.worker.save(update_fields=["status"])

                return

            elif dep_status != Data.STATUS_DONE:
                return

            run_in_executor = False
            if data.process.run:
                try:
                    # Check if execution engine is sound and evaluate workflow.
                    execution_engine_name = data.process.run.get("language", None)
                    execution_engine = self.get_execution_engine(execution_engine_name)
                    run_in_executor = execution_engine_name != "workflow"
                    if not run_in_executor:
                        execution_engine.evaluate(data)
                    else:
                        # Set allocated resources
                        resource_limits = data.get_resource_limits()
                        data.process_memory = resource_limits["memory"]
                        data.process_cores = resource_limits["cores"]

                except (ExecutionError, InvalidEngineError) as error:
                    data.status = Data.STATUS_ERROR
                    data.process_error.append(
                        "Error in process script: {}".format(error)
                    )
                    data.save()
                    if hasattr(data, "worker"):
                        data.worker.status = Worker.STATUS_ERROR_PREPARING
                        data.worker.save(update_fields=["status"])

                    return
            if data.status != Data.STATUS_DONE:
                # The data object may already be marked as done by the execution engine. In this
                # case we must not revert the status to STATUS_WAITING.
                data.status = Data.STATUS_WAITING
            data.save(render_name=True)

            # Actually run the object only if there was nothing with the
            # transaction and was not already evaluated.
            if run_in_executor:
                transaction.on_commit(
                    # Make sure the closure gets the right values here, since they're
                    # changed in the loop.
                    lambda d=data: self._data_execute(d)
                )

        logger.debug(
            __(
                "Manager processing data scan triggered by Data with id {}.",
                data_id,
            )
        )

        try:
            queryset = Data.objects.filter(status=Data.STATUS_RESOLVING)
            # Check if process is in the whitelist or blacklist. The blacklist has
            # priority.
            if self._processes_allow:
                queryset = queryset.filter(process__slug__in=self._processes_allow)
            if self._processes_ignore:
                queryset = queryset.exclude(process__slug__in=self._processes_ignore)

            if data_id is not None:
                # Scan only given data object and its children.
                queryset = queryset.filter(
                    Q(parents=data_id) | Q(id=data_id)
                ).distinct()

            for data in queryset:
                try:
                    with transaction.atomic():
                        process_data_object(data)

                        # All data objects created by the execution engine are commited after this
                        # point and may be processed by other managers running in parallel. At the
                        # same time, the lock for the current data object is released.
                except Exception as error:
                    logger.exception(
                        __(
                            "Unhandled exception in _data_scan while processing data object {}.",
                            data.pk,
                        )
                    )

                    # Unhandled error while processing a data object. We must set its
                    # status to STATUS_ERROR to prevent the object from being retried
                    # on next _data_scan run. We must perform this operation without
                    # using the Django ORM as using the ORM may be the reason the error
                    # occurred in the first place.
                    # Note that this has a side effect: since signals are not emitted,
                    # the data object is not processed and its children are not
                    # transitioned into the error state.
                    error_msg = "Internal error: {}".format(error)
                    process_error_field = Data._meta.get_field("process_error")
                    max_length = process_error_field.base_field.max_length
                    if len(error_msg) > max_length:
                        error_msg = error_msg[: max_length - 3] + "..."

                    try:
                        with connection.cursor() as cursor:
                            cursor.execute(
                                """
                                    UPDATE {table}
                                    SET
                                        status = %(status)s,
                                        process_error = process_error || (%(error)s)::varchar[]
                                    WHERE id = %(id)s
                                """.format(
                                    table=Data._meta.db_table
                                ),
                                {
                                    "status": Data.STATUS_ERROR,
                                    "error": [error_msg],
                                    "id": data.pk,
                                },
                            )
                        self.communicate(data_id=data.pk)
                    except Exception:
                        # If object's state cannot be changed due to some database-related
                        # issue, at least skip the object for this run.
                        logger.exception(
                            __(
                                "Unhandled exception in _data_scan while trying to emit error for {}.",
                                data.pk,
                            )
                        )

        except IntegrityError as exp:
            logger.error(__("IntegrityError in manager {}", exp))
            return

    def get_executor(self):
        """Return an executor instance."""
        return self.executor

    def get_expression_engine(self, name: str):
        """Return an expression engine instance."""
        try:
            return self.expression_engines[name]
        except KeyError:
            raise InvalidEngineError("Unsupported expression engine: {}".format(name))

    def get_execution_engine(self, name: str):
        """Return an execution engine instance."""
        try:
            return self.execution_engines[name]
        except KeyError:
            raise InvalidEngineError("Unsupported execution engine: {}".format(name))

    def load_executor(self, executor_name: str):
        """Load process executor."""
        executor_name = executor_name + ".prepare"
        module = import_module(executor_name)
        return module.FlowExecutorPreparer()  # type: ignore

    def load_expression_engines(self, engines: List[Union[dict, str]]):
        """Load expression engines."""
        return load_engines(self, "ExpressionEngine", "expression_engines", engines)

    def load_execution_engines(self, engines: List[Union[dict, str]]):
        """Load execution engines."""
        return load_engines(self, "ExecutionEngine", "execution_engines", engines)
