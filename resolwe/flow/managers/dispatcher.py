""".. Ignore pydocstyle D400.

==========
Dispatcher
==========

"""
import asyncio
import copy
import inspect
import json
import logging
import os
import shlex
import shutil
from contextlib import suppress
from importlib import import_module
from pathlib import Path, PurePath
from typing import Dict, List, Optional, Tuple, Union

from asgiref.sync import async_to_sync
from channels.db import database_sync_to_async
from channels.exceptions import ChannelFull

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured, PermissionDenied
from django.db import IntegrityError, connection, models, transaction
from django.db.models import OuterRef, Q, Subquery
from django.utils.timezone import now

from resolwe.flow.engine import InvalidEngineError, load_engines
from resolwe.flow.execution_engines import ExecutionError
from resolwe.flow.executors.constants import DATA_ALL_VOLUME, SECRETS_VOLUME
from resolwe.flow.models import Data, DataDependency, Process, Worker
from resolwe.flow.models.utils import referenced_files
from resolwe.storage.connectors import DEFAULT_CONNECTOR_PRIORITY, connectors
from resolwe.storage.models import (
    AccessLog,
    FileStorage,
    ReferencedPath,
    StorageLocation,
)
from resolwe.storage.settings import STORAGE_CONNECTORS, STORAGE_LOCAL_CONNECTOR
from resolwe.utils import BraceMessage as __

from . import consumer, state
from .protocol import ExecutorFiles, WorkerProtocol

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


def dependency_status(data):
    """Return abstracted status of dependencies.

    - ``STATUS_ERROR`` .. one dependency has error status or was deleted
    - ``STATUS_DONE`` .. all dependencies have done status
    - ``None`` .. other

    """
    parents_statuses = set(
        DataDependency.objects.filter(child=data, kind=DataDependency.KIND_IO)
        .distinct("parent__status")
        .values_list("parent__status", flat=True)
    )

    if not parents_statuses:
        return Data.STATUS_DONE

    if None in parents_statuses:
        # Some parents have been deleted.
        return Data.STATUS_ERROR

    if Data.STATUS_ERROR in parents_statuses:
        return Data.STATUS_ERROR

    if len(parents_statuses) == 1 and Data.STATUS_DONE in parents_statuses:
        return Data.STATUS_DONE

    return None


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

    def drain_messages(self):
        """Drain Django Channel messages."""
        async_to_sync(consumer.run_consumer)(timeout=1)

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

    def _marshal_settings(self) -> dict:
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
        # TODO: this is q&d solution for serializing Path objects.
        return json.loads(json.dumps(result, default=str))

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

    def run(self, data: Data, runtime_dir: Path, argv):
        """Select a concrete connector and run the process through it.

        :param data: The :class:`~resolwe.flow.models.Data` object that
            is to be run.
        :param runtime_dir: The directory the executor is run from.
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
        return self.connectors[class_name].submit(data, os.fspath(runtime_dir), argv)

    def _get_per_data_dir(
        self, dir_base: Union[PurePath, str], subpath: Union[PurePath, str]
    ) -> Path:
        """Extend the given base directory with a per-data component.

        The method creates a private path for the
        :class:`~resolwe.flow.models.Data` object, such as::

            ./test_data/1/

        if ``base_dir`` is ``'./test_data'`` and ``subpath`` is ``1``.

        :param dir_base: The base path to be extended. This will usually
            be one of the directories configured in the
            ``FLOW_EXECUTOR`` setting.
        :param subpath: Objects's subpath used for the extending.
        :return: The new path for the :class:`~resolwe.flow.models.Data`
            object.
        :rtype: str
        """
        result = Path(getattr(settings, "FLOW_EXECUTOR", {}).get(dir_base, ""))
        return result / subpath

    def _get_upload_connector_name(self) -> str:
        """Return storage connector that will be used for new data object.

        The current implementation returns the connector with the lowest
        priority.
        """
        return min(
            (connector.priority, connector.name) for connector in connectors.values()
        )[1]

    def _prepare_data_dir(self, data: Data) -> Path:
        """Prepare destination directory where the data will live.

        :param data: The :class:`~resolwe.flow.models.Data` object for
            which to prepare the private execution directory.
        :return: The prepared data directory path.
        :rtype: str
        """
        logger.debug(__("Preparing data directory for Data with id {}.", data.id))
        with transaction.atomic():
            # Create Worker object and set its status to preparing if needed.
            if not Worker.objects.filter(data=data).exists():
                Worker.objects.get_or_create(data=data, status=Worker.STATUS_PREPARING)

            file_storage = FileStorage.objects.create()
            # Data produced by the processing container will be uploaded to the
            # created location.
            data_location = StorageLocation.objects.create(
                file_storage=file_storage,
                url=str(file_storage.id),
                status=StorageLocation.STATUS_PREPARING,
                connector_name=self._get_upload_connector_name(),
            )
            file_storage.data.add(data)

            # Reference 'special' files.
            for file_ in referenced_files(data, include_descriptor=False):
                referenced_path = ReferencedPath.objects.create(path=file_)
                referenced_path.storage_locations.add(data_location)

        output_path = self._get_per_data_dir("DATA_DIR", data_location.url)
        logger.debug(__("Dispatcher creating data dir {}.", output_path))
        logger.debug(
            __(
                "Prepared location {} for data with id {}: {}.",
                data.location,
                data.id,
                output_path,
            )
        )
        dir_mode = getattr(settings, "FLOW_EXECUTOR", {}).get("DATA_DIR_MODE", 0o755)
        output_path.mkdir(mode=dir_mode, parents=True)
        return output_path

    def _prepare_context(self, data: Data, data_dir: Path, runtime_dir: Path, **kwargs):
        """Prepare settings and constants JSONs for the executor.

        Settings and constants provided by other ``resolwe`` modules and
        :class:`~django.conf.settings` are all inaccessible in the
        executor once it is deployed, so they need to be serialized into
        the runtime directory.

        :param data: The :class:`~resolwe.flow.models.Data` object
            being prepared for.
        :param data_dir: The target execution directory for this
            :class:`~resolwe.flow.models.Data` object.
        :param runtime_dir: The target runtime support directory for
            this :class:`~resolwe.flow.models.Data` object; this is
            where the environment is serialized into.
        :param kwargs: Extra settings to include in the main settings
            file.
        """
        files = {}
        secrets = {}
        data_id = data.id
        secrets_dir = runtime_dir / ExecutorFiles.SECRETS_DIR
        container_secrets_dir = SECRETS_VOLUME

        settings_dict = {}
        settings_dict["DATA_DIR"] = os.fspath(data_dir)
        files[ExecutorFiles.EXECUTOR_SETTINGS] = settings_dict

        logger.debug(__("Preparing context for data with id {}.", data_id))
        logger.debug(__("Data {} location id: {}.", data_id, data.location))
        django_settings = {}
        django_settings.update(self._marshal_settings())
        django_settings.update(kwargs)
        files[ExecutorFiles.DJANGO_SETTINGS] = django_settings

        # Add scheduling classes.
        files[ExecutorFiles.PROCESS_META] = {
            k: getattr(Process, k)
            for k in dir(Process)
            if k.startswith("SCHEDULING_CLASS_")
            and isinstance(getattr(Process, k), str)
        }

        # Add Data status constants.
        files[ExecutorFiles.DATA_META] = {
            k: getattr(Data, k)
            for k in dir(Data)
            if k.startswith("STATUS_") and isinstance(getattr(Data, k), str)
        }

        # Prepare storage connectors settings and secrets.
        connectors_settings = copy.deepcopy(STORAGE_CONNECTORS)
        # Local connector in executor in always named 'local'.
        connectors_settings["local"] = connectors_settings.pop(STORAGE_LOCAL_CONNECTOR)
        # Download is done inside container to a DATA_ALL_VOLUME.
        connectors_settings["local"]["config"]["path"] = os.fspath(DATA_ALL_VOLUME)
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
                secrets[base_credentials_name] = ""
                if os.path.isfile(src_credentials):
                    with open(src_credentials, "r") as f:
                        secrets[base_credentials_name] = f.read()
                connector_config["credentials"] = os.fspath(
                    container_secrets_dir / base_credentials_name
                )
        django_settings["STORAGE_CONNECTORS"] = connectors_settings
        django_settings["UPLOAD_CONNECTOR_NAME"] = self._get_upload_connector_name()

        # Extend the settings with whatever the executor wants.
        logger.debug(
            __("Extending settings for data {}, location {}.", data_id, data.location)
        )
        self.executor.extend_settings(data_id, files, secrets)

        # Save the settings into the various files in the settings subdir of
        # the runtime dir.
        settings_subdir = ExecutorFiles.SETTINGS_SUBDIR
        (runtime_dir / settings_subdir).mkdir(exist_ok=True)

        settings_dict[ExecutorFiles.FILE_LIST_KEY] = list(files.keys())
        for file_name in files:
            file_path = runtime_dir / settings_subdir / file_name
            with file_path.open("wt") as json_file:
                json.dump(files[file_name], json_file, cls=SettingsJSONifier)

        # Save the secrets in the runtime dir, with permissions to prevent listing the given
        # directory.
        logger.debug(__("Creating secrets dir: {}.", os.fspath(secrets_dir)))
        secrets_dir.mkdir(mode=0o700)
        for file_name, value in secrets.items():
            file_path = secrets_dir / file_name

            # Set umask to 0 to ensure that we set the correct permissions.
            old_umask = os.umask(0)
            try:
                # We need to use os.open in order to correctly enforce file creation. Otherwise,
                # there is a race condition which can be used to create the file with different
                # ownership/permissions.
                file_descriptor = os.open(
                    file_path, os.O_WRONLY | os.O_CREAT, mode=0o600
                )
                with os.fdopen(file_descriptor, "w") as raw_file:
                    raw_file.write(value)
            finally:
                os.umask(old_umask)

    def _prepare_executor(self, data: Data) -> Tuple[str, Path]:
        """Copy executor sources into the destination directory.

        :param data: The :class:`~resolwe.flow.models.Data` object being
            prepared for.
        :param executor: The fully qualified name of the executor that
            is to be used for this data object.
        :return: Tuple containing the relative fully qualified name of
            the executor class ('relative' to how the executor will be
            run) and the path to the directory where the executor will
            be deployed.
        :rtype: (str, str)
        """
        logger.debug(__("Preparing executor for Data with id {}", data.id))

        # Both of these imports are here only to get the packages' paths.
        import resolwe.flow.executors as executor_package

        source_file = inspect.getsourcefile(executor_package)
        assert source_file is not None
        exec_dir = Path(source_file).parent
        dest_dir = self._get_per_data_dir(
            "RUNTIME_DIR", data.location.default_storage_location.subpath
        )
        dest_package_dir = dest_dir / "executors"
        shutil.copytree(exec_dir, dest_package_dir)
        dir_mode = getattr(settings, "FLOW_EXECUTOR", {}).get("RUNTIME_DIR_MODE", 0o755)
        dest_dir.chmod(dir_mode)

        executor = getattr(settings, "FLOW_EXECUTOR", {}).get(
            "NAME", "resolwe.flow.executors.local"
        )
        class_name = executor.rpartition(".executors.")[-1]
        return ".{}".format(class_name), dest_dir

    def _prepare_storage_connectors(self, runtime_dir: Path):
        """Copy connectors inside executors package."""
        import resolwe.storage.connectors as connectors_package

        source_file = inspect.getsourcefile(connectors_package)
        assert source_file is not None
        exec_dir = Path(source_file).parent
        dest_dir = runtime_dir / "executors"
        dest_package_dir = dest_dir / "connectors"
        shutil.copytree(exec_dir, dest_package_dir)
        dir_mode = getattr(settings, "FLOW_EXECUTOR", {}).get("RUNTIME_DIR_MODE", 0o755)
        dest_dir.chmod(dir_mode)

    def _prepare_script(self, dest_dir: Path, program: str) -> str:
        """Copy the script into the destination directory.

        :param dest_dir: The target directory where the script will be
            saved.
        :param program: The script text to be saved.
        :return: The name of the script file.
        :rtype: str
        """
        script_name = ExecutorFiles.PROCESS_SCRIPT
        dest_file = dest_dir / script_name
        with dest_file.open("wt") as dest_file_obj:
            dest_file_obj.write(program)
        dest_file.chmod(0o700)
        return script_name

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
                await database_sync_to_async(self._data_scan)(
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
                data_location = FileStorage.objects.get(
                    data__id=data_id
                ).default_storage_location

                def handle_error(func, path, exc_info):
                    """Handle permission errors while removing data directories."""
                    if isinstance(exc_info[1], PermissionError):
                        with suppress(FileNotFoundError):
                            os.chmod(path, 0o700)
                            shutil.rmtree(path)

                secrets_dir = (
                    self._get_per_data_dir(
                        "RUNTIME_DIR",
                        str(data_location.subpath),
                    )
                    / ExecutorFiles.SECRETS_DIR
                )
                shutil.rmtree(secrets_dir, onerror=handle_error)

                local_data = self._get_per_data_dir(
                    "DATA_DIR", str(data_location.subpath) + "_work"
                )
                if local_data.is_dir():
                    shutil.rmtree(local_data)

                return data

            def cleanup(data: Data):
                """Run the cleanup."""
                process_scheduling = self.scheduling_class_map[
                    data.process.scheduling_class
                ]
                if "DISPATCHER_MAPPING" in getattr(settings, "FLOW_MANAGER", {}):
                    class_name = settings.FLOW_MANAGER["DISPATCHER_MAPPING"][
                        process_scheduling
                    ]
                else:
                    class_name = getattr(settings, "FLOW_MANAGER", {}).get(
                        "NAME", DEFAULT_CONNECTOR
                    )
                self.connectors[class_name].cleanup(data_id)

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
                        data = await database_sync_to_async(
                            purge_secrets_and_local_data
                        )(data_id)
                        # Run the cleanup.
                        await database_sync_to_async(cleanup)(data)
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
                if await database_sync_to_async(workers_finished)():
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
        connector_priorities = {
            connector_name: connectors[connector_name].priority
            for connector_name in connectors
        }
        connector_priorities[STORAGE_LOCAL_CONNECTOR] = -1
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

    def _data_execute(self, data: Data, program: str):
        """Execute the Data object.

        The activities carried out here include target directory
        preparation, executor copying, setting serialization and actual
        execution of the object.

        :param data: The :class:`~resolwe.flow.models.Data` object to
            execute.
        :param program: The process text the manager got out of
            execution engine evaluation.
        :param executor: The executor to use for this object.
        """
        # Notify dispatcher if there is nothing to do so it can check whether
        # conditions for raising runtime barrier are fulfilled.
        if not program:
            return

        logger.debug(__("Manager preparing Data with id {} for processing.", data.id))

        # Prepare the executor's environment.
        try:
            executor_env_vars = self.get_executor().get_environment_variables()
            program = self._include_environment_variables(program, executor_env_vars)
            data_dir = self._prepare_data_dir(data)
            executor_module, runtime_dir = self._prepare_executor(data)
            self._prepare_storage_connectors(runtime_dir)
            self._lock_inputs_local_storage_locations(data)

            # Execute execution engine specific runtime preparation.
            execution_engine = data.process.run.get("language", None)
            volume_maps = self.get_execution_engine(execution_engine).prepare_runtime(
                runtime_dir, data
            )

            self._prepare_context(
                data, data_dir, runtime_dir, RUNTIME_VOLUME_MAPS=volume_maps
            )
            self._prepare_script(runtime_dir, program)

            argv = [
                "/bin/bash",
                "-c",
                getattr(settings, "FLOW_EXECUTOR", {}).get(
                    "PYTHON", "/usr/bin/env python"
                )
                + " -m executors "
                + executor_module,
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
            logger.error(
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
        logger.info(__("Running {}", runtime_dir))
        self.run(data, runtime_dir, argv)

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

            dep_status = dependency_status(data)

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

            if data.process.run:
                try:
                    execution_engine = data.process.run.get("language", None)
                    # Evaluation by the execution engine may spawn additional data objects and
                    # perform other queries on the database. Queries of all possible execution
                    # engines need to be audited for possibilities of deadlocks in case any
                    # additional locks are introduced. Currently, we only take an explicit lock on
                    # the currently processing object.
                    program = self.get_execution_engine(execution_engine).evaluate(data)
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

                # Set allocated resources:
                resource_limits = data.process.get_resource_limits()
                data.process_memory = resource_limits["memory"]
                data.process_cores = resource_limits["cores"]
            else:
                # If there is no run section, then we should not try to run
                # anything. But the program must not be set to None as then
                # the process will be stuck in waiting state.
                program = ""

            if data.status != Data.STATUS_DONE:
                # The data object may already be marked as done by the execution engine. In this
                # case we must not revert the status to STATUS_WAITING.
                data.status = Data.STATUS_WAITING
            data.save(render_name=True)

            # Actually run the object only if there was nothing with the transaction.
            transaction.on_commit(
                # Make sure the closure gets the right values here, since they're
                # changed in the loop.
                lambda d=data, p=program: self._data_execute(d, p)
            )

        logger.debug(
            __(
                "Manager processing data scan triggered by Data with id {}.",
                data_id,
            )
        )

        try:
            queryset = Data.objects.filter(status=Data.STATUS_RESOLVING)
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
        return module.FlowExecutorPreparer()

    def load_expression_engines(self, engines: List[Union[Dict, str]]):
        """Load expression engines."""
        return load_engines(self, "ExpressionEngine", "expression_engines", engines)

    def load_execution_engines(self, engines: List[Union[Dict, str]]):
        """Load execution engines."""
        return load_engines(self, "ExecutionEngine", "execution_engines", engines)
