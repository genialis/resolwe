""".. Ignore pydocstyle D400.

==========
Dispatcher
==========

"""
import asyncio
import inspect
import json
import logging
import os
import shlex
import shutil
import uuid
from contextlib import suppress
from importlib import import_module

from asgiref.sync import async_to_sync
from channels.db import database_sync_to_async
from channels.exceptions import ChannelFull
from redis.exceptions import ConnectionError as RedisConnectionError

from django.conf import settings
from django.contrib.contenttypes.models import ContentType
from django.core.exceptions import ImproperlyConfigured, PermissionDenied
from django.db import IntegrityError, connection, transaction
from django.db.models import Q
from django.utils.timezone import now

from resolwe.flow.engine import InvalidEngineError, load_engines
from resolwe.flow.execution_engines import ExecutionError
from resolwe.flow.models import Data, DataDependency, DataLocation, Process
from resolwe.test.utils import is_testing
from resolwe.utils import BraceMessage as __

from . import consumer, state
from .protocol import ExecutorFiles, WorkerProtocol

logger = logging.getLogger(__name__)  # pylint: disable=invalid-name

DEFAULT_CONNECTOR = 'resolwe.flow.managers.workload_connectors.local'


class SettingsJSONifier(json.JSONEncoder):
    """Customized JSON encoder, coercing all unknown types into strings.

    Needed due to the class hierarchy coming out of the database,
    which can't be serialized using the vanilla json encoder.
    """

    def default(self, o):  # pylint: disable=method-hidden
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
        DataDependency.objects.filter(
            child=data, kind=DataDependency.KIND_IO
        ).distinct('parent__status').values_list('parent__status', flat=True)
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

    class _SynchronizationManager:
        """Context manager which enables transaction-like semantics.

        Once it is entered, exiting will not be possible until all
        executors have finished, including those that were already
        running when the context was entered.
        """

        def __init__(self):
            """Initialize state."""
            self.value = 0
            self.active = False
            self.condition = asyncio.Condition()
            self.tag_sequence = []

        async def __aenter__(self):
            """Begin synchronized execution context."""
            self.active = True
            return self

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            """Wait for executors to finish, then return."""
            logger.info(__("Waiting for executor count to drop to 0, now it is {}", self.value))

            await self.condition.acquire()
            try:
                await self.condition.wait()
            finally:
                self.condition.release()
            logger.debug(__(
                "Sync semaphore dropped to 0, tag sequence was {}.",
                self.tag_sequence
            ))

            self.active = False
            return False

        async def reset(self):
            """Reset the semaphore to 0."""
            assert not self.active
            await self.condition.acquire()
            self.value = 0
            self.condition.release()

        async def inc(self, tag):
            """Increase executor count by 1."""
            await self.condition.acquire()
            self.value += 1
            logger.debug(__("Sync semaphore increased to {}, tag {}.", self.value, tag))
            self.tag_sequence.append(tag + '-up')
            self.condition.release()

        async def dec(self, tag):
            """Decrease executor count by 1.

            Return ``True`` if the count dropped to 0 as a result.
            """
            ret = False
            await self.condition.acquire()
            try:
                self.value -= 1
                logger.debug(__("Sync semaphore decreased to {}, tag {}.", self.value, tag))
                self.tag_sequence.append(tag + '-down')
                ret = self.value == 0
                if self.active and self.value == 0:
                    self.condition.notify_all()
            finally:
                self.condition.release()
            return ret

    class _SynchronizationManagerDummy:
        """Dummy synchronization manager implementation.

        This is a dummy placeholder variant of
        :class:`~resolwe.flow.managers.dispatcher.Manager._SynchronizationManager`,
        doing nothing. It's needed so that code async initialization can
        be done late enough, after code that already syntactically needs
        the synchronization manager, but doesn't need it to work yet.
        """

        def __init__(self):
            self.active = False
            self.value = 0

        async def inc(self, tag):  # pylint: disable=missing-docstring
            pass

        async def dec(self, tag):  # pylint: disable=missing-docstring
            pass

        async def reset(self):  # pylint: disable=missing-docstring
            pass

    class _SettingsManager:
        """Context manager for settings overrides.

        Because Django's :func:`~django.test.override_settings` is a
        context manager, it would make the code awkward if the manager's
        support for this wasn't also in the form of a context manager.
        """

        def __init__(self, state_key_prefix, **kwargs):
            """Prepare the context manager with the given overrides.

            :param state_key_prefix: The Redis key prefix used by
                :class:`~resolwe.flow.managers.state.ManagerState`.
            :param kwargs: The settings to override.
            """
            self.overrides = kwargs
            self.old_overrides = None
            self.state = state.ManagerState(state_key_prefix)

        def __enter__(self):
            """Begin context with overridden settings."""
            self.old_overrides = self.state.settings_override
            # Get a new copy of the settings, so we can modify them
            merged = self.state.settings_override or {}
            merged.update(self.overrides)
            self.state.settings_override = merged
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            """Clean up the override context."""
            self.state.settings_override = self.old_overrides
            return False

    def discover_engines(self, executor=None):
        """Discover configured engines.

        :param executor: Optional executor module override
        """
        if executor is None:
            executor = getattr(settings, 'FLOW_EXECUTOR', {}).get('NAME', 'resolwe.flow.executors.local')
        self.executor = self.load_executor(executor)
        logger.info(
            __("Loaded '{}' executor.", str(self.executor.__class__.__module__).replace('.prepare', ''))
        )

        expression_engines = getattr(settings, 'FLOW_EXPRESSION_ENGINES', ['resolwe.flow.expression_engines.jinja'])
        self.expression_engines = self.load_expression_engines(expression_engines)
        logger.info(__(
            "Found {} expression engines: {}", len(self.expression_engines), ', '.join(self.expression_engines.keys())
        ))

        execution_engines = getattr(settings, 'FLOW_EXECUTION_ENGINES', ['resolwe.flow.execution_engines.bash'])
        self.execution_engines = self.load_execution_engines(execution_engines)
        logger.info(__(
            "Found {} execution engines: {}", len(self.execution_engines), ', '.join(self.execution_engines.keys())
        ))

    def reset(self, keep_state=False):
        """Reset the shared state and drain Django Channels.

        :param keep_state: If ``True``, do not reset the shared manager
            state (useful in tests, where the settings overrides need to
            be kept). Defaults to ``False``.
        """
        if not keep_state:
            self.state = state.ManagerState(state.MANAGER_STATE_PREFIX)
            self.state.reset()
        async_to_sync(consumer.run_consumer)(timeout=1)
        async_to_sync(self.sync_counter.reset)()

    def __init__(self, *args, **kwargs):
        """Initialize arguments."""
        self.discover_engines()
        self.state = state.ManagerState(state.MANAGER_STATE_PREFIX)

        # Don't call the full self.reset() here, that's only meant for testing
        # since it also runs a dummy consumer to drain channels.
        with suppress(RedisConnectionError):
            # It's awkward to handle documentation and migration testing
            # any other way.
            self.state.reset()

        # The number of executors currently running; used for test synchronization.
        # We need to start out with a dummy object, so that the async
        # infrastructure isn't started too early. In particular, this handles
        # counter functions that are called before any actual synchronization
        # is wanted: in a test, what's called first is the Django signal.
        # This will call communicate(), which will (has to) first try upping
        # the counter value; the future that that produces can be scheduled
        # to the wrong event loop (the application one is started further
        # down communicate() in the synchronization block), which then
        # leads to exceedingly obscure crashes further down the line.
        self.sync_counter = self._SynchronizationManagerDummy()

        # Django's override_settings should be avoided at all cost here
        # to keep the manager as independent as possible, and in
        # particular to avoid overriding with dangerous variables, such
        # as the one controlling Django signal synchronicity. To make
        # such avoidance possible, all settings lookups in the worker
        # should use a private dictionary instead of the global
        # configuration. This variable is maintained around
        # _data_scan() calls, effectively emulating Django's
        # settings but having no effect on anything outside the worker
        # code (in particular, the signal triggers).
        self.settings_actual = {}

        # Ensure there is only one manager instance per process. This
        # is required as other parts of the code access the global
        # manager instance.
        with suppress(ImportError):
            from resolwe.flow import managers
            assert not hasattr(managers, 'manager')

        self.scheduling_class_map = dict(Process.SCHEDULING_CLASS_CHOICES)

        # Check settings for consistency.
        flow_manager = getattr(settings, 'FLOW_MANAGER', {})
        if 'DISPATCHER_MAPPING' in flow_manager and 'NAME' in flow_manager:
            raise ImproperlyConfigured("Key 'DISPATCHER_MAPPING' conflicts with key 'NAME' in FLOW_MANAGER settings.")

        if 'DISPATCHER_MAPPING' in flow_manager:
            mapping = flow_manager['DISPATCHER_MAPPING']

            scheduling_classes = set(self.scheduling_class_map.values())
            map_keys = set(mapping.keys())
            class_difference = scheduling_classes - map_keys
            if class_difference:
                raise ImproperlyConfigured(
                    "Dispatcher manager mapping settings incomplete, missing {}.".format(class_difference)
                )
            connector_list = [mapping[klass] for klass in scheduling_classes]
        else:
            connector_list = [flow_manager.get('NAME', DEFAULT_CONNECTOR)]

        # Pre-load all needed connectors.
        self.connectors = {}
        for module_name in connector_list:
            connector_module = import_module(module_name)
            self.connectors[module_name] = connector_module.Connector()

        logger.info(__(
            "Found {} workload connectors: {}", len(self.connectors), ', '.join(self.connectors.keys())
        ))

        super().__init__(*args, **kwargs)

    def _marshal_settings(self):
        """Marshal Django settings into a serializable object.

        :return: The serialized settings.
        :rtype: dict
        """
        result = {}
        for key in dir(settings):
            if any(map(key.startswith, ['FLOW_', 'RESOLWE_', 'CELERY_'])):
                result[key] = getattr(settings, key)
        return result

    def _include_environment_variables(self, program, executor_vars):
        """Define environment variables."""
        env_vars = {
            'RESOLWE_HOST_URL': self.settings_actual.get('RESOLWE_HOST_URL', 'localhost'),
        }

        set_env = self.settings_actual.get('FLOW_EXECUTOR', {}).get('SET_ENV', {})
        env_vars.update(executor_vars)
        env_vars.update(set_env)

        export_commands = ['export {}={}'.format(key, shlex.quote(value)) for key, value in env_vars.items()]
        return os.linesep.join(export_commands) + os.linesep + program

    def run(self, data, runtime_dir, argv):
        """Select a concrete connector and run the process through it.

        :param data: The :class:`~resolwe.flow.models.Data` object that
            is to be run.
        :param runtime_dir: The directory the executor is run from.
        :param argv: The argument vector used to spawn the executor.
        """
        process_scheduling = self.scheduling_class_map[data.process.scheduling_class]
        if 'DISPATCHER_MAPPING' in getattr(settings, 'FLOW_MANAGER', {}):
            class_name = settings.FLOW_MANAGER['DISPATCHER_MAPPING'][process_scheduling]
        else:
            class_name = getattr(settings, 'FLOW_MANAGER', {}).get('NAME', DEFAULT_CONNECTOR)

        data.scheduled = now()
        data.save(update_fields=['scheduled'])

        async_to_sync(self.sync_counter.inc)('executor')
        return self.connectors[class_name].submit(data, runtime_dir, argv)

    def _get_per_data_dir(self, dir_base, subpath):
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
        # Use Django settings here, because the state must be preserved
        # across events. This also implies the directory settings can't
        # be patched outside the manager and then just sent along in the
        # command packets.
        result = self.settings_actual.get('FLOW_EXECUTOR', {}).get(dir_base, '')
        return os.path.join(result, subpath)

    def _prepare_data_dir(self, data):
        """Prepare destination directory where the data will live.

        :param data: The :class:`~resolwe.flow.models.Data` object for
            which to prepare the private execution directory.
        :return: The prepared data directory path.
        :rtype: str
        """
        logger.debug(__("Preparing data directory for Data with id {}.", data.id))

        with transaction.atomic():
            # Create a temporary random location and then override it with data
            # location id since object has to be created first.
            # TODO Find a better solution, e.g. defer the database constraint.
            temporary_location_string = uuid.uuid4().hex[:10]
            data_location = DataLocation.objects.create(subpath=temporary_location_string)
            data_location.subpath = str(data_location.id)
            data_location.save()
            data_location.data.add(data)

        output_path = self._get_per_data_dir('DATA_DIR', data_location.subpath)
        dir_mode = self.settings_actual.get('FLOW_EXECUTOR', {}).get('DATA_DIR_MODE', 0o755)
        os.mkdir(output_path, mode=dir_mode)
        # os.mkdir is not guaranteed to set the given mode
        os.chmod(output_path, dir_mode)
        return output_path

    def _prepare_context(self, data_id, data_dir, runtime_dir, **kwargs):
        """Prepare settings and constants JSONs for the executor.

        Settings and constants provided by other ``resolwe`` modules and
        :class:`~django.conf.settings` are all inaccessible in the
        executor once it is deployed, so they need to be serialized into
        the runtime directory.

        :param data_id: The :class:`~resolwe.flow.models.Data` object id
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

        settings_dict = {}
        settings_dict['DATA_DIR'] = data_dir
        settings_dict['REDIS_CHANNEL_PAIR'] = state.MANAGER_EXECUTOR_CHANNELS
        files[ExecutorFiles.EXECUTOR_SETTINGS] = settings_dict

        django_settings = {}
        django_settings.update(self.settings_actual)
        django_settings.update(kwargs)
        files[ExecutorFiles.DJANGO_SETTINGS] = django_settings

        # Add scheduling classes.
        files[ExecutorFiles.PROCESS_META] = {
            k: getattr(Process, k) for k in dir(Process)
            if k.startswith('SCHEDULING_CLASS_') and isinstance(getattr(Process, k), str)
        }

        # Add Data status constants.
        files[ExecutorFiles.DATA_META] = {
            k: getattr(Data, k) for k in dir(Data)
            if k.startswith('STATUS_') and isinstance(getattr(Data, k), str)
        }

        # Extend the settings with whatever the executor wants.
        self.executor.extend_settings(data_id, files, secrets)

        # Save the settings into the various files in the runtime dir.
        settings_dict[ExecutorFiles.FILE_LIST_KEY] = list(files.keys())
        for file_name in files:
            file_path = os.path.join(runtime_dir, file_name)
            with open(file_path, 'wt') as json_file:
                json.dump(files[file_name], json_file, cls=SettingsJSONifier)

        # Save the secrets in the runtime dir, with permissions to prevent listing the given
        # directory.
        secrets_dir = os.path.join(runtime_dir, ExecutorFiles.SECRETS_DIR)
        os.makedirs(secrets_dir, mode=0o300)
        for file_name, value in secrets.items():
            file_path = os.path.join(secrets_dir, file_name)

            # Set umask to 0 to ensure that we set the correct permissions.
            old_umask = os.umask(0)
            try:
                # We need to use os.open in order to correctly enforce file creation. Otherwise,
                # there is a race condition which can be used to create the file with different
                # ownership/permissions.
                file_descriptor = os.open(file_path, os.O_WRONLY | os.O_CREAT, mode=0o600)
                with os.fdopen(file_descriptor, 'w') as raw_file:
                    raw_file.write(value)
            finally:
                os.umask(old_umask)

    def _prepare_executor(self, data, executor):
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

        exec_dir = os.path.dirname(inspect.getsourcefile(executor_package))
        dest_dir = self._get_per_data_dir('RUNTIME_DIR', data.location.subpath)
        dest_package_dir = os.path.join(dest_dir, 'executors')
        shutil.copytree(exec_dir, dest_package_dir)
        dir_mode = self.settings_actual.get('FLOW_EXECUTOR', {}).get('RUNTIME_DIR_MODE', 0o755)
        os.chmod(dest_dir, dir_mode)

        class_name = executor.rpartition('.executors.')[-1]
        return '.{}'.format(class_name), dest_dir

    def _prepare_script(self, dest_dir, program):
        """Copy the script into the destination directory.

        :param dest_dir: The target directory where the script will be
            saved.
        :param program: The script text to be saved.
        :return: The name of the script file.
        :rtype: str
        """
        script_name = ExecutorFiles.PROCESS_SCRIPT
        dest_file = os.path.join(dest_dir, script_name)
        with open(dest_file, 'wt') as dest_file_obj:
            dest_file_obj.write(program)
        os.chmod(dest_file, 0o700)
        return script_name

    def override_settings(self, **kwargs):
        """Override global settings within the calling context.

        :param kwargs: The settings overrides. Same use as for
            :func:`django.test.override_settings`.
        """
        return self._SettingsManager(self.state.key_prefix, **kwargs)

    async def handle_control_event(self, message):
        """Handle an event from the Channels layer.

        Channels layer callback, do not call directly.
        """
        cmd = message[WorkerProtocol.COMMAND]
        logger.debug(__("Manager worker got channel command '{}'.", cmd))

        # Prepare settings for use; Django overlaid by state overlaid by
        # anything immediate in the current packet.
        immediates = {}
        if cmd == WorkerProtocol.COMMUNICATE:
            immediates = message.get(WorkerProtocol.COMMUNICATE_SETTINGS, {}) or {}
        override = self.state.settings_override or {}
        override.update(immediates)
        self.settings_actual = self._marshal_settings()
        self.settings_actual.update(override)

        if cmd == WorkerProtocol.COMMUNICATE:
            try:
                await database_sync_to_async(self._data_scan)(**message[WorkerProtocol.COMMUNICATE_EXTRA])
            except Exception:
                logger.exception("Unknown error occured while processing communicate control command.")
                raise
            finally:
                await self.sync_counter.dec('communicate')

        elif cmd == WorkerProtocol.FINISH:
            try:
                data_id = message[WorkerProtocol.DATA_ID]
                data_location = DataLocation.objects.get(data__id=data_id)
                if not getattr(settings, 'FLOW_MANAGER_KEEP_DATA', False):
                    try:
                        def handle_error(func, path, exc_info):
                            """Handle permission errors while removing data directories."""
                            if isinstance(exc_info[1], PermissionError):
                                os.chmod(path, 0o700)
                                shutil.rmtree(path)

                        # Remove secrets directory, but leave the rest of the runtime directory
                        # intact. Runtime directory will be removed during data purge, when the
                        # data object is removed.
                        secrets_dir = os.path.join(
                            self._get_per_data_dir('RUNTIME_DIR', data_location.subpath),
                            ExecutorFiles.SECRETS_DIR
                        )
                        shutil.rmtree(secrets_dir, onerror=handle_error)
                    except OSError:
                        logger.exception("Manager exception while removing data runtime directory.")

                if message[WorkerProtocol.FINISH_SPAWNED]:
                    await database_sync_to_async(self._data_scan)(**message[WorkerProtocol.FINISH_COMMUNICATE_EXTRA])
            except Exception:
                logger.exception(
                    "Unknown error occured while processing finish control command.",
                    extra={'data_id': data_id}
                )
                raise
            finally:
                await self.sync_counter.dec('executor')

        elif cmd == WorkerProtocol.ABORT:
            await self.sync_counter.dec('executor')

        else:
            logger.error(__("Ignoring unknown manager control command '{}'.", cmd))

    def _ensure_counter(self):
        """Ensure the sync counter is a valid non-dummy object."""
        if not isinstance(self.sync_counter, self._SynchronizationManager):
            self.sync_counter = self._SynchronizationManager()

    async def execution_barrier(self):
        """Wait for executors to finish.

        At least one must finish after this point to avoid a deadlock.
        """
        async def _barrier():
            """Enter the sync block and exit the app afterwards."""
            async with self.sync_counter:
                pass
            await consumer.exit_consumer()

        self._ensure_counter()
        await asyncio.wait([
            _barrier(),
            consumer.run_consumer(),
        ])
        self.sync_counter = self._SynchronizationManagerDummy()

    async def communicate(self, data_id=None, run_sync=False, save_settings=True):
        """Scan database for resolving Data objects and process them.

        This is submitted as a task to the manager's channel workers.

        :param data_id: Optional id of Data object which (+ its
            children) should be processes. If it is not given, all
            resolving objects are processed.
        :param run_sync: If ``True``, wait until all processes spawned
            from this point on have finished processing. If no processes
            are spawned, this results in a deadlock, since counts are
            handled on process finish.
        :param save_settings: If ``True``, save the current Django
            settings context to the global state. This should never be
            ``True`` for "automatic" calls, such as from Django signals,
            which can be invoked from inappropriate contexts (such as in
            the listener). For user code, it should be left at the
            default value. The saved settings are in effect until the
            next such call.
        """
        executor = getattr(settings, 'FLOW_EXECUTOR', {}).get('NAME', 'resolwe.flow.executors.local')
        logger.debug(__(
            "Manager sending communicate command on '{}' triggered by Data with id {}.",
            state.MANAGER_CONTROL_CHANNEL,
            data_id,
        ))

        saved_settings = self.state.settings_override
        if save_settings:
            saved_settings = self._marshal_settings()
            self.state.settings_override = saved_settings

        if run_sync:
            self._ensure_counter()
        await self.sync_counter.inc('communicate')
        try:
            await consumer.send_event({
                WorkerProtocol.COMMAND: WorkerProtocol.COMMUNICATE,
                WorkerProtocol.COMMUNICATE_SETTINGS: saved_settings,
                WorkerProtocol.COMMUNICATE_EXTRA: {
                    'data_id': data_id,
                    'executor': executor,
                },
            })
        except ChannelFull:
            logger.exception("ChannelFull error occurred while sending communicate message.")
            await self.sync_counter.dec('communicate')

        if run_sync and not self.sync_counter.active:
            logger.debug(__(
                "Manager on channel '{}' entering synchronization block.",
                state.MANAGER_CONTROL_CHANNEL
            ))
            await self.execution_barrier()
            logger.debug(__(
                "Manager on channel '{}' exiting synchronization block.",
                state.MANAGER_CONTROL_CHANNEL
            ))

    def _data_execute(self, data, program, executor):
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
        if not program:
            return

        logger.debug(__("Manager preparing Data with id {} for processing.", data.id))

        # Prepare the executor's environment.
        try:
            executor_env_vars = self.get_executor().get_environment_variables()
            program = self._include_environment_variables(program, executor_env_vars)
            data_dir = self._prepare_data_dir(data)
            executor_module, runtime_dir = self._prepare_executor(data, executor)

            # Execute execution engine specific runtime preparation.
            execution_engine = data.process.run.get('language', None)
            volume_maps = self.get_execution_engine(execution_engine).prepare_runtime(runtime_dir, data)

            self._prepare_context(data.id, data_dir, runtime_dir, RUNTIME_VOLUME_MAPS=volume_maps)
            self._prepare_script(runtime_dir, program)

            argv = [
                '/bin/bash',
                '-c',
                self.settings_actual.get('FLOW_EXECUTOR', {}).get('PYTHON', '/usr/bin/env python')
                + ' -m executors ' + executor_module
            ]
        except PermissionDenied as error:
            data.status = Data.STATUS_ERROR
            data.process_error.append("Permission denied for process: {}".format(error))
            data.save()
            return
        except OSError as err:
            logger.error(__(
                "OSError occurred while preparing data {} (will skip): {}",
                data.id, err
            ))
            return

        # Hand off to the run() method for execution.
        logger.info(__("Running {}", runtime_dir))
        self.run(data, runtime_dir, argv)

    def _data_scan(self, data_id=None, executor='resolwe.flow.executors.local', **kwargs):
        """Scan for new Data objects and execute them.

        :param data_id: Optional id of Data object which (+ its
            children) should be scanned. If it is not given, all
            resolving objects are processed.
        :param executor: The fully qualified name of the executor to use
            for all :class:`~resolwe.flow.models.Data` objects
            discovered in this pass.
        """
        def process_data_object(data):
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
                return

            elif dep_status != Data.STATUS_DONE:
                return

            if data.process.run:
                try:
                    execution_engine = data.process.run.get('language', None)
                    # Evaluation by the execution engine may spawn additional data objects and
                    # perform other queries on the database. Queries of all possible execution
                    # engines need to be audited for possibilities of deadlocks in case any
                    # additional locks are introduced. Currently, we only take an explicit lock on
                    # the currently processing object.
                    program = self.get_execution_engine(execution_engine).evaluate(data)
                except (ExecutionError, InvalidEngineError) as error:
                    data.status = Data.STATUS_ERROR
                    data.process_error.append("Error in process script: {}".format(error))
                    data.save()
                    return

                # Set allocated resources:
                resource_limits = data.process.get_resource_limits()
                data.process_memory = resource_limits['memory']
                data.process_cores = resource_limits['cores']
            else:
                # If there is no run section, then we should not try to run anything. But the
                # program must not be set to None as then the process will be stuck in waiting
                # state.
                program = ''

            if data.status != Data.STATUS_DONE:
                # The data object may already be marked as done by the execution engine. In this
                # case we must not revert the status to STATUS_WAITING.
                data.status = Data.STATUS_WAITING
            data.save(render_name=True)

            # Actually run the object only if there was nothing with the transaction.
            transaction.on_commit(
                # Make sure the closure gets the right values here, since they're
                # changed in the loop.
                lambda d=data, p=program: self._data_execute(d, p, executor)
            )

        logger.debug(__("Manager processing communicate command triggered by Data with id {}.", data_id))

        if is_testing():
            # NOTE: This is a work-around for Django issue #10827
            # (https://code.djangoproject.com/ticket/10827), same as in
            # TestCaseHelpers._pre_setup(). Because the worker is running
            # independently, it must clear the cache on its own.
            ContentType.objects.clear_cache()

            # Ensure settings overrides apply
            self.discover_engines(executor=executor)

        try:
            queryset = Data.objects.filter(status=Data.STATUS_RESOLVING)
            if data_id is not None:
                # Scan only given data object and its children.
                queryset = queryset.filter(Q(parents=data_id) | Q(id=data_id)).distinct()

            for data in queryset:
                try:
                    with transaction.atomic():
                        process_data_object(data)

                        # All data objects created by the execution engine are commited after this
                        # point and may be processed by other managers running in parallel. At the
                        # same time, the lock for the current data object is released.
                except Exception as error:  # pylint: disable=broad-except
                    logger.exception(__(
                        "Unhandled exception in _data_scan while processing data object {}.",
                        data.pk
                    ))

                    # Unhandled error while processing a data object. We must set its
                    # status to STATUS_ERROR to prevent the object from being retried
                    # on next _data_scan run. We must perform this operation without
                    # using the Django ORM as using the ORM may be the reason the error
                    # occurred in the first place.
                    error_msg = "Internal error: {}".format(error)
                    process_error_field = Data._meta.get_field('process_error')  # pylint: disable=protected-access
                    max_length = process_error_field.base_field.max_length
                    if len(error_msg) > max_length:
                        error_msg = error_msg[:max_length - 3] + '...'

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
                                    table=Data._meta.db_table  # pylint: disable=protected-access
                                ),
                                {
                                    'status': Data.STATUS_ERROR,
                                    'error': [error_msg],
                                    'id': data.pk
                                }
                            )
                    except Exception as error:  # pylint: disable=broad-except
                        # If object's state cannot be changed due to some database-related
                        # issue, at least skip the object for this run.
                        logger.exception(__(
                            "Unhandled exception in _data_scan while trying to emit error for {}.",
                            data.pk
                        ))

        except IntegrityError as exp:
            logger.error(__("IntegrityError in manager {}", exp))
            return

    def get_executor(self):
        """Return an executor instance."""
        return self.executor

    def get_expression_engine(self, name):
        """Return an expression engine instance."""
        try:
            return self.expression_engines[name]
        except KeyError:
            raise InvalidEngineError("Unsupported expression engine: {}".format(name))

    def get_execution_engine(self, name):
        """Return an execution engine instance."""
        try:
            return self.execution_engines[name]
        except KeyError:
            raise InvalidEngineError("Unsupported execution engine: {}".format(name))

    def load_executor(self, executor_name):
        """Load process executor."""
        executor_name = executor_name + '.prepare'
        module = import_module(executor_name)
        return module.FlowExecutorPreparer()

    def load_expression_engines(self, engines):
        """Load expression engines."""
        return load_engines(self, 'ExpressionEngine', 'expression_engines', engines)

    def load_execution_engines(self, engines):
        """Load execution engines."""
        return load_engines(self, 'ExecutionEngine', 'execution_engines', engines)
