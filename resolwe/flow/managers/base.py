""".. Ignore pydocstyle D400.

================
Abstract Manager
================

"""
from __future__ import absolute_import, division, print_function, unicode_literals

import inspect
import json
import logging
import os
import shutil
import time
from importlib import import_module

from channels import Channel
from channels.generic import BaseConsumer
from channels.test import Client

from django.conf import settings
from django.contrib.contenttypes.models import ContentType
from django.db import IntegrityError, transaction

from resolwe.flow.engine import InvalidEngineError, load_engines
from resolwe.flow.execution_engines import ExecutionError
from resolwe.flow.models import Data, Process
from resolwe.flow.utils import iterate_fields
from resolwe.utils import BraceMessage as __

from . import state
from .protocol import ExecutorFiles, WorkerProtocol

logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


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
    """Return abstracted satus of dependencies.

    STATUS_ERROR .. one dependency has error status
    STATUS_DONE .. all dependencies have done status
    None .. other

    """
    for field_schema, fields in iterate_fields(data.input, data.process.input_schema):
        if (field_schema['type'].lower().startswith('data:') or
                field_schema['type'].lower().startswith('list:data:')):
            name = field_schema['name']
            value = fields[name]

            # None values are valid and should be ignored.
            if value is None:
                continue

            if field_schema['type'].lower().startswith('data:'):
                value = [value]

            for uid in value:
                try:
                    _data = Data.objects.get(id=uid)
                except Data.DoesNotExist:
                    return Data.STATUS_ERROR

                if _data.status == Data.STATUS_ERROR:
                    return Data.STATUS_ERROR

                if _data.status != Data.STATUS_DONE:
                    return None

    return Data.STATUS_DONE


class BaseManager(BaseConsumer):
    """Manager handles process job execution."""

    class _SynchronizationManager(object):
        """Context manager which enables transaction-like semantics.

        Once it is entered, exiting will not be possible until all
        executors have finished, including those that were already
        running when the context was entered.
        """

        def __init__(self, state_key_prefix, force_enter):
            """Initialize all flags and state.

            :param state_key_prefix: The Redis key prefix used by
                :class:`~resolwe.flow.managers.state.ManagerState`.
            :param force_enter: If ``True``, do not abort even if a
                synchronization transaction is already active.
            """
            self.state = state.ManagerState(state_key_prefix)
            self.force_enter = force_enter

        def __enter__(self):
            """Begin synchronized execution context."""
            if self.force_enter:
                self.state.sync_execution.set(1)
            else:
                if not self.state.sync_execution.cas(0, 1):
                    raise RuntimeError("Only one user at a time may enter a synchronization transaction.")
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            """Wait for executors to finish, then return."""
            while int(self.state.sync_semaphore) > 0:
                # Random, but Django Channels don't provide blocking behaviour.
                time.sleep(0.5)

            assert self.state.sync_execution.cas(1, 0) == 1
            return False

    class _SettingsManager(object):
        """Context manager for settings overrides.

        Because Django's :meth:`~django.test.override_settings` is a
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

    def discover_engines(self):
        """Discover configured engines."""
        executor = getattr(settings, 'FLOW_EXECUTOR', {}).get('NAME', 'resolwe.flow.executors.local')
        self.executor = self.load_executor(executor)
        expression_engines = getattr(settings, 'FLOW_EXPRESSION_ENGINES', ['resolwe.flow.expression_engines.jinja'])
        self.expression_engines = self.load_expression_engines(expression_engines)
        execution_engines = getattr(settings, 'FLOW_EXECUTION_ENGINES', ['resolwe.flow.execution_engines.bash'])
        self.execution_engines = self.load_execution_engines(execution_engines)

    def drain(self):
        """Drain the control channel without acting on anything."""
        client = Client()
        while client.get_next_message(state.MANAGER_CONTROL_CHANNEL) is not None:
            pass

    def reset(self):
        """Reset the shared state and drain Django Channels."""
        self.state = state.ManagerState(state.MANAGER_STATE_PREFIX)
        self.state.reset()
        self.drain()

    def __init__(self, *args, **kwargs):
        """Initialize arguments.

        :param static: Optional. If ``True``, do only partial
            initialization. This is useful if e.g. a static global
            singleton instance of the manager is needed. In such a case,
            the Django Channels portion (of which it must be a subclass)
            must not be initialized.
        """
        self.discover_engines()
        self.state = state.ManagerState(state.MANAGER_STATE_PREFIX)
        # Django's override_settings should be avoided at all cost here
        # to keep the manager as independent as possible, and in
        # particular to avoid overriding with dangerous variables, such
        # as the one controlling Django signal synchronicity. To make
        # such avoidance possible, all settings lookups in the worker
        # should use a private dictionary instead of the global
        # configuration. This variable is maintained around
        # `_communicate()` calls, effectively emulating Django's
        # settings but having no effect on anything outside the worker
        # code (in particular, the signal triggers).
        self.settings_actual = {}

        # If True, drop ContentType cache on every communicate command.
        # NOTE: This is a work-around for Django issue #10827
        # (https://code.djangoproject.com/ticket/10827), same as in
        # TestCaseHelpers._pre_setup(). Because the worker is running
        # independently, it must clear the cache on its own.
        self._drop_ctypes = getattr(settings, 'FLOW_MANAGER_DISABLE_CTYPE_CACHE', False)

        if 'static' not in kwargs or not kwargs['static']:
            super().__init__(*args, **kwargs)

    def update_routing(self):
        """Update naming information in the Channels' routing layer.

        This should not be needed in normal operation. It's mostly here
        to support the testing infrastructure, where the manner in which
        Django settings are patched and the ordering (in terms of import
        order) where they're needed clash and make things awkward.
        """
        # This is all a bit kludgy, but we want runtime settings
        # patching after the module is already loaded, and Channels
        # wants a class variable.
        setattr(type(self), 'method_mapping', {
            state.MANAGER_CONTROL_CHANNEL: 'handle_control_event',
        })

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

    def _include_environment_variables(self, program):
        """Define environment variables."""
        env_vars = {
            'RESOLWE_HOST_URL': self.settings_actual.get('RESOLWE_HOST_URL', 'localhost'),
        }

        set_env = self.settings_actual.get('FLOW_EXECUTOR', {}).get('SET_ENV', {})
        env_vars.update(set_env)

        # TODO: Use shlex.quote when py2 support dropped
        export_commands = ['export {}="{}"'.format(key, value.replace('"', '\"')) for key, value in env_vars.items()]
        return os.linesep.join(export_commands) + os.linesep + program

    def run(self, data_id, dest_dir, argv, priority='normal', run_sync=False, verbosity=1):
        """Run process.

        :param data_id: The id of the :class:`~resolwe.flow.models.Data`
            object that is to be run.
        :param dest_dir: The directory the
            :class:`~resolwe.flow.models.Data` object should be run from.
        :param argv: The argument vector used to spawn the executor.
        :param priority: The execution priority for this job.
        :param run_sync: If ``True``, the method will not return until
            the manager is finished processing everything from this call
            onwards.
        :param verbosity: Integer logging verbosity level.
        """
        raise NotImplementedError("Subclasses of BaseManager must implement a run() method.")

    def _get_per_data_dir(self, dir_base, data_id):
        """Extend the given base directory with a per-data component.

        The method creates a private path for the
        :class:`~resolwe.flow.models.Data` object, such as::

            ./test_data/1/

        if ``base_dir`` is ``'./test_data'`` and ``data_id`` is ``1``.

        :param dir_base: The base path to be extended. This will usually
            be one of the directories configured in the
            ``FLOW_EXECUTOR`` setting.
        :param data_id: The :class:`~resolwe.flow.models.Data` object's
            id used for the extending.
        :return: The new path for the :class:`~resolwe.flow.models.Data`
            object.
        :rtype: str
        """
        # Use Django settings here, because the state must be preserved
        # across events. This also implies the directory settings can't
        # be patched outside the manager and then just sent along in the
        # command packets.
        result = self.settings_actual.get('FLOW_EXECUTOR', {}).get(dir_base, '')
        return os.path.join(result, str(data_id))

    def _prepare_data_dir(self, data_id):
        """Prepare destination directory where the data will live.

        :param data_id: The :class:`~resolwe.flow.models.Data` object id
            for which to prepare the private execution directory.
        :return: The prepared data directory path.
        :rtype: str
        """
        output_path = self._get_per_data_dir('DATA_DIR', data_id)
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

        settings_dict = {}
        settings_dict['DATA_DIR'] = data_dir
        settings_dict['REDIS_CHANNEL_PAIR'] = state.MANAGER_EXECUTOR_CHANNELS
        files[ExecutorFiles.EXECUTOR_SETTINGS] = settings_dict

        django_settings = {}
        django_settings.update(self.settings_actual)
        django_settings.update(kwargs)
        files[ExecutorFiles.DJANGO_SETTINGS] = django_settings

        # add scheduling classes
        files[ExecutorFiles.PROCESS_META] = {
            k: getattr(Process, k) for k in dir(Process)
            if k.startswith('SCHEDULING_CLASS_') and isinstance(getattr(Process, k), str)
        }

        # add Data status constants
        files[ExecutorFiles.DATA_META] = {
            k: getattr(Data, k) for k in dir(Data)
            if k.startswith('STATUS_') and isinstance(getattr(Data, k), str)
        }

        # extend the settings with whatever the executor wants
        self.executor.extend_settings(data_id, files)

        # save the settings into the various files in the data dir
        settings_dict[ExecutorFiles.FILE_LIST_KEY] = list(files.keys())
        for file_name in files:
            file_path = os.path.join(runtime_dir, file_name)
            with open(file_path, 'wt') as json_file:
                json.dump(files[file_name], json_file, cls=SettingsJSONifier)

    def _prepare_executor(self, data_id, executor):
        """Copy executor sources into the destination directory.

        :param data_id: The :class:`~resolwe.flow.models.Data` object id
            being prepared for.
        :param executor: The fully qualified name of the executor that
            is to be used for this data object.
        :return: Tuple containing the relative fully qualified name of
            the executor class ('relative' to how the executor will be
            run) and the path to the directory where the executor will
            be deployed.
        :rtype: (str, str)
        """
        # Both of these imports are here only to get the packages' paths.
        import resolwe.flow.executors as executor_package
        import resolwe.flow.managers.protocol as protocol_module

        exec_dir = os.path.dirname(inspect.getsourcefile(executor_package))
        dest_dir = self._get_per_data_dir('RUNTIME_DIR', data_id)
        dest_package_dir = os.path.join(dest_dir, 'executors')
        shutil.copytree(exec_dir, dest_package_dir)
        shutil.copy(protocol_module.__file__, dest_package_dir)
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
            :meth:`~django.test.override_settings`.
        """
        return self._SettingsManager(self.state.key_prefix, **kwargs)

    def handle_control_event(self, message):
        """Handle an event from the Channels layer.

        Channels layer callback, do not call directly.
        """
        cmd = message.content[WorkerProtocol.COMMAND]
        logger.debug(__(
            "Manager worker '{}.{}' got channel command '{}' on channel '{}'.",
            self.__class__.__module__,
            self.__class__.__name__,
            cmd,
            message.channel.name
        ))

        # Prepare settings for use; Django overlaid by state overlaid by
        # anything immediate in the current packet.
        immediates = {}
        if cmd == WorkerProtocol.COMMUNICATE:
            immediates = message.content.get(WorkerProtocol.COMMUNICATE_SETTINGS, {}) or {}
        override = self.state.settings_override or {}
        override.update(immediates)
        self.settings_actual = self._marshal_settings()
        self.settings_actual.update(override)

        if cmd == WorkerProtocol.COMMUNICATE:
            try:
                self._communicate(**message.content[WorkerProtocol.COMMUNICATE_EXTRA])
            finally:
                # Clear communicate() claim on the semaphore.
                new_sema = self.state.sync_semaphore.add(-1)
                logger.debug(__("Manager changed sync_semaphore DOWN to {} after _communicate().", new_sema))

        elif cmd == WorkerProtocol.FINISH:
            data_id = message.content[WorkerProtocol.DATA_ID]
            if not getattr(settings, 'FLOW_MANAGER_KEEP_DATA', False):
                shutil.rmtree(self._get_per_data_dir('RUNTIME_DIR', data_id))

            if message.content[WorkerProtocol.FINISH_SPAWNED]:
                new_sema = self.state.sync_semaphore.add(1)
                logger.debug(__("Manager changed sync_semaphore UP to {} in spawn handler.", new_sema))
                try:
                    self._communicate(**message.content[WorkerProtocol.FINISH_COMMUNICATE_EXTRA])
                finally:
                    # Clear communicate() claim on the semaphore.
                    new_sema = self.state.sync_semaphore.add(-1)
                    logger.debug(__("Manager changed sync_semaphore DOWN to {} after _communicate().", new_sema))

            self.state.executor_count.add(-1)
            # Clear execution claim on the semaphore.
            new_sema = self.state.sync_semaphore.add(-1)
            logger.debug(__("Manager changed sync_semaphore DOWN to {} after executor finish.", new_sema))

        else:
            logger.error(__("Ignoring unknown manager control command '{}'.", cmd))

    def synchronized(self, force_enter=False):
        """Enter a synchronization block for the calling context.

        :param force_enter: If ``True``, enter the block even if another
            one is already active. Use this at your own risk.
        """
        return self._SynchronizationManager(self.state.key_prefix, force_enter)

    def execution_barrier(self, force_enter=False):
        """Wait for executors to finish.

        At least one must finish after this point to avoid a deadlock.

        :param force_enter: If ``True``, create the barrier even if
            another synchronization block is already active. Use this at
            your own risk.
        """
        with self.synchronized(force_enter=force_enter):
            self.communicate(verbosity=0)

    def communicate(self, run_sync=False, verbosity=1, save_settings=True):
        """Scan database for resolving Data objects and process them.

        This is submitted as a task to the manager's channel workers.

        :param run_sync: If ``True``, wait until all processes spawned
            from this point on have finished processing. If no processes
            are spawned, this results in a deadlock, since counts are
            handled on process finish.
        :param verbosity: Integer logging verbosity level.
        :param save_settings: If ``True``, save the current Django
            settings context to the global state. This should never be
            ``True`` for "automatic" calls, such as from Django signals,
            which can be invoked from inappropriate contexts (such as in
            the listener). For user code, it should be left at the
            default value. The saved settings are in effect until the
            next such call.
        """
        # Set communicate() claim on the semaphore.
        new_sema = self.state.sync_semaphore.add(1)
        logger.debug(__("Manager changed sync_semaphore UP to {} in communicate().", new_sema))

        executor = getattr(settings, 'FLOW_EXECUTOR', {}).get('NAME', 'resolwe.flow.executors.local')
        logger.debug(__(
            "Manager '{}.{}' sending communicate command on '{}' with executor set to '{}'.",
            self.__class__.__module__,
            self.__class__.__name__,
            state.MANAGER_CONTROL_CHANNEL,
            executor
        ))

        saved_settings = self.state.settings_override
        if save_settings:
            saved_settings = self._marshal_settings()
            self.state.settings_override = saved_settings

        Channel(state.MANAGER_CONTROL_CHANNEL).send({
            WorkerProtocol.COMMAND: WorkerProtocol.COMMUNICATE,
            WorkerProtocol.COMMUNICATE_SETTINGS: saved_settings,
            WorkerProtocol.COMMUNICATE_EXTRA: {
                'run_sync': run_sync,
                'verbosity': verbosity,
                'executor': executor,
            },
        }, immediately=True)

        if run_sync:
            logger.debug(__(
                "Manager '{}.{}' on channel '{}' entering synchronization block.",
                self.__class__.__module__,
                self.__class__.__name__,
                state.MANAGER_CONTROL_CHANNEL
            ))
            with self.synchronized():
                pass
            logger.debug(__(
                "Manager '{}.{}' on channel '{}' exiting synchronization block.",
                self.__class__.__module__,
                self.__class__.__name__,
                state.MANAGER_CONTROL_CHANNEL
            ))

    def _communicate(self, run_sync=False, verbosity=1, executor='resolwe.flow.executors.local', **kwargs):
        """Resolve task dependencies and run the task.

        :param run_sync: If ``True``, wait until all processes spawned
            from this point on have finished processing. If no processes
            are spawned, this results in a deadlock, since counts are
            handled on process finish.
        :param verbosity: Integer logging verbosity level.
        :param executor: The fully qualified name of the executor to use
            for all :class:`~resolwe.flow.models.Data` objects
            discovered in this pass.
        """
        queue = []
        logger.debug(__(
            "Manager '{}.{}' processing communicate command on '{}' for executor '{}'.",
            self.__class__.__module__,
            self.__class__.__name__,
            state.MANAGER_CONTROL_CHANNEL,
            executor
        ))
        if self._drop_ctypes:
            ContentType.objects.clear_cache()
        try:
            for data in Data.objects.filter(status=Data.STATUS_RESOLVING):
                with transaction.atomic():
                    # Lock for update. Note that we want this transaction to be as short as
                    # possible in order to reduce contention and avoid deadlocks. This is
                    # why we do not lock all resolving objects for update, but instead only
                    # lock one object at a time. This allows managers running in parallel
                    # to process different objects.
                    data = Data.objects.select_for_update().get(pk=data.pk)
                    if data.status != Data.STATUS_RESOLVING:
                        # The object might have already been processed while waiting for
                        # the lock to be obtained. In this case, skip the object.
                        continue

                    dep_status = dependency_status(data)

                    if dep_status == Data.STATUS_ERROR:
                        data.status = Data.STATUS_ERROR
                        data.process_error.append("One or more inputs have status ERROR")
                        data.process_rc = 1
                        data.save()
                        continue

                    elif dep_status != Data.STATUS_DONE:
                        continue

                    if data.process.run:
                        try:
                            execution_engine = data.process.run.get('language', None)
                            # Evaluation by the execution engine may spawn additional data objects
                            # and perform other queries on the database. Queries of all possible
                            # execution engines need to be audited for possibilities of deadlocks
                            # in case any additional locks are introduced. Currently, we only take
                            # an explicit lock on the currently processing object.
                            program = self.get_execution_engine(execution_engine).evaluate(data)
                        except (ExecutionError, InvalidEngineError) as error:
                            data.status = Data.STATUS_ERROR
                            data.process_error.append("Error in process script: {}".format(error))
                            data.save()
                            continue
                    else:
                        # If there is no run section, then we should not try to run anything. But the
                        # program must not be set to None as then the process will be stuck in waiting state.
                        program = ''

                    if data.status != Data.STATUS_DONE:
                        # The data object may already be marked as done by the execution engine. In this
                        # case we must not revert the status to STATUS_WAITING.
                        data.status = Data.STATUS_WAITING
                    data.save(render_name=True)

                    if program is not None:
                        try:
                            priority = 'normal'
                            if data.process.persistence == Process.PERSISTENCE_TEMP:
                                # TODO: This should probably be removed.
                                priority = 'high'
                            if data.process.scheduling_class == Process.SCHEDULING_CLASS_INTERACTIVE:
                                priority = 'high'

                            program = self._include_environment_variables(program)

                            data_dir = self._prepare_data_dir(data.id)
                            executor_module, runtime_dir = self._prepare_executor(data.id, executor)
                            self._prepare_context(data.id, data_dir, runtime_dir, verbosity=verbosity)
                            self._prepare_script(runtime_dir, program)
                            argv = [
                                '/bin/bash',
                                '-c',
                                self.settings_actual.get('FLOW_EXECUTOR', {}).get('PYTHON', '/usr/bin/env python') +
                                ' -m executors ' + executor_module
                            ]
                        except OSError as err:
                            logger.error(__(
                                "OSError occurred while preparing data {} (will skip): {}",
                                data.id, err
                            ))
                            continue

                        queue.append((data.id, priority, runtime_dir, argv))

                    # All data objects created by the execution engine are commited after this
                    # point and may be processed by other managers running in parallel. At the
                    # same time, lock for the current data object is released.

        except IntegrityError as exp:
            logger.error(__("IntegrityError in manager {}", exp))
            return

        logger.debug(__(
            "Manager '{}.{}' queued {} data objects to process.",
            self.__class__.__module__,
            self.__class__.__name__,
            len(queue)
        ))

        for data_id, priority, runtime_dir, argv in queue:
            if verbosity >= 1:
                print("Running", runtime_dir)
            self.state.executor_count.add(1)
            # Set execution claim on the semaphore.
            new_sema = self.state.sync_semaphore.add(1)
            logger.debug(__("Manager changed sync_semaphore UP to {} on executor start.", new_sema))
            self.run(data_id, runtime_dir, argv, priority=priority, verbosity=verbosity)

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
