""".. Ignore pydocstyle D400.

========
Listener
========

Standalone Redis client used as a contact point for executors.

.. autoclass:: resolwe.flow.managers.listener.ExecutorListener
    :members:

"""

import asyncio
import json
import logging
import math
import os
import time
import traceback

import aioredis
from asgiref.sync import async_to_sync
from channels.db import database_sync_to_async

from django.conf import settings
from django.contrib.contenttypes.models import ContentType
from django.core.exceptions import ValidationError
from django.db import transaction
from django.db.models import Count
from django.urls import reverse

from django_priority_batch import PrioritizedBatcher

from resolwe.flow.models import Data, DataDependency, Entity, Process
from resolwe.flow.utils import dict_dot, iterate_fields, stats
from resolwe.flow.utils.purge import data_purge
from resolwe.permissions.utils import copy_permissions
from resolwe.test.utils import is_testing
from resolwe.utils import BraceMessage as __

from . import consumer, state
from .protocol import ExecutorProtocol, WorkerProtocol

if settings.USE_TZ:
    from django.utils.timezone import now  # pylint: disable=ungrouped-imports
else:
    import datetime
    now = datetime.datetime.now  # pylint: disable=invalid-name

logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


class ExecutorListener:
    """The contact point implementation for executors."""

    def __init__(self, *args, **kwargs):
        """Initialize attributes.

        :param host: Optional. The hostname where redis is running.
        :param port: Optional. The port where redis is running.
        """
        super().__init__()

        # The Redis connection object.
        self._redis = None
        self._redis_params = kwargs.get('redis_params', {})

        # Running coordination.
        self._should_stop = False
        self._runner_coro = None

        # The verbosity level to pass around to Resolwe utilities
        # such as data_purge.
        self._verbosity = kwargs.get('verbosity', 1)

        # Statistics about how much time each event needed for handling.
        self.service_time = stats.NumberSeriesShape()

        # Statistics about the number of events handled per time interval.
        self.load_avg = stats.SimpleLoadAvg([60, 5 * 60, 15 * 60])

        # Timestamp of last critical load error and level, for throttling.
        self.last_load_log = -math.inf
        self.last_load_level = 0

    async def _make_connection(self):
        """Construct a connection to Redis."""
        return await aioredis.create_redis(
            'redis://{}:{}'.format(
                self._redis_params.get('host', 'localhost'),
                self._redis_params.get('port', 6379)
            ),
            db=int(self._redis_params.get('db', 1))
        )

    async def _call_redis(self, meth, *args, **kwargs):
        """Perform a Redis call and handle connection dropping."""
        while True:
            try:
                if not self._redis:
                    self._redis = await self._make_connection()
                return await meth(self._redis, *args, **kwargs)
            except aioredis.RedisError:
                logger.exception("Redis connection error")
                if self._redis:
                    self._redis.close()
                    await self._redis.wait_closed()
                    self._redis = None
                await asyncio.sleep(3)

    async def clear_queue(self):
        """Reset the executor queue channel to an empty state."""
        conn = await self._make_connection()
        try:
            script = """
                local keys = redis.call('KEYS', ARGV[1])
                redis.call('DEL', unpack(keys))
            """
            await conn.eval(
                script,
                keys=[],
                args=['*{}*'.format(settings.FLOW_MANAGER['REDIS_PREFIX'])],
            )
        finally:
            conn.close()

    async def __aenter__(self):
        """On entering a context, start the listener thread."""
        self._should_stop = False
        self._redis = await self._make_connection()
        self._runner_coro = asyncio.ensure_future(self.run())
        return self

    async def __aexit__(self, typ, value, trace):
        """On exiting a context, kill the listener and wait for it.

        .. note::

            Exceptions are all propagated.
        """
        await self._runner_coro
        self._redis.close()
        await self._redis.wait_closed()
        # Make sure the connection is cleaned up.
        self._redis = None

    def terminate(self):
        """Stop the standalone manager."""
        logger.info(__(
            "Terminating Resolwe listener on channel '{}'.",
            state.MANAGER_EXECUTOR_CHANNELS.queue
        ))
        self._should_stop = True

    def _queue_response_channel(self, obj):
        """Generate the feedback channel name from the object's id.

        :param obj: The Channels message object.
        """
        return '{}.{}'.format(state.MANAGER_EXECUTOR_CHANNELS.queue_response, obj[ExecutorProtocol.DATA_ID])

    # The handle_* methods are all Django synchronized, meaning they're run
    # in separate threads. Having this method be sync and calling async_to_sync
    # on rpush itself would mean reading self._redis from the sync thread,
    # which isn't very tidy. If it's async, it'll be called from the main
    # thread by the async_to_sync calls in handle_*.
    async def _send_reply(self, obj, reply):
        """Send a reply with added standard fields back to executor.

        :param obj: The original Channels message object to which we're
            replying.
        :param reply: The message contents dictionary. The data id is
            added automatically (``reply`` is modified in place).
        """
        reply.update({
            ExecutorProtocol.DATA_ID: obj[ExecutorProtocol.DATA_ID],
        })
        await self._call_redis(aioredis.Redis.rpush, self._queue_response_channel(obj), json.dumps(reply))

    def hydrate_spawned_files(self, exported_files_mapper, filename, data_id):
        """Pop the given file's map from the exported files mapping.

        :param exported_files_mapper: The dict of file mappings this
            process produced.
        :param filename: The filename to format and remove from the
            mapping.
        :param data_id: The id of the :meth:`~resolwe.flow.models.Data`
            object owning the mapping.
        :return: The formatted mapping between the filename and
            temporary file path.
        :rtype: dict
        """
        # JSON only has string dictionary keys, so the Data object id
        # needs to be stringified first.
        data_id = str(data_id)

        if filename not in exported_files_mapper[data_id]:
            raise KeyError("Use 're-export' to prepare the file for spawned process: {}".format(filename))

        export_fn = exported_files_mapper[data_id].pop(filename)

        if exported_files_mapper[data_id] == {}:
            exported_files_mapper.pop(data_id)

        return {'file_temp': export_fn, 'file': filename}

    def handle_update(self, obj, internal_call=False):
        """Handle an incoming ``Data`` object update request.

        :param obj: The Channels message object. Command object format:

            .. code-block:: none

                {
                    'command': 'update',
                    'data_id': [id of the :class:`~resolwe.flow.models.Data`
                               object this command changes],
                    'changeset': {
                        [keys to be changed]
                    }
                }

        :param internal_call: If ``True``, this is an internal delegate
            call, so a reply to the executor won't be sent.
        """
        data_id = obj[ExecutorProtocol.DATA_ID]
        changeset = obj[ExecutorProtocol.UPDATE_CHANGESET]
        if not internal_call:
            logger.debug(
                __("Handling update for Data with id {} (handle_update).", data_id),
                extra={
                    'data_id': data_id,
                    'packet': obj
                }
            )
        try:
            d = Data.objects.get(pk=data_id)
        except Data.DoesNotExist:
            logger.warning(
                "Data object does not exist (handle_update).",
                extra={
                    'data_id': data_id,
                }
            )

            if not internal_call:
                async_to_sync(self._send_reply)(obj, {ExecutorProtocol.RESULT: ExecutorProtocol.RESULT_ERROR})

            async_to_sync(consumer.send_event)({
                WorkerProtocol.COMMAND: WorkerProtocol.ABORT,
                WorkerProtocol.DATA_ID: obj[ExecutorProtocol.DATA_ID],
                WorkerProtocol.FINISH_COMMUNICATE_EXTRA: {
                    'executor': getattr(settings, 'FLOW_EXECUTOR', {}).get('NAME', 'resolwe.flow.executors.local'),
                },
            })

            return

        if changeset.get('status', None) == Data.STATUS_ERROR:
            logger.error(
                __("Error occured while running process '{}' (handle_update).", d.process.slug),
                extra={
                    'data_id': data_id,
                    'api_url': '{}{}'.format(
                        getattr(settings, 'RESOLWE_HOST_URL', ''),
                        reverse('resolwe-api:data-detail', kwargs={'pk': data_id})
                    ),
                }
            )

        if d.status == Data.STATUS_ERROR:
            changeset['status'] = Data.STATUS_ERROR

        if not d.started:
            changeset['started'] = now()
        changeset['modified'] = now()

        for key, val in changeset.items():
            if key in ['process_error', 'process_warning', 'process_info']:
                # Trim process_* fields to not exceed max length of the database field.
                for i, entry in enumerate(val):
                    max_length = Data._meta.get_field(key).base_field.max_length  # pylint: disable=protected-access
                    if len(entry) > max_length:
                        val[i] = entry[:max_length - 3] + '...'

                getattr(d, key).extend(val)

            elif key != 'output':
                setattr(d, key, val)

        if 'output' in changeset:
            if not isinstance(d.output, dict):
                d.output = {}
            for key, val in changeset['output'].items():
                dict_dot(d.output, key, val)

        try:
            with PrioritizedBatcher.global_instance():
                d.save(update_fields=list(changeset.keys()))
        except ValidationError as exc:
            logger.error(
                __(
                    "Validation error when saving Data object of process '{}' (handle_update):\n\n{}",
                    d.process.slug,
                    traceback.format_exc()
                ),
                extra={
                    'data_id': data_id
                }
            )

            d.refresh_from_db()

            d.process_error.append(exc.message)
            d.status = Data.STATUS_ERROR

            try:
                with PrioritizedBatcher.global_instance():
                    d.save(update_fields=['process_error', 'status'])
            except Exception:  # pylint: disable=broad-except
                pass
        except Exception:  # pylint: disable=broad-except
            logger.error(
                __(
                    "Error when saving Data object of process '{}' (handle_update):\n\n{}",
                    d.process.slug,
                    traceback.format_exc()
                ),
                extra={
                    'data_id': data_id
                }
            )

        if not internal_call:
            async_to_sync(self._send_reply)(obj, {ExecutorProtocol.RESULT: ExecutorProtocol.RESULT_OK})

    def handle_finish(self, obj):
        """Handle an incoming ``Data`` finished processing request.

        :param obj: The Channels message object. Command object format:

            .. code-block:: none

                {
                    'command': 'finish',
                    'data_id': [id of the :class:`~resolwe.flow.models.Data` object
                               this command changes],
                    'process_rc': [exit status of the processing]
                    'spawn_processes': [optional; list of spawn dictionaries],
                    'exported_files_mapper': [if spawn_processes present]
                }
        """
        data_id = obj[ExecutorProtocol.DATA_ID]
        logger.debug(
            __("Finishing Data with id {} (handle_finish).", data_id),
            extra={
                'data_id': data_id,
                'packet': obj
            }
        )

        with transaction.atomic():
            # Spawn any new jobs in the request.
            spawned = False
            if ExecutorProtocol.FINISH_SPAWN_PROCESSES in obj:
                if is_testing():
                    # NOTE: This is a work-around for Django issue #10827
                    # (https://code.djangoproject.com/ticket/10827), same as in
                    # TestCaseHelpers._pre_setup(). Because the listener is running
                    # independently, it must clear the cache on its own.
                    ContentType.objects.clear_cache()

                spawned = True
                exported_files_mapper = obj[ExecutorProtocol.FINISH_EXPORTED_FILES]
                logger.debug(
                    __("Spawning new Data objects for Data with id {} (handle_finish).", data_id),
                    extra={
                        'data_id': data_id
                    }
                )

                try:
                    # This transaction is needed because we're running
                    # asynchronously with respect to the main Django code
                    # here; the manager can get nudged from elsewhere.
                    with transaction.atomic():
                        parent_data = Data.objects.get(pk=data_id)

                        # Spawn processes.
                        for d in obj[ExecutorProtocol.FINISH_SPAWN_PROCESSES]:
                            d['contributor'] = parent_data.contributor
                            d['process'] = Process.objects.filter(slug=d['process']).latest()

                            for field_schema, fields in iterate_fields(d.get('input', {}), d['process'].input_schema):
                                type_ = field_schema['type']
                                name = field_schema['name']
                                value = fields[name]

                                if type_ == 'basic:file:':
                                    fields[name] = self.hydrate_spawned_files(exported_files_mapper, value, data_id)
                                elif type_ == 'list:basic:file:':
                                    fields[name] = [self.hydrate_spawned_files(exported_files_mapper, fn, data_id)
                                                    for fn in value]

                            with transaction.atomic():
                                d = Data.objects.create(**d)
                                DataDependency.objects.create(
                                    parent=parent_data,
                                    child=d,
                                    kind=DataDependency.KIND_SUBPROCESS,
                                )

                                # Copy permissions.
                                copy_permissions(parent_data, d)

                                # Entity is added to the collection only when it is
                                # created - when it only contains 1 Data object.
                                entities = Entity.objects.filter(data=d).annotate(num_data=Count('data')).filter(
                                    num_data=1)

                                # Copy collections.
                                for collection in parent_data.collection_set.all():
                                    collection.data.add(d)

                                    # Add entities to which data belongs to the collection.
                                    for entity in entities:
                                        entity.collections.add(collection)

                except Exception:  # pylint: disable=broad-except
                    logger.error(
                        __(
                            "Error while preparing spawned Data objects of process '{}' (handle_finish):\n\n{}",
                            parent_data.process.slug,
                            traceback.format_exc()
                        ),
                        extra={
                            'data_id': data_id
                        }
                    )

            # Data wrap up happens last, so that any triggered signals
            # already see the spawned children. What the children themselves
            # see is guaranteed by the transaction we're in.
            if ExecutorProtocol.FINISH_PROCESS_RC in obj:
                process_rc = obj[ExecutorProtocol.FINISH_PROCESS_RC]

                try:
                    d = Data.objects.get(pk=data_id)
                except Data.DoesNotExist:
                    logger.warning(
                        "Data object does not exist (handle_finish).",
                        extra={
                            'data_id': data_id,
                        }
                    )
                    async_to_sync(self._send_reply)(obj, {ExecutorProtocol.RESULT: ExecutorProtocol.RESULT_ERROR})
                    return

                if process_rc == 0 and not d.status == Data.STATUS_ERROR:
                    changeset = {
                        'status': Data.STATUS_DONE,
                        'process_progress': 100,
                        'finished': now()
                    }
                else:
                    changeset = {
                        'status': Data.STATUS_ERROR,
                        'process_progress': 100,
                        'process_rc': process_rc,
                        'finished': now()
                    }
                obj[ExecutorProtocol.UPDATE_CHANGESET] = changeset
                self.handle_update(obj, internal_call=True)

                if not getattr(settings, 'FLOW_MANAGER_KEEP_DATA', False):
                    try:
                        # Clean up after process
                        data_purge(data_ids=[data_id], delete=True, verbosity=self._verbosity)
                    except Exception:  # pylint: disable=broad-except
                        logger.error(
                            __("Purge error:\n\n{}", traceback.format_exc()),
                            extra={
                                'data_id': data_id
                            }
                        )

        # Notify the executor that we're done.
        async_to_sync(self._send_reply)(obj, {ExecutorProtocol.RESULT: ExecutorProtocol.RESULT_OK})

        # Now nudge the main manager to perform final cleanup. This is
        # needed even if there was no spawn baggage, since the manager
        # may need to know when executors have finished, to keep count
        # of them and manage synchronization.
        async_to_sync(consumer.send_event)({
            WorkerProtocol.COMMAND: WorkerProtocol.FINISH,
            WorkerProtocol.DATA_ID: data_id,
            WorkerProtocol.FINISH_SPAWNED: spawned,
            WorkerProtocol.FINISH_COMMUNICATE_EXTRA: {
                'executor': getattr(settings, 'FLOW_EXECUTOR', {}).get('NAME', 'resolwe.flow.executors.local'),
            },
        })

    def handle_abort(self, obj):
        """Handle an incoming ``Data`` abort processing request.

        .. IMPORTANT::

            This only makes manager's state consistent and doesn't
            affect Data object in any way. Any changes to the Data
            must be applied over ``handle_update`` method.

        :param obj: The Channels message object. Command object format:

            .. code-block:: none

                {
                    'command': 'abort',
                    'data_id': [id of the :class:`~resolwe.flow.models.Data` object
                               this command was triggered by],
                }
        """
        async_to_sync(consumer.send_event)({
            WorkerProtocol.COMMAND: WorkerProtocol.ABORT,
            WorkerProtocol.DATA_ID: obj[ExecutorProtocol.DATA_ID],
            WorkerProtocol.FINISH_COMMUNICATE_EXTRA: {
                'executor': getattr(settings, 'FLOW_EXECUTOR', {}).get('NAME', 'resolwe.flow.executors.local'),
            },
        })

    def handle_log(self, obj):
        """Handle an incoming log processing request.

        :param obj: The Channels message object. Command object format:

            .. code-block:: none

                {
                    'command': 'log',
                    'message': [log message]
                }
        """
        record_dict = json.loads(obj[ExecutorProtocol.LOG_MESSAGE])
        record_dict['msg'] = record_dict['msg']

        executors_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'executors')
        record_dict['pathname'] = os.path.join(executors_dir, record_dict['pathname'])

        logger.handle(logging.makeLogRecord(record_dict))

    def _make_stats(self):
        """Create a stats snapshot."""
        return {
            'load_avg': self.load_avg.to_dict(),
            'service_time': self.service_time.to_dict(),
        }

    async def push_stats(self):
        """Push current stats to Redis."""
        snapshot = self._make_stats()
        try:
            serialized = json.dumps(snapshot)
            await self._call_redis(aioredis.Redis.set, state.MANAGER_LISTENER_STATS, serialized)
            await self._call_redis(aioredis.Redis.expire, state.MANAGER_LISTENER_STATS, 3600)
        except TypeError:
            logger.error(__(
                "Listener can't serialize statistics:\n\n{}",
                traceback.format_exc()
            ))
        except aioredis.RedisError:
            logger.error(__(
                "Listener can't store updated statistics:\n\n{}",
                traceback.format_exc()
            ))

    def check_critical_load(self):
        """Check for critical load and log an error if necessary."""
        if self.load_avg.intervals['1m'].value > 1:
            if self.last_load_level == 1 and time.time() - self.last_load_log < 30:
                return
            self.last_load_log = time.time()
            self.last_load_level = 1
            logger.error(
                "Listener load limit exceeded, the system can't handle this!",
                extra=self._make_stats()
            )

        elif self.load_avg.intervals['1m'].value > 0.8:
            if self.last_load_level == 0.8 and time.time() - self.last_load_log < 30:
                return
            self.last_load_log = time.time()
            self.last_load_level = 0.8
            logger.warning(
                "Listener load approaching critical!",
                extra=self._make_stats()
            )

        else:
            self.last_load_log = -math.inf
            self.last_load_level = 0

    async def run(self):
        """Run the main listener run loop.

        Doesn't return until :meth:`terminate` is called.
        """
        logger.info(__(
            "Starting Resolwe listener on channel '{}'.",
            state.MANAGER_EXECUTOR_CHANNELS.queue
        ))
        while not self._should_stop:
            await self.push_stats()
            ret = await self._call_redis(aioredis.Redis.blpop, state.MANAGER_EXECUTOR_CHANNELS.queue, timeout=1)
            if ret is None:
                self.load_avg.add(0)
                continue
            remaining = await self._call_redis(aioredis.Redis.llen, state.MANAGER_EXECUTOR_CHANNELS.queue)
            self.load_avg.add(remaining + 1)
            self.check_critical_load()
            _, item = ret
            try:
                item = item.decode('utf-8')
                logger.debug(__("Got command from executor: {}", item))
                obj = json.loads(item)
            except json.JSONDecodeError:
                logger.error(
                    __("Undecodable command packet:\n\n{}"),
                    traceback.format_exc()
                )
                continue

            command = obj.get(ExecutorProtocol.COMMAND, None)
            if command is None:
                continue

            service_start = time.perf_counter()

            handler = getattr(self, 'handle_' + command, None)
            if handler:
                try:
                    await database_sync_to_async(handler)(obj)
                except Exception:  # pylint: disable=broad-except
                    logger.error(__(
                        "Executor command handling error:\n\n{}",
                        traceback.format_exc()
                    ))
            else:
                logger.error(
                    __("Unknown executor command '{}'.", command),
                    extra={'decoded_packet': obj}
                )

            # We do want to measure wall-clock time elapsed, because
            # system load will impact event handling performance. On
            # a lagging system, good internal performance is meaningless.
            service_end = time.perf_counter()
            self.service_time.update(service_end - service_start)
        logger.info(__(
            "Stopping Resolwe listener on channel '{}'.",
            state.MANAGER_EXECUTOR_CHANNELS.queue
        ))
