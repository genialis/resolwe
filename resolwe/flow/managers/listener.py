"""Standalone Redis client used as a contact point for executors."""

import json
import logging
import traceback
from signal import SIGINT, signal
from threading import Event, Thread

import redis
from channels import Channel

from django.conf import settings
from django.contrib.contenttypes.models import ContentType
from django.core.exceptions import ValidationError
from django.db import transaction
from django.db.models import Count
from django.urls import reverse

from resolwe.flow.models import Data, DataDependency, Entity, Process
from resolwe.flow.utils import dict_dot, iterate_fields
from resolwe.flow.utils.purge import data_purge
from resolwe.permissions.utils import copy_permissions
from resolwe.utils import BraceMessage as __

from . import state
from .protocol import ExecutorProtocol, WorkerProtocol

if settings.USE_TZ:
    from django.utils.timezone import now  # pylint: disable=ungrouped-imports
else:
    import datetime
    now = datetime.datetime.now  # pylint: disable=invalid-name

logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


class ExecutorListener(Thread):
    """The contact point implementation for executors."""

    def __init__(self, *args, **kwargs):
        """Initialize attributes.

        :param host: Optional. The hostname where redis is running.
        :param port: Optional. The port where redis is running.
        """
        super().__init__()

        # The Redis connection object.
        self._redis = redis.StrictRedis(**kwargs.get('redis_params', {}))

        # An condition variable used to asynchronously signal when the
        # listener should terminate.
        self._terminated = Event()

        # The verbosity level to pass around to Resolwe utilities
        # such as data_purge.
        self._verbosity = kwargs.get('verbosity', 1)

        # If True, drop ContentType cache on every Data spawn.
        # NOTE: This is a work-around for Django issue #10827
        # (https://code.djangoproject.com/ticket/10827), same as in
        # TestCaseHelpers._pre_setup(). Because the listener is running
        # independently, it must clear the cache on its own.
        self._drop_ctypes = getattr(settings, 'FLOW_MANAGER_DISABLE_CTYPE_CACHE', False)

    def clear_queue(self):
        """Reset the executor queue channel to an empty state."""
        self._redis.delete(state.MANAGER_EXECUTOR_CHANNELS.queue)

    def start(self, *args, **kwargs):
        """Start the listener thread."""
        signal(SIGINT, self._sigint)
        logger.info(__(
            "Starting Resolwe listener on channel '{}'.",
            state.MANAGER_EXECUTOR_CHANNELS.queue
        ))
        super().start(*args, **kwargs)

    def __enter__(self):
        """On entering a context, start the listener thread."""
        self.start()
        return self

    def __exit__(self, typ, value, trace):
        """On exiting a context, kill the listener and wait for it.

        .. note::

            Exceptions are all propagated.
        """
        self.terminate()
        self.join()

        # re-raise all exceptions
        return False

    def terminate(self):
        """Stop the standalone manager."""
        logger.info(__(
            "Terminating Resolwe listener on channel '{}'.",
            state.MANAGER_EXECUTOR_CHANNELS.queue
        ))
        self._terminated.set()

    def _sigint(self, signum, frame):
        """Terminate the listener on various signals."""
        self.terminate()

    def _queue_response_channel(self, obj):
        """Generate the feedback channel name from the object's id.

        :param obj: The Channels message object.
        """
        return '{}.{}'.format(state.MANAGER_EXECUTOR_CHANNELS.queue_response, obj[ExecutorProtocol.DATA_ID])

    def _send_reply(self, obj, reply):
        """Send a reply with added standard fields back to executor.

        :param obj: The original Channels message object to which we're
            replying.
        :param reply: The message contents dictionary. The data id is
            added automatically (``reply`` is modified in place).
        """
        reply.update({
            ExecutorProtocol.DATA_ID: obj[ExecutorProtocol.DATA_ID],
        })
        self._redis.rpush(self._queue_response_channel(obj), json.dumps(reply))

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

            .. code:: none

                {
                    'command': 'update',
                    'id': [id of the :class:`~resolwe.flow.models.Data`
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
                __("Resolwe listener: handle_update[{}].", data_id),
                extra={
                    'data_id': data_id,
                    'packet': obj
                }
            )
        try:
            d = Data.objects.get(pk=data_id)
        except Data.DoesNotExist:
            logger.error(__("Resolwe listener (handle_update[{}]): Data object does not exist.", data_id))
            return

        if changeset.get('status', None) == Data.STATUS_ERROR:
            logger.error(
                __(
                    "Resolwe listener (handle_update[{}]): Error occured while running a '{}' process.",
                    data_id,
                    d.process.name
                ),
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

        for key, val in changeset.items():
            if key != 'output':
                setattr(d, key, val)
        if 'output' in changeset:
            if not isinstance(d.output, dict):
                d.output = {}
            for key, val in changeset['output'].items():
                dict_dot(d.output, key, val)

        try:
            d.save(update_fields=list(changeset.keys()))
        except ValidationError as exc:
            logger.error(
                __(
                    "Resolwe listener (handle_update[{}]): Validation error on Data object save:\n\n{}",
                    data_id,
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
                d.save(update_fields=['process_error', 'status'])
            except Exception:  # pylint: disable=broad-except
                pass
        except Exception:  # pylint: disable=broad-except
            logger.error(
                __(
                    "Resolwe listener (handle_update[{}]): Error on Data object save:\n\n{}",
                    data_id,
                    traceback.format_exc()
                ),
                extra={
                    'data_id': data_id
                }
            )

        if not internal_call:
            self._send_reply(obj, {ExecutorProtocol.RESULT: ExecutorProtocol.RESULT_OK})

    def handle_finish(self, obj):
        """Handle an incoming ``Data`` finished processing request.

        :param obj: The Channels message object. Command object format:

            .. code:: none

                {
                    'command': 'finish',
                    'id': [id of the :class:`~resolwe.flow.models.Data` object
                          this command changes],
                    'process_rc': [exit status of the processing]
                    'spawn_processes': [optional; list of spawn dictionaries],
                    'exported_files_mapper': [if spawn_processes present]
                }
        """
        data_id = obj[ExecutorProtocol.DATA_ID]
        logger.debug(
            __("Resolwe listener: handle_finish[{}].", data_id),
            extra={
                'data_id': data_id,
                'packet': obj
            }
        )

        with transaction.atomic():
            # Spawn any new jobs in the request.
            spawned = False
            if ExecutorProtocol.FINISH_SPAWN_PROCESSES in obj:
                if self._drop_ctypes:
                    ContentType.objects.clear_cache()
                spawned = True
                exported_files_mapper = obj[ExecutorProtocol.FINISH_EXPORTED_FILES]
                logger.debug(
                    __("Resolwe listener (handle_finish[{}]): Spawning new Data objects.", data_id),
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
                            "Resolwe listener (handle_finish[{}]): Error while preparing spawned Data objects:\n\n{}",
                            data_id,
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
                    logger.error(__("Resolwe listener (handle_finish[{}]): Data object does not exist.", data_id))
                    self._send_reply(obj, {ExecutorProtocol.RESULT: ExecutorProtocol.RESULT_OK})
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
        self._send_reply(obj, {ExecutorProtocol.RESULT: ExecutorProtocol.RESULT_OK})

        # Now nudge the main manager to perform final cleanup. This is
        # needed even if there was no spawn baggage, since the manager
        # may need to know when executors have finished, to keep count
        # of them and manage synchronization.
        Channel(state.MANAGER_CONTROL_CHANNEL).send({
            WorkerProtocol.COMMAND: WorkerProtocol.FINISH,
            WorkerProtocol.DATA_ID: data_id,
            WorkerProtocol.FINISH_SPAWNED: spawned,
            WorkerProtocol.FINISH_COMMUNICATE_EXTRA: {
                'executor': getattr(settings, 'FLOW_EXECUTOR', {}).get('NAME', 'resolwe.flow.executors.local'),
            },
        })

    def run(self):
        """Run the main listener run loop.

        Doesn't return until :meth:`terminate` is called.
        """
        while not self._terminated.is_set():
            ret = self._redis.blpop(state.MANAGER_EXECUTOR_CHANNELS.queue, timeout=1)
            if ret is None:
                continue
            _, item = ret
            try:
                obj = json.loads(item.decode('utf-8'))
            except json.JSONDecodeError:
                logger.error(
                    __("Resolwe listener: Undecodable command packet:\n\n{}"),
                    traceback.format_exc()
                )
                continue

            command = obj.get(ExecutorProtocol.COMMAND, None)
            if command is None:
                continue

            handler = getattr(self, 'handle_' + command, None)
            if handler:
                try:
                    handler(obj)
                except Exception:  # pylint: disable=broad-except
                    logger.error(__(
                        "Resolwe listener: Executor command handling error:\n\n{}",
                        traceback.format_exc()
                    ))
            else:
                logger.error(
                    __("Resolwe listener: Unknown executor command '{}'.", command),
                    extra={'decoded_packet': obj}
                )
