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
import logging
import pickle
from contextlib import suppress
from datetime import datetime
from functools import lru_cache
from os import getpid
from time import time
from typing import Any, ChainMap, Dict, Iterable, List, Optional, Set, Union

import redis
import zmq
import zmq.asyncio
from asgiref.sync import async_to_sync
from channels.db import database_sync_to_async
from channels.layers import get_channel_layer

from django.conf import settings
from django.contrib.auth import get_user_model
from django.core.exceptions import ValidationError
from django.db import transaction
from django.utils.timezone import now

from django_priority_batch import PrioritizedBatcher

from resolwe.flow.executors.socket_utils import (
    BaseProtocol,
    Message,
    PeerIdentity,
    Response,
    ResponseStatus,
)
from resolwe.flow.executors.zeromq_utils import ZMQCommunicator
from resolwe.flow.managers import consumer
from resolwe.flow.managers.protocol import WorkerProtocol
from resolwe.flow.managers.state import LISTENER_CONTROL_CHANNEL  # noqa: F401
from resolwe.flow.models import Data, Storage, Worker
from resolwe.flow.utils import iterate_schema
from resolwe.storage.models import AccessLog
from resolwe.test.utils import is_testing
from resolwe.utils import BraceMessage as __

# Register plugins by importing them.
from .basic_commands_plugin import BasicCommands  # noqa: F401
from .bootstrap_plugin import BootstrapCommands  # noqa: F401
from .plugin import plugin_manager
from .python_process_plugin import PythonProcess  # noqa: F401

logger = logging.getLogger(__name__)
User = get_user_model()


class RedisCache:
    """The Redis cache."""

    key_expiration = 24 * 3600
    cached_fields_set = {"status", "started", "worker__status"}

    def __init__(self, redis: redis.Redis, *args, **kwargs):
        """Set the cached field set."""
        # The set of fields on the data object that can be safely cached inside
        # Redis to avoid hitting the database (we are assuming Redis is faster).
        # The list contains a set of commonly used fields that do not change
        # from the outside (or do no harm if they do).

        self._redis = redis

        super().__init__(*args, **kwargs)

        # Erase the entire cache on start when testing.
        if is_testing():
            self.clear()

    def get_redis_key(self, data_id: int, field_name: str) -> str:
        """Get the redis key from the field name.

        When testing every listener process must have its own redis shared
        storage since data ids will repeat when tests are run in parallel.
        """
        if is_testing():
            return (
                f"resolwe-listener-{getpid()}-data-{data_id}-redis-cache-{field_name}"
            )
        else:
            return f"resolwe-listener-data-{data_id}-redis-cache-{field_name}"

    def get_cache(
        self,
        data_id: int,
        field_names: Iterable[str],
    ) -> Dict[str, Any]:
        """Obtain the set of fields from the redis cache.

        If field_name is requested that is not cached the request is silently
        ignored.

        The query is run in a transaction.
        """

        def get_redis_data(
            pipeline: redis.client.Pipeline,
        ) -> Dict[str, Optional[Union[str, datetime]]]:
            def get_redis_field(
                redis_key: str, field_name: str
            ) -> Optional[Union[str, datetime]]:
                """Get the redis data from the specific key."""
                value: Optional[bytes] = pipeline.get(redis_key)  # type: ignore
                return None if value is None else pickle.loads(value)

            return {
                field_name: get_redis_field(
                    self.get_redis_key(data_id, field_name), field_name
                )
                for field_name in field_names
                if field_name in self.cached_fields_set
            }

        watches = [
            self.get_redis_key(data_id, field_name)
            for field_name in field_names
            if field_name in self.cached_fields_set
        ]
        return self._redis.transaction(
            get_redis_data, *watches, value_from_callable=True
        )

    def clear(self, data_id: Optional[int] = None):
        """Clear the entire Redis cache for the given data object.

        When data_id is not given the entire cache is cleared.
        """
        if data_id is not None:
            redis_keys = [
                self.get_redis_key(data_id, field_name)
                for field_name in self.cached_fields_set
            ]
        else:
            redis_keys = list(self._redis.scan_iter(match="resolwe-listener-*"))
        if redis_keys:
            self._redis.delete(*redis_keys)

    def set_cache(self, data_id: int, field_values: Dict[str, Any]):
        """Set the redis cache in the transaction.

        When field is not in the set of cached fields it is silentry ignored.
        """

        def update_cache(pipeline: redis.client.Pipeline):
            """Update the cache using pipeline."""
            for field_name, field_value in field_values.items():
                if field_name not in self.cached_fields_set:
                    continue
                key = self.get_redis_key(data_id, field_name)
                if field_value is None:
                    pipeline.delete(key)
                else:
                    pipeline.set(key, pickle.dumps(field_value))
                    pipeline.expire(key, RedisCache.key_expiration)

        self._redis.transaction(update_cache)


class Processor:
    """Process the messages sent by the workers.

    Some of the data attributes are cached by redis.

    Important: the consistincy of the cache is guaranteed only when the
    `_update_data`  or `_save_data` method is used to save data.
    """

    def __init__(self, listener: "ListenerProtocol", redis: redis.Redis):
        """Initialize variables."""
        self._listener = listener
        self._return_codes: Dict[PeerIdentity, int] = dict()
        self._redis_cache = RedisCache(redis)

    def get_data_fields(self, data_id: int, field_names: Union[List[str], str]) -> Any:
        """Get the data fields for the data with the given id.

        Note: no permission check is done so only call this method on Data
            objects user is clear to access.

        :attr data_id: the id of the data object.
        :attr field_names: the names of the fields. This can be a sequence or
            a string.
        :return: a single value when field_names is a string or a tuple of
            values when it is a sequence of strings.
        """
        fields = [field_names] if isinstance(field_names, str) else field_names
        # Construct the set of fields to retrieve from the database/redis.
        database_fields = set(fields) - RedisCache.cached_fields_set

        # Get the redis cache.
        redis_cache = self._redis_cache.get_cache(data_id, fields)

        # When returned fields have value None we have to read them from the
        # database.
        database_fields.update(
            name for name in redis_cache if redis_cache[name] is None
        )

        # Make sure to only read the data if database fields are not empty.
        # Django interprets empty dict as "read all fields".
        database_data = {}
        if database_fields:
            database_data = (
                Data.objects.filter(pk=data_id).values(*database_fields).get()
            )
        # Now update redis cache from the values read from the database. The
        # values we can update are the ones that had None value in Redis.
        self._redis_cache.set_cache(data_id, database_data)

        combined = ChainMap(database_data, redis_cache)
        result = [combined[field_name] for field_name in fields]
        return result[0] if isinstance(field_names, str) else result

    @lru_cache(maxsize=100)
    def contributor_id(self, data_id: int) -> int:
        """Get the id of the user that created the given data object.

        This function is cached since contributor is immutable.
        """
        return self.contributor(data_id).id

    @lru_cache(maxsize=100)
    def contributor(self, data_id: int):
        """Get the user that created the given data objects.

        This function is cached since contributor is immutable.
        """
        return User.objects.get(data__id=data_id)

    def data(self, data_id: int) -> Data:
        """Get the data object for the given data id.

        Use with caution: loading the entire data object is slow.
        """
        return Data.objects.get(pk=data_id)

    def worker(self, data_id: int) -> Worker:
        """Get the worker object.

        The evaluation is done when needed in order not to make potentially
        failing SQL query in the constructor.

        This method uses Django ORM.
        """
        return Worker.objects.get(data=data_id)

    def storage_fields(self, data_id) -> Set[str]:
        """Get the names of storage fields in the schema.

        :attrs data_id: the id of the data object to get the fields for.
        """
        output_schema = self.get_data_fields(data_id, "process__output_schema")
        return {
            field_name
            for schema, _, field_name in iterate_schema({}, output_schema)
            if schema["type"].startswith("basic:json:")
        }

    def save_storage(self, key: str, content: str, data: Data) -> Storage:
        """Save storage field and add it to the given data.

        :return: the pk of the saved storage object.
        :raise TypeError: when the data object does not exist.
        """

        storage_pk = data.output.get(key)
        storage_defaults = {
            "json": content,
            "name": f"Storage for data id {data.pk}",
            "contributor_id": data.contributor_id,
        }
        storage: Storage = Storage.objects.update_or_create(
            pk=storage_pk, defaults=storage_defaults
        )[0]
        storage.data.add(data)
        return storage

    def _unlock_all_inputs(self, data_id: int):
        """Unlock all data objects that were locked by the given data.

        If exception occurs during unlocking we can not do much but ignore it.
        The method is called when dispatcher is nudged.
        """
        with suppress(Exception):
            query = AccessLog.objects.filter(cause_id=data_id)
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

    def _save_error(self, data: Data, error: str):
        """Log error to Data object, ignore possible exceptions.

        The status of Data object is changed to ``Data.STATUS_ERROR``.
        """
        logger.debug("Saving error to data object %s", data.id)
        if error not in data.process_error:
            data.process_error.append(error)
        data.status = Data.STATUS_ERROR
        try:
            self._save_data(data, ["process_error", "status"])
        except Exception:
            logger.exception("Error when saving error to the data object.")

    def _log_exception(
        self,
        data: Data,
        error: str,
        extra: Optional[dict] = None,
        save_to_data_object: bool = True,
    ):
        """Log current exception and optionally store it to the data object.

        When argument ``save_to_data_object`` is false the status of the
        data object is not changed.
        """
        if extra is None:
            extra = dict()
        extra.update({"data_id": data.id})
        logger.exception(error, extra=extra)
        if save_to_data_object:
            self._save_error(data, error)

    def _log_error(
        self,
        data: Data,
        error: str,
        extra: dict = {},
        save_to_data_object: bool = True,
    ):
        """Log error and optionally store it to the data object.

        When argument ``save_to_data_object`` is false the status of the
        data object is not changed.
        """
        extra.update({"data_id": data.id})
        logger.error(error, extra=extra)
        if save_to_data_object:
            assert data is not None, "Can not save error to None object."
            self._save_error(data, error)

    def _save_data(self, data: Data, changes: Iterable[str]):
        """Update the data object with the given id.

        Changes are dictionary mapping from field names to field values.

        :attr changes: dictionary with changes to be saved.
        :attr data_id: the given data id.

        :raises: exception when data object cannot be saved.
        """
        # Set the redis cache.
        self._redis_cache.set_cache(
            data.id, {change: getattr(data, change) for change in changes}
        )
        # Save the data object.
        data.save(update_fields=changes)

    def _update_data(self, data_id: int, changes: Dict[str, Any]):
        """Update the data object with the given id.

        Changes are dictionary mapping from field names to field values.

        :attr changes: dictionary with changes to be saved.
        :attr data_id: the given data id.

        :raises: exception when data object cannot be saved.
        """
        # Update the redis cache.
        self._redis_cache.set_cache(data_id, changes)
        # Update the data object.
        Data.objects.filter(pk=data_id).update(**changes)

    def _update_worker(self, data_id: int, changes: Dict[str, Any]):
        """Update the worker object for the given data.

        :raises: exception when data object cannot be saved.
        """
        Worker.objects.filter(data__pk=data_id).update(**changes)

    def _save_database_terminate(self, data_id: int):
        """Save error to the database."""
        data = self.data(data_id)
        self._save_error(data, "Processing was cancelled.")

    async def terminate(self, peer_identity: PeerIdentity):
        """Send the terminate command to the worker for the given data object.

        Peer should terminate by itself and send finish message back to us.
        """
        try:
            await database_sync_to_async(
                self._save_database_terminate, thread_sensitive=False
            )(int(peer_identity))
            logger.debug("Sending terminate command to the peer '%s'.", peer_identity)
            await self._listener.communicator.send_command(
                Message.command("terminate", "Terminate worker"),
                peer_identity=peer_identity,
                timeout=5,
            )
            logger.debug("Terminate command to the peer '%s' sent.", peer_identity)
        except Exception:
            logger.exception("Error terminating worker.")

    async def peer_not_responding(self, data_id: int):
        """Peer is not responding, abort the processing.

        TODO: what to do with unresponsive peer which wakes up later and
        starts sending commands to the listener? Currently the situation
        is handled by sending "terminate" command to it and ignore its
        commands.
        """

        def update_database():
            """Update the database status."""
            with transaction.atomic():
                error_message = "Processing task is not responding."
                data = self.data(data_id)
                self._log_error(data, error_message)
                self._save_data(data, ["process_error", "status"])
                data.worker.status = Worker.STATUS_NONRESPONDING
                data.worker.save()

        logger.debug(__("Peer with id={} is not responding.", data_id))
        await database_sync_to_async(update_database, thread_sensitive=False)()
        await self.notify_dispatcher_abort_async(data_id)

    def _can_process_object(
        self, worker_status: str, data_status: str, command_name: str
    ) -> bool:
        """Check if the given data and worker status are ok to process.

        When data is in Data.STATUS_ERROR some commands are still allowed:
        - referenced_files: to store a list of filest to a data object.
        - finish: to set a return code and finalize the data object.
        -
        """
        acceptable_worker_statuses = (
            Worker.STATUS_PROCESSING,
            Worker.STATUS_PREPARING,
            Worker.STATUS_FINISHED_PREPARING,
        )
        unacceptable_data_statuses = (
            Data.STATUS_DONE,
            Data.STATUS_DIRTY,
            Data.STATUS_ERROR,
        )

        if worker_status not in acceptable_worker_statuses:
            return False

        allowed_commands_error = ["referenced_files", "finish"]
        if data_status == Data.STATUS_ERROR and command_name in allowed_commands_error:
            return True

        return data_status not in unacceptable_data_statuses

    def process_command(self, identity: PeerIdentity, message: Message) -> Response:
        """Process a single command from the peer.

        This command is run in the database_sync_to_async so it is safe to
        perform Django ORM operations inside.

        All exceptions will be handled and logged inside this method. The error

        """
        data_id = abs(int(identity))

        # Do not proccess messages from Workers that have already finish
        # processing data objects.
        worker_status, data_status, started = self.get_data_fields(
            data_id, ["worker__status", "status", "started"]
        )
        if not self._can_process_object(
            worker_status, data_status, message.command_name
        ):
            return message.respond_error(
                f"Unable to process the data object {data_id} with status {data_status}."
            )
        handler_name = f"handle_{message.command_name}"
        logger.debug(__("Message for handler {} received.", handler_name))
        handler = plugin_manager.get_handler(message.command_name)
        if not handler:
            error = f"Unknow command '{message.command_name}'."
            self._log_error(self.data(data_id), error, save_to_data_object=False)
            return message.respond_error(error)

        # Set the data started on the first command.
        if started is None:
            self._update_data(data_id, {"started": now()})

        try:
            with PrioritizedBatcher.global_instance():
                logger.debug(__("Invoking handler {}.", handler_name))
                response = handler(data_id, message, self)
                # Check if data status was changed by the handler.
                if self.get_data_fields(data_id, "status") == Data.STATUS_ERROR:
                    response.status = ResponseStatus.ERROR
                return response
        except ValidationError as err:
            error = (
                f"Validation error when running handler {handler_name} for "
                f"Data object with the id {data_id}: {err}."
            )
            self._log_exception(self.data(data_id), error)
            return message.respond_error("Validation error")
        except Exception as err:
            error = (
                f"Exception when running handler {handler_name} for data "
                f"object with the id {data_id}: {err}."
            )
            self._log_exception(self.data(data_id), error)
            return message.respond_error(f"Error in command handler '{handler_name}'.")

    def notify_dispatcher_abort(self, data_id: int):
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
        async_to_sync(self.notify_dispatcher_abort_async)(data_id)

    async def notify_dispatcher_abort_async(self, data_id: int):
        """Notify dispatcher that processing was aborted.

        :param obj: The Channels message object. Command object format:

            .. code-block:: none

                {
                    'command': 'abort',
                    'data_id': [id of the :class:`~resolwe.flow.models.Data` object
                               this command was triggered by],
                }
        """
        await database_sync_to_async(self._unlock_all_inputs, thread_sensitive=False)(
            data_id
        )
        await consumer.send_event(
            {
                WorkerProtocol.COMMAND: WorkerProtocol.ABORT,
                WorkerProtocol.DATA_ID: data_id,
                WorkerProtocol.FINISH_COMMUNICATE_EXTRA: {
                    "executor": getattr(settings, "FLOW_EXECUTOR", {}).get(
                        "NAME", "resolwe.flow.executors.local"
                    ),
                },
            }
        )
        logger.debug("notify_dispatcher_abort: consumer event sent")

    def notify_dispatcher_finish(self, data_id: int):
        """Notify dispatcher that the processing is finished.

        See ``notify_dispatcher_abort`` for message format.
        """
        async_to_sync(self.notify_dispatcher_finish_async)(data_id)

    async def notify_dispatcher_finish_async(self, data_id: int):
        """Notify dispatcher that the processing is finished.

        See ``notify_dispatcher_abort`` for message format.
        """
        await database_sync_to_async(self._unlock_all_inputs, thread_sensitive=False)(
            data_id
        )
        await consumer.send_event(
            {
                WorkerProtocol.COMMAND: WorkerProtocol.FINISH,
                WorkerProtocol.DATA_ID: data_id,
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
            ZMQCommunicator(zmq_socket, "listener <-> workers", logger),
            logger,
        )
        self.communicator.heartbeat_handler = self.heartbeat_handler

        self._redis = redis.from_url(settings.REDIS_CONNECTION_STRING)
        self._message_processor = Processor(self, self._redis)

    async def heartbeat_handler(self, peer_identity: PeerIdentity):
        """Handle the heartbeat messages."""
        # The redis key contains the timestamp when the worker was last seen.
        # When the key does not exist in the redis database create one with the
        # current timestamp.
        try:
            one_day = 24 * 3600
            data_id = abs(int(peer_identity))
            redis_key = f"resolwe-worker-{data_id}"
            self._redis.set(redis_key, int(time()))
            self._redis.expire(redis_key, one_day)
        except Exception:
            logger.exception("Exception in heartbeat handler.")

    async def heartbeat_task(self, check_interval=60):
        """Periodically check workers.

        This task must be cancelled to stop.

        :attr check_interval: how many seconds to wait between two consecutive
            checks.
        """
        while True:
            logger.debug("Checking workers.")
            await self.check_workers()
            await asyncio.sleep(check_interval)

    async def check_workers(self):
        """Check all workers and possibly mark them as stalled."""

        def get_data():
            """Get the data from the database.

            The list of workers in non-final state should not be long and can
            be safely stored in a list.
            """
            return list(
                Worker.objects.exclude(status__in=Worker.FINAL_STATUSES).values_list(
                    "data_id", "status"
                )
            )

        default_timeout = 600
        one_day = 24 * 3600
        one_week = 7 * one_day
        non_responsive_timeout = {
            Worker.STATUS_FINISHED_PREPARING: 7200,
            Worker.STATUS_PREPARING: one_week,
        }

        current_timestamp = int(time())
        for data_id, worker_status in await database_sync_to_async(
            get_data, thread_sensitive=False
        )():
            redis_key = f"resolwe-worker-{data_id}"
            last_seen = self._redis.get(redis_key)
            if last_seen is None:
                self._redis.set(redis_key, current_timestamp)
                self._redis.expire(redis_key, one_day)
            else:
                last_seen = int(last_seen)
            without_heartbeat = current_timestamp - (last_seen or current_timestamp)
            if without_heartbeat > non_responsive_timeout.get(
                worker_status, default_timeout
            ):
                try:
                    await self._message_processor.peer_not_responding(data_id)
                except Exception:
                    self.logger.exception(
                        "Exception updating unresponsive peer status."
                    )

    async def handle_liveness_probe(
        self, message: Message, peer_identity: PeerIdentity
    ) -> Response[bool]:
        """Respond to the liveness probe."""
        return message.respond_ok(True)

    async def default_command_handler(
        self, received_message: Message, peer_identity: PeerIdentity
    ) -> Response:
        """Process command."""
        # Executor uses separate identity of the form f"e_{data.id}".
        response = await database_sync_to_async(
            self._message_processor.process_command, thread_sensitive=False
        )(peer_identity, received_message)

        self.logger.debug(__("Response time: {}", received_message.time_elapsed()))
        return response

    async def post_finish(self, message: Message, peer_identity: PeerIdentity):
        """Notify dispatcher after finish command was received."""
        try:
            data_id = abs(int(peer_identity))
            with suppress(Data.DoesNotExist):
                await self._message_processor.notify_dispatcher_finish_async(data_id)
        except Exception:
            logger.exception("Error processing post finish command.")


def handle_exceptions(loop, context):
    """Log uncaught exceptions in asyncio."""
    msg = context.get("exception", context["message"])
    future = context.get("future")
    if future is not None:
        name = future.get_coro().__name__
        logger.error(f"Caught exception from {name}: {msg}")
    else:
        logger.error(f"Caught exception: {msg}")


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
        self._channels_listener: Optional[asyncio.Future] = None
        self._communicating_future: Optional[asyncio.Future] = None
        self._heartbeat_future: Optional[asyncio.Future] = None

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
    def listener_protocol(self) -> ListenerProtocol:
        """Return the listener protocol object.

        Used to lazy create object when property is accessed.
        """
        if self._listener_protocol is None:
            self._listener_protocol = ListenerProtocol(
                self.hosts, self.port, self.protocol, self.zmq_socket
            )
        return self._listener_protocol

    async def channels_listener(self):
        """Listen for terminate command and forward it to worker."""
        # Use different channel name when running tests.
        if is_testing():
            redis_prefix = getattr(settings, "FLOW_MANAGER", {}).get("REDIS_PREFIX", "")
            LISTENER_CONTROL_CHANNEL = "{}.listener".format(redis_prefix)  # noqa: F811

        logger.debug("Listener for terminate on channel %s", LISTENER_CONTROL_CHANNEL)
        channel_layer = get_channel_layer()
        while True:
            message = await channel_layer.receive(LISTENER_CONTROL_CHANNEL)
            message_type, peer_identity = message.get("type"), message.get("identity")
            if message_type == "terminate" and peer_identity is not None:
                logger.debug("Terminating peer '%s'.", peer_identity)
                await self.listener_protocol._message_processor.terminate(peer_identity)
            else:
                logger.error(__("Received unknown channels message '{}'.", message))

    async def __aenter__(self):
        """On entering a context, start the listener thread."""
        loop = asyncio.get_event_loop()
        loop.set_exception_handler(handle_exceptions)
        self._should_stop = None
        self._runner_future = asyncio.ensure_future(self.run())
        self._channels_listener = asyncio.ensure_future(self.channels_listener())
        self._heartbeat_future = asyncio.ensure_future(
            self.listener_protocol.heartbeat_task()
        )
        return self

    async def __aexit__(self, typ, value, trace):
        """On exiting a context, kill the listener and wait for it.

        .. note::

            Exceptions are all propagated.
        """
        logger.debug("Listener exiting context.")
        assert self._runner_future is not None
        assert self._channels_listener is not None
        assert self._heartbeat_future is not None
        self._channels_listener.cancel()
        self._heartbeat_future.cancel()
        self.terminate()
        logger.debug("Awaiting runner future.")
        await asyncio.gather(self._runner_future)
        self._listener_protocol = None
        logger.debug("Listener exited context.")

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
