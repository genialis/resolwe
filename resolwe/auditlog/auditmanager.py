"""Audit manager class."""

import logging
from collections import defaultdict
from contextlib import contextmanager
from contextvars import ContextVar
from enum import IntEnum
from typing import (
    Dict,
    Iterator,
    List,
    NamedTuple,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
)

from django.db import models
from django.http import HttpRequest, HttpResponse

logger = logging.getLogger(__name__)

Field = str
ContentType = str
ObjectId = Union[int, str]
ObjectIds = Set[ObjectId]
Fields = Set[str]

# The audit manager collecting entries for the current unit of work (an HTTP
# request or a worker task), or None when no collection is active. A context
# variable (and not a thread local) is used so the manager activated in a
# coroutine is also active inside the database threads spawned by
# (database_)sync_to_async and concurrent asyncio tasks do not share state.
ACTIVE_AUDIT_MANAGER: ContextVar[Optional["AuditManager"]] = ContextVar(
    "active_audit_manager", default=None
)

# Per-model cache of field names, used when access to all fields is registered.
_MODEL_FIELDS: Dict[Type[models.Model], Fields] = dict()


class AccessType(IntEnum):
    """Type of the access.

    The priority is determined by numeric value of attributes: higher value
    means higher priority.
    """

    NONE = 0
    READ = 1
    UPDATE = 2
    CREATE = 3
    DELETE = 4

    def __str__(self) -> str:
        """Return the string representation of the access type object."""
        return self.name


class IterableAccessItem(NamedTuple):
    """An single item obtained by iterating over AccessLogger object."""

    content_type: ContentType
    object_id: ObjectId
    access_type: AccessType
    fields: Fields


class AccessLogger:
    """Track accesses to models.

    The basic unit is field on a model and access is tracked for every field.
    """

    def __init__(self):
        """Initialize."""
        self._access: Dict[ContentType, Dict[ObjectId, Dict[AccessType, Fields]]] = (
            defaultdict(lambda: defaultdict(lambda: defaultdict(set)))
        )

    def register_access(
        self,
        content_type: ContentType,
        object_id: ObjectId,
        access_type: AccessType,
        fields: Fields,
    ):
        """Register access to the object fields."""
        self._access[content_type][object_id][access_type].update(fields)

    def __iter__(self) -> Iterator[IterableAccessItem]:
        """Return iterator over stored access entries."""
        for content_type, model_access in self._access.items():
            for object_id, field_access_types in model_access.items():
                for access_type, fields in field_access_types.items():
                    yield IterableAccessItem(
                        content_type, object_id, access_type, fields
                    )


class AuditManager:
    """Audit manager class.

    The audit manager collects accesses to the model instances for one unit
    of work (an HTTP request or a worker task) and emits them to the audit
    logger when the unit of work completes. A unit of work is delimited by
    the :func:`audit_context` context manager; the request middleware uses it
    under the hood.

    Accesses registered outside an active audit context are discarded:
    without a well defined emit point they would only accumulate in the
    memory of long-running processes such as the workers and the listener.
    """

    def __init__(self):
        """Initialize AccessLogger and custom messages list."""
        self._custom_messages: List[Tuple[str, tuple, dict]] = []
        self._access_loger = AccessLogger()

    @staticmethod
    def current() -> Optional["AuditManager"]:
        """Return the audit manager active in the current context, if any."""
        return ACTIVE_AUDIT_MANAGER.get()

    def get_content_type(self, model_class: Type[models.Model]) -> str:
        """Get the content-type from the model."""
        return model_class._meta.label

    def get_model_fields(self, model_class: Type[models.Model]) -> Fields:
        """Get all the model fields.

        Cache them to avoid excessive iteration.
        """
        if model_class not in _MODEL_FIELDS:
            _MODEL_FIELDS[model_class] = set(
                field.name for field in model_class._meta.fields
            )
        return _MODEL_FIELDS[model_class]

    @staticmethod
    def log_message(message: str, *args, **kwargs):
        """Log custom message to the active audit manager.

        The message is discarded when no audit context is active.
        """
        manager = AuditManager.current()
        if manager is None:
            return
        manager._custom_messages.append((message, args, kwargs))

    @staticmethod
    def register_access(
        model_class: Type[models.Model],
        object_id: ObjectId,
        access_type: AccessType,
        fields: Optional[Fields] = None,
    ):
        """Register access to the object with the active audit manager.

        When fields are not given the access to all fields in the model is
        logged. The access is discarded when no audit context is active.
        """
        manager = AuditManager.current()
        if manager is None:
            return
        fields = fields or manager.get_model_fields(model_class)
        content_type = manager.get_content_type(model_class)
        manager._access_loger.register_access(
            content_type, object_id, access_type, fields
        )

    def emit(
        self,
        request: Optional[HttpRequest] = None,
        response: Optional[HttpResponse] = None,
        extra: Optional[dict] = None,
    ):
        """Log gathered data to the audit logger.

        The context of the emitted records (user id, request id...) is
        extracted from the request/response pair when given (the middleware)
        or taken from the extra dictionary (the workers).
        """
        from resolwe.auditlog.logger import logger as audit_logger

        for access_item in self._access_loger:
            audit_logger.log_object_access(
                request,
                response,
                access_item.content_type,
                access_item.object_id,
                access_item.access_type,
                access_item.fields,
                extra=extra,
            )

        for message, args, kwargs in self._custom_messages:
            kwargs["request"] = request
            kwargs["response"] = response
            if extra:
                kwargs.setdefault("extra", dict()).update(extra)
            audit_logger.info(message, *args, **kwargs)


@contextmanager
def audit_context(
    user_id: Optional[ObjectId] = None,
    context_id: Optional[str] = None,
    auto_emit: bool = True,
):
    """Collect and emit audit log entries for one unit of work.

    Model accesses and custom audit messages are only collected while an
    audit context is active. On exit the collected entries are emitted to
    the audit logger (also when the unit of work raised an exception) and
    the collected data is released.

    The request middleware, which must emit with the full request/response
    context available only after the response is computed, sets ``auto_emit``
    to ``False`` and emits explicitly.

    :param user_id: the id of the user the unit of work is performed for.
    :param context_id: the identifier correlating all records of this unit
        of work, emitted as the ``request_id`` of the records.
    :param auto_emit: when set to ``True`` the collected entries are emitted
        on exit.
    """
    manager = AuditManager()
    token = ACTIVE_AUDIT_MANAGER.set(manager)
    try:
        yield manager
    finally:
        ACTIVE_AUDIT_MANAGER.reset(token)
        if auto_emit:
            extra = {}
            if user_id is not None:
                extra["user_id"] = user_id
            if context_id is not None:
                extra["request_id"] = context_id
            try:
                manager.emit(extra=extra)
            except Exception:
                logger.exception("Error emitting the audit log.")
