"""Audit manager class."""

import threading
from collections import defaultdict
from enum import IntEnum
from typing import Dict, Iterator, List, NamedTuple, Optional, Set, Tuple, Type, Union

from django.db import models
from django.http import HttpRequest, HttpResponse

GLOBAL_AUDIT_MANAGER = threading.local()

Field = str
ContentType = str
ObjectId = Union[int, str]
ObjectIds = Set[ObjectId]
Fields = Set[str]


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
        #        self._access =
        self._access: Dict[
            ContentType, Dict[ObjectId, Dict[AccessType, Fields]]
        ] = defaultdict(lambda: defaultdict(lambda: defaultdict(set)))

    def clear(self):
        """Clear all accesses."""
        self._access.clear()

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

    Base object registers access to the per-request instance of audit manager
    created and stored by the custom middleware.
    """

    def __init__(self):
        """Initialize AccessLogger and custom messages list."""
        self._fields: Dict[Type[models.Model], Fields] = dict()
        self._custom_messages: List[Tuple[str, tuple, dict]] = []
        self._access_loger = AccessLogger()

    def get_content_type(self, model_class: Type[models.Model]) -> str:
        """Get the content-type from the model."""
        return model_class._meta.label

    def get_model_fields(self, model_class: Type[models.Model]) -> Fields:
        """Get all the model fields.

        Cache them to avoid excessive iteration.
        """
        if model_class not in self._fields:
            self._fields[model_class] = set(
                field.name for field in model_class._meta.fields
            )
        return self._fields[model_class]

    @staticmethod
    def log_message(message: str, *args, **kwargs):
        """Log custom message."""
        AuditManager.global_instance()._custom_messages.append((message, args, kwargs))

    @staticmethod
    def register_access(
        model_class: Type[models.Model],
        object_id: ObjectId,
        access_type: AccessType,
        fields: Optional[Fields] = None,
    ):
        """Register access to the object.

        When fields are not given the access to all fields in the model is
        logged.
        """
        manager = AuditManager.global_instance()
        fields = fields or manager.get_model_fields(model_class)
        content_type = manager.get_content_type(model_class)
        manager._access_loger.register_access(
            content_type, object_id, access_type, fields
        )

    def emit(self, request: HttpRequest, response: HttpResponse):
        """Log gathered data to the audit logger."""

        from resolwe.auditlog.logger import logger

        for access_item in self._access_loger:
            logger.log_object_access(
                request,
                response,
                access_item.content_type,
                access_item.object_id,
                access_item.access_type,
                access_item.fields,
            )

        for message, args, kwargs in self._custom_messages:
            kwargs["request"] = request
            kwargs["response"] = response
            logger.info(message, *args, **kwargs)

    def reset(self):
        """Clear all stored data."""
        self._access_loger.clear()
        self._custom_messages.clear()

    @staticmethod
    def global_instance() -> "AuditManager":
        """Return the thread-scope global instance of the audit manager."""
        if not hasattr(GLOBAL_AUDIT_MANAGER, "instance"):
            GLOBAL_AUDIT_MANAGER.instance = AuditManager()
        return GLOBAL_AUDIT_MANAGER.instance
