"""Resolwe Python process support."""

import logging
import sys

from .communicator import communicator
from .descriptor import Persistence, ProcessDescriptor, SchedulingClass, ValidationError
from .fields import (
    BooleanField,
    DataField,
    DateField,
    DateTimeField,
    DirField,
    FileField,
    FileHtmlField,
    FloatField,
    GroupField,
    IntegerField,
    JsonField,
    ListField,
    SecretField,
    StringField,
    TextField,
    UrlField,
)
from .models import Collection, Data, Entity
from .runtime import Process

try:
    from plumbum import local as Cmd

    # Log plumbum commands to standard output.

    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(name)s[%(process)s]: %(message)s"
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    plumbum_logger = logging.getLogger("plumbum.local")
    plumbum_logger.setLevel(logging.DEBUG)
    plumbum_logger.addHandler(handler)


except ImportError:
    # Generate a dummy class that defers the ImportError to a point where
    # the package actually tries to be used.
    class MissingPlumbumCmd:
        """Missing plumbum command dummy class."""

        def __getitem__(self, cmd):
            """Raise ImportError exception."""
            raise ImportError("Missing 'plumbum' Python package.")

    Cmd = MissingPlumbumCmd()


__all__ = (
    "BooleanField",
    "Cmd",
    "DataField",
    "DateField",
    "DateTimeField",
    "DirField",
    "FileField",
    "FileHtmlField",
    "FloatField",
    "GroupField",
    "IntegerField",
    "JsonField",
    "ListField",
    "Persistence",
    "Process",
    "ProcessDescriptor",
    "SchedulingClass",
    "SecretField",
    "StringField",
    "ValidationError",
    "TextField",
    "UrlField",
    "Data",
    "Entity",
    "Collection",
    "communicator",
)


# XXX: ImportedFormat is available from resolwe-runtime-utils v2.0.0. Docker
# images with older version of resolwe-runtime-utils should skip the import.
try:
    from resolwe_runtime_utils import ImportedFormat

    __all__ += (ImportedFormat,)
except ImportError:
    pass
