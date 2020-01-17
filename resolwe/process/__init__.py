"""Resolwe Python process support."""
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
from .runtime import Cmd, Inputs, Outputs, Process

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
    "Inputs",
    "IntegerField",
    "JsonField",
    "ListField",
    "Outputs",
    "Persistence",
    "Process",
    "ProcessDescriptor",
    "SchedulingClass",
    "SecretField",
    "StringField",
    "ValidationError",
    "TextField",
    "UrlField",
)


# XXX: ImportedFormat is available from resolwe-runtime-utils v2.0.0. Docker
# images with older version of resolwe-runtime-utils should skip the import.
try:
    from resolwe_runtime_utils import ImportedFormat

    __all__ += (ImportedFormat,)
except ImportError:
    pass
