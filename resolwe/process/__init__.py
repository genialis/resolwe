"""Resolwe Python process support."""
from .runtime import *

# XXX: ImportedFormat is available from resolwe-runtime-utils v2.0.0. Docker
# images with older version of resolwe-runtime-utils should skip the import.
try:
    from resolwe_runtime_utils import ImportedFormat
except ImportError:
    pass
