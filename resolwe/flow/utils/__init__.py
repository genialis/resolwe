""".. Ignore pydocstyle D400.

==============
Flow Utilities
==============

.. automodule:: resolwe.flow.utils.exceptions
   :members:

.. automodule:: resolwe.flow.utils.stats
   :members:

"""
import functools
import hashlib
import json
from pathlib import Path
from typing import Dict

from versionfield.version import Version

from django.apps import apps
from django.conf import settings
from django.db import models

from .decorators import retry
from .iterators import iterate_dict, iterate_fields, iterate_schema

__all__ = (
    "dict_dot",
    "get_apps_tools",
    "get_data_checksum",
    "iterate_dict",
    "iterate_fields",
    "iterate_schema",
    "retry",
)


def get_data_checksum(proc_input: Dict, proc_slug: str, proc_version: Version) -> str:
    """Compute checksum of processor inputs, name and version."""
    checksum = hashlib.sha256()
    checksum.update(json.dumps(proc_input, sort_keys=True).encode("utf-8"))
    checksum.update(proc_slug.encode("utf-8"))
    checksum.update(str(proc_version).encode("utf-8"))
    return checksum.hexdigest()


def dict_dot(d: Dict, k, val=None, default=None):
    """Get or set value using a dot-notation key in a multilevel dict."""
    if val is None and k == "":
        return d

    def set_default(dict_or_model, key, default_value):
        """Set default field value."""
        if isinstance(dict_or_model, models.Model):
            if not hasattr(dict_or_model, key):
                setattr(dict_or_model, key, default_value)

            return getattr(dict_or_model, key)
        else:
            return dict_or_model.setdefault(key, default_value)

    def get_item(dict_or_model, key):
        """Get field value."""
        if isinstance(dict_or_model, models.Model):
            return getattr(dict_or_model, key)
        else:
            return dict_or_model[key]

    def set_item(dict_or_model, key, value):
        """Set field value."""
        if isinstance(dict_or_model, models.Model):
            setattr(dict_or_model, key, value)
        else:
            dict_or_model[key] = value

    if val is None and callable(default):
        # Get value, default for missing
        return functools.reduce(
            lambda a, b: set_default(a, b, default()), k.split("."), d
        )

    elif val is None:
        # Get value, error on missing
        return functools.reduce(get_item, k.split("."), d)

    else:
        # Set value
        try:
            k, k_last = k.rsplit(".", 1)
            set_item(dict_dot(d, k, default=dict), k_last, val)
        except ValueError:
            set_item(d, k, val)
        return val


def get_apps_tools() -> Dict[str, Path]:
    """Get applications' tools and their paths.

    Return a dict with application names as keys and paths to tools'
    directories as values. Applications without tools are omitted.

    :raises KeyError: when RESOLWE_CUSTOM_TOOLS_PATHS settings is no a list.
    """
    tools_paths = {}

    for app_config in apps.get_app_configs():
        proc_path = Path(app_config.path) / "tools"
        if proc_path.is_dir():
            tools_paths[app_config.name] = proc_path

    custom_tools_paths = getattr(settings, "RESOLWE_CUSTOM_TOOLS_PATHS", [])
    if not isinstance(custom_tools_paths, list):
        raise KeyError("`RESOLWE_CUSTOM_TOOLS_PATHS` setting must be a list.")

    for seq, custom_path in enumerate(custom_tools_paths):
        custom_key = "_custom_{}".format(seq)
        tools_paths[custom_key] = Path(custom_path)

    return tools_paths
