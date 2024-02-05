""".. Ignore pydocstyle D400.

===================================
Resolwe Test Helpers and Decorators
===================================

"""

import functools
import json
import os
import shlex
import shutil
import subprocess
import unittest
from contextlib import suppress
from sys import platform

import wrapt

from django.conf import settings
from django.core.exceptions import ValidationError
from django.test import override_settings, tag

from resolwe.flow.utils import dict_dot, iterate_fields
from resolwe.storage.connectors import connectors
from resolwe.storage.models import FileStorage, StorageLocation

__all__ = (
    "check_installed",
    "check_docker",
    "create_data_location",
    "with_custom_executor",
    "with_docker_executor",
    "with_null_executor",
    "with_resolwe_host",
    "is_testing",
)

TAG_PROCESS = "resolwe.process"


def save_storage(data):
    """Parse output field and create Storage objects if needed."""
    for field_schema, fields, path in iterate_fields(
        data.output, data.process.output_schema, ""
    ):
        name = field_schema["name"]
        value = fields[name]
        if field_schema.get("type", "").startswith("basic:json:"):
            if value and not data.pk:
                raise ValidationError(
                    "Data object must be `created` before creating `basic:json:` fields"
                )

            if isinstance(value, int):
                # already in Storage
                continue

            if isinstance(value, str):
                file_path = data.location.get_path(filename=value)
                if os.path.isfile(file_path):
                    try:
                        with open(file_path) as file_handler:
                            value = json.load(file_handler)
                    except json.JSONDecodeError:
                        with open(file_path) as file_handler:
                            content = file_handler.read()
                            content = content.rstrip()
                            raise ValidationError(
                                "Value of '{}' must be a valid JSON, current: {}".format(
                                    name, content
                                )
                            )

            existing_storage_pk = None
            with suppress(KeyError):
                existing_storage_pk = dict_dot(data._original_output, path)

            if isinstance(existing_storage_pk, int):
                data.storages.filter(pk=existing_storage_pk).update(json=value)
                fields[name] = existing_storage_pk
            else:
                storage = data.storages.create(
                    name="Storage for data id {}".format(data.pk),
                    contributor=data.contributor,
                    json=value,
                )
                fields[name] = storage.pk


def create_data_location(subpath=None):
    """Create equivalent of old DataLocation object.

    When argument is None, store the ID of the file storage object in the
    subpath.
    """
    file_storage = FileStorage.objects.create()
    if subpath is None:
        subpath = file_storage.pk

    data_connector = [
        connector.name
        for connector in connectors.for_storage("data")
        if connector.mountable
    ][0]

    StorageLocation.objects.create(
        url=subpath, file_storage=file_storage, connector_name=data_connector
    )
    return file_storage


def check_installed(command):
    """Check if the given command is installed.

    :param str command: name of the command

    :return: (indicator of the availability of the command, message saying
              command is not available)
    :rtype: tuple(bool, str)

    """
    if shutil.which(command):
        return True, ""
    else:
        return False, "Command '{}' is not found.".format(command)


def check_docker():
    """Check if Docker is installed and working.

    :return: (indicator of the availability of Docker, reason for
              unavailability)
    :rtype: tuple(bool, str)

    """
    command = getattr(settings, "FLOW_DOCKER_COMMAND", "docker")
    info_command = "{} info".format(command)
    available, reason = True, ""
    # TODO: Use subprocess.DEVNULL after dropping support for Python 2
    with open(os.devnull, "wb") as DEVNULL:
        try:
            subprocess.check_call(
                shlex.split(info_command), stdout=DEVNULL, stderr=subprocess.STDOUT
            )
        except OSError:
            available, reason = False, "Docker command '{}' not found".format(command)
        except subprocess.CalledProcessError:
            available, reason = (
                False,
                "Docker command '{}' returned non-zero "
                "exit status".format(info_command),
            )
    return available, reason


def with_custom_executor(wrapped=None, **custom_executor_settings):
    """Decorate unit test to run processes with a custom executor.

    :param dict custom_executor_settings: custom ``FLOW_EXECUTOR``
        settings with which you wish to override the current settings

    """
    if wrapped is None:
        return functools.partial(with_custom_executor, **custom_executor_settings)

    @wrapt.decorator
    def wrapper(wrapped_method, instance, args, kwargs):
        from resolwe.flow.managers import manager  # To prevent circular imports.

        executor_settings = settings.FLOW_EXECUTOR.copy()
        executor_settings.update(custom_executor_settings)

        try:
            with override_settings(FLOW_EXECUTOR=executor_settings):
                manager.discover_engines()

                # Re-run the post_register_hook
                manager.get_executor().post_register_hook(verbosity=0)

                # Run the actual unit test method.
                return wrapped_method(*args, **kwargs)
        finally:
            # Re-run engine discovery as the settings have changed.
            manager.discover_engines()

    return wrapper(wrapped)


def with_docker_executor(wrapped=None):
    """Decorate unit test to run processes with the Docker executor."""
    if wrapped is None:
        return functools.partial(with_docker_executor)

    @wrapt.decorator
    def wrapper(wrapped_method, instance, args, kwargs):
        return unittest.skipUnless(*check_docker())(
            with_custom_executor(
                NAME="resolwe.flow.executors.docker",
            )(wrapped_method)
        )(*args, **kwargs)

    return wrapper(wrapped)


@wrapt.decorator
def with_null_executor(wrapped_method, instance, args, kwargs):
    """Decorate unit test to run processes with the Null executor."""
    return with_custom_executor(NAME="resolwe.flow.executors.null")(wrapped_method)(
        *args, **kwargs
    )


@wrapt.decorator
def with_resolwe_host(wrapped_method, instance, args, kwargs):
    """Decorate unit test to give it access to a live Resolwe host.

    Set ``RESOLWE_HOST_URL`` setting to the address where the testing
    live Resolwe host listens to.

    .. note::

        This decorator must be used with a (sub)class of
        :class:`~django.test.LiveServerTestCase` which starts a live
        Django server in the background.

    """
    if not hasattr(instance, "server_thread"):
        raise AttributeError(
            "with_resolwe_host decorator must be used with a "
            "(sub)class of LiveServerTestCase that has the "
            "'server_thread' attribute"
        )
    host = instance.server_thread.host
    if platform not in ["linux", "linux2"]:
        host = "host.docker.internal"
    resolwe_host_url = "http://{host}:{port}".format(
        host=host, port=instance.server_thread.port
    )
    with override_settings(RESOLWE_HOST_URL=resolwe_host_url):
        return with_custom_executor(NETWORK="host")(wrapped_method)(*args, **kwargs)


def generate_process_tag(slug):
    """Generate test tag for a given process."""
    return "{}.{}".format(TAG_PROCESS, slug)


def tag_process(*slugs):
    """Decorate unit test to tag it for a specific process."""
    slugs = [generate_process_tag(slug) for slug in slugs]
    slugs.append(TAG_PROCESS)  # Also tag with a general process tag.
    return tag(*slugs)


def has_process_tag(test, slug):
    """Check if a given test method/class is tagged for a specific process."""
    tags = getattr(test, "tags", set())
    return generate_process_tag(slug) in tags


def get_processes_from_tags(test):
    """Extract process slugs from tags."""
    tags = getattr(test, "tags", set())
    slugs = set()

    for tag_name in tags:
        if not tag_name.startswith("{}.".format(TAG_PROCESS)):
            continue

        slugs.add(tag_name[len(TAG_PROCESS) + 1 :])

    return slugs


def is_testing():
    """Return current testing status.

    This assumes that the Resolwe test runner is being used.
    """
    from resolwe.test_helpers.test_runner import is_testing

    return is_testing()
