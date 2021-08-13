""".. Ignore pydocstyle D400.

.. autoclass:: resolwe.flow.executors.docker.run.FlowExecutor
    :members:

"""
# pylint: disable=logging-format-interpolation
import asyncio
import copy
import functools
import json
import logging
import os
import random
import string
import tempfile
import time
from contextlib import suppress
from pathlib import Path
from typing import Any, Dict, Iterable, Tuple, Type

import docker

from .. import constants
from ..connectors import connectors
from ..connectors.baseconnector import BaseStorageConnector
from ..global_settings import LOCATION_SUBPATH, PROCESS_META, SETTINGS
from ..local.run import FlowExecutor as LocalFlowExecutor
from ..protocol import ExecutorFiles
from .seccomp import SECCOMP_POLICY

# Limits of containers' access to memory. We set the limit to ensure
# processes are stable and do not get killed by OOM signal.
DOCKER_MEMORY_HARD_LIMIT_BUFFER = 100
DOCKER_MEMORY_SWAP_RATIO = 2
DOCKER_MEMORY_SWAPPINESS = 1

logger = logging.getLogger(__name__)


def _random_string(size: int = 5, chars=string.ascii_lowercase + string.digits):
    """Generate and return random string."""
    return "".join(random.choice(chars) for x in range(size))


def retry(
    max_retries: int = 3,
    retry_exceptions: Tuple[Type[Exception], ...] = (
        docker.errors.ImageNotFound,
        docker.errors.APIError,
    ),
    min_sleep: int = 1,
    max_sleep: int = 10,
):
    """Try to call decorated method max_retries times before giving up.

    The calls are retried when function raises exception in retry_exceptions.

    :param max_retries: maximal number of calls before giving up.
    :param retry_exceptions: retry call if one of these exceptions is raised.
    :param min_sleep: minimal sleep between calls (in seconds).
    :param max_sleep: maximal sleep between calls (in seconds).
    :returns: return value of the called method.
    :raises: the last exceptions raised by the method call if none of the
      retries were successfull.
    """

    def decorator_retry(func):
        @functools.wraps(func)
        def wrapper_retry(*args, **kwargs):
            last_error: Exception = Exception("Retry failed")
            sleep: int = 0
            for retry in range(max_retries):
                try:
                    time.sleep(sleep)
                    return func(*args, **kwargs)
                except retry_exceptions as err:
                    sleep = min(max_sleep, min_sleep * (2 ** retry))
                    last_error = err
            raise last_error

        return wrapper_retry

    return decorator_retry


class FlowExecutor(LocalFlowExecutor):
    """Docker executor."""

    name = "docker"

    def __init__(self, *args, **kwargs):
        """Initialize attributes."""
        super().__init__(*args, **kwargs)
        container_name_prefix = SETTINGS.get("FLOW_EXECUTOR", {}).get(
            "CONTAINER_NAME_PREFIX", "resolwe"
        )
        self.container_name = self._generate_container_name(container_name_prefix)
        self.tools_volumes = []
        self.command = SETTINGS.get("FLOW_DOCKER_COMMAND", "docker")
        self.tmpdir = tempfile.TemporaryDirectory()

    # Setup Docker volumes.
    def _new_volume(
        self, config: Dict[str, Any], mount_path: Path, read_only: bool = True
    ) -> Tuple[str, Dict[str, str]]:
        """Generate a new volume entry.

        :param config: must include 'path' and may include 'selinux_label'.
        :param mount_moint: mount point for the volume.
        """
        options = set()
        if "selinux_label" in config:
            options.add(config["selinux_label"])
        options.add("ro" if read_only else "rw")

        return (
            os.fspath(config["path"]),
            {"bind": os.fspath(mount_path), "mode": ",".join(options)},
        )

    def _get_upload_dir(self) -> str:
        """Get upload path.

        : returns: the path of the first mountable connector for storage
            'upload'.

        :raises RuntimeError: if no applicable connector is found.
        """
        for connector in connectors.for_storage("upload"):
            if connector.mountable:
                return f"/upload_{connector.name}"
        raise RuntimeError("No mountable upload connector is defined.")

    def _get_mountable_connectors(self) -> Iterable[Tuple[str, BaseStorageConnector]]:
        """Iterate through all the storages and find mountable connectors.

        :returns: list of tuples (storage_name, connector).
        """
        return (
            (storage_name, connector)
            for storage_name in SETTINGS["FLOW_STORAGE"]
            for connector in connectors.for_storage(storage_name)
            if connector.mountable
        )

    def _get_volumes(self, subpaths=False) -> Dict[str, Tuple[Dict, Path]]:
        """Get writeable volumes from settings.

        :attr subpaths: when True the location subpath in added to the volume
            path.

        :returns: mapping between volume name and tuple (config, mount_point).
        """
        results = dict()
        volume_mountpoint = {
            constants.PROCESSING_VOLUME_NAME: constants.PROCESSING_VOLUME,
            constants.INPUTS_VOLUME_NAME: constants.INPUTS_VOLUME,
            constants.SECRETS_VOLUME_NAME: constants.SECRETS_VOLUME,
            constants.SOCKETS_VOLUME_NAME: constants.SOCKETS_VOLUME,
        }

        for volume_name, volume in SETTINGS["FLOW_VOLUMES"].items():
            if "read_only" not in volume["config"]:
                if volume["type"] == "host_path":
                    config = copy.deepcopy(volume["config"])
                    if subpaths:
                        config["path"] = Path(config["path"]) / LOCATION_SUBPATH
                    results[volume_name] = (config, volume_mountpoint[volume_name])
                elif volume["type"] == "temporary_directory":
                    config = copy.deepcopy(volume["config"])
                    volume_path = Path(self.tmpdir.name) / volume_name
                    mode = config.get("mode", 0o700)
                    volume_path.mkdir(exist_ok=True, mode=mode)
                    config["path"] = volume_path
                    results[volume_name] = (config, volume_mountpoint[volume_name])
                else:
                    raise RuntimeError(
                        "Only 'host_type' and 'temporary_directory' volumes are "
                        " supported by Docker executor,"
                        f"requested '{volume['config']['type']}' for {volume_name}."
                    )

        assert (
            constants.PROCESSING_VOLUME_NAME in results
        ), "Processing volume must be defined."
        return results

    def _init_volumes(self) -> Dict:
        """Prepare volumes for init container."""
        mount_points = [
            (config, mount_point, False)
            for config, mount_point in self._get_volumes().values()
        ]
        mount_points += [
            (connector.config, Path("/") / f"{storage_name}_{connector.name}", False)
            for storage_name, connector in self._get_mountable_connectors()
        ]
        return dict([self._new_volume(*mount_point) for mount_point in mount_points])

    def _communicator_volumes(self) -> Dict[str, Dict]:
        """Prepare volumes for communicator container."""
        mount_points = [
            (connector.config, Path("/") / f"{storage_name}_{connector.name}", False)
            for storage_name, connector in self._get_mountable_connectors()
        ]
        volumes = self._get_volumes()
        mount_points += [
            (*volumes[constants.SECRETS_VOLUME_NAME], False),
            (*volumes[constants.SOCKETS_VOLUME_NAME], False),
        ]
        return dict([self._new_volume(*mount_point) for mount_point in mount_points])

    def _processing_volumes(self) -> Dict:
        """Prepare volumes for processing container."""
        # Expose processing and (possibly) input volume RW.
        mount_points = [
            (config, mount_point, False)
            for config, mount_point in self._get_volumes(True).values()
        ]
        # Expose mountable connectors ('upload' RW, othern 'RO').
        mount_points += [
            (
                connector.config,
                Path("/") / f"{storage_name}_{connector.name}",
                storage_name != "upload",
            )
            for storage_name, connector in self._get_mountable_connectors()
        ]

        mount_points += [
            (
                {"path": self.runtime_dir / "executors" / ExecutorFiles.SOCKET_UTILS},
                Path("/socket_utils.py"),
                False,
            ),
            (
                {"path": self.runtime_dir / constants.BOOTSTRAP_PYTHON_RUNTIME},
                Path("/") / constants.BOOTSTRAP_PYTHON_RUNTIME,
                False,
            ),
            (
                {"path": self.runtime_dir / "communicator.py"},
                Path("/communicator.py"),
                False,
            ),
            (
                {
                    "path": self.runtime_dir
                    / "executors"
                    / ExecutorFiles.STARTUP_PROCESSING_SCRIPT
                },
                Path("/start.py"),
                False,
            ),
            (
                {"path": self.runtime_dir / "executors" / ExecutorFiles.CONSTANTS},
                Path("/constants.py"),
                True,
            ),
        ]
        # Generate dummy passwd and create mappings for it. This is required because some tools
        # inside the container may try to lookup the given UID/GID and will crash if they don't
        # exist. So we create minimal user/group files.

        temporary_directory = Path(self.tmpdir.name)
        passwd_path = temporary_directory / "passwd"
        group_path = temporary_directory / "group"

        with passwd_path.open("wt") as passwd_file:
            passwd_file.write(
                "root:x:0:0:root:/root:/bin/bash\n"
                + f"user:x:{os.getuid()}:{os.getgid()}:user:{os.fspath(constants.PROCESSING_VOLUME)}:/bin/bash\n"
            )
        with group_path.open("wt") as group_file:
            group_file.write("root:x:0:\n" + f"user:x:{os.getgid()}:user\n")

        mount_points += [
            ({"path": passwd_path}, Path("/etc/passwd"), True),
            ({"path": group_path}, Path("/etc/group"), True),
        ]

        # Create mount points for tools.
        mount_points += [
            ({"path": Path(tool)}, Path("/usr/local/bin/resolwe") / str(index), True)
            for index, tool in enumerate(self.get_tools_paths())
        ]
        return dict([self._new_volume(*mount_point) for mount_point in mount_points])

    async def start(self):
        """Start process execution."""
        memory = (
            self.process["resource_limits"]["memory"] + DOCKER_MEMORY_HARD_LIMIT_BUFFER
        )
        memory_swap = int(memory * DOCKER_MEMORY_SWAP_RATIO)
        network = "bridge"
        if "network" in self.resources:
            # Configure Docker network mode for the container (if specified).
            # By default, current Docker versions use the 'bridge' mode which
            # creates a network stack on the default Docker bridge.
            network = SETTINGS.get("FLOW_EXECUTOR", {}).get("NETWORK", "")

        security_options = []
        if not SETTINGS.get("FLOW_DOCKER_DISABLE_SECCOMP", False):
            security_options.append(f"seccomp={json.dumps(SECCOMP_POLICY)}")

        processing_image = self.requirements.get(
            "image",
            SETTINGS.get(
                "FLOW_DOCKER_DEFAULT_PROCESSING_CONTAINER_IMAGE",
                "public.ecr.aws/s4q6j6e8/resolwe/base:ubuntu-20.04",
            ),
        )
        communicator_image = SETTINGS.get(
            "FLOW_DOCKER_COMMUNICATOR_IMAGE",
            "public.ecr.aws/s4q6j6e8/resolwe/com:latest",
        )
        ulimits = []
        if (
            self.process["scheduling_class"]
            == PROCESS_META["SCHEDULING_CLASS_INTERACTIVE"]
        ):
            # TODO: This is not very good as each child gets the same limit.
            # Note: Ulimit does not work as expected on multithreaded processes
            # Limit is increased by factor 1.2 for processes with 2-8 threads.
            # TODO: This should be changed for processes with over 8 threads.
            cpu_time_interactive = SETTINGS.get(
                "FLOW_PROCESS_RESOURCE_DEFAULTS", {}
            ).get("cpu_time_interactive", 30)
            cpu_limit = int(cpu_time_interactive * 1.2)
            ulimits.append(
                docker.types.Ulimit(name="cpu", soft=cpu_limit, hard=cpu_limit)
            )

        environment = {
            "LISTENER_SERVICE_HOST": self.listener_connection[0],
            "LISTENER_SERVICE_PORT": self.listener_connection[1],
            "LISTENER_PROTOCOL": self.listener_connection[2],
            "DATA_ID": self.data_id,
            "RUNNING_IN_CONTAINER": 1,
            "RUNNING_IN_DOCKER": 1,
            "GENIALIS_UID": os.getuid(),
            "GENIALIS_GID": os.getgid(),
            "FLOW_MANAGER_KEEP_DATA": SETTINGS.get("FLOW_MANAGER_KEEP_DATA", False),
            "DESCRIPTOR_CHUNK_SIZE": 100,
            "MOUNTED_CONNECTORS": ",".join(
                connector.name
                for connector in connectors.values()
                if connector.mountable
            ),
        }
        with suppress(RuntimeError):
            environment["UPLOAD_DIR"] = self._get_upload_dir()

        autoremove = SETTINGS.get("FLOW_DOCKER_AUTOREMOVE", False)

        # Add random string between container name and init. Since check for
        # existing stdout file has been moved inside init container we should
        # use different containers name in case one init contaner is still
        # running when another one is fired (or when containers are not purged
        # automatically): otherwise executor will fail to start the init
        # container due to name clash.
        init_container_name = f"{self.container_name}-{_random_string()}-init"

        init_arguments = {
            "auto_remove": autoremove,
            "volumes": self._init_volumes(),
            "command": ["/usr/local/bin/python3", "-m", "executors.init_container"],
            "image": communicator_image,
            "name": init_container_name,
            "detach": True,
            "cpu_quota": 1000000,
            "mem_limit": "4000m",
            "mem_reservation": "200m",
            "network_mode": network,
            "user": f"{os.getuid()}:{os.getgid()}",
            "environment": environment,
        }
        communication_arguments = {
            "auto_remove": autoremove,
            "volumes": self._communicator_volumes(),
            "command": ["/usr/local/bin/python", "/startup.py"],
            "image": communicator_image,
            "name": f"{self.container_name}-communicator",
            "detach": True,
            "cpu_quota": 100000,
            "mem_limit": "4000m",
            "mem_reservation": "200m",
            "network_mode": network,
            "cap_drop": ["all"],
            "security_opt": security_options,
            "user": f"{os.getuid()}:{os.getgid()}",
            "environment": environment,
        }
        processing_arguments = {
            "auto_remove": autoremove,
            "volumes": self._processing_volumes(),
            "command": ["python3", "/start.py"],
            "image": processing_image,
            "network_mode": f"container:{self.container_name}-communicator",
            "working_dir": os.fspath(constants.PROCESSING_VOLUME),
            "detach": True,
            "cpu_quota": self.process["resource_limits"]["cores"] * (10 ** 6),
            "mem_limit": f"{memory}m",
            "mem_reservation": f"{self.process['resource_limits']['memory']}m",
            "mem_swappiness": DOCKER_MEMORY_SWAPPINESS,
            "memswap_limit": f"{memory_swap}m",
            "name": self.container_name,
            "cap_drop": ["all"],
            "security_opt": security_options,
            "user": f"{os.getuid()}:{os.getgid()}",
            "ulimits": ulimits,
            "environment": environment,
        }

        @retry(max_retries=5)
        def transfer_image(client, image_name):
            """Transfer missing image, retry 5 times."""
            client.images.pull(image_name)

        client = docker.from_env()
        # Pull all the images.
        try:
            try:
                logger.debug("Pulling processing image %s.", processing_image)
                client.images.get(processing_image)
            except docker.errors.ImageNotFound:
                transfer_image(client, processing_image)
            try:
                logger.debug("Pulling communicator image %s.", communicator_image)
                client.images.get(communicator_image)
            except docker.errors.ImageNotFound:
                transfer_image(client, communicator_image)

        except docker.errors.APIError:
            logger.exception("Docker API error")
            raise RuntimeError("Docker API error")

        loop = asyncio.get_event_loop()
        start_time = time.time()
        try:
            init_container = client.containers.run(**init_arguments)
        except docker.errors.APIError as error:
            await self.communicator.finish(
                {"error": f"Error starting init container: {error}"}
            )
            raise

        init_container_status = await loop.run_in_executor(None, init_container.wait)

        # Return code is as follows:
        # - 0: no error occured, continue processing.
        # - 1: error running init container, abort processing and log error.
        # - 2: data exists in the processing volume, abort processing.
        init_rc = init_container_status["StatusCode"]
        if init_rc != 0:
            logger.error("Init container returned %s instead of 0.", init_rc)
            # Do not set error on data objects where previous data exists.
            if init_rc == 1:
                await self.communicator.finish(
                    {"error": f"Init container returned {init_rc} instead of 0."}
                )
            return

        try:
            communication_container = client.containers.run(**communication_arguments)
        except docker.errors.APIError as error:
            await self.communicator.finish(
                {"error": f"Error starting communication container: {error}"}
            )
            raise
        try:
            processing_container = client.containers.run(**processing_arguments)
        except docker.errors.APIError as e:
            await self.communicator.finish(
                {"error": f"Error starting processing container: {e}"}
            )
            with suppress(docker.errors.APIError):
                communication_container.stop(timeout=1)
            raise

        end_time = time.time()
        logger.info(
            "It took {:.2f}s for Docker containers to start".format(
                end_time - start_time
            )
        )
        with suppress(docker.errors.NotFound):
            await loop.run_in_executor(None, communication_container.wait)
        with suppress(docker.errors.NotFound):
            await loop.run_in_executor(None, processing_container.wait)
