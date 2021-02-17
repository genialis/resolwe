""".. Ignore pydocstyle D400.

.. autoclass:: resolwe.flow.executors.docker.run.FlowExecutor
    :members:

"""
# pylint: disable=logging-format-interpolation
import asyncio
import functools
import json
import logging
import os
import platform
import time
from contextlib import suppress
from pathlib import Path
from typing import Dict, Optional, Tuple, Type, Union

import docker

from .. import constants
from ..global_settings import PROCESS_META, SETTINGS, STORAGE_LOCATION
from ..local.run import FlowExecutor as LocalFlowExecutor
from ..protocol import ExecutorFiles
from .seccomp import SECCOMP_POLICY

# Limits of containers' access to memory. We set the limit to ensure
# processes are stable and do not get killed by OOM signal.
DOCKER_MEMORY_HARD_LIMIT_BUFFER = 100
DOCKER_MEMORY_SWAP_RATIO = 2
DOCKER_MEMORY_SWAPPINESS = 1

logger = logging.getLogger(__name__)


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
        self.runtime_dir = Path(SETTINGS["FLOW_EXECUTOR"].get("RUNTIME_DIR", ""))

    # Setup Docker volumes.
    def _new_volume(
        self,
        kind: str,
        base_dir_name: Optional[str],
        mount_point: Union[str, Path],
        path: Union[str, Path] = "",
        read_only: bool = True,
    ) -> Tuple[str, Dict[str, str]]:
        """Generate a new volume entry.

        :param kind: Kind of volume, which is used for getting extra options from
            settings (the ``FLOW_DOCKER_VOLUME_EXTRA_OPTIONS`` setting)
        :param base_dir_name: Name of base directory setting for volume source path
        :param volume: Destination volume mount point
        :param path: Optional additional path atoms appended to source path
        :param read_only: True to make the volume read-only
        """
        options = set(
            SETTINGS.get("FLOW_DOCKER_VOLUME_EXTRA_OPTIONS", {})
            .get(kind, "")
            .split(",")
        ).difference(["", "ro", "rw"])
        options.add("ro" if read_only else "rw")

        base_path = Path(SETTINGS["FLOW_EXECUTOR"].get(base_dir_name, ""))
        return (
            os.fspath(base_path / path),
            {
                "bind": os.fspath(mount_point),
                "mode": ",".join(options),
            },
        )

    def _init_volumes(self) -> Dict:
        """Prepare volumes for init container."""
        storage_url = Path(STORAGE_LOCATION["url"])

        # Create local data dir.
        base_path = Path(SETTINGS["FLOW_EXECUTOR"].get("DATA_DIR", ""))
        local_data = base_path / f"{storage_url}_work"
        local_data.mkdir(exist_ok=True)

        return dict(
            [
                self._new_volume(
                    "data_all", "DATA_DIR", constants.DATA_ALL_VOLUME, read_only=False
                ),
                self._new_volume(
                    "secrets",
                    "RUNTIME_DIR",
                    constants.SECRETS_VOLUME,
                    storage_url / ExecutorFiles.SECRETS_DIR,
                ),
                self._new_volume(
                    "settings",
                    "RUNTIME_DIR",
                    "/settings",
                    storage_url / ExecutorFiles.SETTINGS_SUBDIR,
                ),
            ]
        )

    def _communicator_volumes(self) -> Dict:
        """Prepare volumes for communicator container."""
        storage_url = Path(STORAGE_LOCATION["url"])
        communicator_volumes = [
            self._new_volume(
                "data",
                "DATA_DIR",
                constants.DATA_VOLUME,
                storage_url,
                read_only=False,
            ),
            self._new_volume(
                "data_all", "DATA_DIR", constants.DATA_ALL_VOLUME, read_only=False
            ),
            self._new_volume(
                "secrets",
                "RUNTIME_DIR",
                constants.SECRETS_VOLUME,
                storage_url / ExecutorFiles.SECRETS_DIR,
            ),
            self._new_volume(
                "settings",
                "RUNTIME_DIR",
                "/settings",
                storage_url / ExecutorFiles.SETTINGS_SUBDIR,
            ),
            self._new_volume(
                "sockets",
                "RUNTIME_DIR",
                constants.SOCKETS_VOLUME,
                storage_url / ExecutorFiles.SOCKETS_SUBDIR,
                read_only=False,
            ),
        ]
        return dict(communicator_volumes)

    def _processing_volumes(self) -> Dict:
        """Prepare volumes for processing container."""
        storage_url = Path(STORAGE_LOCATION["url"])

        # Create local data dir.
        base_path = Path(SETTINGS["FLOW_EXECUTOR"].get("DATA_DIR", ""))
        local_data = base_path / f"{storage_url}_work"
        local_data.mkdir(exist_ok=True)

        processing_volumes = [
            self._new_volume(
                "data",
                "DATA_DIR",
                constants.DATA_VOLUME,
                storage_url,
                read_only=False,
            ),
            self._new_volume(
                "data_local",
                "DATA_DIR",
                constants.DATA_LOCAL_VOLUME,
                f"{storage_url}_work",
                read_only=False,
            ),
            self._new_volume("data_all", "DATA_DIR", constants.DATA_ALL_VOLUME),
            self._new_volume(
                "upload", "UPLOAD_DIR", constants.UPLOAD_VOLUME, read_only=False
            ),
            self._new_volume(
                "secrets",
                "RUNTIME_DIR",
                constants.SECRETS_VOLUME,
                storage_url / ExecutorFiles.SECRETS_DIR,
            ),
            self._new_volume(
                "sockets",
                "RUNTIME_DIR",
                constants.SOCKETS_VOLUME,
                storage_url / ExecutorFiles.SOCKETS_SUBDIR,
                read_only=False,
            ),
            self._new_volume(
                "socket_utils",
                "RUNTIME_DIR",
                "/socket_utils.py",
                storage_url / "executors" / ExecutorFiles.SOCKET_UTILS,
            ),
            self._new_volume(
                "socket_utils",
                "RUNTIME_DIR",
                "/start.py",
                storage_url / "executors" / ExecutorFiles.STARTUP_PROCESSING_SCRIPT,
            ),
            self._new_volume(
                "constants",
                "RUNTIME_DIR",
                "/constants.py",
                storage_url / "executors" / ExecutorFiles.CONSTANTS,
            ),
        ]

        # Generate dummy passwd and create mappings for it. This is required because some tools
        # inside the container may try to lookup the given UID/GID and will crash if they don't
        # exist. So we create minimal user/group files.

        passwd_path = self.runtime_dir / storage_url / "passwd"
        group_path = self.runtime_dir / storage_url / "group"

        with passwd_path.open("wt") as passwd_file:
            passwd_file.write(
                "root:x:0:0:root:/root:/bin/bash\n"
                + f"user:x:{os.getuid()}:{os.getgid()}:user:{os.fspath(constants.DATA_LOCAL_VOLUME)}:/bin/bash\n"
            )
        with group_path.open("wt") as group_file:
            group_file.write("root:x:0:\n" + f"user:x:{os.getgid()}:user\n")

        processing_volumes += [
            self._new_volume("users", None, "/etc/passwd", passwd_path),
            self._new_volume("users", None, "/etc/group", group_path),
        ]

        # Create volumes for tools.
        processing_volumes += [
            self._new_volume(
                "tools",
                None,
                Path("/usr/local/bin/resolwe") / str(index),
                Path(tool),
            )
            for index, tool in enumerate(self.get_tools_paths())
        ]

        # Create volumes for runtime (all read-only).
        processing_volumes += [
            self._new_volume(
                "runtime",
                "RUNTIME_DIR",
                dst,
                storage_url / src,
            )
            for src, dst in SETTINGS.get("RUNTIME_VOLUME_MAPS", {}).items()
        ]

        # Add any extra volumes verbatim.
        processing_volumes += SETTINGS.get("FLOW_DOCKER_EXTRA_VOLUMES", [])
        return dict(processing_volumes)

    def _data_dir_clean(self, storage_url: Path) -> bool:
        """Check if data dir does not contain old log file."""
        log = Path(SETTINGS["FLOW_EXECUTOR"]["DATA_DIR"]) / storage_url / "stdout.txt"
        return not log.is_file()

    async def start(self):
        """Start process execution."""
        # Old log file is present, do not run the process again.
        storage_url = Path(STORAGE_LOCATION["url"])

        if not self._data_dir_clean(storage_url):
            logger.error("Stdout or jsonout file already exists, aborting.")
            return

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

        # Make sure that sockets dir exists.
        os.makedirs(
            self.runtime_dir / storage_url / ExecutorFiles.SOCKETS_SUBDIR, exist_ok=True
        )

        logger.debug("Checking existence of docker image: %s.", processing_image)

        listener_settings = SETTINGS.get("FLOW_EXECUTOR", {}).get(
            "LISTENER_CONNECTION", {}
        )
        environment = {
            "CONTAINER_TIMEOUT": constants.CONTAINER_TIMEOUT,
            "SOCKETS_VOLUME": constants.SOCKETS_VOLUME,
            "COMMUNICATION_PROCESSING_SOCKET": constants.COMMUNICATION_PROCESSING_SOCKET,
            "SCRIPT_SOCKET": constants.SCRIPT_SOCKET,
            "UPLOAD_FILE_SOCKET": constants.UPLOAD_FILE_SOCKET,
            "LISTENER_IP": listener_settings.get("hosts", {}).get(
                "docker", "127.0.0.1"
            ),
            "LISTENER_PORT": listener_settings.get("port", 53893),
            "LISTENER_PROTOCOL": listener_settings.get("protocol", "tcp"),
            "DATA_ID": self.data_id,
            "LOCATION_SUBPATH": os.fspath(storage_url),
            "DATA_LOCAL_VOLUME": os.fspath(constants.DATA_LOCAL_VOLUME),
            "DATA_ALL_VOLUME": os.fspath(constants.DATA_ALL_VOLUME),
            "DATA_VOLUME": os.fspath(constants.DATA_VOLUME),
            "UPLOAD_VOLUME": os.fspath(constants.UPLOAD_VOLUME),
            "SECRETS_DIR": os.fspath(constants.SECRETS_VOLUME),
            "RUNNING_IN_CONTAINER": 1,
            "RUNNING_IN_DOCKER": 1,
            "FLOW_MANAGER_KEEP_DATA": SETTINGS.get("FLOW_MANAGER_KEEP_DATA", False),
            "DATA_ALL_VOLUME_SHARED": True,
            "DESCRIPTOR_CHUNK_SIZE": 100,
            # Must init container set permissions.
            "INIT_SET_PERMISSIONS": False,
            "UPLOAD_CONNECTOR_NAME": SETTINGS.get("UPLOAD_CONNECTOR_NAME", "local"),
        }

        autoremove = SETTINGS.get("FLOW_DOCKER_AUTOREMOVE", False)
        # Docker on MacOSX usus different settings
        if platform.system() == "Darwin":
            environment["LISTENER_IP"] = "host.docker.internal"

        init_arguments = {
            "auto_remove": autoremove,
            "volumes": self._init_volumes(),
            "command": ["/usr/local/bin/python3", "-m", "executors.init_container"],
            "image": communicator_image,
            "name": f"{self.container_name}-init",
            "detach": True,
            "cpu_quota": 1000000,
            "mem_limit": f"4000m",
            "mem_reservation": f"200m",
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
            "cpu_quota": 100000,  # TODO: how much?
            "mem_limit": f"4000m",  # TODO: how much?
            "mem_reservation": f"200m",
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
            "working_dir": os.fspath(constants.DATA_LOCAL_VOLUME),
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
        init_container = client.containers.run(**init_arguments)
        init_container_status = await loop.run_in_executor(None, init_container.wait)

        if init_container_status["StatusCode"] != 0:
            logger.error(
                "Init container exit code was %s instead of 0, aborting.",
                init_container_status["StatusCode"],
            )
            return

        communication_container = client.containers.run(**communication_arguments)

        processing_container = client.containers.run(**processing_arguments)
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
