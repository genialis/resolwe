""".. Ignore pydocstyle D400.

====================
Kubernetes Connector
====================

"""
import hashlib
import json
import logging
import os
import re
import time
from contextlib import suppress
from enum import Enum
from importlib.util import find_spec
from pathlib import Path
from typing import Any, Dict, Iterable, Tuple

import kubernetes

from django.conf import settings
from django.db.models import Sum
from django.db.models.functions import Coalesce

from resolwe.flow.executors import constants
from resolwe.flow.models import Data, DataDependency, Process
from resolwe.flow.utils.decorators import retry
from resolwe.storage import settings as storage_settings
from resolwe.storage.connectors import connectors
from resolwe.storage.connectors.baseconnector import BaseStorageConnector
from resolwe.utils import BraceMessage as __

from .base import BaseConnector

# TODO: is this really needed?
# Limits of containers' access to memory. We set the limit to ensure
# processes are stable and do not get killed by OOM signal.
KUBERNETES_MEMORY_HARD_LIMIT_BUFFER = 2000

# Timeout (in seconds) to wait for response from kubernetes API.
KUBERNETES_TIMEOUT = 30

logger = logging.getLogger(__name__)


def get_mountable_connectors() -> Iterable[Tuple[str, BaseStorageConnector]]:
    """Iterate through all the storages and find mountable connectors.

    :returns: list of tuples (storage_name, connector).
    """
    return [
        (storage_name, connector)
        for storage_name in storage_settings.FLOW_STORAGE
        for connector in connectors.for_storage(storage_name)
        if connector.mountable
    ]


def get_upload_dir() -> str:
    """Get the upload path.

    : returns: the path of the first mountable connector for storage
        'upload'.

    :raises RuntimeError: if no applicable connector is found.
    """
    for connector in connectors.for_storage("upload"):
        if connector.mountable:
            return f"/upload_{connector.name}"
    raise RuntimeError("No mountable upload connector is defined.")


def unique_volume_name(base_name: str, data_id: int) -> str:
    """Get unique persistent volume claim name."""
    return f"{base_name}-{data_id}"


def sanitize_kubernetes_label(label: str, trim_end: bool = True) -> str:
    """Make sure kubernetes label complies with the rules.

    See the URL bellow for details.

    https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/
    """
    max_length = 63
    sanitized_label = re.sub("[^0-9a-zA-Z\-]+", "-", label).strip("-_.")
    if len(sanitized_label) > max_length:
        logger.warning(__("Label '%s' is too long and was truncated.", label))
        if trim_end:
            sanitized_label = sanitized_label[-max_length:].strip("-_.")
        else:
            sanitized_label = sanitized_label[:max_length].strip("-_.")
    return sanitized_label


class ConfigLocation(Enum):
    """The enum specifying where to read the configuration from."""

    INCLUSTER = "incluster"
    KUBECTL = "kubectl"


class Connector(BaseConnector):
    """Kubernetes-based connector for job execution."""

    def __init__(self):
        """Initialize."""
        self._config_location = ConfigLocation(
            settings.KUBERNETES_DISPATCHER_CONFIG_LOCATION
        )
        self._initialize_variables()

    def _initialize_variables(self):
        """Init variables.

        This has to be done for every run since settings values may be overriden for tests.
        """
        self.kubernetes_namespace = getattr(settings, "KUBERNETES_SETTINGS", {}).get(
            "namespace", "default"
        )
        self.tools_path_prefix = Path("/usr/local/bin/resolwe")

    def _prepare_environment(
        self,
        data: Data,
        listener_connection: Tuple[str, str, str],
        tools_configmaps: Dict[str, str],
    ) -> list:
        """Prepare environmental variables."""
        host, port, protocol = listener_connection
        environment = {
            "LISTENER_SERVICE_HOST": host,
            "LISTENER_SERVICE_PORT": port,
            "LISTENER_PROTOCOL": protocol,
            "DATA_ID": data.id,
            "FLOW_MANAGER_KEEP_DATA": getattr(
                settings, "FLOW_MANAGER_KEEP_DATA", False
            ),
            "RUNNING_IN_CONTAINER": 1,
            "RUNNING_IN_KUBERNETES": 1,
            "GENIALIS_UID": os.getuid(),
            "GENIALIS_GID": os.getgid(),
            "DESCRIPTOR_CHUNK_SIZE": 100,
            "MOUNTED_CONNECTORS": ",".join(
                connector.name
                for connector in connectors.values()
                if connector.mountable
            ),
            "TOOLS_PATHS": ":".join(
                f"{self.tools_path_prefix / tools_name}"
                for tools_name in tools_configmaps
            ),
        }
        with suppress(RuntimeError):
            environment["UPLOAD_DIR"] = get_upload_dir()

        return [
            {"name": name, "value": str(value)} for name, value in environment.items()
        ]

    def _create_configmap_if_needed(self, name: str, content: Dict, core_api: Any):
        """Create configmap if necessary."""
        try:
            core_api.read_namespaced_config_map(
                name=name, namespace=self.kubernetes_namespace
            )
        except kubernetes.client.rest.ApiException:
            # The configmap is not found, create one.
            configmap_description = {
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": name},
                "data": content,
            }
            # The configmap might have already been created in the meantime.
            with suppress(kubernetes.client.rest.ApiException):
                core_api.create_namespaced_config_map(
                    body=configmap_description,
                    namespace=self.kubernetes_namespace,
                    _request_timeout=KUBERNETES_TIMEOUT,
                )

    def _get_tools_configmaps(self, core_api) -> Dict[str, str]:
        """Get and return configmaps for tools."""
        description_configmap_name = (
            getattr(settings, "KUBERNETES_TOOLS_CONFIGMAPS", None) or "tools-configmaps"
        )
        configmap = core_api.read_namespaced_config_map(
            name=description_configmap_name, namespace=self.kubernetes_namespace
        )
        return configmap.data

    def _get_files_configmap_name(self, location_subpath: Path, core_api: Any):
        """Get or create configmap for files.

        We have to map group, passwd and startup files inside containers.
        This is done using configmap which is mostly static. This command
        returns description for this configmap.
        """
        passwd_content = "root:x:0:0:root:/root:/bin/bash\n"
        passwd_content += f"user:x:{os.getuid()}:{os.getgid()}:user:{os.fspath(constants.PROCESSING_VOLUME)}:/bin/bash\n"
        group_content = "root:x:0:\n"
        group_content += f"user:x:{os.getgid()}:user\n"

        data = dict()
        modules = {
            "communicator": "resolwe.process.communicator",
            "bootstrap-python-runtime": "resolwe.process.bootstrap_python_runtime",
            "socket-utils": "resolwe.flow.executors.socket_utils",
            "constants": "resolwe.flow.executors.constants",
            "startup-script": "resolwe.flow.executors.startup_processing_container",
        }
        for module, full_module_name in modules.items():
            spec = find_spec(full_module_name)
            assert (
                spec is not None and spec.origin is not None
            ), f"Unable to determine module {full_module_name} source code."
            data[module] = Path(spec.origin).read_text()

        data.update(
            {
                "passwd": passwd_content,
                "group": group_content,
            }
        )

        data_md5 = hashlib.md5(json.dumps(data, sort_keys=True).encode()).hexdigest()
        configmap_name = f"configmap-files-{data_md5}"

        self._create_configmap_if_needed(configmap_name, data, core_api)
        return configmap_name

    def _volumes(
        self,
        data_id: int,
        location_subpath: Path,
        core_api: Any,
        tools_configmaps: Dict[str, str],
    ) -> list:
        """Prepare all volumes."""

        def volume_from_config(volume_name: str, volume_config: dict):
            """Get configuration for kubernetes for given volume."""
            claim_name = volume_config["config"]["name"]
            if self._should_create_pvc(volume_config):
                claim_name = unique_volume_name(claim_name, data_id)
            if volume_config["type"] == "persistent_volume":
                return {
                    "name": volume_name,
                    "persistentVolumeClaim": {"claimName": claim_name},
                }
            elif volume_config["type"] == "host_path":
                return {
                    "name": volume_name,
                    "hostPath": {"path": os.fspath(volume_config["config"]["path"])},
                }
            elif volume_config["type"] == "temporary_directory":
                return {"name": volume_name, "emptyDir": {}}
            else:
                raise RuntimeError(
                    f"Unsupported volume configuration: {volume_config}."
                )

        files_configmap_name = self._get_files_configmap_name(
            location_subpath, core_api
        )

        volumes = [
            {
                "name": "files-volume",
                "configMap": {"name": files_configmap_name},
            }
        ]

        for tools_name, configmap_name in tools_configmaps.items():
            volumes.append(
                {
                    "name": f"tools-{tools_name}",
                    "configMap": {"name": configmap_name, "defaultMode": 0o755},
                }
            )

        volumes += [
            volume_from_config(volume_name, volume_config)
            for volume_name, volume_config in storage_settings.FLOW_VOLUMES.items()
            if volume_config["config"]["name"] != "tools"
        ]

        for storage_name, connector in get_mountable_connectors():
            claim_name = connector.config.get("persistent_volume_claim", None)
            volume_data: Dict[str, Any] = {
                "name": connector.name,
            }
            if claim_name:
                volume_data["persistentVolumeClaim"] = {"claimName": claim_name}
            else:
                volume_data["hostPath"] = (
                    {"path": os.fspath(connector.config["path"])},
                )
            volumes.append(volume_data)

        return volumes

    def _init_container_mountpoints(self):
        """Prepare mountpoints for init container.

        Processing and input volume (if defined) and all mountable connectors
        are mounted inside container.
        """
        mount_points = [
            {
                "name": constants.PROCESSING_VOLUME_NAME,
                "mountPath": os.fspath(constants.PROCESSING_VOLUME),
                "readOnly": False,
            },
            {
                "name": constants.SECRETS_VOLUME_NAME,
                "mountPath": os.fspath(constants.SECRETS_VOLUME),
                "readOnly": False,
            },
        ]
        if constants.INPUTS_VOLUME_NAME in storage_settings.FLOW_VOLUMES:
            mount_points.append(
                {
                    "name": constants.INPUTS_VOLUME_NAME,
                    "mountPath": os.fspath(constants.INPUTS_VOLUME),
                    "readOnly": False,
                }
            )
        mount_points += [
            {
                "name": connector.name,
                "mountPath": f"/{storage_name}_{connector.name}",
                "readOnly": False,
            }
            for storage_name, connector in get_mountable_connectors()
        ]
        return mount_points

    def _communicator_mountpoints(self, location_subpath: Path) -> list:
        """Mountpoints for communicator container.

        Socket directory and mountable connectors are mounted inside.
        """
        mount_points = [
            {
                "name": constants.SOCKETS_VOLUME_NAME,
                "mountPath": os.fspath(constants.SOCKETS_VOLUME),
                "readOnly": False,
            },
            {
                "name": constants.SECRETS_VOLUME_NAME,
                "mountPath": os.fspath(constants.SECRETS_VOLUME),
                "readOnly": False,
            },
        ]
        mount_points += [
            {
                "name": connector.name,
                "mountPath": f"/{storage_name}_{connector.name}",
                "readOnly": False,
            }
            for storage_name, connector in get_mountable_connectors()
        ]
        return mount_points

    def _processing_mountpoints(
        self, location_subpath: Path, tools_configmaps: Dict[str, str]
    ):
        """Mountpoints for processing container.

        Processing and input volume (if defined) and all mountable connectors
        are mounted inside container. All except processing volume are mounted
        read-only.
        """
        mount_points = [
            {
                "name": constants.PROCESSING_VOLUME_NAME,
                "mountPath": os.fspath(constants.PROCESSING_VOLUME),
                "subPath": os.fspath(location_subpath),
                "readOnly": False,
            },
        ]
        if constants.INPUTS_VOLUME_NAME in storage_settings.FLOW_VOLUMES:
            mount_points.append(
                {
                    "name": constants.INPUTS_VOLUME_NAME,
                    "mountPath": os.fspath(constants.INPUTS_VOLUME),
                    "readOnly": False,
                }
            )
        mount_points += [
            {
                "name": connector.name,
                "mountPath": f"/{storage_name}_{connector.name}",
                "readOnly": storage_name != "upload",
            }
            for storage_name, connector in get_mountable_connectors()
        ]

        mount_points += [
            {
                "name": "files-volume",
                "mountPath": "/etc/passwd",
                "subPath": "passwd",
            },
            {
                "name": "files-volume",
                "mountPath": "/etc/group",
                "subPath": "group",
            },
            {
                "name": "files-volume",
                "mountPath": "/socket_utils.py",
                "subPath": "socket-utils",
            },
            {
                "name": "files-volume",
                "mountPath": "/processing.py",
                "subPath": "startup-script",
            },
            {
                "name": "files-volume",
                "mountPath": "/constants.py",
                "subPath": "constants",
            },
            {
                "name": "files-volume",
                "mountPath": f"/{constants.BOOTSTRAP_PYTHON_RUNTIME}",
                "subPath": "bootstrap-python-runtime",
            },
            {
                "name": "files-volume",
                "mountPath": "/communicator.py",
                "subPath": "communicator",
            },
            {
                "name": constants.SOCKETS_VOLUME_NAME,
                "mountPath": os.fspath(constants.SOCKETS_VOLUME),
            },
            {
                "name": constants.SECRETS_VOLUME_NAME,
                "mountPath": os.fspath(constants.SECRETS_VOLUME),
                "readOnly": True,
            },
        ]
        for tool_name in tools_configmaps:
            mount_points.append(
                {
                    "name": f"tools-{tool_name}",
                    "mountPath": f"{self.tools_path_prefix / tool_name}",
                }
            )
        return mount_points

    def _persistent_volume_claim(
        self, claim_name: str, size: int, volume_config: Dict
    ) -> Dict[str, Any]:
        """Prepare claim for persistent volume."""
        return {
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "metadata": {"name": claim_name},
            "spec": {
                "accessModes": ["ReadWriteOnce"],
                "storageClassName": volume_config.get("storageClassName", "gp2"),
                "resources": {
                    "requests": {
                        "storage": size,
                    }
                },
            },
        }

    def _data_inputs_size(self, data: Data, safety_buffer: int = 2**30) -> int:
        """Get the size of data inputs.

        Also add 10% of the volume size + 2GB as safety buffer. When having a
        large filesystem with lots of files the filesystem overhead can be
        significant so a fixed safety buffer alone is not sufficient.

        :returns: the size of the input data in bytes.
        """
        inputs_size = (
            Data.objects.filter(
                children_dependency__child=data,
                children_dependency__kind=DataDependency.KIND_IO,
            )
            .aggregate(total_size=Coalesce(Sum("size"), 0))
            .get("total_size")
        )
        assert isinstance(inputs_size, int)
        return int(1.1 * (inputs_size + safety_buffer))

    def _should_create_pvc(self, volume_config: dict) -> bool:
        """Return True if pvc for the given volume should be created."""
        if volume_config["type"] != "persistent_volume":
            return False

        return volume_config["config"].get("create_pvc", False)

    def _image_mapper(self, image_name: str, mapper: Dict[str, str]) -> str:
        """Transform the image name if necessary.

        When image_name starts with one of the keys defined in the mapper the
        matching part is replaced with the corresponding value.
        """
        for key in mapper:
            if image_name.startswith(key):
                return f"{mapper[key]}{image_name[len(key):]}"
        return image_name

    @retry(
        logger=logger,
        max_retries=5,
        retry_exceptions=(kubernetes.config.config_exception.ConfigException,),
        cleanup_callback=lambda: kubernetes.config.kube_config._cleanup_temp_files(),
    )
    def _load_kubernetes_config(self):
        """Load the kubernetes configuration.

        :raises kubernetes.config.config_exception.ConfigException: when the
            configuration could not be read.
        """
        (
            kubernetes.config.load_incluster_config()
            if self._config_location == ConfigLocation.INCLUSTER
            else kubernetes.config.load_kube_config()
        )

    def start(self, data: Data, listener_connection: Tuple[str, str, str]):
        """Start process execution.

        Construct kubernetes job description and pass it to the kubernetes.
        """
        try:
            self._load_kubernetes_config()
        except kubernetes.config.config_exception.ConfigException as exception:
            logger.exception("Could not load the kubernetes configuration.")
            raise exception

        batch_api = kubernetes.client.BatchV1Api()
        core_api = kubernetes.client.CoreV1Api()

        tools_configmaps = self._get_tools_configmaps(core_api)
        container_environment = self._prepare_environment(
            data, listener_connection, tools_configmaps
        )
        location_subpath = Path(data.location.subpath)

        container_name_prefix = (
            getattr(settings, "FLOW_EXECUTOR", {})
            .get("CONTAINER_NAME_PREFIX", "resolwe")
            .replace("_", "-")
            .lower()
        )
        container_name = self._generate_container_name(container_name_prefix, data.pk)

        # Set resource limits.
        requests = dict()
        limits = data.get_resource_limits()

        requests["cpu"] = limits.pop("cores")
        limits["cpu"] = requests["cpu"] + 1
        # Overcommit CPU by 20%.
        requests["cpu"] *= 0.8

        # The memory in the database is stored in megabytes but the kubertenes
        # requires memory in bytes.
        # We request 10% less memory than stored in the database and set limit
        # at 10% more plus KUBERNETES_MEMORY_HARD_LIMIT_BUFFER. The processes
        # usually require 16GB, 32GB... and since the node usualy has 64GB of
        # memory and some of it is consumed by the system processes only one
        # process process that requires 32GB can run on a node instead of 2.

        requests["memory"] = 0.9 * limits["memory"]
        limits["memory"] = 1.1 * limits["memory"] + KUBERNETES_MEMORY_HARD_LIMIT_BUFFER
        limits["memory"] *= 2**20  # 2 ** 20 = mebibyte
        requests["memory"] *= 2**20

        # Get the limits and requests for the communicator container.
        communicator_limits = getattr(
            settings,
            "FLOW_KUBERNETES_COMMUNICATOR_LIMITS",
            {"memory": "256M", "cpu": 0.1},
        )
        communicator_requests = getattr(
            settings,
            "FLOW_KUBERNETES_COMMUNICATOR_REQUESTS",
            {"memory": "256M", "cpu": 0.1},
        )

        resources = data.process.requirements.get("resources", {})
        network = "bridge"
        use_host_network = False
        if "network" in resources:
            # Configure Docker network mode for the container (if specified).
            # By default, current Docker versions use the 'bridge' mode which
            # creates a network stack on the default Docker bridge.
            network = getattr(settings, "FLOW_EXECUTOR", {}).get("NETWORK", "")
            use_host_network = network == "host"

        # Generate and set seccomp policy to limit syscalls.
        security_context = {
            "runAsUser": os.getuid(),
            "runAsGroup": os.getgid(),
            "allowPrivilegeEscalation": False,
            "privileged": False,
            "capabilities": {"drop": ["ALL"]},
        }

        annotations = dict()

        # Do not evict job from node.
        annotations["cluster-autoscaler.kubernetes.io/safe-to-evict"] = "false"

        if not getattr(settings, "FLOW_DOCKER_DISABLE_SECCOMP", False):
            # The path is a relative path in the kubelet root
            # directory:
            # <seccomp_root>/<path>, where <seccomp_root> is defined via the
            # --seccomp-profile-root flag on the Kubelet. If the
            # --seccomp-profile-root flag is not defined, the default path will
            # be used, which is <root-dir>/seccomp where <root-dir> is
            # specified by the --root-dir flag.
            # https://kubernetes.io/docs/concepts/policy/pod-security-policy/
            #
            # The file is transfered to kubelets with daemonset ? Currently I
            # mount my /tmp directory to the /seccomp directory in minikube.
            annotations["seccomp.security.alpha.kubernetes.io/pod"] = "runtime/default"

        mapper = getattr(settings, "FLOW_CONTAINER_IMAGE_MAP", {})
        communicator_image = getattr(
            settings,
            "FLOW_DOCKER_COMMUNICATOR_IMAGE",
            "public.ecr.aws/s4q6j6e8/resolwe/com:latest",
        )
        communicator_image = self._image_mapper(communicator_image, mapper)

        requirements = data.process.requirements.get("executor", {}).get("docker", {})
        processing_container_image = str(
            requirements.get(
                "image",
                getattr(
                    settings,
                    "FLOW_DOCKER_DEFAULT_PROCESSING_CONTAINER_IMAGE",
                    "public.ecr.aws/s4q6j6e8/resolwe/base:ubuntu-20.04",
                ),
            ),
        )
        processing_container_image = self._image_mapper(
            processing_container_image, mapper
        )

        affinity = {}
        kubernetes_affinity = getattr(settings, "FLOW_KUBERNETES_AFFINITY", None)
        if kubernetes_affinity:
            affinity = {
                "nodeAffinity": {
                    "requiredDuringSchedulingIgnoredDuringExecution": {
                        "nodeSelectorTerms": [
                            {
                                "matchExpressions": [
                                    {
                                        "key": "nodegroup",
                                        "operator": "In",
                                        "values": [kubernetes_affinity],
                                    }
                                ]
                            }
                        ]
                    }
                }
            }

        job_type = dict(Process.SCHEDULING_CLASS_CHOICES)[data.process.scheduling_class]
        job_description = {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {"name": sanitize_kubernetes_label(container_name)},
            "spec": {
                # Keep finished pods around for ten seconds. If job is not
                # deleted its PVC claim persists and it causes PV to stay
                # around.
                # This can be changed by running a cron job that periodically
                # checks for PVC that can be deleted.
                "ttlSecondsAfterFinished": 300,
                "template": {
                    "metadata": {
                        "name": sanitize_kubernetes_label(container_name),
                        "labels": {
                            "app": "resolwe",
                            "data_id": str(data.pk),
                            "process": sanitize_kubernetes_label(data.process.slug),
                            "job_type": sanitize_kubernetes_label(job_type),
                        },
                        "annotations": annotations,
                    },
                    "spec": {
                        "affinity": affinity,
                        "hostNetwork": use_host_network,
                        "volumes": self._volumes(
                            data.id, location_subpath, core_api, tools_configmaps
                        ),
                        "initContainers": [
                            {
                                "name": sanitize_kubernetes_label(
                                    f"{container_name}-init"
                                ),
                                "image": communicator_image,
                                "imagePullPolicy": "Always",
                                "workingDir": "/",
                                "command": ["/usr/local/bin/python3"],
                                "args": ["-m", "executors.init_container"],
                                "securityContext": {"privileged": True},
                                "volumeMounts": self._init_container_mountpoints(),
                                "env": container_environment,
                            },
                        ],
                        "containers": [
                            {
                                "name": sanitize_kubernetes_label(container_name),
                                "image": processing_container_image,
                                "resources": {"limits": limits, "requests": requests},
                                "securityContext": security_context,
                                "env": container_environment,
                                "workingDir": os.fspath(constants.PROCESSING_VOLUME),
                                "imagePullPolicy": "Always",
                                "command": ["/usr/bin/python3"],
                                "args": ["/processing.py"],
                                "volumeMounts": self._processing_mountpoints(
                                    location_subpath, tools_configmaps
                                ),
                            },
                            {
                                "name": sanitize_kubernetes_label(
                                    f"{container_name}-communicator"
                                ),
                                "image": communicator_image,
                                "imagePullPolicy": "Always",
                                "resources": {
                                    "limits": communicator_limits,
                                    "requests": communicator_requests,
                                },
                                "securityContext": security_context,
                                "env": container_environment,
                                "command": ["/usr/local/bin/python3"],
                                "args": ["/startup.py"],
                                "volumeMounts": self._communicator_mountpoints(
                                    location_subpath
                                ),
                            },
                        ],
                        "restartPolicy": "Never",
                    },
                },
                "backoffLimit": 0,
            },
        }
        start_time = time.time()

        processing_name = constants.PROCESSING_VOLUME_NAME
        input_name = constants.INPUTS_VOLUME_NAME
        if self._should_create_pvc(storage_settings.FLOW_VOLUMES[processing_name]):
            claim_name = unique_volume_name(
                storage_settings.FLOW_VOLUMES[processing_name]["config"]["name"],
                data.id,
            )
            claim_size = limits.pop("storage", 200) * (2**30)  # Default 200 gibibytes
            core_api.create_namespaced_persistent_volume_claim(
                body=self._persistent_volume_claim(
                    claim_name,
                    claim_size,
                    storage_settings.FLOW_VOLUMES[processing_name]["config"],
                ),
                namespace=self.kubernetes_namespace,
                _request_timeout=KUBERNETES_TIMEOUT,
            )
        if input_name in storage_settings.FLOW_VOLUMES:
            if self._should_create_pvc(storage_settings.FLOW_VOLUMES[input_name]):
                claim_size = self._data_inputs_size(data)
                claim_name = unique_volume_name(
                    storage_settings.FLOW_VOLUMES[input_name]["config"]["name"],
                    data.id,
                )
                core_api.create_namespaced_persistent_volume_claim(
                    body=self._persistent_volume_claim(
                        claim_name,
                        claim_size,
                        storage_settings.FLOW_VOLUMES[input_name]["config"],
                    ),
                    namespace=self.kubernetes_namespace,
                    _request_timeout=KUBERNETES_TIMEOUT,
                )

        logger.debug(f"Creating namespaced job: {job_description}")
        batch_api.create_namespaced_job(
            body=job_description,
            namespace=self.kubernetes_namespace,
            _request_timeout=KUBERNETES_TIMEOUT,
        )
        end_time = time.time()
        logger.info(
            "It took {:.2f}s to send config to kubernetes".format(end_time - start_time)
        )

    def _generate_container_name(self, prefix: str, data_id: int):
        """Generate unique container name.

        Name of the kubernetes container should contain only lower case
        alpfanumeric characters and dashes. Underscores are not allowed.
        """
        return "{}-{}".format(prefix, data_id)

    def submit(self, data: Data, argv):
        """Run process.

        For details, see
        :meth:`~resolwe.flow.managers.workload_connectors.base.BaseConnector.submit`.
        """
        (host, port, protocol) = argv[-1].rsplit(" ", maxsplit=3)[-3:]
        self._initialize_variables()
        self.start(data, (host, port, protocol))

        logger.debug(
            __(
                "Connector '{}' running for Data with id {} ({}).",
                self.__class__.__module__,
                data.id,
                repr(argv),
            )
        )

    def cleanup(self, data_id: int):
        """Remove the persistent volume claims created by the executor."""
        try:
            self._load_kubernetes_config()
        except kubernetes.config.config_exception.ConfigException as exception:
            logger.exception("Could not load the kubernetes configuration.")
            raise exception

        core_api = kubernetes.client.CoreV1Api()
        claim_names = [
            unique_volume_name(type_, data_id)
            for type_ in [
                constants.PROCESSING_VOLUME_NAME,
                constants.INPUTS_VOLUME_NAME,
            ]
            if type_ in storage_settings.FLOW_VOLUMES
            and self._should_create_pvc(storage_settings.FLOW_VOLUMES[type_])
        ]
        for persistent_claim_name in claim_names:
            logger.debug("Kubernetes: removing claim %s.", persistent_claim_name)
            with suppress(kubernetes.client.rest.ApiException):
                core_api.delete_namespaced_persistent_volume_claim(
                    name=persistent_claim_name,
                    namespace=self.kubernetes_namespace,
                    _request_timeout=KUBERNETES_TIMEOUT,
                )
