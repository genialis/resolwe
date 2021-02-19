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
import shlex
import time
from base64 import b64encode
from contextlib import suppress
from math import ceil
from pathlib import Path
from typing import Any, Dict, List

import kubernetes

from django.conf import settings
from django.db.models import Sum
from django.db.models.functions import Coalesce

from resolwe.flow.executors import constants
from resolwe.flow.executors.prepare import BaseFlowExecutorPreparer
from resolwe.flow.executors.protocol import ExecutorFiles
from resolwe.flow.models import Data, DataDependency
from resolwe.utils import BraceMessage as __

from .base import BaseConnector

# TODO: is this really needed?
# Limits of containers' access to memory. We set the limit to ensure
# processes are stable and do not get killed by OOM signal.
KUBERNETES_MEMORY_HARD_LIMIT_BUFFER = 2000


logger = logging.getLogger(__name__)


class Connector(BaseConnector):
    """Kubernetes-based connector for job execution."""

    def __init__(self):
        """Initialization."""
        self._initialize_variables()

    def _initialize_variables(self):
        """Init variables.

        This has to be done for every run since settings values may be overriden for tests.
        """
        self.efs_root = getattr(settings, "KUBERNETES_SETTINGS", {}).get(
            "efs_mount", "/efs"
        )
        upload_dir = Path(settings.FLOW_EXECUTOR.get("UPLOAD_DIR", ""))
        self.kubernetes_namespace = getattr(settings, "KUBERNETES_SETTINGS", {}).get(
            "namespace", "default"
        )
        self.data_dir = Path(settings.FLOW_EXECUTOR.get("DATA_DIR", ""))
        self.runtime_dir = Path(settings.FLOW_EXECUTOR.get("RUNTIME_DIR", ""))
        self.efs_upload_dir = upload_dir.relative_to(self.efs_root)
        self.efs_data_dir = self.data_dir.relative_to(self.efs_root)
        self.efs_runtime_dir = self.runtime_dir.relative_to(self.efs_root)

    def _dict_from_directory(self, directory: Path) -> Dict[str, str]:
        """Get dictionary from given directory.

        File names are keys and corresponding file contents are values.
        """
        return {
            entry.name: entry.read_text()
            for entry in directory.glob("*")
            if entry.is_file()
        }

    def _prepare_environment(self, data: Data) -> list:
        """Prepare environmental variables."""
        listener_settings = getattr(settings, "FLOW_EXECUTOR", {}).get(
            "LISTENER_CONNECTION", {}
        )
        environment = {
            "CONTAINER_TIMEOUT": constants.CONTAINER_TIMEOUT,
            "SOCKETS_VOLUME": os.fspath(constants.SOCKETS_VOLUME),
            "COMMUNICATION_PROCESSING_SOCKET": constants.COMMUNICATION_PROCESSING_SOCKET,
            "SCRIPT_SOCKET": constants.SCRIPT_SOCKET,
            "UPLOAD_FILE_SOCKET": constants.UPLOAD_FILE_SOCKET,
            "LISTENER_IP": listener_settings.get("hosts", {}).get(
                "kubernetes", "127.0.0.1"
            ),
            "LISTENER_PORT": listener_settings.get("port", 53893),
            "LISTENER_PROTOCOL": listener_settings.get("protocol", "tcp"),
            "DATA_ID": data.id,
            "LOCATION_SUBPATH": data.location.subpath,
            "DATA_LOCAL_VOLUME": os.fspath(constants.DATA_LOCAL_VOLUME),
            "DATA_ALL_VOLUME": os.fspath(constants.DATA_ALL_VOLUME),
            "DATA_VOLUME": os.fspath(constants.DATA_VOLUME),
            "UPLOAD_VOLUME": os.fspath(constants.UPLOAD_VOLUME),
            "INPUTS_VOLUME": os.fspath(constants.INPUTS_VOLUME),
            "SECRETS_DIR": os.fspath(constants.SECRETS_VOLUME),
            "TMP_DIR": os.fspath(constants.TMPDIR),
            "FLOW_MANAGER_KEEP_DATA": getattr(
                settings, "FLOW_MANAGER_KEEP_DATA", False
            ),
            "RUNNING_IN_CONTAINER": 1,
            "RUNNING_IN_KUBERNETES": 1,
            "GENIALIS_UID": os.getuid(),
            "GENIALIS_GID": os.getgid(),
            "DESCRIPTOR_CHUNK_SIZE": 100,
            "UPLOAD_CONNECTOR_NAME": self._get_upload_connector_name(data),
            # Is the DATA_ALL volume shared between containers. This is needed
            # in init container to know how to download missing data.
            "DATA_ALL_VOLUME_SHARED": False,
            # Must init container set permissions.
            "INIT_SET_PERMISSIONS": True,
        }

        return [
            {"name": name, "value": str(value)} for name, value in environment.items()
        ]

    def _get_upload_connector_name(self, data: Data) -> str:
        """Get the connector for data upload.

        Read the connector name from the StorageLocation class created by the
        dispatcher.
        """
        return data.location.default_storage_location.connector_name

    def _prepare_secrets(self, secrets: Dict[str, str]) -> Dict[str, str]:
        """Base64 encode every value and transform it to string.

        The original dictionary is not modifies.
        """
        return {
            key: b64encode(value.encode()).decode() for key, value in secrets.items()
        }

    def _generate_secrets(self, name: str, directory: Path) -> Dict[str, Any]:
        """Prepare secrets from the given directory."""
        return {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": name},
            "data": self._prepare_secrets(self._dict_from_directory(directory)),
        }

    def _get_configmap(self, name: str, directory: Path) -> Dict[str, Any]:
        """Prepare config map from the given directory."""
        return {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": name},
            "data": self._dict_from_directory(directory),
        }

    def _ebs_claim_name(self, type_: str, data_id: int) -> str:
        """Get EBS claim name."""
        return f"pvc-{type_}-{data_id}"

    def _get_files_configmap_name(self, location_subpath: Path, core_api: Any):
        """Get or create configmap for files.

        We have to map group, passwd and startup files inside containers.
        This is done using configmap which is mostly static. This command
        returns description for this configmap.
        """
        socket_utils_path: Path = (
            self.runtime_dir
            / location_subpath
            / "executors"
            / ExecutorFiles.SOCKET_UTILS
        )
        processing_startup_path: Path = (
            self.runtime_dir
            / location_subpath
            / "executors"
            / ExecutorFiles.STARTUP_PROCESSING_SCRIPT
        )
        constants_path: Path = (
            self.runtime_dir / location_subpath / "executors" / ExecutorFiles.CONSTANTS
        )

        startup_content = processing_startup_path.read_text()
        socket_utils_content = socket_utils_path.read_text()
        constants_content = constants_path.read_text()
        passwd_content = "root:x:0:0:root:/root:/bin/bash\n"
        passwd_content += f"user:x:{os.getuid()}:{os.getgid()}:user:{os.fspath(constants.DATA_LOCAL_VOLUME)}:/bin/bash\n"
        group_content = "root:x:0:\n"
        group_content += f"user:x:{os.getgid()}:user\n"

        data = {
            "passwd": passwd_content,
            "group": group_content,
            "startup-script": startup_content,
            "socket-utils": socket_utils_content,
            "constants": constants_content,
        }

        data_md5 = hashlib.md5(json.dumps(data, sort_keys=True).encode()).hexdigest()
        configmap_name = f"configmap-files-{data_md5}"

        logger.debug(f"Files configmap: {configmap_name}")

        try:
            core_api.read_namespaced_config_map(
                name=configmap_name, namespace=self.kubernetes_namespace
            )
        except kubernetes.client.rest.ApiException:
            # The configmap is not found, create one.
            configmap_description = {
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": configmap_name},
                "data": data,
            }
            # The configmap might have already been created in the meantime.
            with suppress(kubernetes.client.rest.ApiException):
                core_api.create_namespaced_config_map(
                    body=configmap_description, namespace=self.kubernetes_namespace
                )

        return configmap_name

    def _volumes(
        self,
        secrets_name: str,
        configmap_name: str,
        efs_claim_name: str,
        ebs_local_claim_name: str,
        ebs_input_data_claim_name: str,
        location_subpath: Path,
        core_api: Any,
    ) -> list:
        """Prepare all volumes."""
        files_configmap_name = self._get_files_configmap_name(
            location_subpath, core_api
        )

        return [
            {
                "name": "secrets-volume",
                "secret": {"secretName": secrets_name},
            },
            {
                "name": "settings-volume",
                "configMap": {"name": configmap_name},
            },
            {
                "name": "files-volume",
                "configMap": {"name": files_configmap_name},
            },
            {
                "name": "efs-root",
                "persistentVolumeClaim": {"claimName": efs_claim_name},
            },
            {
                "name": "ebs-root",
                "persistentVolumeClaim": {"claimName": ebs_local_claim_name},
            },
            {
                "name": "ebs-input-data",
                "persistentVolumeClaim": {"claimName": ebs_input_data_claim_name},
            },
            {"name": "sockets-volume", "emptyDir": {}},
        ]

    def _communicator_mountpoints(self, location_subpath: Path) -> list:
        """Mountpoints for communicator container."""
        mount_points = [
            {
                "name": "efs-root",
                "mountPath": os.fspath(constants.DATA_ALL_VOLUME),
                "subPath": os.fspath(self.efs_data_dir),
                "readOnly": False,
            },
            {
                "name": "efs-root",
                "mountPath": os.fspath(constants.DATA_VOLUME),
                "subPath": os.fspath(self.efs_data_dir / location_subpath),
                "readOnly": False,
            },
            {
                "name": "settings-volume",
                "mountPath": os.fspath(constants.SETTINGS_VOLUME),
                "readOnly": True,
            },
            {
                "name": "secrets-volume",
                "mountPath": os.fspath(constants.SECRETS_VOLUME),
                "readOnly": False,
            },
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
                "name": "sockets-volume",
                "mountPath": os.fspath(constants.SOCKETS_VOLUME),
            },
        ]
        return mount_points

    def _init_container_mountpoints(self):
        """Prepare mountpoints for init container.

        It needs local EBS volume to set permissions and settings & secrets to
        transfer input data.
        """
        return [
            {
                "name": "efs-root",
                "mountPath": os.fspath(constants.DATA_ALL_VOLUME),
                "subPath": os.fspath(self.efs_data_dir),
                "readOnly": False,
            },
            {
                "name": "ebs-root",
                "mountPath": os.fspath(constants.DATA_LOCAL_VOLUME),
                "readOnly": False,
            },
            {
                "name": "ebs-input-data",
                "mountPath": os.fspath(constants.INPUTS_VOLUME),
                "readOnly": False,
            },
            {
                "name": "settings-volume",
                "mountPath": os.fspath(constants.SETTINGS_VOLUME),
                "readOnly": True,
            },
            {
                "name": "secrets-volume",
                "mountPath": os.fspath(constants.SECRETS_VOLUME),
                "readOnly": False,
            },
        ]

    def _processing_mountpoints(self, location_subpath: Path):
        """Mountpoints for processing container."""
        mount_points = [
            {
                "name": "ebs-input-data",
                "mountPath": os.fspath(constants.DATA_ALL_VOLUME),
                "readOnly": True,
            },
            {
                "name": "efs-root",
                "mountPath": os.fspath(constants.DATA_VOLUME),
                "subPath": os.fspath(self.efs_data_dir / location_subpath),
                "readOnly": False,
            },
            {
                "name": "ebs-root",
                "mountPath": os.fspath(constants.DATA_LOCAL_VOLUME),
                "subPath": os.fspath(location_subpath),
                "readOnly": False,
            },
            {
                "name": "efs-root",
                "mountPath": os.fspath(constants.UPLOAD_VOLUME),
                "subPath": os.fspath(self.efs_upload_dir),
                "readOnly": False,
            },
            {
                "name": "secrets-volume",
                "mountPath": os.fspath(constants.SECRETS_VOLUME),
                "readOnly": True,
            },
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
                "name": "sockets-volume",
                "mountPath": os.fspath(constants.SOCKETS_VOLUME),
            },
        ]

        # Create volumes for tools. Only consider tools located on efs.
        preparer = BaseFlowExecutorPreparer()
        tools_paths = preparer.get_tools_paths()
        mount_points += [
            {
                "name": "efs-root",
                "mountPath": os.fspath(Path("/usr/local/bin/resolwe") / str(index)),
                "subPath": os.fspath(Path(tool).relative_to(self.efs_root)),
            }
            for index, tool in enumerate(tools_paths)
        ]

        # Create volumes for runtime (all read-only).
        django_settings_path: Path = (
            self.runtime_dir
            / location_subpath
            / ExecutorFiles.SETTINGS_SUBDIR
            / ExecutorFiles.DJANGO_SETTINGS
        )

        django_settings = json.loads(django_settings_path.read_text())
        mount_points += [
            {
                "name": "efs-root",
                "mountPath": dst,
                "subPath": os.fspath(self.efs_runtime_dir / location_subpath / src),
            }
            for src, dst in django_settings.get("RUNTIME_VOLUME_MAPS", {}).items()
        ]
        # Add any extra volumes verbatim.
        mount_points += getattr(settings, "FLOW_DOCKER_EXTRA_VOLUMES", [])
        logger.debug(f"Runtime volume mount points: {mount_points}.")
        return mount_points

    def _persistent_ebs_claim(
        self, persistent_claim_name: str, volume_size_in_bytes: int
    ) -> Dict[str, Any]:
        """Prepare claim for EBS Amazon storage."""
        return {
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "metadata": {"name": persistent_claim_name},
            "spec": {
                "accessModes": ["ReadWriteOnce"],
                "storageClassName": "gp2",
                "resources": {
                    "requests": {
                        "storage": volume_size_in_bytes,
                    }
                },
            },
        }

    def _fix_permissions(self) -> List[str]:
        """Prepare EBS volume for processing.

        This means creating subdirectories named f"{data_id}" and
        f"{data_id}/{constants.TMPDIR}" inside DATA_LOCAL_VOLUME and changing
        its ownership to the user and group running in the processing container
        (uid and gid arguments).
        """
        command = (
            "sh -c 'mkdir ${DATA_LOCAL_VOLUME}/${LOCATION_SUBPATH};"
            "mkdir ${DATA_LOCAL_VOLUME}/${LOCATION_SUBPATH}/${TMP_DIR};"
            f"chown -R {os.getuid()}:{os.getgid()} "
            + "${DATA_LOCAL_VOLUME}/${LOCATION_SUBPATH}'"
        )
        logger.debug(f"Kubernetes fix permissions command: {command}.")
        return shlex.split(command)

    def _data_inputs_size(self, data: Data, safety_buffer: int = 2 ** 30) -> int:
        """Get the size of data inputs.

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
        return ceil((inputs_size + safety_buffer) / (2 ** 30)) * (2 ** 30)

    def _sanitize_kubernetes_label(self, label: str) -> str:
        """Make sure kubernetes label complies with the rules.

        See the URL bellow for details.

        https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/
        """
        max_length = 63
        replaced = re.sub("[^0-9a-zA-Z._\-]+", "_", label).strip("-_.")
        if len(replaced) > max_length:
            return replaced[-max_length:].strip("-_.")
        return replaced

    def start(self, data: Data):
        """Start process execution.

        Construct kubernetes job description and pass it to the kubernetes.
        """
        location_subpath = Path(data.location.subpath)

        if not self._check_first_run(location_subpath):
            logger.error("Stdout or jsonout file already exists, aborting.")
            return

        # Create kubernetes API every time otherwise it will time out
        # eventually and raise API exception.
        kubernetes.config.load_kube_config()
        batch_api = kubernetes.client.BatchV1Api()
        core_api = kubernetes.client.CoreV1Api()

        container_name_prefix = (
            getattr(settings, "FLOW_EXECUTOR", {})
            .get("CONTAINER_NAME_PREFIX", "resolwe")
            .replace("_", "-")
            .lower()
        )
        container_name = self._generate_container_name(container_name_prefix, data.id)
        job_name = f"job-{container_name}"
        # Prepare configmap description.
        configmap_name = f"configmap-{container_name}"
        settings_path = self.runtime_dir / location_subpath / "settings"
        configmap_description = self._get_configmap(configmap_name, settings_path)

        # Prepare secrets description.
        secrets_name = f"secrets-{container_name}"
        secrets_path = self.runtime_dir / location_subpath / ExecutorFiles.SECRETS_DIR
        secrets_description = self._generate_secrets(secrets_name, secrets_path)

        efs_claim_name = getattr(settings, "KUBERNETES_SETTINGS", {}).get(
            "efs_claim_name", "efs-resolwe-root"
        )

        # Set resource limits.
        requests = dict()
        limits = data.process.get_resource_limits()

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
        limits["memory"] *= 2 ** 20  # 2 ** 20 = mebibyte
        requests["memory"] *= 2 ** 20

        ebs_local_claim_name = self._ebs_claim_name("local", data.pk)
        ebs_local_claim_size = limits.pop("storage", 200) * (
            2 ** 30
        )  # Default 200 gibibytes

        ebs_input_data_claim_name = self._ebs_claim_name("inputs", data.pk)
        ebs_input_data_claim_size = self._data_inputs_size(data)

        # TODO: no ulimits on kubernetes??
        # See https://github.com/kubernetes/kubernetes/issues/3595
        # Get limit defaults.
        # limit_defaults = SETTINGS.get("FLOW_PROCESS_RESOURCE_DEFAULTS", {})

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

        communicator_image = getattr(
            settings,
            "FLOW_DOCKER_COMMUNICATOR_IMAGE",
            "public.ecr.aws/s4q6j6e8/resolwe/com:latest",
        )

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

        job_description = {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {"name": job_name},
            "spec": {
                # Keep finished pods around for ten seconds. If job is not
                # deleted its PVC claim persists and it causes PV to stay
                # around.
                # This can be changed by running a cron job that periodically
                # checks for PVC that can be deleted.
                "ttlSecondsAfterFinished": 10,
                "template": {
                    "metadata": {
                        "name": job_name,
                        "labels": {
                            "app": "resolwe",
                            "data_id": str(data.pk),
                            "process": self._sanitize_kubernetes_label(
                                data.process.slug
                            ),
                            "image": self._sanitize_kubernetes_label(
                                processing_container_image
                            ),
                        },
                        "annotations": annotations,
                    },
                    "spec": {
                        "hostNetwork": use_host_network,
                        "volumes": self._volumes(
                            secrets_name,
                            configmap_name,
                            efs_claim_name,
                            ebs_local_claim_name,
                            ebs_input_data_claim_name,
                            location_subpath,
                            core_api,
                        ),
                        "initContainers": [
                            {
                                "name": f"{container_name}-init",
                                "image": communicator_image,
                                "imagePullPolicy": "Always",
                                "workingDir": "/",
                                "command": ["/usr/local/bin/python3"],
                                "args": ["-m", "executors.init_container"],
                                "securityContext": {"privileged": True},
                                "volumeMounts": self._init_container_mountpoints(),
                                "env": self._prepare_environment(data),
                            },
                        ],
                        "containers": [
                            {
                                "name": container_name,
                                "image": processing_container_image,
                                "resources": {"limits": limits, "requests": requests},
                                # TODO: uncomment after test
                                "securityContext": security_context,
                                "env": self._prepare_environment(data),
                                # TODO: uncomment after trial!
                                "workingDir": os.fspath(constants.DATA_LOCAL_VOLUME),
                                "imagePullPolicy": "Always",
                                "command": ["/usr/bin/python3"],
                                "args": ["/processing.py"],
                                "volumeMounts": self._processing_mountpoints(
                                    location_subpath
                                ),
                            },
                            {
                                "name": f"{container_name}-communicator",
                                "image": communicator_image,
                                "imagePullPolicy": "Always",
                                "resources": {
                                    "limits": {"cpu": 1, "memory": "1024M"},
                                    "requests": {"memory": "256M", "cpu": 0.1},
                                },
                                # TODO: uncomment after test
                                "securityContext": security_context,
                                "env": self._prepare_environment(data),
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

        core_api.create_namespaced_persistent_volume_claim(
            body=self._persistent_ebs_claim(ebs_local_claim_name, ebs_local_claim_size),
            namespace=self.kubernetes_namespace,
        )
        core_api.create_namespaced_persistent_volume_claim(
            body=self._persistent_ebs_claim(
                ebs_input_data_claim_name, ebs_input_data_claim_size
            ),
            namespace=self.kubernetes_namespace,
        )

        core_api.create_namespaced_config_map(
            body=configmap_description, namespace=self.kubernetes_namespace
        )
        core_api.create_namespaced_secret(
            body=secrets_description, namespace=self.kubernetes_namespace
        )
        batch_api.create_namespaced_job(
            body=job_description, namespace=self.kubernetes_namespace
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

    def _check_first_run(self, location_subpath: Path) -> bool:
        """Check that stdout file does not exist.

        :returns: True if files do not already exists, false otherwise.
        """

        def _create_file(path: Path):
            """Ensure stdout and jsonout files are not already there."""
            return os.open(os.fspath(path), os.O_WRONLY | os.O_CREAT | os.O_EXCL)

        with suppress(FileExistsError):
            os.close(_create_file(self.data_dir / location_subpath / "stdout.txt"))
            os.close(_create_file(self.data_dir / location_subpath / "jsonout.txt"))
            return True
        return False

    def submit(self, data: Data, runtime_dir: str, argv: Any):
        """Run process.

        For details, see
        :meth:`~resolwe.flow.managers.workload_connectors.base.BaseConnector.submit`.
        """
        self._initialize_variables()
        self.start(data)

        logger.debug(
            __(
                "Connector '{}' running for Data with id {} ({}).",
                self.__class__.__module__,
                data.id,
                repr(argv),
            )
        )

    def cleanup(self, data_id: int):
        """Remove the EBS storage used by the executor."""
        kubernetes.config.load_kube_config()
        core_api = kubernetes.client.CoreV1Api()
        claim_names = [
            self._ebs_claim_name(type_, data_id) for type_ in ["local", "inputs"]
        ]
        for ebs_claim_name in claim_names:
            logger.debug("Kubernetes: removing claim %s.", ebs_claim_name)
            with suppress(kubernetes.client.rest.ApiException):
                core_api.delete_namespaced_persistent_volume_claim(
                    name=ebs_claim_name, namespace=self.kubernetes_namespace
                )
