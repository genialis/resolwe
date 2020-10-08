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
from pathlib import Path
from typing import Any, Dict, List

import kubernetes

from django.conf import settings

from resolwe.flow.executors import constants
from resolwe.flow.executors.prepare import BaseFlowExecutorPreparer
from resolwe.flow.executors.protocol import ExecutorFiles
from resolwe.flow.models import Data
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
            "SECRETS_DIR": os.fspath(constants.SECRETS_VOLUME),
            "TMP_DIR": os.fspath(constants.TMPDIR),
            "FLOW_MANAGER_KEEP_DATA": getattr(
                settings, "FLOW_MANAGER_KEEP_DATA", False
            ),
            "RUNNING_IN_CONTAINER": 1,
            "RUNNING_IN_KUBERNETES": 1,
        }

        return [
            {"name": name, "value": str(value)} for name, value in environment.items()
        ]

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

    def _ebs_claim_name(self, data_id: int) -> str:
        """Get EBS claim name."""
        return f"pvc-{data_id}"

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
            core_api.create_namespaced_config_map(
                body=configmap_description, namespace=self.kubernetes_namespace
            )

        return configmap_name

    def _volumes(
        self,
        secrets_name: str,
        configmap_name: str,
        efs_claim_name: str,
        ebs_claim_name: str,
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
                "persistentVolumeClaim": {"claimName": ebs_claim_name},
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
            # Secrets are purged by the communicaiton container so they must
            # not be mounted read only.
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

    def _fix_permissions_mountpoints(self):
        """Prepare mountpoints for fix permissions init container."""
        return [
            {
                "name": "ebs-root",
                "mountPath": os.fspath(constants.DATA_LOCAL_VOLUME),
                "readOnly": False,
            },
        ]

    def _processing_mountpoints(self, location_subpath: Path):
        """Mountpoints for processing container."""
        mount_points = [
            {
                "name": "efs-root",
                "mountPath": os.fspath(constants.DATA_ALL_VOLUME),
                "subPath": os.fspath(self.efs_data_dir),
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
            # {
            #     "name": "fsx-root",
            #     "mountPath": constants.DATA_VOLUME,
            #     "subPath": os.fspath(location_subpath),
            #     "readOnly": False,
            # },
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

    def start(self, data: Data):
        """Start process execution.

        Construct kubernetes job description and pass it to the kubernetes.
        """
        location_subpath = Path(data.location.subpath)

        if not self._check_first_run(location_subpath):
            logger.error("Stdout or jsonout file already exists, aborting.")
            return

        # Make sure that tmp dir exists.
        os.makedirs(
            self.data_dir / location_subpath / constants.TMPDIR,
            mode=0o755,
            exist_ok=True,
        )
        # Make sure that sockets dir exists.
        os.makedirs(
            self.runtime_dir / location_subpath / ExecutorFiles.SOCKETS_SUBDIR,
            mode=0o755,
            exist_ok=True,
        )

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

        requests["memory"] = limits["memory"] * (10 ** 6)
        limits["memory"] += KUBERNETES_MEMORY_HARD_LIMIT_BUFFER
        limits["memory"] *= 10 ** 6

        ebs_claim_name = self._ebs_claim_name(data.id)
        ebs_claim_size = limits.get(
            "storage", 100 * ((2 ** 10) ** 3)
        )  # Default 100Gi bytes

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
            annotations[
                "seccomp.security.alpha.kubernetes.io/pod"
            ] = "localhost/profiles/seccomp.json"

        communicator_image = getattr(
            settings, "DOCKER_COMMUNICATOR_IMAGE", "resolwe/com:python-3.8"
        )

        requirements = data.process.requirements.get("executor", {}).get("docker", {})

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
                            "data_id": str(data.id),
                            "process": re.sub(
                                "[^0-9a-zA-Z._\-]+", "_", data.process.slug
                            ),
                            "image": re.sub(
                                "[^0-9a-zA-Z._\-]+",
                                "_",
                                requirements.get(
                                    "image", constants.DEFAULT_CONTAINER_IMAGE
                                ),
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
                            ebs_claim_name,
                            location_subpath,
                            core_api,
                        ),
                        "initContainers": [
                            {
                                "name": f"{container_name}-fix-permissions",
                                "image": "busybox",
                                "imagePullPolicy": "IfNotPresent",
                                "command": self._fix_permissions(),
                                "securityContext": {"privileged": True},
                                "volumeMounts": self._fix_permissions_mountpoints(),
                                "env": self._prepare_environment(data),
                            },
                        ],
                        "containers": [
                            {
                                "name": container_name,
                                "image": requirements.get(
                                    "image", constants.DEFAULT_CONTAINER_IMAGE
                                ),
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
                                    "limits": {"cpu": 0.5, "memory": "1024M"},
                                    "requests": {"memory": "256M"},
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
            body=self._persistent_ebs_claim(ebs_claim_name, ebs_claim_size),
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
        ebs_claim_name = self._ebs_claim_name(data_id)
        logger.debug("Kubernetes: removing claim %s.", ebs_claim_name)
        core_api.delete_namespaced_persistent_volume_claim(
            name=ebs_claim_name, namespace=self.kubernetes_namespace
        )
