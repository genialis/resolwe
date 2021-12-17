""".. Ignore pydocstyle D400.

=========================================================================
Collect Processes' tools and store them in configmap object in Kubernetes
=========================================================================

"""
import hashlib
import json
from pathlib import Path
from typing import Any, Dict

import kubernetes

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from resolwe.flow.executors.prepare import BaseFlowExecutorPreparer
from resolwe.flow.managers.workload_connectors.kubernetes import (
    sanitize_kubernetes_label,
)

# Timeout (in seconds) to wait for response from kubernetes API.
KUBERNETES_TIMEOUT = 30
KUBERNETES_NAMESPACE = getattr(settings, "KUBERNETES_SETTINGS", {}).get(
    "namespace", "default"
)


class Command(BaseCommand):
    """Copies tools' files from different locations into Configmap object.

    Multiple configmaps are created and
    """

    help = "Collect tools' files and store them as Configmap in Kubernetes."

    def add_arguments(self, parser):
        """Command arguments."""
        parser.add_argument(
            "--noinput",
            "--no-input",
            action="store_false",
            dest="interactive",
            help="Do NOT prompt the user for input of any kind.",
        )

    def get_confirmation(self):
        """Get user confirmation to proceed."""
        message = (
            "\n"
            "This will RECREATE all configmaps describing tools.\n"
            "Are you sure you want to do this?\n"
            "\n"
            "Type 'yes' to continue, or 'no' to cancel."
        )

        if input("".join(message)) != "yes":
            raise CommandError("Collecting tools cancelled.")

    def update_configmap(self, name: str, content: Dict, core_api: Any):
        """Updata/create configmap."""
        meta = kubernetes.client.V1ObjectMeta(name=name, namespace=KUBERNETES_NAMESPACE)
        configmap = kubernetes.client.V1ConfigMap(
            data=content, metadata=meta, kind="ConfigMap"
        )
        try:
            core_api.patch_namespaced_config_map(
                name=name, namespace=KUBERNETES_NAMESPACE, body=configmap
            )
        except kubernetes.client.rest.ApiException:
            # The configmap was not found, create it.
            core_api.create_namespaced_config_map(
                namespace=KUBERNETES_NAMESPACE, body=configmap
            )

    def update_tools_configmaps(self, core_api):
        """Create or update configmaps for tools."""

        def dict_from_directory(directory: Path) -> Dict[str, str]:
            """Get dictionary from given directory.

            File names are keys and corresponding file contents are values.
            """
            return {
                entry.name: entry.read_text()
                for entry in directory.glob("*")
                if entry.is_file()
            }

        configmaps = dict()
        preparer = BaseFlowExecutorPreparer()
        tools_paths = preparer.get_tools_paths(from_applications=True)
        for tool_path in tools_paths:
            tool_path = Path(tool_path)
            data = dict_from_directory(tool_path)
            data_md5 = hashlib.md5(
                json.dumps(data, sort_keys=True).encode()
            ).hexdigest()
            configmap_name = sanitize_kubernetes_label(
                f"tools-{tool_path.name}-{data_md5}"
            )
            self.update_configmap(configmap_name, data, core_api)
            configmaps[sanitize_kubernetes_label(tool_path.name)] = configmap_name

        description_configmap_name = getattr(
            settings, "KUBERNETES_TOOLS_CONFIGMAPS", "tools-configmaps"
        )
        self.update_configmap(description_configmap_name, configmaps, core_api)

    def handle(self, **options):
        """Collect tools."""
        try:
            kubernetes.config.load_incluster_config()
        except kubernetes.config.config_exception.ConfigException:
            kubernetes.config.load_kube_config()

        core_api = kubernetes.client.CoreV1Api()

        if options["interactive"]:
            self.get_confirmation()

        self.update_tools_configmaps(core_api=core_api)
