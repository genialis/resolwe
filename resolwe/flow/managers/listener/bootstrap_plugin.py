"""Bootstrap the containers."""

import copy
import json
import logging
import os
import shlex
import threading
from collections import defaultdict
from importlib import import_module
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Tuple, Union

from django.conf import settings
from django.core.exceptions import PermissionDenied
from django.forms.models import model_to_dict

from resolwe.flow.engine import BaseEngine, InvalidEngineError, load_engines
from resolwe.flow.executors import constants
from resolwe.flow.executors.socket_utils import Message, Response
from resolwe.flow.managers.protocol import ExecutorFiles
from resolwe.flow.models import Data, Process, Worker
from resolwe.storage import settings as storage_settings
from resolwe.test.utils import is_testing
from resolwe.utils import BraceMessage as __

from .plugin import ListenerPlugin, listener_plugin_manager

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from resolwe.flow.managers.listener.listener import Processor


class BootstrapCommands(ListenerPlugin):
    """Basic listener handlers."""

    plugin_manager = listener_plugin_manager

    def __init__(self) -> None:
        """Initialize."""
        super().__init__()
        self._executor_preparer = self._load_executor_preparer()
        self._get_program_lock = threading.Lock()
        self._bootstrap_cache: Dict[str, Any] = defaultdict(dict)
        self._execution_engines = self._load_execution_engines()
        logger.info(
            __(
                "Found {} execution engines: {}",
                len(self._execution_engines),
                ", ".join(self._execution_engines.keys()),
            )
        )
        self._expression_engines = self._load_expression_engines()
        logger.info(
            __(
                "Found {} expression engines: {}",
                len(self._expression_engines),
                ", ".join(self._expression_engines.keys()),
            )
        )

    def _bootstrap_prepare_static_cache(self):
        """Prepare cache for bootstrap."""

        def marshal_settings() -> dict:
            """Marshal Django settings into a serializable object.

            :return: The serialized settings.
            :rtype: dict
            """
            result = {}
            for key in dir(settings):
                if any(
                    map(
                        key.startswith,
                        [
                            "FLOW_",
                            "RESOLWE_",
                            "CELERY_",
                            "KUBERNETES_",
                        ],
                    )
                ):
                    result[key] = getattr(settings, key)
            result.update(
                {
                    "USE_TZ": settings.USE_TZ,
                    "FLOW_EXECUTOR_TOOLS_PATHS": self._executor_preparer.get_tools_paths(),
                    "FLOW_STORAGE": storage_settings.FLOW_STORAGE,
                }
            )
            # TODO: this is q&d solution for serializing Path objects.
            return json.loads(json.dumps(result, default=str))

        # Prepare Django settings.
        if "settings" not in self._bootstrap_cache:
            logger.debug("Preparing settings static cache.")
            self._bootstrap_cache["settings"] = marshal_settings()
            logger.debug("Settings static cache marshalled.")
            connectors_settings = copy.deepcopy(storage_settings.STORAGE_CONNECTORS)
            for connector_settings in connectors_settings.values():
                # Fix class name for inclusion in the executor.
                klass = connector_settings["connector"]
                klass = "executors." + klass.rsplit(".storage.")[-1]
                connector_settings["connector"] = klass
                connector_config = connector_settings["config"]
                # Prepare credentials for executor.
                if "credentials" in connector_config:
                    src_credentials = connector_config["credentials"]
                    base_credentials_name = os.path.basename(src_credentials)

                    self._bootstrap_cache["connector_secrets"][
                        base_credentials_name
                    ] = ""
                    if os.path.isfile(src_credentials):
                        with open(src_credentials, "r") as f:
                            self._bootstrap_cache["connector_secrets"][
                                base_credentials_name
                            ] = f.read()
                    connector_config["credentials"] = os.fspath(
                        constants.SECRETS_VOLUME / base_credentials_name
                    )
            logger.debug("Connector settings prepared.")
            self._bootstrap_cache["settings"][
                "STORAGE_CONNECTORS"
            ] = connectors_settings
            self._bootstrap_cache["settings"][
                "FLOW_VOLUMES"
            ] = storage_settings.FLOW_VOLUMES

            # Prepare process meta data.
            self._bootstrap_cache["process_meta"] = {
                k: getattr(Process, k)
                for k in dir(Process)
                if k.startswith("SCHEDULING_CLASS_")
                and isinstance(getattr(Process, k), str)
            }
            logger.debug("Process settings prepared.")
            self._bootstrap_cache["process"] = dict()

    def bootstrap_prepare_process_cache(self, data: Data):
        """Prepare cache for process with the given id."""
        if data.process_id not in self._bootstrap_cache["process"]:
            self._bootstrap_cache["process"][data.process_id] = model_to_dict(
                data.process
            )
            self._bootstrap_cache["process"][data.process_id][
                "resource_limits"
            ] = data.process.get_resource_limits()

    def handle_init_completed(
        self, data_id: int, message: Message[str], manager: "Processor"
    ) -> Response[str]:
        """Handle init completed request."""
        manager._update_worker(
            data_id, changes={"status": Worker.STATUS_FINISHED_PREPARING}
        )
        return message.respond_ok("")

    def handle_bootstrap(
        self, data_id: int, message: Message[Tuple[int, str]], manager: "Processor"
    ) -> Response[Dict]:
        """Handle bootstrap request.

        :raises RuntimeError: when settings name is not known.
        """
        data_id, settings_name = message.message_data
        logger.debug(
            __("Bootstraping peer for id {} for settings {}.", data_id, settings_name)
        )
        data = Data.objects.get(pk=data_id)
        logger.debug(__("Read data for peer with id {}.", data_id))

        if is_testing():
            self._bootstrap_cache = defaultdict(dict)
        self._bootstrap_prepare_static_cache()
        logger.debug(__("Prepared static cache for peer {}.", data_id))

        response: Dict[str, Any] = dict()
        self.bootstrap_prepare_process_cache(data)
        logger.debug(__("Prepared process cache for peer {}.", data_id))

        if settings_name == "executor":
            response[ExecutorFiles.EXECUTOR_SETTINGS] = {
                "DATA_DIR": data.location.get_path()
            }
            response[ExecutorFiles.LOCATION_SUBPATH] = data.location.subpath
            response[ExecutorFiles.DJANGO_SETTINGS] = self._bootstrap_cache[
                "settings"
            ].copy()
            response[ExecutorFiles.PROCESS_META] = self._bootstrap_cache["process_meta"]
            response[ExecutorFiles.PROCESS] = self._bootstrap_cache["process"][
                data.process.id
            ]

        elif settings_name == "init":
            response[ExecutorFiles.DJANGO_SETTINGS] = {
                "STORAGE_CONNECTORS": self._bootstrap_cache["settings"][
                    "STORAGE_CONNECTORS"
                ],
                "FLOW_STORAGE": storage_settings.FLOW_STORAGE,
                "FLOW_VOLUMES": storage_settings.FLOW_VOLUMES,
            }

            if hasattr(settings, constants.INPUTS_VOLUME_NAME):
                response[ExecutorFiles.DJANGO_SETTINGS][
                    constants.INPUTS_VOLUME_NAME
                ] = self._bootstrap_cache["settings"][constants.INPUTS_VOLUME_NAME]
            response[ExecutorFiles.SECRETS_DIR] = self._bootstrap_cache[
                "connector_secrets"
            ]
            try:
                response[ExecutorFiles.SECRETS_DIR].update(data.resolve_secrets())
            except PermissionDenied as e:
                data.process_error.append(str(e))
                data.save(update_fields=["process_error"])
                raise
            response[ExecutorFiles.LOCATION_SUBPATH] = data.location.subpath
        elif settings_name == "communication":
            response[ExecutorFiles.DJANGO_SETTINGS] = {
                "STORAGE_CONNECTORS": self._bootstrap_cache["settings"][
                    "STORAGE_CONNECTORS"
                ],
                "FLOW_STORAGE": storage_settings.FLOW_STORAGE,
            }
            response[ExecutorFiles.LOCATION_SUBPATH] = data.location.subpath
        else:
            raise RuntimeError(
                f"Settings {settings_name} sent by peer with id {data_id} unknown."
            )
        return message.respond_ok(response)

    def handle_get_script(
        self, data_id: int, message: Message[str], manager: "Processor"
    ) -> Response[str]:
        """Return script for the current Data object."""
        return message.respond_ok(self.get_program(data_id))

    def handle_log(
        self,
        data_id: int,
        message: Message[Union[str, bytes, bytearray]],
        manager: "Processor",
    ) -> Response[str]:
        """Handle an incoming log processing request.

        :param obj: The Channels message object. Command object format:

            .. code-block:: none

                {
                    'command': 'log',
                    'message': [log message]
                }
        """
        record_dict = json.loads(message.message_data)
        record_dict["args"] = tuple(record_dict["args"])

        executors_dir = Path(__file__).parents[1] / "executors"
        record_dict["pathname"] = os.fspath(executors_dir / record_dict["pathname"])
        logger.handle(logging.makeLogRecord(record_dict))
        return message.respond_ok("OK")

    def get_program(self, data_id: int) -> str:
        """Get a program for given data object."""
        # When multiple get_script commands run in parallel the string
        # escaping will be fragile at best. Process this command here
        # without offloading it to the listener.
        data = Data.objects.get(pk=data_id)

        with self._get_program_lock:
            execution_engine_name = data.process.run.get("language", None)
            program = [self._get_execution_engine(execution_engine_name).evaluate(data)]
            # TODO: should executor be changed? Definitely for tests (from case to case).
            # Should I use file prepared by the Dispatcher (that takes care of that)?
            env_vars = self.get_executor().get_environment_variables()
            settings_env_vars = {
                "RESOLWE_HOST_URL": getattr(settings, "RESOLWE_HOST_URL", "localhost"),
            }
            additional_env_vars = getattr(settings, "FLOW_EXECUTOR", {}).get(
                "SET_ENV", {}
            )
            env_vars.update(settings_env_vars)
            env_vars.update(additional_env_vars)
            export_commands = [
                "export {}={}".format(key, shlex.quote(value))
                for key, value in env_vars.items()
            ]

            # Add tools to the path. The tools paths environment variable must
            # be prepared by the executor.
            export_commands += ["export PATH=$PATH:$TOOLS_PATHS"]

            # Disable brace expansion and set echo.
            echo_commands = ["set -x +B"]

            commands = echo_commands + export_commands + program
            return os.linesep.join(commands)

    def _get_execution_engine(self, name):
        """Return an execution engine instance."""
        try:
            return self._execution_engines[name]
        except KeyError:
            raise InvalidEngineError("Unsupported execution engine: {}".format(name))

    def _load_execution_engines(self) -> Dict[str, BaseEngine]:
        """Load execution engines."""
        execution_engines = getattr(
            settings, "FLOW_EXECUTION_ENGINES", ["resolwe.flow.execution_engines.bash"]
        )
        return load_engines(
            self, "ExecutionEngine", "execution_engines", execution_engines
        )

    def _load_expression_engines(self) -> Dict[str, BaseEngine]:
        """Load expression engines."""
        expression_engines = getattr(
            settings,
            "FLOW_EXPRESSION_ENGINES",
            ["resolwe.flow.expression_engines.jinja"],
        )
        return load_engines(
            self, "ExpressionEngine", "expression_engines", expression_engines
        )

    def _load_executor_preparer(self) -> Any:
        """Load and return the executor preparer class."""
        executor_name = (
            getattr(settings, "FLOW_EXECUTOR", {}).get(
                "NAME", "resolwe.flow.executors.docker"
            )
            + ".prepare"
        )
        return import_module(executor_name).FlowExecutorPreparer()

    def get_executor(self) -> Any:
        """Get the executor preparer class."""
        return self._executor_preparer

    def get_expression_engine(self, name: str):
        """Return an expression engine instance."""
        try:
            return self._expression_engines[name]
        except KeyError:
            raise InvalidEngineError("Unsupported expression engine: {}".format(name))
