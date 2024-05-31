""".. Ignore pydocstyle D400.

===============
Local Connector
===============

"""

import logging
import os
import subprocess

from resolwe.flow.managers.listener.listener import LISTENER_PUBLIC_KEY
from resolwe.flow.models import Data
from resolwe.storage import settings as storage_settings
from resolwe.utils import BraceMessage as __

from .base import BaseConnector

logger = logging.getLogger(__name__)


class Connector(BaseConnector):
    """Local connector for job execution."""

    def submit(self, data: Data, argv):
        """Run process locally.

        For details, see
        :meth:`~resolwe.flow.managers.workload_connectors.base.BaseConnector.submit`.
        """
        logger.debug(
            __(
                "Connector '{}' running for Data with id {} ({}).",
                self.__class__.__module__,
                data.id,
                repr(argv),
            )
        )
        process_environment = os.environ.copy()
        process_environment["LISTENER_PUBLIC_KEY"] = LISTENER_PUBLIC_KEY
        process_environment["CURVE_PRIVATE_KEY"] = data.worker.private_key
        process_environment["CURVE_PUBLIC_KEY"] = data.worker.public_key
        runtime_dir = storage_settings.FLOW_VOLUMES["runtime"]["config"]["path"]
        subprocess.Popen(
            argv, env=process_environment, cwd=runtime_dir, stdin=subprocess.DEVNULL
        ).wait()
