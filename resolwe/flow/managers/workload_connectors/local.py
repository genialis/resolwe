""".. Ignore pydocstyle D400.

===============
Local Connector
===============

"""
import logging
import subprocess

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
        runtime_dir = storage_settings.FLOW_VOLUMES["runtime"]["config"]["path"]
        subprocess.Popen(argv, cwd=runtime_dir, stdin=subprocess.DEVNULL).wait()

    def cleanup(self, data_id: int):
        """Cleanup."""
