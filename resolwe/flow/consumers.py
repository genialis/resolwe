"""Channels consumers for Resolwe Flow."""
import logging

from channels.consumer import SyncConsumer

from resolwe.flow.utils.purge import data_purge
from resolwe.utils import BraceMessage as __

logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


class PurgeConsumer(SyncConsumer):
    """Purge consumer."""

    def purge_run(self, event):
        """Run purge for the object with ``data_id`` specified in ``event`` argument."""
        data_id = event['data_id']
        verbosity = event['verbosity']

        try:
            logger.info(__("Running purge for Data id {}.", data_id))
            data_purge(data_ids=[data_id], verbosity=verbosity, delete=True)
        except Exception:  # pylint: disable=broad-except
            logger.exception("Error while purging data object.", extra={'data_id': data_id})
