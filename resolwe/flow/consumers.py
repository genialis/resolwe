"""Channels consumers for Resolwe Flow."""
import logging

from channels.consumer import SyncConsumer

from resolwe.flow.utils.purge import location_purge
from resolwe.utils import BraceMessage as __

logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


class PurgeConsumer(SyncConsumer):
    """Purge consumer."""

    def purge_run(self, event):
        """Run purge for the object with ``location_id`` specified in ``event`` argument."""
        location_id = event['location_id']
        verbosity = event['verbosity']

        try:
            logger.info(__("Running purge for location id {}.", location_id))
            location_purge(location_id=location_id, delete=True, verbosity=verbosity)
        except Exception:  # pylint: disable=broad-except
            logger.exception("Error while purging location.", extra={'location_id': location_id})
