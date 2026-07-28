"""Listener authenticator."""

import logging
from collections import defaultdict
from typing import Optional

from channels.db import database_sync_to_async
from zmq.auth.asyncio import AsyncioAuthenticator

from resolwe.flow.models import Worker
from resolwe.flow.utils import Singleton

logger = logging.getLogger(__name__)


class ZMQAuthenticator(AsyncioAuthenticator, Singleton):
    """The singleton AsyncioAuthenticator authenticator.

    It is also used for handling curve callbacks.
    """

    def __init__(self, *args, **kwargs):
        """Initialize the mapping between key and the data objects.

        The key is the public key of the worker, and the value is the list of data ids
        the worker is allowed to access.
        """
        super().__init__(*args, **kwargs)
        self._authorizations: dict[bytes, set[int]] = defaultdict(set)
        self.configure_curve_callback(domain="*", credentials_provider=self)

    async def callback(self, domain: str, worker_public_key: bytes) -> bool:
        """Authenticate the client.

        Every client is accepted. On first connection, its key is added to the
        authorization dictionary.
        """

        def get_data_ids(worker_public_key: bytes) -> set[int]:
            """Get the data ids the worker is allowed to access."""
            acceptable_worker_statuses = [
                Worker.STATUS_PREPARING,
                Worker.STATUS_PROCESSING,
                Worker.STATUS_FINISHED_PREPARING,
            ]
            results = Worker.objects.filter(public_key=worker_public_key).values_list(
                "status", "data_id"
            )
            return {
                data_id
                for status, data_id in results
                if status in acceptable_worker_statuses
            }

        try:
            assert domain == "*", "Only domain '*' is supported."

            data_ids = await database_sync_to_async(
                get_data_ids, thread_sensitive=False
            )(worker_public_key)
            self._authorizations[worker_public_key] = data_ids

        except Exception:
            logger.exception("Exception in curve auth callback.")

        return True

    def can_access_data(self, worker_public_key: bytes, data_id: int) -> bool:
        """Can worker access the given data object."""
        return data_id in self._authorizations.get(worker_public_key, [])

    def authorization_keys(self) -> set[bytes]:
        """Return the worker public keys currently authorized."""
        return set(self._authorizations.keys())

    def prune_authorizations(
        self,
        active_data_ids: set[int],
        candidate_keys: Optional[set[bytes]] = None,
    ):
        """Remove authorizations of workers without active data objects.

        The method is called periodically by the listener with the data ids
        of the workers in non-final statuses. Without the pruning the
        authorization dictionary grows by one entry for every worker ever
        connected for the process lifetime.

        Only the entries under ``candidate_keys`` (all entries when not
        given) are considered for removal: an entry added while the caller
        was reading the active data ids from the database may belong to a
        worker created after the read, so it must survive until the next
        pruning cycle.
        """
        candidates = (
            self._authorizations.keys() if candidate_keys is None else candidate_keys
        )
        stale_keys = [
            worker_public_key
            for worker_public_key in candidates
            if not (
                self._authorizations.get(worker_public_key, set()) & active_data_ids
            )
        ]
        for worker_public_key in stale_keys:
            self._authorizations.pop(worker_public_key, None)

    def clear_authorizations(self):
        """Clear the authorization dictionary."""
        self._authorizations.clear()

    def authorize_client(self, worker_public_key: bytes, data_id: int):
        """Authorize the client to access the data object."""
        self._authorizations[worker_public_key].add(data_id)
