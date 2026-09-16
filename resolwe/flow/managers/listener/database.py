"""Database helpers for the listener."""

import functools
import logging
import random
import time
from contextlib import contextmanager
from typing import Callable, TypeVar

from django.conf import settings
from django.db import DatabaseError, connection, transaction

from resolwe.utils import BraceMessage as __

logger = logging.getLogger(__name__)

# Attempts per write, setting ``LISTENER_DATABASE_WRITE_ATTEMPTS``; 1 disables
# the retries.
DEFAULT_DATABASE_WRITE_ATTEMPTS = 4

# Sleep (in seconds) before the first retry, doubled every time and spread by
# a random factor so the handlers that failed together do not retry together.
INITIAL_RETRY_SLEEP = 1
RETRY_SLEEP_SPREAD = (0.5, 1.5)

# Statement timeout (in seconds) of the repeated writes, setting
# ``LISTENER_DATABASE_WRITE_TIMEOUT``. A falsy value keeps the session timeout.
DEFAULT_DATABASE_WRITE_TIMEOUT = 30

# Errors after which the transaction was rolled back, so the write is safe to
# repeat. Connection errors are excluded: their outcome is unknown.
RETRIABLE_SQLSTATES = frozenset(
    {
        "57014",  # query_canceled: the statement timeout expired.
        "55P03",  # lock_not_available: the lock timeout expired.
        "40001",  # serialization_failure.
        "40P01",  # deadlock_detected.
    }
)

FunctionType = TypeVar("FunctionType", bound=Callable)


def is_retriable_database_error(error: BaseException) -> bool:
    """Tell whether the given database error is safe to retry."""
    # Django wraps the driver error, which carries the code: psycopg 3 in
    # 'sqlstate', psycopg 2 in 'pgcode'.
    cause = getattr(error, "__cause__", None)
    sqlstate = getattr(cause, "sqlstate", None) or getattr(cause, "pgcode", None)
    return sqlstate in RETRIABLE_SQLSTATES


@contextmanager
def write_transaction():
    """Open the outermost transaction with the write timeout applied.

    Nested, the block would be a savepoint and the timeout would outlive it,
    shortening the rest of the outer transaction. A falsy timeout keeps the
    session one.

    :raises RuntimeError: when a transaction is already open.
    """
    if connection.in_atomic_block:
        raise RuntimeError("The write transaction must be the outermost one.")
    timeout = getattr(
        settings, "LISTENER_DATABASE_WRITE_TIMEOUT", DEFAULT_DATABASE_WRITE_TIMEOUT
    )
    with transaction.atomic():
        if timeout and connection.vendor == "postgresql":
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT set_config('statement_timeout', %s, true)",
                    [str(int(timeout * 1000))],
                )
        yield


def retry_database_writes(func: FunctionType) -> FunctionType:
    """Repeat the decorated write when the database aborts it.

    The function must open its own transaction, see :func:`write_transaction`.
    The attempts run in the handler thread, so a repeated write holds its slot
    for at most the attempts times the write timeout, plus the sleeps.

    :raises DatabaseError: the error of the last attempt.
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        attempts = max(
            1,
            getattr(
                settings,
                "LISTENER_DATABASE_WRITE_ATTEMPTS",
                DEFAULT_DATABASE_WRITE_ATTEMPTS,
            ),
        )
        sleep = INITIAL_RETRY_SLEEP
        attempt = 0
        while True:
            attempt += 1
            try:
                return func(*args, **kwargs)
            except DatabaseError as error:
                if attempt >= attempts or not is_retriable_database_error(error):
                    raise
                pause = sleep * random.uniform(*RETRY_SLEEP_SPREAD)
                logger.warning(
                    __(
                        "Database error in '{}' (attempt {} of {}), retrying in {:.1f}s: {}",
                        func.__qualname__,
                        attempt,
                        attempts,
                        pause,
                        error,
                    )
                )
                time.sleep(pause)
                sleep *= 2

    return wrapper
