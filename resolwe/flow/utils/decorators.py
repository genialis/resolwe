""".. Ignore pydocstyle D400.

==================
Resolwe Decorators
==================

Utils functions.

"""

import logging
import time
from typing import Callable, Optional, Sequence, Type

import wrapt


def retry(
    logger: Optional[logging.Logger],
    max_retries: int = 3,
    retry_exceptions: Sequence[Type[Exception]] = (Exception,),
    min_sleep: int = 1,
    max_sleep: int = 10,
    cleanup_callback: Optional[Callable] = None,
):
    """Retry decorator.

    A method decorated with it will be called up to ``max_retries`` times with
    an exponential timeout ranging from ``min_sleep`` to ``max_sleep`` between
    retries. The method will be retried if it raises one of the exception in
    the ``retry_exceptions`` sequence.

    :attr cleanup_callback: the callback to call after exception and the next
        retry of the decorated function. It is given the same arguments as the
        decorated function. Any exception raised by the cleanup_handler is
        logged but not raised.

    :raises Exception: when all retries raise exception the last one is
        re-raised.
    """

    @wrapt.decorator
    def wrapper(wrapped, instance, args, kwargs):
        """Retry on tranfser error."""
        for retry in range(1, max_retries + 1):
            try:
                return wrapped(*args, **kwargs)
            except retry_exceptions as exception:
                # Log the exception on retry for inspection.
                if retry < max_retries:
                    sleep = min(max_sleep, min_sleep * (2 ** (retry - 1)))
                    if logger is not None:
                        logger.exception(
                            "Retry %d/%d got exception, will retry in %.2f seconds.",
                            retry,
                            max_retries,
                            sleep,
                        )
                    if cleanup_callback is not None:
                        try:
                            cleanup_callback(*args, **kwargs)
                        except Exception:
                            logger.exception("Exception in callback handler.")
                    time.sleep(sleep)
                # Raise exception if all retries have failed.
                else:
                    if logger is not None:
                        logger.exception(
                            "Retry %d/%d got exception, re-raising it.",
                            retry,
                            max_retries,
                        )
                    raise exception

    return wrapper
