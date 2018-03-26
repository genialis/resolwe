"""Utility functions for communicating with the manager."""
# pylint: disable=logging-format-interpolation
import json
import logging
import traceback

# NOTE: If the imports here are changed, the executors' requirements.txt
# file must also be updated accordingly.
import redis

from .global_settings import DATA, EXECUTOR_SETTINGS, SETTINGS
from .protocol import ExecutorProtocol

logger = logging.getLogger(__name__)  # pylint: disable=invalid-name

_REDIS_RETRIES = 60
# This channel name will be used for all listener communication; Data object-specific.
_response_channel = EXECUTOR_SETTINGS.get('REDIS_CHANNEL_PAIR', ('', ''))[1]  # pylint: disable=invalid-name
QUEUE_RESPONSE_CHANNEL = '{}.{}'.format(_response_channel, DATA.get('id', 0))

# The Redis connection instance used to communicate with the manager listener.
redis_conn = redis.StrictRedis(  # pylint: disable=invalid-name
    **SETTINGS.get('FLOW_EXECUTOR', {}).get('REDIS_CONNECTION', {})
)


def send_manager_command(cmd, expect_reply=True, extra_fields={}):
    """Send a properly formatted command to the manager.

    :param cmd: The command to send (:class:`str`).
    :param expect_reply: If ``True``, wait for the manager to reply
        with an acknowledgement packet.
    :param extra_fields: A dictionary of extra information that's
        merged into the packet body (i.e. not under an extra key).
    """
    packet = {
        ExecutorProtocol.DATA_ID: DATA['id'],
        ExecutorProtocol.COMMAND: cmd,
    }
    packet.update(extra_fields)

    # TODO what happens here if the push fails? we don't have any realistic recourse,
    # so just let it explode and stop processing
    queue_channel = EXECUTOR_SETTINGS['REDIS_CHANNEL_PAIR'][0]
    try:
        redis_conn.rpush(queue_channel, json.dumps(packet))
    except Exception:  # pylint: disable=broad-except
        logger.error("Error sending command to manager:\n\n{}".format(traceback.format_exc()))
        raise

    if not expect_reply:
        return

    for _ in range(_REDIS_RETRIES):
        response = redis_conn.blpop(QUEUE_RESPONSE_CHANNEL, timeout=1)
        if response:
            break
    else:
        # NOTE: If there's still no response after a few seconds, the system is broken
        # enough that it makes sense to give up; we're isolated here, so if the manager
        # doesn't respond, we can't really do much more than just crash
        raise RuntimeError("No response from the manager after {} retries.".format(_REDIS_RETRIES))

    _, item = response
    result = json.loads(item.decode('utf-8'))[ExecutorProtocol.RESULT]
    assert result in [ExecutorProtocol.RESULT_OK, ExecutorProtocol.RESULT_ERROR]

    if result == ExecutorProtocol.RESULT_OK:
        return True

    return False
