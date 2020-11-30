""".. Ignore pydocstyle D400.

=====
State
=====

Constants used by the dispatcher.
.. autofunction:: resolwe.flow.managers.state.update_constants
"""

# This module should not import anything local, or there will be circular
# dependencies, since the constants are needed in various sub-modules inside
# resolwe.flow.managers.

from collections import namedtuple

from django.conf import settings

ManagerChannelPair = namedtuple("ManagerChannelPair", ["queue", "queue_response"])

MANAGER_CONTROL_CHANNEL = "DUMMY.control"
MANAGER_EXECUTOR_CHANNELS = ManagerChannelPair("DUMMY.queue", "DUMMY.queue_response")


def update_constants():
    """Recreate channel name constants with changed settings.

    This kludge is mostly needed due to the way Django settings are
    patched for testing and how modules need to be imported throughout
    the project. On import time, settings are not patched yet, but some
    of the code needs static values immediately. Updating functions such
    as this one are then needed to fix dummy values.
    """
    global MANAGER_CONTROL_CHANNEL, MANAGER_EXECUTOR_CHANNELS
    redis_prefix = getattr(settings, "FLOW_MANAGER", {}).get("REDIS_PREFIX", "")

    MANAGER_CONTROL_CHANNEL = "{}.control".format(redis_prefix)
    MANAGER_EXECUTOR_CHANNELS = ManagerChannelPair(
        "{}.result_queue".format(redis_prefix),
        "{}.result_queue_response".format(redis_prefix),
    )


update_constants()
