""".. Ignore pydocstyle D400.

=====================================
Django Channels Routing Configuration
=====================================

"""

from channels.routing import ChannelNameRouter, ProtocolTypeRouter

from resolwe.flow.managers import state
from resolwe.flow.managers.consumer import (
    CHANNEL_HEALTH_CHECK,
    HealtCheckConsumer,
    ManagerConsumer,
)

channel_routing = ProtocolTypeRouter(
    {
        "channel": ChannelNameRouter(
            {
                state.MANAGER_CONTROL_CHANNEL: ManagerConsumer,
                CHANNEL_HEALTH_CHECK: HealtCheckConsumer.as_asgi(),
            }
        ),
    }
)
