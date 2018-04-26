""".. Ignore pydocstyle D400.

=====================================
Django Channels Routing Configuration
=====================================

"""

from channels.routing import ChannelNameRouter, ProtocolTypeRouter

from resolwe.flow.managers import state
from resolwe.flow.managers.consumer import ManagerConsumer

channel_routing = ProtocolTypeRouter({  # pylint: disable=invalid-name
    'channel': ChannelNameRouter({
        state.MANAGER_CONTROL_CHANNEL: ManagerConsumer,
    }),
})
