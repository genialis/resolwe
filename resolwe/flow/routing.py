""".. Ignore pydocstyle D400.

=====================================
Django Channels Routing Configuration
=====================================

"""

from channels.routing import route_class

from resolwe.flow.managers.consumer import ManagerConsumer

channel_routing = [  # pylint: disable=invalid-name
    route_class(ManagerConsumer),
]
