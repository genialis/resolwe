""".. Ignore pydocstyle D400.

=====================================
Django Channels Routing Configuration
=====================================

"""

from channels.routing import route_class

from resolwe.flow.managers import manager

# All routing information has to be set and finished at the point when the 'route_class'
# instance below is created, as it will scan the handler class' 'method_mapping' during init
# and register itself. This means the manager's 'update_routing' method needs to be called
# before 'channel_routing' is created. It does need to be called, because the manager is
# created too soon for settings overrides (such as for testing channels) to take effect.
# Make sure this file is not imported too early (best: just leave it to Channels, the framework
# imports this at 'runworkers' time); the import prerequisites are: loaded and patched settings,
# and a loaded manager.

manager.update_routing()
channel_routing = [  # pylint: disable=invalid-name
    route_class(type(manager)),
]
