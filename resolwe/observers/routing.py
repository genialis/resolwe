"""Routing rules for websocket connections."""

from channels.routing import ChannelNameRouter, ProtocolTypeRouter, URLRouter
from django.urls import path

from .consumers import BACKGROUND_TASK_CHANNEL, BackgroundTaskConsumer, ClientConsumer

application = ProtocolTypeRouter(
    {
        # Client-facing WebSocket Consumers.
        "websocket": URLRouter(
            [path("ws/<slug:session_id>", ClientConsumer.as_asgi())]
        ),
        "channel": ChannelNameRouter(
            {
                BACKGROUND_TASK_CHANNEL: BackgroundTaskConsumer.as_asgi(),
            }
        ),
    }
)
