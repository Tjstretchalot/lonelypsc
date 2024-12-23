import asyncio

import aiohttp

from lonelypsc.config.config import PubSubBroadcasterConfig
from lonelypsc.config.ws_config import WebsocketPubSubConfig


def make_websocket_connect_task(
    config: WebsocketPubSubConfig,
    broadcaster: PubSubBroadcasterConfig,
    client_session: aiohttp.ClientSession,
) -> asyncio.Task[aiohttp.ClientWebSocketResponse]:
    """Creates the standard task to connect to the given broadcaster within the
    given session, usually for creating a CONNECTING state

    Args:
        config (WebsocketPubSubConfig): how the subscriber is configured
        broadcaster (PubSubBroadcasterConfig): the broadcaster to connect to
        client_session (aiohttp.ClientSession): the session to use for the connection
    """
    return asyncio.create_task(
        client_session.ws_connect(
            broadcaster["host"],
            timeout=aiohttp.ClientWSTimeout(
                ws_receive=config.websocket_receive_timeout
            ),
            heartbeat=config.websocket_heartbeat_interval,
        )
    )
