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
            broadcaster["host"] + "/v1/websocket",
            # WARN: do not use ClientWSTimeout ws_receive, which will ignore
            # heartbeats, meaning it will timeout unless there are actual
            # notify/subscribe messages being sent. the heartbeat interval
            # is acting as our receive timeout
            timeout=aiohttp.ClientWSTimeout(
                ws_receive=None, ws_close=config.websocket_close_timeout
            ),
            heartbeat=config.websocket_heartbeat_interval,
        )
    )
