import sys
from typing import Any, Coroutine, cast

import aiohttp

from lonelypsc.config.config import PubSubBroadcasterConfig
from lonelypsc.config.ws_config import WebsocketPubSubConfig
from lonelypsc.util.task import TaskHandle, create_task


def make_websocket_connect_task(
    config: WebsocketPubSubConfig,
    broadcaster: PubSubBroadcasterConfig,
    client_session: aiohttp.ClientSession,
) -> TaskHandle[aiohttp.ClientWebSocketResponse]:
    """Creates the standard task to connect to the given broadcaster within the
    given session, usually for creating a CONNECTING state

    Args:
        config (WebsocketPubSubConfig): how the subscriber is configured
        broadcaster (PubSubBroadcasterConfig): the broadcaster to connect to
        client_session (aiohttp.ClientSession): the session to use for the connection
    """
    websocket_url = broadcaster["host"] + "/v1/websocket"
    websocket_timeout = aiohttp.ClientWSTimeout(
        ws_receive=None, ws_close=config.websocket_close_timeout
    )

    if sys.version_info < (3, 11):
        # aiohttp omits the decode_text=True overload on Python 3.10.
        return create_task(
            cast(
                Coroutine[Any, Any, aiohttp.ClientWebSocketResponse],
                client_session.ws_connect(
                    websocket_url,
                    # WARN: do not use ClientWSTimeout ws_receive, which will ignore
                    # heartbeats, meaning it will timeout unless there are actual
                    # notify/subscribe messages being sent. the heartbeat interval
                    # is acting as our receive timeout
                    timeout=websocket_timeout,
                    heartbeat=config.websocket_heartbeat_interval,
                    decode_text=True,
                ),
            )
        )

    return create_task(
        client_session.ws_connect(
            websocket_url,
            # WARN: do not use ClientWSTimeout ws_receive, which will ignore
            # heartbeats, meaning it will timeout unless there are actual
            # notify/subscribe messages being sent. the heartbeat interval
            # is acting as our receive timeout
            timeout=websocket_timeout,
            heartbeat=config.websocket_heartbeat_interval,
        )
    )
