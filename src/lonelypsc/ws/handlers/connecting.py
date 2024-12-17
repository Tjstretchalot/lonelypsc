import asyncio
import secrets
from typing import TYPE_CHECKING

import aiohttp
from lonelypsp.stateful.constants import SubscriberToBroadcasterStatefulMessageType
from lonelypsp.stateful.messages.configure import S2B_Configure, serialize_s2b_configure

from lonelypsc.ws.handle_connection_failure import handle_connection_failure
from lonelypsc.ws.handlers.protocol import StateHandler
from lonelypsc.ws.state import State, StateConfiguring, StateType
from lonelypsc.ws.util import make_websocket_read_task


async def handle_connecting(state: State) -> State:
    """Tries to connect to the given broadcaster; if unsuccessful,
    moves to either CONNECTING (with the next broadcaster),
    WAITING_RETRY, or CLOSED. If successful, moves to CONFIGURING
    """
    assert state.type == StateType.CONNECTING

    session = aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(
            connect=state.config.websocket_open_timeout,
        )
    )
    try:
        websocket = await session.ws_connect(
            state.broadcaster["host"],
            timeout=aiohttp.ClientWSTimeout(
                ws_receive=state.config.websocket_receive_timeout
            ),
            heartbeat=state.config.websocket_heartbeat_interval,
        )
        subscriber_nonce = secrets.token_bytes(32)
        return StateConfiguring(
            type=StateType.CONFIGURING,
            client_session=session,
            config=state.config,
            broadcaster=state.broadcaster,
            websocket=websocket,
            retry=state.retry,
            tasks=state.tasks,
            subscriber_nonce=subscriber_nonce,
            send_task=asyncio.create_task(
                websocket.send_bytes(
                    serialize_s2b_configure(
                        S2B_Configure(
                            type=SubscriberToBroadcasterStatefulMessageType.CONFIGURE,
                            subscriber_nonce=subscriber_nonce,
                            enable_zstd=state.config.allow_compression,
                            enable_training=state.config.allow_training_compression,
                            initial_dict=state.config.initial_compression_dict_id or 0,
                        ),
                        minimal_headers=state.config.websocket_minimal_headers,
                    )
                )
            ),
            read_task=make_websocket_read_task(websocket),
        )
    except Exception as e:
        await session.close()
        return await handle_connection_failure(
            config=state.config, retry=state.retry, tasks=state.tasks, exception=e
        )
    except BaseException:
        await session.close()
        raise


if TYPE_CHECKING:
    _: StateHandler = handle_connecting
