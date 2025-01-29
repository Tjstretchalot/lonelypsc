import time

from lonelypsp.stateful.constants import SubscriberToBroadcasterStatefulMessageType
from lonelypsp.stateful.messages.continue_receive import (
    S2B_ContinueReceive,
    serialize_s2b_continue_receive,
)

from lonelypsc.ws.handlers.open.websocket_url import (
    make_for_send_websocket_url_and_change_counter,
)
from lonelypsc.ws.state import StateOpen, UnsentAckContinueReceive


async def send_continue_receive(
    state: StateOpen, message: UnsentAckContinueReceive
) -> None:
    """Produces the required authorization header and sends the continue receive message"""

    url = make_for_send_websocket_url_and_change_counter(state)
    tracing = b""  # TODO: tracing
    authorization = await state.config.authorize_stateful_continue_receive(
        tracing=tracing,
        identifier=message.identifier,
        part_id=message.part_id,
        url=url,
        now=time.time(),
    )
    serd_message = serialize_s2b_continue_receive(
        S2B_ContinueReceive(
            type=SubscriberToBroadcasterStatefulMessageType.CONTINUE_RECEIVE,
            identifier=message.identifier,
            part_id=message.part_id,
            authorization=authorization,
            tracing=tracing,
        ),
        minimal_headers=state.config.websocket_minimal_headers,
    )
    await state.websocket.send_bytes(serd_message)
