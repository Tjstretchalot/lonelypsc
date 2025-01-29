import time

from lonelypsp.stateful.constants import SubscriberToBroadcasterStatefulMessageType
from lonelypsp.stateful.messages.confirm_receive import (
    S2B_ConfirmReceive,
    serialize_s2b_confirm_receive,
)

from lonelypsc.ws.handlers.open.websocket_url import (
    make_for_send_websocket_url_and_change_counter,
)
from lonelypsc.ws.state import StateOpen, UnsentAckConfirmReceive


async def send_confirm_receive(
    state: StateOpen, message: UnsentAckConfirmReceive
) -> None:
    """Produces the required authorization header and sends the confirm receive message"""

    url = make_for_send_websocket_url_and_change_counter(state)
    tracing = b""  # TODO: tracing
    num_subscribers = 1
    authorization = await state.config.authorize_confirm_receive(
        tracing=tracing,
        identifier=message.identifier,
        num_subscribers=num_subscribers,
        url=url,
        now=time.time(),
    )
    serd_message = serialize_s2b_confirm_receive(
        S2B_ConfirmReceive(
            type=SubscriberToBroadcasterStatefulMessageType.CONFIRM_RECEIVE,
            identifier=message.identifier,
            authorization=authorization,
            tracing=tracing,
            num_subscribers=num_subscribers,
        ),
        minimal_headers=state.config.websocket_minimal_headers,
    )
    await state.websocket.send_bytes(serd_message)
