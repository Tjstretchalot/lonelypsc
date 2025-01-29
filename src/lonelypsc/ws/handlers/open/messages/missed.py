import asyncio
import time
from typing import TYPE_CHECKING

from lonelypsp.stateful.messages.missed import B2S_Missed

from lonelypsc.ws.handlers.open.messages.protocol import MessageChecker
from lonelypsc.ws.handlers.open.websocket_url import (
    make_for_receive_websocket_url_and_change_counter,
)
from lonelypsc.ws.state import (
    ReceivingAuthorizingMissed,
    ReceivingState,
    StateOpen,
)


def check_missed(state: StateOpen, message: B2S_Missed) -> None:
    """Handles the subscriber receiving the a message from the broadcaster that
    the broadcaster may not have sent a message that the subscriber should have
    received
    """
    assert state.receiving is None, "invariant violated: check_missed while receiving"
    state.receiving = ReceivingAuthorizingMissed(
        type=ReceivingState.AUTHORIZING_MISSED,
        message=message,
        authorization_task=asyncio.create_task(
            state.config.is_missed_allowed(
                tracing=message.tracing,
                recovery=make_for_receive_websocket_url_and_change_counter(state),
                topic=message.topic,
                now=time.time(),
                authorization=message.authorization,
            )
        ),
    )


if TYPE_CHECKING:
    _: MessageChecker[B2S_Missed] = check_missed
