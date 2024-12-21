import asyncio
from typing import TYPE_CHECKING

from lonelypsp.stateful.messages.confirm_notify import B2S_ConfirmNotify

from lonelypsc.client import PubSubError, PubSubIrrecoverableError
from lonelypsc.ws.handlers.open.messages.protocol import MessageChecker
from lonelypsc.ws.state import (
    InternalMessageStateSent,
    InternalMessageStateType,
    StateOpen,
)


def check_confirm_notify(state: StateOpen, message: B2S_ConfirmNotify) -> None:
    """Handles the subscriber receiving the a message from the broadcaster that
    it has successfully processed a message the subscriber previously sent with
    NOTIFY or NOTIFY_STREAM, raising an error if this isn't the acknowledgement
    the subscriber was expecting.
    """
    try:
        expected = state.expected_acks.popleft()
    except IndexError:
        raise PubSubError(f"{message.type} received when not expecting any acks")

    if expected.type != message.type:
        raise PubSubError(f"expected {expected.type}, got {message.type}")
    if expected.identifier != message.identifier:
        raise PubSubError(
            f"expected {message.type} {expected.identifier!r}, got {message.identifier!r}"
        )

    try:
        internal_message = state.sent_notifications.popleft()
    except IndexError:
        raise PubSubIrrecoverableError(
            f"invariant violated: expected {expected.type}, but no corresponding internal message"
        )

    if internal_message.identifier != message.identifier:
        raise PubSubIrrecoverableError(
            f"invariant violated: expected {expected.type} {message.identifier!r}, got {internal_message.identifier!r}"
        )

    state.backgrounded.add(
        asyncio.create_task(
            internal_message.callback(
                InternalMessageStateSent(type=InternalMessageStateType.SENT)
            )
        )
    )


if TYPE_CHECKING:
    _: MessageChecker[B2S_ConfirmNotify] = check_confirm_notify
