from typing import TYPE_CHECKING

from lonelypsp.stateful.messages.confirm_unsubscribe import (
    B2S_ConfirmUnsubscribeExact,
)

from lonelypsc.client import PubSubError
from lonelypsc.ws.handlers.open.messages.protocol import MessageChecker
from lonelypsc.ws.state import StateOpen


def check_confirm_unsubscribe_exact(
    state: StateOpen, message: B2S_ConfirmUnsubscribeExact
) -> None:
    """Handles the subscriber receiving a message from the broadcaster that it has
    successfully subscribed to a specific topic, raising an error if this isn't the
    acknowledgement the subscriber was expecting.
    """
    try:
        expected = state.expected_acks.popleft()
    except IndexError:
        raise PubSubError(f"{message.type} received when not expecting any acks")

    if expected.type != message.type:
        raise PubSubError(f"expected {expected.type}, got {message.type}")
    if expected.topic != message.topic:
        raise PubSubError(
            f"expected {message.type} {expected.topic!r}, got {message.topic!r}"
        )

    state.exact_subscriptions.remove(expected.topic)


if TYPE_CHECKING:
    _: MessageChecker[B2S_ConfirmUnsubscribeExact] = check_confirm_unsubscribe_exact
