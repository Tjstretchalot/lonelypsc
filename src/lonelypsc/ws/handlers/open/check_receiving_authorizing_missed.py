from lonelypsp.auth.config import AuthResult

from lonelypsc.client import PubSubError
from lonelypsc.ws.check_result import CheckResult
from lonelypsc.ws.state import (
    ReceivedMessageType,
    ReceivedMissedMessage,
    ReceivingState,
    StateOpen,
)


def check_receiving_authorizing_missed(state: StateOpen) -> CheckResult:
    """
    Tries to move from receiving state `AUTHORIZING_MISSED` to None

    Raises an error if the authorization result is not "ok"
    """
    if (
        state.receiving is None
        or state.receiving.type != ReceivingState.AUTHORIZING_MISSED
    ):
        return CheckResult.CONTINUE

    if not state.receiving.authorization_task.done():
        return CheckResult.CONTINUE

    result = state.receiving.authorization_task.result()
    if result != AuthResult.OK:
        raise PubSubError(f"authorization failed: {result}")

    state.received.put_nowait(
        ReceivedMissedMessage(
            type=ReceivedMessageType.MISSED, topic=state.receiving.message.topic
        )
    )
    state.receiving = None
    return CheckResult.RESTART
