from lonelypsp.auth.config import AuthResult

from lonelypsc.client import PubSubError
from lonelypsc.ws.check_result import CheckResult
from lonelypsc.ws.handlers.open.handle_authorized_receive import (
    handle_authorized_receive,
)
from lonelypsc.ws.state import ReceivingState, StateOpen


def check_receiving_authorizing(state: StateOpen) -> CheckResult:
    """
    Tries to move from receiving state `AUTHORIZING` to either WAITING_COMPRESSOR or None

    Raises an error if the authorization result is not "ok"
    """
    if state.receiving is None or state.receiving.type != ReceivingState.AUTHORIZING:
        return CheckResult.CONTINUE

    if not state.receiving.authorization_task.done():
        return CheckResult.CONTINUE

    result = state.receiving.authorization_task.result()
    if result != AuthResult.OK:
        raise PubSubError(f"authorization failed: {result}")

    handle_authorized_receive(state, state.receiving.first, state.receiving.body)
    return CheckResult.RESTART
