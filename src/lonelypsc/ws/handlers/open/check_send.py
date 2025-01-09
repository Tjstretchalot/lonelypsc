from lonelypsc.client import PubSubError
from lonelypsc.ws.check_result import CheckResult
from lonelypsc.ws.internal_callbacks import sweep_internal_message
from lonelypsc.ws.state import SendingState, StateOpen


def check_sending(state: StateOpen) -> CheckResult:
    """Checks if sending is done, and if so, verifies it was successful
    then sets it to None, otherwise raises an exception
    """
    if state.sending is None or not state.sending.task.done():
        return CheckResult.CONTINUE

    if state.sending.task.exception() is not None:
        # let cleanup be handled in cleanup_open, which will discover
        # the error again, so avoid having it repeated in the stack trace
        raise PubSubError("send failed")

    if state.sending.type == SendingState.INTERNAL_MESSAGE:
        sweep_internal_message(state.sending.internal_message)

    state.sending = None

    return CheckResult.RESTART
