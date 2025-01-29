from lonelypsc.client import PubSubError
from lonelypsc.ws.check_result import CheckResult
from lonelypsc.ws.state import (
    ReceivingState,
    StateOpen,
)


def check_receiving_authorizing_simple(state: StateOpen) -> CheckResult:
    """
    Tries to move from receiving state `AUTHORIZING_SIMPLE` to None

    Raises an error if one is found
    """
    if (
        state.receiving is None
        or state.receiving.type != ReceivingState.AUTHORIZING_SIMPLE
    ):
        return CheckResult.CONTINUE

    if not state.receiving.task.done():
        return CheckResult.CONTINUE

    if state.receiving.task.exception() is not None:
        raise PubSubError(
            "failed to handle simple message (probably authorization failed)"
        ) from state.receiving.task.exception()

    state.receiving = None
    return CheckResult.RESTART
