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

    try:
        state.receiving.task.result()
    except BaseException as exc:
        raise PubSubError(
            "failed to handle simple message (probably authorization failed)"
        ) from exc

    state.receiving = None
    return CheckResult.RESTART
