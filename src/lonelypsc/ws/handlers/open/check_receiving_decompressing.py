from lonelypsc.ws.check_result import CheckResult
from lonelypsc.ws.state import ReceivingState, StateOpen


def check_receiving_decompressing(state: StateOpen) -> CheckResult:
    """
    Tries to move from receiving notify state `DECOMPRESSING` to None

    Raises an exception if the decompression task fails
    """
    if state.receiving is None or state.receiving.type != ReceivingState.DECOMPRESSING:
        return CheckResult.CONTINUE

    if not state.receiving.task.done():
        return CheckResult.CONTINUE

    msg = state.receiving.task.result()
    state.received.put_nowait(msg)
    state.receiving = None
    return CheckResult.RESTART
