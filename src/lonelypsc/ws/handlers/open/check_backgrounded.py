from lonelypsc.client import PubSubIrrecoverableError
from lonelypsc.ws.check_result import CheckResult
from lonelypsc.ws.state import StateOpen


def check_backgrounded(state: StateOpen) -> CheckResult:
    if not any(j.done() for j in state.backgrounded):
        return CheckResult.CONTINUE

    new_backgrounded = set()

    for bknd in state.backgrounded:
        if not bknd.done():
            new_backgrounded.add(bknd)
            continue

        if bknd.exception() is None:
            continue

        # avoids duplicating the error as it will be found during cleanup
        # again
        raise PubSubIrrecoverableError("saw backgrounded task failed")

    state.backgrounded = new_backgrounded
    return CheckResult.RESTART
