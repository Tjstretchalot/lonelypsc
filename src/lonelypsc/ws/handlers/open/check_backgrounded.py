from lonelypsc.client import PubSubIrrecoverableError
from lonelypsc.ws.check_result import CheckResult
from lonelypsc.ws.state import StateOpen


def check_backgrounded(state: StateOpen) -> CheckResult:
    if not any(j.done() for j in state.backgrounded):
        return CheckResult.CONTINUE

    new_backgrounded = set()
    errors = []

    for bknd in state.backgrounded:
        if not bknd.done():
            new_backgrounded.add(bknd)
            continue

        try:
            bknd.result()
        except BaseException as exc:
            errors.append(exc)

    state.backgrounded = new_backgrounded
    if errors:
        raise PubSubIrrecoverableError("saw backgrounded task failed") from errors[0]
    return CheckResult.RESTART
