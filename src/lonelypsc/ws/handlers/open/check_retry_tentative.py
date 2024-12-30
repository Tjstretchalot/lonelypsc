import time

from lonelypsc.ws.check_result import CheckResult
from lonelypsc.ws.state import (
    OpenRetryInformationStable,
    OpenRetryInformationType,
    StateOpen,
)


def check_retry_tentative(state: StateOpen) -> CheckResult:
    """Tries to change the states stability level from tentative to stable,
    returning CONTINUE if it cannot and RESTART if it does
    """
    if state.retry.type != OpenRetryInformationType.TENTATIVE:
        return CheckResult.CONTINUE

    if state.retry.stable_at > time.time():
        return CheckResult.CONTINUE

    state.retry = OpenRetryInformationStable(type=OpenRetryInformationType.STABLE)
    return CheckResult.RESTART
