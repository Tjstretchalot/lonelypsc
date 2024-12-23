from lonelypsc.ws.check_result import CheckResult
from lonelypsc.ws.internal_callbacks import sweep_internal_message
from lonelypsc.ws.state import StateOpen


def check_sent_notifications(state: StateOpen) -> CheckResult:
    """Sweeps all the sent notifications (that haven't been acknowledged), raising
    an error if any of the backgrounded internal message state callback tasks
    failed.
    """
    for msg in state.sent_notifications:
        sweep_internal_message(msg)
    return CheckResult.CONTINUE
