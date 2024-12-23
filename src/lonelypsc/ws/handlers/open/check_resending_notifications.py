import asyncio

from lonelypsc.ws.check_result import CheckResult
from lonelypsc.ws.handlers.open.send_internal_message import send_internal_message
from lonelypsc.ws.internal_callbacks import sweep_internal_message
from lonelypsc.ws.state import SendingInternalMessage, SendingState, StateOpen


def check_resending_notifications(state: StateOpen) -> CheckResult:
    """First, sweeps all the resending notifications, raising an error
    if any of the backgrounded internal message state callback tasks
    failed.

    Then, checks if the left item from resending_notifications can be moved
    to sending, doing so if possible and nothing otherwise
    """
    if not state.resending_notifications:
        return CheckResult.CONTINUE

    for msg in state.resending_notifications:
        sweep_internal_message(msg)

    if state.sending is not None:
        return CheckResult.CONTINUE

    msg = state.resending_notifications.pop()
    state.sending = SendingInternalMessage(
        type=SendingState.INTERNAL_MESSAGE,
        task=asyncio.create_task(send_internal_message(state, msg)),
        internal_message=msg,
    )
    return CheckResult.RESTART
