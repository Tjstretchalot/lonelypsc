import asyncio

from lonelypsc.ws.check_result import CheckResult
from lonelypsc.ws.handlers.open.send_internal_message import send_internal_message
from lonelypsc.ws.state import SendingInternalMessage, SendingState, StateOpen


def check_unsent_notifications(state: StateOpen) -> CheckResult:
    """Checks if the left item from unsent_notifications can be moved
    to sending, doing so if possible and nothing otherwise
    """
    if state.sending is not None or state.unsent_notifications.empty():
        return CheckResult.CONTINUE

    msg = state.unsent_notifications.get_nowait()
    state.sending = SendingInternalMessage(
        type=SendingState.INTERNAL_MESSAGE,
        task=asyncio.create_task(send_internal_message(state, msg)),
        internal_message=msg,
    )
    return CheckResult.RESTART
