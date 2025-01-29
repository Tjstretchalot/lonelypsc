import asyncio

from lonelypsp.stateful.constants import SubscriberToBroadcasterStatefulMessageType

from lonelypsc.ws.check_result import CheckResult
from lonelypsc.ws.handlers.open.sends.confirm_receive import send_confirm_receive
from lonelypsc.ws.handlers.open.sends.continue_receive import send_continue_receive
from lonelypsc.ws.state import SendingSimple, SendingState, StateOpen


def check_unsent_acks(state: StateOpen) -> CheckResult:
    """Checks if the left item from unsent acks can be moved to send_task;
    doing so if possible and nothing otherwise
    """
    if state.sending is not None:
        return CheckResult.CONTINUE

    if not state.unsent_acks:
        return CheckResult.CONTINUE

    next_ack = state.unsent_acks.popleft()
    ack_co = (
        send_confirm_receive(state, next_ack)
        if next_ack.type == SubscriberToBroadcasterStatefulMessageType.CONFIRM_RECEIVE
        else send_continue_receive(state, next_ack)
    )

    state.sending = SendingSimple(
        type=SendingState.SIMPLE,
        task=asyncio.create_task(ack_co),
    )
    return CheckResult.RESTART
