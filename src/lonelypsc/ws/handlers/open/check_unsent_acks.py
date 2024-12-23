import asyncio

from lonelypsc.ws.check_result import CheckResult
from lonelypsc.ws.state import SendingSimple, SendingState, StateOpen


def check_unsent_acks(state: StateOpen) -> CheckResult:
    """Checks if the left item from unsent acks can be moved to send_task;
    doing so if possible and nothing otherwise
    """
    if state.sending is not None:
        return CheckResult.CONTINUE

    if not state.unsent_acks:
        return CheckResult.CONTINUE

    state.sending = SendingSimple(
        type=SendingState.SIMPLE,
        task=asyncio.create_task(
            state.websocket.send_bytes(state.unsent_acks.popleft())
        ),
    )
    return CheckResult.RESTART
