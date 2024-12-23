import asyncio
from typing import TYPE_CHECKING

from lonelypsp.util.bounded_deque import BoundedDequeFullError

from lonelypsc.client import PubSubIrrecoverableError
from lonelypsc.ws.check_result import CheckResult
from lonelypsc.ws.handlers.open.check_management_tasks import check_management_tasks
from lonelypsc.ws.handlers.open.check_read import check_read_task
from lonelypsc.ws.handlers.open.check_receiving_authorizing import (
    check_receiving_authorizing,
)
from lonelypsc.ws.handlers.open.check_receiving_decompressing import (
    check_receiving_decompressing,
)
from lonelypsc.ws.handlers.open.check_receiving_waiting_compressor import (
    check_receiving_waiting_compressor,
)
from lonelypsc.ws.handlers.open.check_resending_notifications import (
    check_resending_notifications,
)
from lonelypsc.ws.handlers.open.check_send import check_send_task
from lonelypsc.ws.handlers.open.check_sent_notifications import check_sent_notifications
from lonelypsc.ws.handlers.open.check_unsent_acks import check_unsent_acks
from lonelypsc.ws.handlers.open.check_unsent_notifications import (
    check_unsent_notifications,
)
from lonelypsc.ws.handlers.open.cleanup import cleanup_open
from lonelypsc.ws.handlers.protocol import StateHandler
from lonelypsc.ws.state import State, StateType


async def handle_open(state: State) -> State:
    """The core inner loop for the websocket client; processes incoming
    messages and sends outgoing messages in a deterministic order
    """
    assert state.type == StateType.OPEN
    try:
        if state.cancel_requested.is_set():
            raise PubSubIrrecoverableError("cancel requested")

        if check_receiving_authorizing(state) == CheckResult.RESTART:
            return state
        if check_receiving_waiting_compressor(state) == CheckResult.RESTART:
            return state
        if check_receiving_decompressing(state) == CheckResult.RESTART:
            return state
        if check_read_task(state) == CheckResult.RESTART:
            return state
        if check_send_task(state) == CheckResult.RESTART:
            return state
        if check_unsent_acks(state) == CheckResult.RESTART:
            return state
        if check_management_tasks(state) == CheckResult.RESTART:
            return state
        if check_resending_notifications(state) == CheckResult.RESTART:
            return state
        if check_unsent_notifications(state) == CheckResult.RESTART:
            return state
        if check_sent_notifications(state) == CheckResult.RESTART:
            return state

        raise NotImplementedError
    except (
        NotImplementedError,
        AssertionError,
        PubSubIrrecoverableError,
        asyncio.CancelledError,
        KeyboardInterrupt,
        # full queues typically cause dropped state, e.g., what we are meant
        # to be subscribed to, which can't be recovered from
        BoundedDequeFullError,
        asyncio.QueueFull,
    ) as e:
        return await cleanup_open(state, e, irrecoverable=True)
    except BaseException as e:
        return await cleanup_open(state, e, irrecoverable=False)


if TYPE_CHECKING:
    _: StateHandler = handle_open
