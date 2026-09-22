import asyncio
import math
import sys
import time
from typing import Any, List

from lonelypsc.util.task import TaskHandle, create_task
from lonelypsc.ws.state import (
    OpenRetryInformationType,
    ReceivingState,
    SendingState,
    StateOpen,
)

if sys.version_info >= (3, 11):
    from typing import Never
else:
    from typing import NoReturn as Never


async def wait_something_changed(state: StateOpen) -> None:
    """Waits for something to do"""
    managed_by_us: List[TaskHandle[Any]] = []
    other_tasks: List[TaskHandle[Any]] = []

    try:
        managed_by_us.append(create_task(state.cancel_requested.wait()))
        if state.retry.type == OpenRetryInformationType.TENTATIVE:
            managed_by_us.append(
                create_task(
                    asyncio.sleep(math.ceil(state.retry.stable_at - time.time()))
                )
            )

        other_tasks.extend(state.compressors.get_compressor_tasks())

        if state.unsent_notifications.empty():
            managed_by_us.append(
                create_task(state.unsent_notifications.wait_not_empty())
            )

        other_tasks.extend(
            t.callback.task
            for t in state.resending_notifications
            if t.callback.task is not None
        )
        other_tasks.extend(
            t.callback.task
            for t in state.sent_notifications
            if t.callback.task is not None
        )

        if state.management_tasks.empty():
            managed_by_us.append(create_task(state.management_tasks.wait_not_empty()))

        if state.receiving is None:
            ...  # avoids a nesting level
        elif state.receiving.type == ReceivingState.INCOMPLETE:
            ...  # waiting on read_task
        elif (
            state.receiving.type == ReceivingState.AUTHORIZING
            or state.receiving.type == ReceivingState.AUTHORIZING_MISSED
        ):
            other_tasks.append(state.receiving.authorization_task)
        elif state.receiving.type == ReceivingState.AUTHORIZING_SIMPLE:
            other_tasks.append(state.receiving.task)
        elif state.receiving.type == ReceivingState.WAITING_COMPRESSOR:
            ...  # already have compressor tasks in other_tasks
        elif state.receiving.type == ReceivingState.DECOMPRESSING:
            other_tasks.append(state.receiving.task)
        else:
            _assert_never(state.receiving)

        if state.received.full():
            managed_by_us.append(create_task(state.received.wait_not_full()))

        if state.sending is None:
            ...  # avoids a nesting level
        elif state.sending.type == SendingState.SIMPLE:
            other_tasks.append(state.sending.task)
        elif state.sending.type == SendingState.MANAGEMENT_TASK:
            other_tasks.append(state.sending.task)
        elif state.sending.type == SendingState.INTERNAL_MESSAGE:
            if state.sending.internal_message.callback.task is not None:
                other_tasks.append(state.sending.internal_message.callback.task)
            other_tasks.append(state.sending.task)
        else:
            _assert_never(state.sending)

        other_tasks.append(state.read_task)
        other_tasks.extend(state.backgrounded)

        await asyncio.wait(
            [task.task for task in managed_by_us + other_tasks],
            return_when=asyncio.FIRST_COMPLETED,
        )
    finally:
        await asyncio.gather(*(task.cancel_and_check() for task in managed_by_us))


def _assert_never(v: Never) -> None:
    raise AssertionError(f"Unhandled value: {v!r}")
