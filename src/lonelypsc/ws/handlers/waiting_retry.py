import asyncio
import time
from typing import TYPE_CHECKING

from lonelypsc.ws.handle_connection_failure import (
    cleanup_tasks_and_raise,
    cleanup_tasks_and_raise_on_error,
)
from lonelypsc.ws.handlers.protocol import StateHandler
from lonelypsc.ws.state import State, StateClosed, StateConnecting, StateType


async def handle_waiting_retry(state: State) -> State:
    """Waits for the retry delay to pass, then moves to the CONNECTING state"""
    assert state.type == StateType.WAITING_RETRY

    cleaning_up = False

    try:
        try:
            await asyncio.wait_for(
                state.cancel_requested.wait(),
                timeout=state.retry_at - time.time(),
            )
            cleaning_up = True
            await cleanup_tasks_and_raise_on_error(state.tasks, "cancel requested")
            return StateClosed(type=StateType.CLOSED)
        except asyncio.TimeoutError:
            ...

        try:
            broadcaster = next(state.retry.iterator)
        except StopIteration:
            raise Exception("retry iterator yielded no broadcasters to try")

        return StateConnecting(
            type=StateType.CONNECTING,
            config=state.config,
            cancel_requested=state.cancel_requested,
            broadcaster=broadcaster,
            retry=state.retry,
            tasks=state.tasks,
        )
    except BaseException as e:
        if cleaning_up:
            raise

        await cleanup_tasks_and_raise(state.tasks, "failed to retry", e)


if TYPE_CHECKING:
    _: StateHandler = handle_waiting_retry
