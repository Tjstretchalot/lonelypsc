import asyncio
import time
from typing import TYPE_CHECKING

from lonelypsc.ws.handle_connection_failure import cleanup_tasks_and_raise
from lonelypsc.ws.handlers.protocol import StateHandler
from lonelypsc.ws.state import State, StateConnecting, StateType


async def handle_waiting_retry(state: State) -> State:
    """Waits for the retry delay to pass, then moves to the CONNECTING state"""
    assert state.type == StateType.WAITING_RETRY

    try:
        await asyncio.sleep(state.retry_at - time.time())

        try:
            broadcaster = next(state.retry.iterator)
        except StopIteration:
            raise Exception("retry iterator yielded no broadcasters to try")

        return StateConnecting(
            type=StateType.CONNECTING,
            config=state.config,
            broadcaster=broadcaster,
            retry=state.retry,
            tasks=state.tasks,
        )
    except BaseException as e:
        await cleanup_tasks_and_raise(state.tasks, "failed to retry", e)


if TYPE_CHECKING:
    _: StateHandler = handle_waiting_retry
