import asyncio
import time
from typing import TYPE_CHECKING

import aiohttp

from lonelypsc.ws.handle_connection_failure import (
    cleanup_tasks_and_raise,
    cleanup_tasks_and_raise_on_error,
)
from lonelypsc.ws.handlers.protocol import StateHandler
from lonelypsc.ws.state import State, StateClosed, StateConnecting, StateType
from lonelypsc.ws.websocket_connect_task import make_websocket_connect_task


async def handle_waiting_retry(state: State) -> State:
    """
    Lets the backgrounded tasks finish, waits for the retry delay to pass, then moves
    to the CONNECTING state
    """
    assert state.type == StateType.WAITING_RETRY

    cleaning_up = False

    if state.backgrounded:
        # not necessary to wait on cancel_requested here as backgrounded tasks are
        # always allowed to finish, even in cleanup_tasks_and_raise

        await asyncio.wait(state.backgrounded, return_when=asyncio.ALL_COMPLETED)
        if any(bknd.exception() is not None for bknd in state.backgrounded):
            await cleanup_tasks_and_raise_on_error(
                state.tasks, state.backgrounded, "backgrounded sweep failed"
            )
            raise AssertionError("cleanup_tasks_and_raise_on_error should have raised")

        state.backgrounded = set()
        return state

    try:
        try:
            await asyncio.wait_for(
                state.cancel_requested.wait(),
                timeout=state.retry_at - time.time(),
            )
            cleaning_up = True
            await cleanup_tasks_and_raise_on_error(
                state.tasks, state.backgrounded, "cancel requested"
            )
            return StateClosed(type=StateType.CLOSED)
        except asyncio.TimeoutError:
            ...

        try:
            broadcaster = next(state.retry.iterator)
        except StopIteration:
            raise Exception("retry iterator yielded no broadcasters to try")

        client_session = aiohttp.ClientSession()
        return StateConnecting(
            type=StateType.CONNECTING,
            config=state.config,
            client_session=client_session,
            websocket_task=make_websocket_connect_task(
                state.config, broadcaster, client_session
            ),
            cancel_requested=state.cancel_requested,
            broadcaster=broadcaster,
            retry=state.retry,
            tasks=state.tasks,
            backgrounded=state.backgrounded,
        )
    except BaseException as e:
        if cleaning_up:
            raise

        await cleanup_tasks_and_raise(
            state.tasks, state.backgrounded, "failed to retry", e
        )


if TYPE_CHECKING:
    _: StateHandler = handle_waiting_retry
