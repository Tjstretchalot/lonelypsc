import asyncio
import time
from typing import TYPE_CHECKING

import aiohttp

from lonelypsc.client import PubSubCancelRequested, PubSubIrrecoverableError
from lonelypsc.ws.check_result import (
    CheckResult,
    CheckStateChangerResult,
    CheckStateChangerResultContinue,
    CheckStateChangerResultDone,
)
from lonelypsc.ws.handle_connection_failure import (
    cleanup_tasks_and_raise,
)
from lonelypsc.ws.handlers.protocol import StateHandler
from lonelypsc.ws.handlers.util.state_specific_cleanup import handle_via_composition
from lonelypsc.ws.internal_callbacks import sweep_internal_message
from lonelypsc.ws.state import (
    State,
    StateConnecting,
    StateType,
    StateWaitingRetry,
)
from lonelypsc.ws.websocket_connect_task import make_websocket_connect_task


async def handle_waiting_retry(state: State) -> State:
    """
    Lets the backgrounded tasks finish, waits for the retry delay to pass, then moves
    to the CONNECTING state
    """
    assert state.type == StateType.WAITING_RETRY
    return await handle_via_composition(
        state=state, core=_core, recover=_recover, shutdown=_shutdown
    )


async def _core(state: StateWaitingRetry) -> State:
    """Happy path to move from WAITING_RETRY to CONNECTING, raises an exception to
    request recover/shutdown
    """
    if (result := await _check_canceled(state)).type == CheckResult.RESTART:
        return result.state

    if (result := await _check_timeout(state)).type == CheckResult.RESTART:
        return result.state

    _sweep_backgrounded(state)
    _sweep_resending(state)
    await _wait_something_changed(state)
    return state


async def _wait_something_changed(state: StateWaitingRetry) -> None:
    """Waits for something to need to be done about the given state"""
    wait_cancel = asyncio.create_task(state.cancel_requested.wait())
    wait_timeout = asyncio.create_task(asyncio.sleep(state.retry_at - time.time()))
    await asyncio.wait(
        [
            wait_cancel,
            wait_timeout,
            *state.backgrounded,
            *[
                msg.callback.task
                for msg in state.tasks.resending_notifications
                if msg.callback.task is not None
            ],
        ],
        return_when=asyncio.FIRST_COMPLETED,
    )
    wait_cancel.cancel()
    wait_timeout.cancel()


async def _check_canceled(state: StateWaitingRetry) -> CheckStateChangerResult:
    if not state.cancel_requested.is_set():
        return CheckStateChangerResultContinue(type=CheckResult.CONTINUE)

    raise PubSubCancelRequested()


def _sweep_backgrounded(state: StateWaitingRetry) -> None:
    """Cleans out done background tasks, raising an irrecoverable error if any failed"""
    new_backgrounded = set()

    for bknd in state.backgrounded:
        if not bknd.done():
            new_backgrounded.add(bknd)
            continue

        if bknd.exception() is not None:
            raise PubSubIrrecoverableError("background task failed")

    state.backgrounded = new_backgrounded


def _sweep_resending(state: StateWaitingRetry) -> None:
    """Progresses resending notification state callbacks, raising an error if any failed"""
    for msg in state.tasks.resending_notifications:
        sweep_internal_message(msg)


async def _check_timeout(state: StateWaitingRetry) -> CheckStateChangerResult:
    """Checks if the retry delay has passed"""
    if time.time() < state.retry_at:
        return CheckStateChangerResultContinue(type=CheckResult.CONTINUE)

    # this should be the first next on this iterator
    try:
        broadcaster = next(state.retry.iterator)
    except StopIteration:
        raise PubSubIrrecoverableError("no broadcasters to try")

    client_session = aiohttp.ClientSession()
    return CheckStateChangerResultDone(
        type=CheckResult.RESTART,
        state=StateConnecting(
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
        ),
    )


async def _recover(state: StateWaitingRetry, /, *, cause: BaseException) -> State:
    """Recovery is not possible for WAITING_RETRY, so re-raises, marking the
    cause as irrecoverable
    """
    raise PubSubIrrecoverableError("recovery not possible") from cause


async def _shutdown(state: StateWaitingRetry, /, *, cause: BaseException) -> State:
    """Move the state machine towards the CLOSED state by cleaning up and bubbling"""
    await cleanup_tasks_and_raise(
        state.tasks, state.backgrounded, "failed to retry", cause
    )


if TYPE_CHECKING:
    _: StateHandler = handle_waiting_retry
