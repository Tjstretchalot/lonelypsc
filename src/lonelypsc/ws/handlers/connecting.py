import asyncio
import secrets
from typing import TYPE_CHECKING

from lonelypsp.stateful.constants import SubscriberToBroadcasterStatefulMessageType
from lonelypsp.stateful.messages.configure import S2B_Configure, serialize_s2b_configure

from lonelypsc.util.errors import combine_multiple_exceptions
from lonelypsc.ws.check_result import (
    CheckResult,
    CheckStateChangerResult,
    CheckStateChangerResultContinue,
    CheckStateChangerResultDone,
)
from lonelypsc.ws.handle_connection_failure import (
    cleanup_tasks_and_raise,
    cleanup_tasks_and_raise_on_error,
    handle_connection_failure,
)
from lonelypsc.ws.handlers.protocol import StateHandler
from lonelypsc.ws.internal_callbacks import sweep_internal_message
from lonelypsc.ws.state import (
    State,
    StateClosed,
    StateConfiguring,
    StateConnecting,
    StateType,
)
from lonelypsc.ws.util import make_websocket_read_task


async def handle_connecting(state: State) -> State:
    """Tries to connect to the given broadcaster; if unsuccessful,
    moves to either CONNECTING (with the next broadcaster),
    WAITING_RETRY, or CLOSED. If successful, moves to CONFIGURING.
    May need multiple calls to progress
    """
    # TODO: take the error wrapper from the open handler, make that
    # an async context manager (plug in a cleanup function passed
    # irrecoverable bool), then should be consistent for every handler
    assert state.type == StateType.CONNECTING

    if (result := await _check_canceled(state)).type == CheckResult.RESTART:
        return result.state

    if (result := await _check_websocket(state)).type == CheckResult.RESTART:
        return result.state

    await _sweep_resending_notifications(state)
    await _sweep_backgrounded(state)
    await _wait_something_changed(state)
    return state


async def _wait_something_changed(state: StateConnecting) -> None:
    wait_cancel = asyncio.create_task(state.cancel_requested.wait())
    await asyncio.wait(
        [
            wait_cancel,
            state.websocket_task,
            *[
                msg.callback.task
                for msg in state.tasks.resending_notifications
                if msg.callback.task is not None
            ],
            *state.backgrounded,
        ],
        return_when=asyncio.FIRST_COMPLETED,
    )
    wait_cancel.cancel()


async def _check_canceled(state: StateConnecting) -> CheckStateChangerResult:
    if not state.cancel_requested.is_set():
        return CheckStateChangerResultContinue(type=CheckResult.CONTINUE)

    await state.client_session.close()
    await cleanup_tasks_and_raise_on_error(
        state.tasks, state.backgrounded, "cancel requested"
    )
    return CheckStateChangerResultDone(
        type=CheckResult.RESTART, state=StateClosed(type=StateType.CLOSED)
    )


async def _sweep_backgrounded(state: StateConnecting) -> None:
    new_backgrounded = set()

    for bknd in state.backgrounded:
        if not bknd.done():
            new_backgrounded.add(bknd)
            continue

        if bknd.exception() is None:
            continue

        # avoids duplicating the error as it will be found during cleanup
        # again
        await _cleanup_and_raise(
            state, Exception("backgrounded task failed"), "backgrounded task failed"
        )

    state.backgrounded = new_backgrounded


async def _sweep_resending_notifications(state: StateConnecting) -> None:
    try:
        for msg in state.tasks.resending_notifications:
            sweep_internal_message(msg)
    except BaseException as e:
        await _cleanup_and_raise(e, "sweeping internal messages")


async def _cleanup_and_raise(
    state: StateConnecting, e: BaseException, msg: str
) -> None:
    try:
        await state.client_session.close()
    except BaseException as e2:
        e = combine_multiple_exceptions(
            "failed to close session",
            [e, e2],
        )

    await cleanup_tasks_and_raise(
        state.tasks,
        state.backgrounded,
        msg,
        e,
    )


async def _check_websocket(state: StateConnecting) -> CheckStateChangerResult:
    if not state.websocket_task.done():
        return CheckStateChangerResultContinue(type=CheckResult.CONTINUE)

    try:
        websocket = state.websocket_task.result()
    except BaseException as e:
        try:
            await state.client_session.close()
        except BaseException as e2:
            e = combine_multiple_exceptions(
                "failed to connect websocket then failed to close session",
                [e, e2],
            )

        return CheckStateChangerResultDone(
            type=CheckResult.RESTART,
            state=await handle_connection_failure(
                config=state.config,
                cancel_requested=state.cancel_requested,
                retry=state.retry,
                tasks=state.tasks,
                exception=e,
                backgrounded=state.backgrounded,
            ),
        )

    subscriber_nonce = secrets.token_bytes(32)
    return CheckStateChangerResultDone(
        type=CheckResult.RESTART,
        state=StateConfiguring(
            type=StateType.CONFIGURING,
            client_session=state.client_session,
            config=state.config,
            cancel_requested=state.cancel_requested,
            broadcaster=state.broadcaster,
            websocket=websocket,
            retry=state.retry,
            tasks=state.tasks,
            subscriber_nonce=subscriber_nonce,
            send_task=asyncio.create_task(
                websocket.send_bytes(
                    serialize_s2b_configure(
                        S2B_Configure(
                            type=SubscriberToBroadcasterStatefulMessageType.CONFIGURE,
                            subscriber_nonce=subscriber_nonce,
                            enable_zstd=state.config.allow_compression,
                            enable_training=state.config.allow_training_compression,
                            initial_dict=state.config.initial_compression_dict_id or 0,
                        ),
                        minimal_headers=state.config.websocket_minimal_headers,
                    )
                )
            ),
            read_task=make_websocket_read_task(websocket),
            backgrounded=state.backgrounded,
        ),
    )


if TYPE_CHECKING:
    _: StateHandler = handle_connecting
