import asyncio
import secrets
from typing import TYPE_CHECKING, List

from lonelypsp.stateful.constants import SubscriberToBroadcasterStatefulMessageType
from lonelypsp.stateful.messages.configure import S2B_Configure, serialize_s2b_configure

from lonelypsc.client import PubSubCancelRequested, PubSubIrrecoverableError
from lonelypsc.util.errors import combine_multiple_exceptions
from lonelypsc.ws.check_result import (
    CheckResult,
    CheckStateChangerResult,
    CheckStateChangerResultContinue,
    CheckStateChangerResultDone,
)
from lonelypsc.ws.handle_connection_failure import (
    cleanup_tasks_and_raise,
    handle_connection_failure,
)
from lonelypsc.ws.handlers.protocol import StateHandler
from lonelypsc.ws.handlers.util.read_from_websocket import make_websocket_read_task
from lonelypsc.ws.handlers.util.state_specific_cleanup import handle_via_composition
from lonelypsc.ws.internal_callbacks import sweep_internal_message
from lonelypsc.ws.state import (
    State,
    StateConfiguring,
    StateConnecting,
    StateType,
)


async def handle_connecting(state: State) -> State:
    """Tries to connect to the given broadcaster; if unsuccessful,
    moves to either CONNECTING (with the next broadcaster),
    WAITING_RETRY, or CLOSED. If successful, moves to CONFIGURING.
    May need multiple calls to progress
    """
    assert state.type == StateType.CONNECTING
    return await handle_via_composition(
        state,
        core=_core,
        recover=_recover,
        shutdown=_shutdown,
    )


async def _core(state: StateConnecting) -> State:
    if (result := await _check_canceled(state)).type == CheckResult.RESTART:
        return result.state

    if (result := await _check_websocket(state)).type == CheckResult.RESTART:
        return result.state

    await _sweep_resending_notifications(state)
    await _sweep_backgrounded(state)
    await _wait_something_changed(state)
    return state


async def _recover(state: StateConnecting, /, *, cause: BaseException) -> State:
    try:
        await state.client_session.close()
    except Exception as e:
        cause = combine_multiple_exceptions(
            "failed to close session", [e], context=cause
        )

    return await handle_connection_failure(
        config=state.config,
        cancel_requested=state.cancel_requested,
        retry=state.retry,
        tasks=state.tasks,
        exception=cause,
        backgrounded=state.backgrounded,
    )


async def _shutdown(state: StateConnecting, /, *, cause: BaseException) -> State:
    cleanup_excs: List[BaseException] = []
    if state.websocket_task.done():
        try:
            websocket = state.websocket_task.result()
            await websocket.close()
        except BaseException as e:
            cleanup_excs.append(e)
    try:
        await state.client_session.close()
    except BaseException as e:
        cleanup_excs.append(e)

    if cleanup_excs:
        cause = combine_multiple_exceptions(
            "failed to close websocket", cleanup_excs, context=cause
        )

    await cleanup_tasks_and_raise(
        state.tasks,
        state.backgrounded,
        "shutdown from connecting",
        cause,
    )


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

    raise PubSubCancelRequested()


async def _sweep_backgrounded(state: StateConnecting) -> None:
    if not any(j.done() for j in state.backgrounded):
        return

    new_backgrounded = set()

    for bknd in state.backgrounded:
        if not bknd.done():
            new_backgrounded.add(bknd)
            continue

        if bknd.exception() is None:
            continue

        # avoids duplicating the error as it will be found during cleanup
        # again
        raise PubSubIrrecoverableError("saw backgrounded task failed")

    state.backgrounded = new_backgrounded


async def _sweep_resending_notifications(state: StateConnecting) -> None:
    for msg in state.tasks.resending_notifications:
        sweep_internal_message(msg)


async def _check_websocket(state: StateConnecting) -> CheckStateChangerResult:
    if not state.websocket_task.done():
        return CheckStateChangerResultContinue(type=CheckResult.CONTINUE)

    websocket = state.websocket_task.result()
    subscriber_nonce = secrets.token_bytes(32)
    tracing = b""  # TODO: tracing
    enable_zstd = state.config.allow_compression
    enable_training = state.config.allow_training_compression
    initial_dict = state.config.initial_compression_dict_id or 0
    authorization = await state.config.authorize_stateful_configure(
        tracing=tracing,
        subscriber_nonce=subscriber_nonce,
        enable_zstd=enable_zstd,
        enable_training=enable_training,
        initial_dict=initial_dict,
    )
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
                            enable_zstd=enable_zstd,
                            enable_training=enable_training,
                            initial_dict=initial_dict,
                            authorization=authorization,
                            tracing=tracing,
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
