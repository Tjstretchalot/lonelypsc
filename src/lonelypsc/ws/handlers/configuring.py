import asyncio
import base64
import hashlib
import io
import time
from typing import TYPE_CHECKING, Any, Set, cast

from lonelypsp.auth.config import AuthResult
from lonelypsp.stateful.constants import BroadcasterToSubscriberStatefulMessageType
from lonelypsp.stateful.messages.confirm_configure import B2S_ConfirmConfigureParser
from lonelypsp.stateful.parser_helpers import parse_b2s_message_prefix
from lonelypsp.util.bounded_deque import BoundedDeque
from lonelypsp.util.drainable_asyncio_queue import DrainableAsyncioQueue

from lonelypsc.client import (
    PubSubCancelRequested,
    PubSubError,
    PubSubIrrecoverableError,
)
from lonelypsc.types.websocket_message import WSMessageBytes
from lonelypsc.ws.check_result import (
    CheckResult,
    CheckStateChangerResult,
    CheckStateChangerResultContinue,
    CheckStateChangerResultDone,
)
from lonelypsc.ws.compressor import CompressorStoreImpl
from lonelypsc.ws.handlers.protocol import StateHandler
from lonelypsc.ws.handlers.util.read_from_websocket import make_websocket_read_task
from lonelypsc.ws.handlers.util.state_specific_cleanup import handle_via_composition
from lonelypsc.ws.state import (
    ClosingRetryInformationCannotRetry,
    ClosingRetryInformationType,
    ClosingRetryInformationWantRetry,
    ManagementTask,
    ManagementTaskSubscribeExact,
    ManagementTaskSubscribeGlob,
    ManagementTaskType,
    OpenRetryInformationTentative,
    OpenRetryInformationType,
    SendingSimple,
    SendingState,
    State,
    StateClosing,
    StateConfiguring,
    StateOpen,
    StateType,
)


async def handle_configuring(state: State) -> State:
    """Waits for the broadcaster to respond with the confirm configure message,
    then moves to the OPEN state

    If there are errors, handles them in the same way as in CONNECTING
    """
    assert state.type == StateType.CONFIGURING
    return await handle_via_composition(
        state, core=_core, recover=_recover, shutdown=_shutdown
    )


async def _core(state: StateConfiguring) -> State:
    """
    Happy path to progress configuring state, raises an error if cleanup code
    needs to be called, typically PubSubError for recoverable errors and
    PubSubIrrecoverableError for unrecoverable errors
    """
    if await _check_send_task(state) == CheckResult.RESTART:
        return state

    if (result := await _check_read_task(state)).type == CheckResult.RESTART:
        return result.state

    if (result := await _check_cancel_requested(state)).type == CheckResult.RESTART:
        return result.state

    if (result := _check_backgrounded(state)).type == CheckResult.RESTART:
        return result.state

    wait_cancel_requested = asyncio.create_task(state.cancel_requested.wait())
    await asyncio.wait(
        [
            state.read_task,
            *([state.send_task] if state.send_task is not None else []),
            wait_cancel_requested,
            *state.backgrounded,
        ],
        return_when=asyncio.FIRST_COMPLETED,
    )
    wait_cancel_requested.cancel()
    return state


async def _check_send_task(state: StateConfiguring) -> CheckResult:
    """Checks if the subscriber is done with the application part of sending the
    CONFIGURE message
    """
    if state.send_task is None or not state.send_task.done():
        return CheckResult.CONTINUE

    task = state.send_task
    state.send_task = None
    task.result()
    return CheckResult.RESTART


async def _check_read_task(state: StateConfiguring) -> CheckStateChangerResult:
    """Checks if the broadcaster has confirmed the configure message yet"""
    if not state.read_task.done():
        return CheckStateChangerResultContinue(type=CheckResult.CONTINUE)

    raw_message = state.read_task.result()
    if raw_message["type"] == "websocket.disconnect":
        raise Exception("disconnected before confirming configure")

    if "bytes" not in raw_message:
        raise Exception("received non-bytes non-disconnect message")

    message = cast(WSMessageBytes, raw_message)
    payload = message["bytes"]

    stream = io.BytesIO(payload)
    prefix = parse_b2s_message_prefix(stream)

    if prefix.type != BroadcasterToSubscriberStatefulMessageType.CONFIRM_CONFIGURE:
        raise PubSubError(
            f"received unexpected message before confirm configure: {prefix}"
        )

    parsed_message = B2S_ConfirmConfigureParser.parse(prefix.flags, prefix.type, stream)

    auth_result = await state.config.is_stateful_confirm_configure_allowed(
        message=parsed_message, now=time.time()
    )
    if auth_result != AuthResult.OK:
        raise PubSubError(f"confirm configure authorization failed: {auth_result}")

    connection_nonce = hashlib.sha256(
        state.subscriber_nonce + parsed_message.broadcaster_nonce
    ).digest()

    management_tasks: DrainableAsyncioQueue[ManagementTask] = DrainableAsyncioQueue(
        max_size=state.config.max_expected_acks
    )
    for topic in state.tasks.exact_subscriptions:
        management_tasks.put_nowait(
            ManagementTaskSubscribeExact(
                type=ManagementTaskType.SUBSCRIBE_EXACT, topic=topic
            )
        )
    for glob in state.tasks.glob_subscriptions:
        management_tasks.put_nowait(
            ManagementTaskSubscribeGlob(
                type=ManagementTaskType.SUBSCRIBE_GLOB, glob=glob
            )
        )

    for task in state.tasks.unsorted.drain():
        management_tasks.put_nowait(task)

    return CheckStateChangerResultDone(
        type=CheckResult.RESTART,
        state=StateOpen(
            type=StateType.OPEN,
            client_session=state.client_session,
            config=state.config,
            cancel_requested=state.cancel_requested,
            broadcaster=state.broadcaster,
            broadcaster_counter=1,
            subscriber_counter=-1,
            nonce_b64=base64.b64encode(connection_nonce).decode("ascii"),
            websocket=state.websocket,
            retry=OpenRetryInformationTentative(
                type=OpenRetryInformationType.TENTATIVE,
                stable_at=time.time() + state.config.outgoing_min_reconnect_interval,
                continuation=state.retry,
            ),
            compressors=CompressorStoreImpl(),
            unsent_notifications=state.tasks.unsent_notifications,
            resending_notifications=state.tasks.resending_notifications,
            sent_notifications=BoundedDeque(maxlen=state.config.max_sent_notifications),
            exact_subscriptions=set(),
            glob_subscriptions=set(),
            management_tasks=management_tasks,
            expected_acks=BoundedDeque(maxlen=state.config.max_expected_acks),
            receiving=None,
            received=DrainableAsyncioQueue(max_size=state.config.max_received),
            unsent_acks=BoundedDeque(maxlen=state.config.max_unsent_acks),
            sending=(
                None
                if state.send_task is None
                else SendingSimple(type=SendingState.SIMPLE, task=state.send_task)
            ),
            read_task=make_websocket_read_task(state.websocket),
            backgrounded=state.backgrounded,
        ),
    )


async def _check_cancel_requested(state: StateConfiguring) -> CheckStateChangerResult:
    """Checks if it has been requested that the state move towards CLOSED"""
    if not state.cancel_requested.is_set():
        return CheckStateChangerResultContinue(type=CheckResult.CONTINUE)
    raise PubSubCancelRequested()


def _check_backgrounded(state: StateConfiguring) -> CheckStateChangerResult:
    """Cleans out done background tasks, raising an irrecoverable error if any
    failed
    """
    if not any(task.done() for task in state.backgrounded):
        return CheckStateChangerResultContinue(type=CheckResult.CONTINUE)

    new_backgrounded: Set[asyncio.Task[Any]] = set()
    for task in state.backgrounded:
        if not task.done():
            new_backgrounded.add(task)
            continue

        if task.exception() is not None:
            raise PubSubIrrecoverableError("background task failed")

    state.backgrounded = new_backgrounded
    return CheckStateChangerResultDone(type=CheckResult.RESTART, state=state)


async def _cleanup(state: StateConfiguring) -> None:
    """Cancels any pending tasks; can be called multiple times"""
    if state.send_task is not None:
        state.send_task.cancel()
    state.read_task.cancel()


async def _recover(state: StateConfiguring, /, *, cause: BaseException) -> State:
    """Attempts to move to the next broadcaster"""
    await _cleanup(state)
    return StateClosing(
        type=StateType.CLOSING,
        config=state.config,
        cancel_requested=state.cancel_requested,
        broadcaster=state.broadcaster,
        client_session=state.client_session,
        websocket=state.websocket,
        retry=(
            ClosingRetryInformationWantRetry(
                type=ClosingRetryInformationType.WANT_RETRY,
                retry=state.retry,
                tasks=state.tasks,
                exception=cause,
            )
        ),
        backgrounded=state.backgrounded,
    )


async def _shutdown(state: StateConfiguring, /, *, cause: BaseException) -> State:
    """Moves towards the CLOSED state"""
    await _cleanup(state)
    return StateClosing(
        type=StateType.CLOSING,
        config=state.config,
        cancel_requested=state.cancel_requested,
        broadcaster=state.broadcaster,
        client_session=state.client_session,
        websocket=state.websocket,
        retry=ClosingRetryInformationCannotRetry(
            type=ClosingRetryInformationType.CANNOT_RETRY,
            tasks=state.tasks,
            exception=cause,
        ),
        backgrounded=state.backgrounded,
    )


if TYPE_CHECKING:
    _: StateHandler = handle_configuring
