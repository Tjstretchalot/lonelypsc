import asyncio
import base64
import hashlib
import io
import time
from typing import TYPE_CHECKING, Literal, Union, cast

from lonelypsp.compat import fast_dataclass
from lonelypsp.stateful.constants import BroadcasterToSubscriberStatefulMessageType
from lonelypsp.stateful.messages.confirm_configure import B2S_ConfirmConfigureParser
from lonelypsp.stateful.parser_helpers import parse_b2s_message_prefix
from lonelypsp.util.bounded_deque import BoundedDeque
from lonelypsp.util.drainable_asyncio_queue import DrainableAsyncioQueue

from lonelypsc.types.websocket_message import WSMessageBytes
from lonelypsc.ws.check_result import CheckResult
from lonelypsc.ws.compressor import CompressorStoreImpl
from lonelypsc.ws.handlers.protocol import StateHandler
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
    State,
    StateClosing,
    StateConfiguring,
    StateOpen,
    StateType,
)
from lonelypsc.ws.util import make_websocket_read_task


@fast_dataclass
class CheckStateChangerResultContinue:
    type: Literal[CheckResult.CONTINUE]


@fast_dataclass
class CheckStateChangerResultDone:
    type: Literal[CheckResult.RESTART]
    state: State


CheckStateChangerResult = Union[
    CheckStateChangerResultContinue, CheckStateChangerResultDone
]


async def handle_configuring(state: State) -> State:
    """Waits for the broadcaster to respond with the confirm configure message,
    then moves to the OPEN state

    If there are errors, handles them in the same way as in CONNECTING
    """
    assert state.type == StateType.CONFIGURING

    try:
        if await _check_send_task(state) == CheckResult.RESTART:
            return state

        if (result := await _check_read_task(state)).type == CheckResult.RESTART:
            return result.state

        if (result := await _check_cancel_requested(state)).type == CheckResult.RESTART:
            return result.state

        wait_cancel_requested = asyncio.create_task(state.cancel_requested.wait())
        await asyncio.wait(
            [
                state.read_task,
                *([state.send_task] if state.send_task is not None else []),
                wait_cancel_requested,
            ],
            return_when=asyncio.FIRST_COMPLETED,
        )
        wait_cancel_requested.cancel()
        return state
    except BaseException as e:
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
                    exception=e,
                )
                if isinstance(e, Exception)
                else ClosingRetryInformationCannotRetry(
                    type=ClosingRetryInformationType.CANNOT_RETRY,
                    tasks=state.tasks,
                    exception=e,
                )
            ),
        )


async def _check_send_task(state: StateConfiguring) -> CheckResult:
    if state.send_task is None or not state.send_task.done():
        return CheckResult.CONTINUE

    task = state.send_task
    state.send_task = None
    task.result()
    return CheckResult.RESTART


async def _check_read_task(state: StateConfiguring) -> CheckStateChangerResult:
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
        raise Exception(
            f"received unexpected message before confirm configure: {prefix}"
        )

    parsed_message = B2S_ConfirmConfigureParser.parse(prefix.flags, prefix.type, stream)
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
            received=DrainableAsyncioQueue(max_size=state.config.max_received),
            send_task=state.send_task,
            read_task=make_websocket_read_task(state.websocket),
        ),
    )


async def _check_cancel_requested(state: StateConfiguring) -> CheckStateChangerResult:
    if not state.cancel_requested.is_set():
        return CheckStateChangerResultContinue(type=CheckResult.CONTINUE)

    await _cleanup(state)
    return CheckStateChangerResultDone(
        type=CheckResult.RESTART,
        state=StateClosing(
            type=StateType.CLOSING,
            config=state.config,
            cancel_requested=state.cancel_requested,
            broadcaster=state.broadcaster,
            client_session=state.client_session,
            websocket=state.websocket,
            retry=ClosingRetryInformationCannotRetry(
                type=ClosingRetryInformationType.CANNOT_RETRY,
                tasks=state.tasks,
                exception=Exception("cancel requested"),
            ),
        ),
    )


async def _cleanup(state: StateConfiguring) -> None:
    if state.send_task is not None:
        state.send_task.cancel()
    state.read_task.cancel()


if TYPE_CHECKING:
    _: StateHandler = handle_configuring
