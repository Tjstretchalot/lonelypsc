import asyncio
from typing import Any, List, Set

from lonelypsp.stateful.constants import BroadcasterToSubscriberStatefulMessageType

from lonelypsc.config.config import BroadcastersShuffler
from lonelypsc.util.errors import combine_multiple_exceptions
from lonelypsc.ws.internal_callbacks import (
    inform_internal_message,
)
from lonelypsc.ws.state import (
    ClosingRetryInformationCannotRetry,
    ClosingRetryInformationType,
    ClosingRetryInformationWantRetry,
    InternalMessageStateSent,
    InternalMessageStateType,
    OpenRetryInformationType,
    ReceivedMessageType,
    ReceivingState,
    RetryInformation,
    SendingState,
    StateClosing,
    StateOpen,
    StateType,
    TasksOnceOpen,
)


async def cleanup_open(
    state: StateOpen, exception: BaseException, *, irrecoverable: bool
) -> StateClosing:
    """Cleanup the open state, returning the closing state which will actually
    close the websocket connection
    """
    for task in state.compressors.get_compressor_tasks():
        task.cancel()

    if state.receiving is not None:
        if state.receiving.type == ReceivingState.INCOMPLETE:
            state.receiving.body.close()
            if state.receiving.authorization_task is not None:
                state.receiving.authorization_task.cancel()
        elif state.receiving.type == ReceivingState.AUTHORIZING:
            state.receiving.body.close()
            state.receiving.authorization_task.cancel()
        elif state.receiving.type == ReceivingState.WAITING_COMPRESSOR:
            state.receiving.compressed_body.close()
        else:
            if not state.receiving.task.cancel():
                try:
                    msg = state.receiving.task.result()
                    try:
                        state.received.put_nowait(msg)
                    except BaseException:
                        if msg.type == ReceivedMessageType.LARGE:
                            msg.stream.close()
                        raise
                except BaseException as e:
                    exception = combine_multiple_exceptions(
                        "error while cleaning up receiving decompressing",
                        [e],
                        context=exception,
                    )

    if state.sending is not None:
        state.sending.task.cancel()

    state.read_task.cancel()

    backgrounded_errors: List[BaseException] = []
    continuing_backgrounded: Set[asyncio.Task[Any]] = set()
    for task in state.backgrounded:
        if task.done():
            exc = task.exception()
            if exc is not None:
                backgrounded_errors.append(exc)
        else:
            continuing_backgrounded.add(task)

    if backgrounded_errors:
        irrecoverable = True
        exception = combine_multiple_exceptions(
            "backgrounded tasks found failed during cleanup, before canceling",
            backgrounded_errors,
            context=exception,
        )

    # received -> expected that caller kept a reference (see ws_client) for
    # cleanup, intentionally not cleaning/draining it here

    tasks_once_open = TasksOnceOpen(
        exact_subscriptions=state.exact_subscriptions,
        glob_subscriptions=state.glob_subscriptions,
        unsorted=state.management_tasks,
        unsent_notifications=state.unsent_notifications,
        resending_notifications=state.resending_notifications
        + list(state.sent_notifications),
    )

    if (
        state.sending is not None
        and state.sending.type == SendingState.INTERNAL_MESSAGE
        and not any(
            m.identifier == state.sending.internal_message.identifier
            for m in state.sent_notifications
        )
    ):
        tasks_once_open.resending_notifications.append(state.sending.internal_message)

    for internal_message in tasks_once_open.resending_notifications:
        # intentionally SENT to indicate that the file stream can be closed and
        # reopened once the message is about to be sent (which will inform it
        # that it is in RESENDING)
        #
        # relies on inform_internal_message not making unnecessary calls if the
        # state already matches
        #
        # also will get switched to DROPPED_SENT before scheduling the task if
        # not retrying, and if retrying will be swept while progressing the state
        inform_internal_message(
            internal_message,
            InternalMessageStateSent(type=InternalMessageStateType.SENT),
        )

    while state.expected_acks:
        ack = state.expected_acks.popleft()
        if (
            ack.type
            == BroadcasterToSubscriberStatefulMessageType.CONFIRM_SUBSCRIBE_EXACT
        ):
            tasks_once_open.exact_subscriptions.add(ack.topic)
        elif (
            ack.type
            == BroadcasterToSubscriberStatefulMessageType.CONFIRM_SUBSCRIBE_GLOB
        ):
            tasks_once_open.glob_subscriptions.add(ack.glob)
        elif (
            ack.type
            == BroadcasterToSubscriberStatefulMessageType.CONFIRM_UNSUBSCRIBE_EXACT
        ):
            tasks_once_open.exact_subscriptions.discard(ack.topic)
        elif (
            ack.type
            == BroadcasterToSubscriberStatefulMessageType.CONFIRM_UNSUBSCRIBE_GLOB
        ):
            tasks_once_open.glob_subscriptions.discard(ack.glob)

    retry: RetryInformation
    if state.retry.type == OpenRetryInformationType.TENTATIVE:
        retry = state.retry.continuation
    else:
        shuffler = BroadcastersShuffler(state.config.broadcasters)
        retry = RetryInformation(
            shuffler=shuffler, iteration=0, iterator=iter(shuffler)
        )

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
                retry=retry,
                tasks=tasks_once_open,
                exception=exception,
            )
            if not irrecoverable
            else ClosingRetryInformationCannotRetry(
                type=ClosingRetryInformationType.CANNOT_RETRY,
                tasks=tasks_once_open,
                exception=exception,
            )
        ),
        backgrounded=continuing_backgrounded,
    )
