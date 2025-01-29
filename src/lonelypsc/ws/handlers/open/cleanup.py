import os
from typing import List

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
    InternalMessageType,
    OpenRetryInformationType,
    ReceivedMessageType,
    ReceivingState,
    RetryInformation,
    SendingState,
    State,
    StateClosing,
    StateOpen,
    StateType,
    TasksOnceOpen,
)


async def recover_open(state: StateOpen, /, *, cause: BaseException) -> State:
    """Recovery function for OPEN; cleans up state-specific resources and
    tries to move towards retrying, raising an error if one occurs during
    cleanup such that recovery is no longer possible
    """
    _cancel_tasks(state)
    bknd_errors = _sweep_backgrounded(state)
    if bknd_errors:
        raise combine_multiple_exceptions("backgrounded tasks failed", bknd_errors)

    tasks_once_open = _build_tasks(state)

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
        retry=ClosingRetryInformationWantRetry(
            type=ClosingRetryInformationType.WANT_RETRY,
            retry=retry,
            tasks=tasks_once_open,
            exception=cause,
        ),
        backgrounded=state.backgrounded,
    )


def _cancel_tasks(state: StateOpen) -> None:
    """Cancels tasks that are websocket dependent on the state, with
    special handling for the receiving information which may not have
    a single cancelable task for cleanup
    """
    for task in state.compressors.get_compressor_tasks():
        task.cancel()

    if state.receiving is not None:
        try:
            if state.receiving.type == ReceivingState.INCOMPLETE:
                state.receiving.body.close()
                if state.receiving.authorization_task is not None:
                    state.receiving.authorization_task.cancel()
            elif state.receiving.type == ReceivingState.AUTHORIZING:
                state.receiving.body.close()
                state.receiving.authorization_task.cancel()
            elif state.receiving.type == ReceivingState.AUTHORIZING_MISSED:
                state.receiving.authorization_task.cancel()
            elif state.receiving.type == ReceivingState.AUTHORIZING_SIMPLE:
                state.receiving.task.cancel()
            elif state.receiving.type == ReceivingState.WAITING_COMPRESSOR:
                state.receiving.compressed_body.close()
            else:
                if not state.receiving.task.cancel():
                    msg = state.receiving.task.result()
                    try:
                        state.received.put_nowait(msg)
                    except BaseException:
                        if msg.type == ReceivedMessageType.LARGE:
                            msg.stream.close()
                        raise
        finally:
            state.receiving = None

    if state.sending is not None:
        state.sending.task.cancel()

    state.read_task.cancel()


def _sweep_backgrounded(state: StateOpen) -> List[BaseException]:
    """Removes done tasks from the states backgrounded set and returns
    any exceptions that were found
    """
    new_backgrounded = set()
    errors: List[BaseException] = []
    for task in state.backgrounded:
        if task.done():
            exc = task.exception()
            if exc is not None:
                errors.append(exc)
        else:
            new_backgrounded.add(task)
    state.backgrounded = new_backgrounded
    return errors


def _build_tasks(state: StateOpen) -> TasksOnceOpen:
    """Builds the tasks that need to be continued with the next connection or
    cleaned up in CLOSING, mutating the state in such a way that this can be
    called multiple times for the same result
    """
    while state.expected_acks:
        ack = state.expected_acks.popleft()
        if (
            ack.type
            == BroadcasterToSubscriberStatefulMessageType.CONFIRM_SUBSCRIBE_EXACT
        ):
            state.exact_subscriptions.add(ack.topic)
        elif (
            ack.type
            == BroadcasterToSubscriberStatefulMessageType.CONFIRM_SUBSCRIBE_GLOB
        ):
            state.glob_subscriptions.add(ack.glob)
        elif (
            ack.type
            == BroadcasterToSubscriberStatefulMessageType.CONFIRM_UNSUBSCRIBE_EXACT
        ):
            state.exact_subscriptions.discard(ack.topic)
        elif (
            ack.type
            == BroadcasterToSubscriberStatefulMessageType.CONFIRM_UNSUBSCRIBE_GLOB
        ):
            state.glob_subscriptions.discard(ack.glob)

    if (
        state.sending is not None
        and state.sending.type == SendingState.INTERNAL_MESSAGE
        and not any(
            m.identifier == state.sending.internal_message.identifier
            for m in state.sent_notifications
        )
    ):
        if state.sending.internal_message.type == InternalMessageType.LARGE:
            state.sending.internal_message.stream.seek(0, os.SEEK_SET)
        state.resending_notifications.append(state.sending.internal_message)
        state.sending = None

    tasks_once_open = TasksOnceOpen(
        exact_subscriptions=state.exact_subscriptions,
        glob_subscriptions=state.glob_subscriptions,
        unsorted=state.management_tasks,
        unsent_notifications=state.unsent_notifications,
        resending_notifications=state.resending_notifications
        + list(state.sent_notifications),
    )

    for internal_message in tasks_once_open.resending_notifications:
        inform_internal_message(
            internal_message,
            InternalMessageStateSent(type=InternalMessageStateType.SENT),
        )

    return tasks_once_open


async def shutdown_open(state: StateOpen, /, *, cause: BaseException) -> StateClosing:
    """Shutdown function for OPEN; cleans up state-specific resources and
    moves towards the CLOSED state. Doesn't raise errors
    """
    _cancel_tasks(state)
    bknd_errors = _sweep_backgrounded(state)
    if bknd_errors:
        cause = combine_multiple_exceptions(
            "irrecoverable error + backgrounded errors", bknd_errors
        )

    tasks_to_cleanup = _build_tasks(state)

    return StateClosing(
        type=StateType.CLOSING,
        config=state.config,
        cancel_requested=state.cancel_requested,
        broadcaster=state.broadcaster,
        client_session=state.client_session,
        websocket=state.websocket,
        retry=ClosingRetryInformationCannotRetry(
            type=ClosingRetryInformationType.CANNOT_RETRY,
            tasks=tasks_to_cleanup,
            exception=cause,
        ),
        backgrounded=state.backgrounded,
    )
