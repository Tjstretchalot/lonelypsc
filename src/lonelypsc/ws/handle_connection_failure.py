import asyncio
import random
import sys
import time
from typing import List

from lonelypsc.config.ws_config import WebsocketPubSubConfig
from lonelypsc.util.errors import combine_multiple_exceptions
from lonelypsc.ws.state import (
    InternalMessageStateDroppedSent,
    InternalMessageStateDroppedUnsent,
    InternalMessageStateType,
    RetryInformation,
    State,
    StateClosed,
    StateConnecting,
    StateType,
    StateWaitingRetry,
    TasksOnceOpen,
)

if sys.version_info >= (3, 11):
    from typing import Never
else:
    from typing import NoReturn as Never


async def handle_connection_failure(
    *,
    config: WebsocketPubSubConfig,
    cancel_requested: asyncio.Event,
    retry: RetryInformation,
    tasks: TasksOnceOpen,
    exception: BaseException,
) -> State:
    """Handles a connection failure by either moving to the next broadcaster,
    moving to WAITING_RETRY, or moving to CLOSED.

    This assumes:

    - all necessary cleanup for the previous connection is already completed
    - suppressing the exception if retrying is not an issue

    Args:
        config (WebsocketPubSubConfig): The configuration for the subscriber
        cancel_requested (asyncio.Event): whether the state machine is trying to
            gracefully shutdown (set) or not (not set)
        retry (RetryInformation): how to determine the next broadcaster
        tasks (TasksOnceOpen): the tasks that need to be performed if a broadcaster
            can be reached or canceled if moving to closed
        exception (BaseException): the exception that caused the connection failure;
            will be included somewhere in the error if no retries are possible

    Returns:
        the new state for the state machine

    Raises:
        BaseException: if no more retries can be attempted, raises the exception
            instead of directly returning StateClosed; this should eventually be
            caught and transition to StateClosed before raising the connection all
            the way to outside this library
    """

    if cancel_requested.is_set():
        await cleanup_tasks_and_raise_on_error(tasks, "cancel requested")
        return StateClosed(type=StateType.CLOSED)

    try:
        next_broadcaster = next(retry.iterator)
        return StateConnecting(
            type=StateType.CONNECTING,
            config=config,
            cancel_requested=cancel_requested,
            broadcaster=next_broadcaster,
            retry=retry,
            tasks=tasks,
        )
    except StopIteration:
        ...

    if retry.iteration < config.outgoing_initial_connect_retries:
        retry.iteration += 1
        retry.iterator = iter(retry.shuffler)

        return StateWaitingRetry(
            type=StateType.WAITING_RETRY,
            config=config,
            cancel_requested=cancel_requested,
            retry=retry,
            tasks=tasks,
            retry_at=time.time() + (2 ** (retry.iteration - 1) + random.random()),
        )

    await cleanup_tasks_and_raise(tasks, "retries exhausted", exception)


async def cleanup_tasks_and_return_errors(tasks: TasksOnceOpen) -> List[BaseException]:
    """Cleans up the given tasks, returning any errors that occurred"""
    cleanup_excs: List[BaseException] = []
    while tasks.resending_notifications:
        notif = tasks.resending_notifications.pop()
        try:
            await notif.callback(
                InternalMessageStateDroppedSent(
                    type=InternalMessageStateType.DROPPED_SENT
                )
            )
        except BaseException as e:
            cleanup_excs.append(e)

    for notif in tasks.unsent_notifications.drain():
        try:
            await notif.callback(
                InternalMessageStateDroppedUnsent(
                    type=InternalMessageStateType.DROPPED_UNSENT
                )
            )
        except BaseException as e:
            cleanup_excs.append(e)

    return cleanup_excs


async def cleanup_tasks_and_raise_on_error(tasks: TasksOnceOpen, message: str) -> None:
    """Cleans up the given tasks list and raises an exception if any errors occurred"""
    cleanup_excs = await cleanup_tasks_and_return_errors(tasks)

    if cleanup_excs:
        raise combine_multiple_exceptions(message, cleanup_excs)


async def cleanup_tasks_and_raise(
    tasks: TasksOnceOpen, message: str, cause: BaseException
) -> Never:
    """Cleans up the given tasks list and raises the given exception;
    if closing the tasks raises an exception, that exception is combined
    with the original exception and raised
    """
    cleanup_excs = await cleanup_tasks_and_return_errors(tasks)

    if cleanup_excs:
        raise combine_multiple_exceptions(
            "failed to cleanup tasks",
            cleanup_excs,
            context=cause,
        )

    to_raise: BaseException
    if isinstance(cause, Exception):
        to_raise = Exception(message)
    else:
        to_raise = BaseException(message)

    to_raise.__cause__ = cause
    raise to_raise
