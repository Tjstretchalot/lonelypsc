import asyncio
import random
import sys
import time
from typing import Any, List, Set

import aiohttp

from lonelypsc.config.ws_config import WebsocketPubSubConfig
from lonelypsc.util.errors import combine_multiple_exceptions
from lonelypsc.ws.internal_callbacks import finalize_internal_callback
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
from lonelypsc.ws.websocket_connect_task import make_websocket_connect_task

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
    backgrounded: Set[asyncio.Task[Any]]
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
        backgrounded (Set[asyncio.Task[Any]]): the set of backgrounded tasks that
            if they fail it must be treated as irrecoverable, but whose result otherwise
            is unimportant. these are assumed to be like notification callbacks in the
            sense that they don't need cancellation and should be called even after an
            interrupt before shutting down

    Returns:
        the new state for the state machine

    Raises:
        BaseException: if no more retries can be attempted, raises the exception
            instead of directly returning StateClosed; this should eventually be
            caught and transition to StateClosed before raising the connection all
            the way to outside this library
    """

    if cancel_requested.is_set():
        await cleanup_tasks_and_raise_on_error(tasks, backgrounded, "cancel requested")
        return StateClosed(type=StateType.CLOSED)

    new_backgrounded = set()
    for bknd in backgrounded:
        if not bknd.done():
            new_backgrounded.add(bknd)
            continue

        if bknd.exception() is not None:
            await cleanup_tasks_and_raise(
                tasks,
                backgrounded,
                "backgrounded sweep failed",
                Exception("backgrounded task failed"),
            )

    try:
        next_broadcaster = next(retry.iterator)
        client_session = aiohttp.ClientSession()
        return StateConnecting(
            type=StateType.CONNECTING,
            config=config,
            client_session=client_session,
            websocket_task=make_websocket_connect_task(
                config, next_broadcaster, client_session
            ),
            cancel_requested=cancel_requested,
            broadcaster=next_broadcaster,
            retry=retry,
            tasks=tasks,
            backgrounded=new_backgrounded,
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
            backgrounded=new_backgrounded,
        )

    await cleanup_tasks_and_raise(
        tasks, new_backgrounded, "retries exhausted", exception
    )


async def cleanup_tasks_and_return_errors(
    tasks: TasksOnceOpen, backgrounded: Set[asyncio.Task[Any]]
) -> List[BaseException]:
    """Cleans up the given tasks, returning any errors that occurred"""
    cleanup_excs: List[BaseException] = []
    while tasks.resending_notifications:
        notif = tasks.resending_notifications.pop()
        backgrounded.add(
            asyncio.create_task(
                finalize_internal_callback(
                    notif.callback,
                    InternalMessageStateDroppedSent(
                        type=InternalMessageStateType.DROPPED_SENT
                    ),
                )
            )
        )

    for notif in tasks.unsent_notifications.drain():
        backgrounded.add(
            asyncio.create_task(
                finalize_internal_callback(
                    notif.callback,
                    InternalMessageStateDroppedUnsent(
                        type=InternalMessageStateType.DROPPED_UNSENT
                    ),
                )
            )
        )

    for bknd in backgrounded:
        try:
            await bknd
        except BaseException as e:
            cleanup_excs.append(e)

    return cleanup_excs


async def cleanup_tasks_and_raise_on_error(
    tasks: TasksOnceOpen, backgrounded: Set[asyncio.Task[Any]], message: str
) -> None:
    """Cleans up the given tasks and raises an exception if any errors occurred"""
    cleanup_excs = await cleanup_tasks_and_return_errors(tasks, backgrounded)

    if cleanup_excs:
        raise combine_multiple_exceptions(message, cleanup_excs)


async def cleanup_tasks_and_raise(
    tasks: TasksOnceOpen,
    backgrounded: Set[asyncio.Task[Any]],
    message: str,
    cause: BaseException,
) -> Never:
    """Cleans up the given tasks list and raises the given exception;
    if closing the tasks raises an exception, that exception is combined
    with the original exception and raised
    """
    cleanup_excs = await cleanup_tasks_and_return_errors(tasks, backgrounded)

    if cleanup_excs:
        raise combine_multiple_exceptions(
            message,
            cleanup_excs,
            context=cause,
        )

    raise cause
