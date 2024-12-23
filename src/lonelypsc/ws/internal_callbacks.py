import asyncio
from contextlib import asynccontextmanager
from typing import AsyncGenerator, List, Union

from lonelypsc.util.errors import combine_multiple_exceptions
from lonelypsc.ws.state import (
    InternalMessage,
    InternalMessageState,
    InternalMessageStateAcknowledged,
    InternalMessageStateAndCallback,
    InternalMessageStateDroppedSent,
    InternalMessageStateDroppedUnsent,
    InternalMessageStateResending,
    InternalMessageStateSent,
    InternalMessageStateType,
    InternalMessageStateUnsent,
)


def _is_intermediate_state(state: InternalMessageState) -> bool:
    return state.type in (
        InternalMessageStateType.UNSENT,
        InternalMessageStateType.SENT,
        InternalMessageStateType.RESENDING,
    )


def sweep_internal_message(message: InternalMessage) -> None:
    """The active management referenced in `inform_internal_message`; checks on the
    progress of the state callback, raising an exception if it completed with an
    error and otherwise moving it closer to the best known state for the message.
    """
    if message.callback.task is not None and message.callback.task.done():
        finished = message.callback.task
        message.callback.task = None
        finished.result()  # raises exception if there was one

    if message.callback.queued is not None and message.callback.task is None:
        assert _is_intermediate_state(message.callback.state)
        message.callback.state = message.callback.queued
        message.callback.queued = None
        message.callback.task = asyncio.create_task(
            message.callback.callback(message.callback.state)
        )


def inform_internal_message(
    message: InternalMessage,
    state: Union[
        InternalMessageStateUnsent,
        InternalMessageStateSent,
        InternalMessageStateResending,
    ],
) -> None:
    """Queues a task to be scheduled to inform the callback of the new state of the
    message.

    In order for this to work, the internal message must still be actively
    monitored until it reaches a final state by continuously calling
    sweep_internal_message until the final state is reached, at which point
    finalize_internal_callback can be scheduled on the event loop and tracked to
    completion

    Args:
        message (InternalMessage): the message to inform
        state (InternalMessageState): the non-final state to inform the message of
    """

    if message.callback.state.type == state.type:
        message.callback.queued = None
        return

    message.callback.queued = state


@asynccontextmanager
async def readable_internal_message(
    message: InternalMessage,
) -> AsyncGenerator[None, None]:
    """Acts as an asynchronous context manager that, while it is active,
    the messages stream is readable (either in UNSENT or RESENDING)

    Raises an exception if this is not possible through valid transitions
    (i.e., the message is not in UNSENT, SENT, or RESENDING)
    """

    message.callback.queued = None

    while True:
        assert _is_intermediate_state(message.callback.state), "final state reached"
        if message.callback.task is None:
            break

        await message.callback.task
        message.callback.queued = None
        sweep_internal_message(message)

    future: asyncio.Future[None] = asyncio.get_running_loop().create_future()

    async def _task_target() -> None:
        await future

    try:
        message.callback.task = asyncio.create_task(_task_target())

        if message.callback.state.type == InternalMessageStateType.SENT:
            message.callback.state = InternalMessageStateResending(
                type=InternalMessageStateType.RESENDING,
            )
            await message.callback.callback(message.callback.state)

        assert message.callback.state.type in (
            InternalMessageStateType.UNSENT,
            InternalMessageStateType.RESENDING,
        )
        message.callback.queued = None
        yield
    finally:
        future.set_result(None)


async def finalize_internal_callback(
    callback: InternalMessageStateAndCallback,
    final_state: Union[
        InternalMessageStateAcknowledged,
        InternalMessageStateDroppedUnsent,
        InternalMessageStateDroppedSent,
    ],
) -> None:
    """Returns a coroutine that runs until the callback has been notified of
    its final state; consumes errors on the way, raising them at the end.

    Args:
        callback (InternalMessageStateAndCallback): the callback to finalize
        final_state (InternalMessageState (final)): the final state of the message
    """

    exceptions: List[BaseException] = []
    while callback.task is not None:
        if callback.task.done():
            if (exc := callback.task.exception()) is not None:
                exceptions.append(exc)
            callback.task = None
            break

        try:
            await callback.task
            # NOTE: callback.task may have changed here as we may not have
            # been the first thing scheduled
        except BaseException:
            ...  # will see it in the next iteration if still relevant

    assert _is_intermediate_state(callback.state), "final state already reached"
    callback.state = final_state  # prevents future tasks from being scheduled

    try:
        await callback.callback(final_state)
    except BaseException as e:
        exceptions.append(e)

    if exceptions:
        raise combine_multiple_exceptions("error while finalizing callback", exceptions)
