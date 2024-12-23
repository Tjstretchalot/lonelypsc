from typing import List

from lonelypsc.util.errors import combine_multiple_exceptions
from lonelypsc.ws.handle_connection_failure import (
    cleanup_tasks_and_raise,
    handle_connection_failure,
)
from lonelypsc.ws.internal_callbacks import sweep_internal_message
from lonelypsc.ws.state import (
    ClosingRetryInformationCannotRetry,
    ClosingRetryInformationType,
    State,
    StateType,
)


async def handle_closing(state: State) -> State:
    """Finishes closing the given websocket then either retries with
    the next broadcaster, queues retrying with the next iteration of
    the broadcasters, or gives up
    """
    assert state.type == StateType.CLOSING

    cleanup_excs: List[BaseException] = []
    try:
        await state.websocket.close()
    except BaseException as e:
        cleanup_excs.append(e)

    try:
        await state.client_session.close()
    except BaseException as e:
        cleanup_excs.append(e)

    if state.retry.tasks is not None:
        sweep_excs: List[BaseException] = []
        for msg in state.retry.tasks.resending_notifications:
            try:
                sweep_internal_message(msg)
            except BaseException as e:
                sweep_excs.append(e)

        if sweep_excs:
            if state.retry.type == ClosingRetryInformationType.WANT_RETRY:
                state.retry = ClosingRetryInformationCannotRetry(
                    type=ClosingRetryInformationType.CANNOT_RETRY,
                    tasks=state.retry.tasks,
                    exception=combine_multiple_exceptions(
                        "failed to sweep internal messages before retrying",
                        sweep_excs,
                        context=state.retry.exception,
                    ),
                )
            else:
                cleanup_excs.extend(sweep_excs)

    if (
        cleanup_excs
        and state.retry.type == ClosingRetryInformationType.WANT_RETRY
        and not all(isinstance(e, Exception) for e in cleanup_excs)
    ):
        state.retry = ClosingRetryInformationCannotRetry(
            type=ClosingRetryInformationType.CANNOT_RETRY,
            tasks=state.retry.tasks,
            exception=combine_multiple_exceptions(
                "failed to close websocket before retrying",
                cleanup_excs,
                context=state.retry.exception,
            ),
        )

    if state.retry.type == ClosingRetryInformationType.WANT_RETRY:
        return await handle_connection_failure(
            config=state.config,
            cancel_requested=state.cancel_requested,
            retry=state.retry.retry,
            tasks=state.retry.tasks,
            exception=state.retry.exception,
            backgrounded=state.backgrounded,
        )

    if state.retry.tasks is not None:
        await cleanup_tasks_and_raise(
            state.retry.tasks, state.backgrounded, "closing", state.retry.exception
        )

    raise state.retry.exception
