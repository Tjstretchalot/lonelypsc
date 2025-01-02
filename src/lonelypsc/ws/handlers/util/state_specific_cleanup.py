import asyncio
from typing import Generic, Protocol, TypeVar

from lonelypsp.util.bounded_deque import BoundedDequeFullError

from lonelypsc.client import PubSubIrrecoverableError
from lonelypsc.util.errors import combine_multiple_exceptions
from lonelypsc.ws.state import State

T = TypeVar("T")
T_contra = TypeVar("T_contra", contravariant=True)


class GenericStateHandler(Generic[T_contra], Protocol):
    """Like a state handler but with the incoming state type already restricted"""

    async def __call__(self, state: T_contra, /) -> State: ...


class RecoveryStateHandler(Generic[T_contra], Protocol):
    """Like a state handler but given the cause that it was run"""

    async def __call__(self, state: T_contra, /, *, cause: BaseException) -> State: ...


async def handle_via_composition(
    state: T,
    *,
    core: GenericStateHandler[T],
    recover: RecoveryStateHandler[T],
    shutdown: RecoveryStateHandler[T],
) -> State:
    """Returns the new state to move to using the standard error handling
    approach.

    Args:
        core (StateHandler for T): the handler that will return the happy path state
            to move to or raise an error
        recovery (StateHandler with exception for T): the handler that will
            cleanup any state-specific resources optimistically such that it can
            move onto the next state, raising an error if it is now
            irrecoverable
        shutdown (StateHandler with exception  for T): the handler that will cleanup
            any state-specific resources pessimistically and eventually bubble the error
            up (possible after going through CLOSING)

    Returns:
        the next state
    """
    exception: BaseException
    try:
        return await core(state)
    except (
        NotImplementedError,
        AssertionError,
        PubSubIrrecoverableError,
        asyncio.CancelledError,
        KeyboardInterrupt,
        # full queues typically cause dropped state, e.g., what we are meant
        # to be subscribed to, which can't be recovered from
        BoundedDequeFullError,
        asyncio.QueueFull,
    ) as e:
        recoverable = False
        exception = e
    except BaseException as be:
        recoverable = True
        exception = be

    if recoverable:
        try:
            return await recover(state, cause=exception)
        except BaseException as be:
            recoverable = False
            exception = combine_multiple_exceptions(
                "while recovering from state", [be], context=exception
            )

    return await shutdown(state, cause=exception)
