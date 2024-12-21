from enum import Enum, auto
from typing import TYPE_CHECKING, Literal, Union

from lonelypsp.compat import fast_dataclass

if TYPE_CHECKING:
    from lonelypsc.ws.state import State


class CheckResult(Enum):
    """The handler function is composed into parts; each part can either request
    we return back to the top of the handler function, continue to the next part
    of the handler function, or raise an exception to start the cleanup and disconnect
    process.

    Although the return type could be annotated as a boolean, for clarity
    we instead use an enum
    """

    RESTART = auto()
    """Return to the top of the handler function because we made progress"""

    CONTINUE = auto()
    """Continue to the next part of the handler function"""


@fast_dataclass
class CheckStateChangerResultContinue:
    """
    Generally only used as a union type when a check function may switch state
    types, which can never be done concurrently with another function that might
    do so
    """

    type: Literal[CheckResult.CONTINUE]
    """Indicates that nothing was changed and the handler should proceed to the next check,
    if any, otherwise wait for something to change and restart
    """


@fast_dataclass
class CheckStateChangerResultDone:
    """
    Generally only used if the check function might switch states, which can
    never be done concurrently with another function that might do so
    """

    type: Literal[CheckResult.RESTART]
    """Indicates that the check function changed something and the handler should return
    the new state.
    """
    state: "State"


CheckStateChangerResult = Union[
    CheckStateChangerResultContinue, CheckStateChangerResultDone
]
"""The result of a check function that might switch state types rather than just
potentially mutating the state without changing the type
"""
