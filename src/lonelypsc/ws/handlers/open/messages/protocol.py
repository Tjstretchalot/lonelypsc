from typing import Generic, Protocol, TypeVar

from lonelypsp.stateful.message import B2S_Message

from lonelypsc.ws.state import StateOpen

T = TypeVar("T", bound=B2S_Message, contravariant=True)


class MessageChecker(Generic[T], Protocol):
    """Protocol to ensure that we are being consistent with how functions within
    this folder are defined for handling messages received from the broadcaster
    """

    def __call__(self, state: StateOpen, message: T) -> None: ...
