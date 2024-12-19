from typing import TYPE_CHECKING

from lonelypsc.ws.handlers.protocol import StateHandler
from lonelypsc.ws.state import State, StateType


async def handle_open(state: State) -> State:
    """The core inner loop for the websocket client; processes incoming
    messages and sends outgoing messages in a deterministic order
    """
    assert state.type == StateType.OPEN
    raise NotImplementedError


if TYPE_CHECKING:
    _: StateHandler = handle_open
