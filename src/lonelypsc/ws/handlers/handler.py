from typing import TYPE_CHECKING, Dict

from lonelypsc.ws.handlers.closing import handle_closing
from lonelypsc.ws.handlers.configuring import handle_configuring
from lonelypsc.ws.handlers.connecting import handle_connecting
from lonelypsc.ws.handlers.protocol import StateHandler
from lonelypsc.ws.handlers.waiting_retry import handle_waiting_retry
from lonelypsc.ws.state import State, StateType

_handlers: Dict[StateType, StateHandler] = {
    StateType.CONNECTING: handle_connecting,
    StateType.CONFIGURING: handle_configuring,
    StateType.WAITING_RETRY: handle_waiting_retry,
    StateType.CLOSING: handle_closing,
}


async def handle_any(state: State) -> State:
    """Handle any state by delegating to the appropriate handler. This will raise
    a KeyError for the CLOSED state, which does not need any further handling.

    Raises KeyError if no handler is found for the state type.
    """
    return await _handlers[state.type](state)


if TYPE_CHECKING:
    _: StateHandler = handle_any
