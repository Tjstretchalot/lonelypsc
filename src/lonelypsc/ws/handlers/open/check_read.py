import io
from typing import cast

from lonelypsp.stateful.parser import B2S_AnyMessageParser
from lonelypsp.stateful.parser_helpers import parse_b2s_message_prefix

from lonelypsc.client import PubSubError
from lonelypsc.types.websocket_message import WSMessageBytes
from lonelypsc.ws.check_result import CheckResult
from lonelypsc.ws.handlers.open.messages.generic import check_read_message
from lonelypsc.ws.handlers.util.read_from_websocket import make_websocket_read_task
from lonelypsc.ws.state import ReceivingState, StateOpen


def check_read_task(state: StateOpen) -> CheckResult:
    """Checks if the read task is done, if so, verifies it was successful and
    the payload makes sense, otherwise raises an exception

    Skips checking the read task if still processing the previous message
    """

    if (
        state.receiving is not None
        and state.receiving.type != ReceivingState.INCOMPLETE
    ):
        return CheckResult.CONTINUE

    if not state.read_task.done():
        return CheckResult.CONTINUE

    raw_result = state.read_task.result()
    if raw_result["type"] == "websocket.disconnect":
        raise PubSubError("broadcaster disconnected")
    if "bytes" not in raw_result:
        raise PubSubError("unexpected message type received")

    payload = cast(WSMessageBytes, raw_result)["bytes"]
    state.read_task = make_websocket_read_task(state.websocket)

    reader = io.BytesIO(payload)
    prefix = parse_b2s_message_prefix(reader)

    message = B2S_AnyMessageParser.parse(prefix.flags, prefix.type, reader)
    check_read_message(state, message)
    return CheckResult.RESTART
