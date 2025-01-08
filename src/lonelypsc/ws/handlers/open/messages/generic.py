from typing import Dict, cast

from lonelypsp.stateful.constants import BroadcasterToSubscriberStatefulMessageType
from lonelypsp.stateful.message import B2S_Message

from lonelypsc.client import PubSubError
from lonelypsc.ws.handlers.open.messages.confirm_notify import check_confirm_notify
from lonelypsc.ws.handlers.open.messages.confirm_subscribe_exact import (
    check_confirm_subscribe_exact,
)
from lonelypsc.ws.handlers.open.messages.confirm_subscribe_glob import (
    check_confirm_subscribe_glob,
)
from lonelypsc.ws.handlers.open.messages.confirm_unsubscribe_exact import (
    check_confirm_unsubscribe_exact,
)
from lonelypsc.ws.handlers.open.messages.confirm_unsubscribe_glob import (
    check_confirm_unsubscribe_glob,
)
from lonelypsc.ws.handlers.open.messages.continue_notify import check_continue_notify
from lonelypsc.ws.handlers.open.messages.disable_zstd_custom import (
    check_disable_zstd_custom,
)
from lonelypsc.ws.handlers.open.messages.enable_zstd_custom import (
    check_enable_zstd_custom,
)
from lonelypsc.ws.handlers.open.messages.enable_zstd_preset import (
    check_enable_zstd_preset,
)
from lonelypsc.ws.handlers.open.messages.missed import check_missed
from lonelypsc.ws.handlers.open.messages.protocol import MessageChecker
from lonelypsc.ws.handlers.open.messages.receive_stream import check_receive_stream
from lonelypsc.ws.state import StateOpen

_handlers = cast(
    Dict[
        BroadcasterToSubscriberStatefulMessageType,
        MessageChecker[B2S_Message],  # constrained further by the key
    ],
    {
        BroadcasterToSubscriberStatefulMessageType.CONFIRM_NOTIFY: check_confirm_notify,
        BroadcasterToSubscriberStatefulMessageType.CONFIRM_SUBSCRIBE_EXACT: check_confirm_subscribe_exact,
        BroadcasterToSubscriberStatefulMessageType.CONFIRM_SUBSCRIBE_GLOB: check_confirm_subscribe_glob,
        BroadcasterToSubscriberStatefulMessageType.CONFIRM_UNSUBSCRIBE_EXACT: check_confirm_unsubscribe_exact,
        BroadcasterToSubscriberStatefulMessageType.CONFIRM_UNSUBSCRIBE_GLOB: check_confirm_unsubscribe_glob,
        BroadcasterToSubscriberStatefulMessageType.CONTINUE_NOTIFY: check_continue_notify,
        BroadcasterToSubscriberStatefulMessageType.DISABLE_ZSTD_CUSTOM: check_disable_zstd_custom,
        BroadcasterToSubscriberStatefulMessageType.ENABLE_ZSTD_CUSTOM: check_enable_zstd_custom,
        BroadcasterToSubscriberStatefulMessageType.ENABLE_ZSTD_PRESET: check_enable_zstd_preset,
        BroadcasterToSubscriberStatefulMessageType.MISSED: check_missed,
        BroadcasterToSubscriberStatefulMessageType.RECEIVE_STREAM: check_receive_stream,
    },
)


def check_read_message(state: StateOpen, message: B2S_Message) -> None:
    """Handles receiving the given message from the broadcaster in the given state,
    raising an exception if it does not make sense

    Does not support RECEIVE_STREAM which has a slightly different function signature
    """
    handler = _handlers.get(message.type)
    if handler is None:
        raise PubSubError(f"unexpected message type: {message.type}")
    handler(state, message)
