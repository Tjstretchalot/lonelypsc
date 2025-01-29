import asyncio
import time
from typing import TYPE_CHECKING

from lonelypsp.auth.config import AuthResult
from lonelypsp.stateful.messages.disable_zstd_custom import B2S_DisableZstdCustom

from lonelypsc.client import PubSubError
from lonelypsc.ws.handlers.open.messages.protocol import MessageChecker
from lonelypsc.ws.handlers.open.websocket_url import (
    make_for_receive_websocket_url_and_change_counter,
)
from lonelypsc.ws.state import (
    ReceivingAuthorizingSimple,
    ReceivingState,
    StateOpen,
)


async def _target(state: StateOpen, message: B2S_DisableZstdCustom) -> None:
    receive_url = make_for_receive_websocket_url_and_change_counter(state)
    auth_result = await state.config.is_stateful_disable_zstd_custom_allowed(
        url=receive_url, message=message, now=time.time()
    )
    if auth_result != AuthResult.OK:
        raise PubSubError(f"disable zstd custom authorization failed: {auth_result}")
    try:
        state.compressors.remove_compressor(message.identifier)
    except KeyError:
        raise PubSubError(f"unknown compressor {message.identifier}")


def check_disable_zstd_custom(state: StateOpen, message: B2S_DisableZstdCustom) -> None:
    """Handles the subscriber receiving the a message from the broadcaster that
    it has discarded a custom dictionary it sent earlier, so we can discard it
    as well.
    """
    assert state.receiving is None, "already have receiving task"
    state.receiving = ReceivingAuthorizingSimple(
        type=ReceivingState.AUTHORIZING_SIMPLE,
        task=asyncio.create_task(_target(state, message)),
    )


if TYPE_CHECKING:
    _: MessageChecker[B2S_DisableZstdCustom] = check_disable_zstd_custom
