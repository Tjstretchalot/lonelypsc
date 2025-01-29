import asyncio
import time
from typing import TYPE_CHECKING

from lonelypsp.auth.config import AuthResult
from lonelypsp.stateful.messages.enable_zstd_custom import B2S_EnableZstdCustom

from lonelypsc.client import PubSubError
from lonelypsc.ws.compressor import (
    CompressorPreparing,
    CompressorReady,
    CompressorState,
)
from lonelypsc.ws.handlers.open.messages.protocol import MessageChecker
from lonelypsc.ws.handlers.open.websocket_url import (
    make_for_receive_websocket_url_and_change_counter,
)
from lonelypsc.ws.state import (
    ReceivingAuthorizingSimple,
    ReceivingState,
    StateOpen,
)

try:
    import zstandard
except ImportError:
    ...


def _make_compressor(message: B2S_EnableZstdCustom) -> CompressorReady:
    zdict = zstandard.ZstdCompressionDict(message.dictionary)
    zdict.precompute_compress(level=message.compression_level)
    return CompressorReady(
        type=CompressorState.READY,
        identifier=message.identifier,
        level=message.compression_level,
        min_size=message.min_size,
        max_size=message.max_size,
        data=zdict,
        compressors=list(),
        decompressors=list(),
    )


async def _target(state: StateOpen, message: B2S_EnableZstdCustom) -> None:
    receive_url = make_for_receive_websocket_url_and_change_counter(state)
    auth_result = await state.config.is_stateful_enable_zstd_custom_allowed(
        url=receive_url, message=message, now=time.time()
    )
    if auth_result != AuthResult.OK:
        raise PubSubError(f"enable zstd custom authorization failed: {auth_result}")
    if not state.config.allow_compression:
        raise PubSubError("compression is disabled but a dictionary was received")
    if not state.config.allow_training_compression:
        raise PubSubError(
            "compression training is disabled but a dictionary was received"
        )

    state.compressors.add_compressor(
        CompressorPreparing(
            type=CompressorState.PREPARING,
            identifier=message.identifier,
            task=asyncio.create_task(asyncio.to_thread(_make_compressor, message)),
        )
    )


def check_enable_zstd_custom(state: StateOpen, message: B2S_EnableZstdCustom) -> None:
    """Handles the subscriber receiving the a message from the broadcaster that
    the broadcaster has created a dictionary for compressing the types of
    messages that have been sent over this websocket
    """
    assert state.receiving is None, "already have receiving task"
    state.receiving = ReceivingAuthorizingSimple(
        type=ReceivingState.AUTHORIZING_SIMPLE,
        task=asyncio.create_task(_target(state, message)),
    )


if TYPE_CHECKING:
    _: MessageChecker[B2S_EnableZstdCustom] = check_enable_zstd_custom
