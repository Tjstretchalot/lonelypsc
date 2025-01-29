import asyncio
import time
from typing import TYPE_CHECKING, Optional

from lonelypsp.auth.config import AuthResult
from lonelypsp.stateful.messages.enable_zstd_preset import B2S_EnableZstdPreset

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


async def _make_compressor(
    state: StateOpen, message: B2S_EnableZstdPreset
) -> CompressorReady:
    zdict: Optional[zstandard.ZstdCompressionDict] = None
    if message.identifier != 1:
        zdict = await state.config.get_compression_dictionary_by_id(
            message.identifier, level=message.compression_level
        )
        if zdict is None:
            raise PubSubError(f"unknown preset dictionary {message.identifier}")
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


async def _target(state: StateOpen, message: B2S_EnableZstdPreset) -> None:
    receive_url = make_for_receive_websocket_url_and_change_counter(state)
    auth_result = await state.config.is_stateful_enable_zstd_preset_allowed(
        url=receive_url, message=message, now=time.time()
    )
    if auth_result != AuthResult.OK:
        raise PubSubError(f"enable zstd custom authorization failed: {auth_result}")
    if message.identifier == 0:
        return

    if not state.config.allow_compression:
        raise PubSubError("compression is disabled but a preset was requested")

    state.compressors.add_compressor(
        CompressorPreparing(
            type=CompressorState.PREPARING,
            identifier=message.identifier,
            task=asyncio.create_task(_make_compressor(state, message)),
        )
    )


def check_enable_zstd_preset(state: StateOpen, message: B2S_EnableZstdPreset) -> None:
    """Handles the subscriber receiving the a message from the broadcaster that
    the broadcaster intends to use a specific preset dictionary for compressing
    the messages on this websocket
    """
    assert state.receiving is None, "already have receiving task"
    state.receiving = ReceivingAuthorizingSimple(
        type=ReceivingState.AUTHORIZING_SIMPLE,
        task=asyncio.create_task(_target(state, message)),
    )


if TYPE_CHECKING:
    _: MessageChecker[B2S_EnableZstdPreset] = check_enable_zstd_preset
