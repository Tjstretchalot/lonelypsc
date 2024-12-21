import asyncio
from typing import TYPE_CHECKING

from lonelypsp.stateful.messages.enable_zstd_custom import B2S_EnableZstdCustom

from lonelypsc.client import PubSubError
from lonelypsc.ws.compressor import (
    CompressorPreparing,
    CompressorReady,
    CompressorState,
)
from lonelypsc.ws.handlers.open.messages.protocol import MessageChecker
from lonelypsc.ws.state import (
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


def check_enable_zstd_custom(state: StateOpen, message: B2S_EnableZstdCustom) -> None:
    """Handles the subscriber receiving the a message from the broadcaster that
    the broadcaster has created a dictionary for compressing the types of
    messages that have been sent over this websocket
    """
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


if TYPE_CHECKING:
    _: MessageChecker[B2S_EnableZstdCustom] = check_enable_zstd_custom
