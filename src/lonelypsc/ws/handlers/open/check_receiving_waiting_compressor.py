import asyncio
import hashlib
import io
import os
import tempfile
from typing import IO, cast

from lonelypsp.stateful.messages.receive_stream import B2S_ReceiveStreamStartCompressed
from lonelypsp.stateful.parser_helpers import read_exact

from lonelypsc.client import PubSubError
from lonelypsc.types.sync_io import SyncIOBaseLikeIO
from lonelypsc.ws.check_result import CheckResult
from lonelypsc.ws.compressor import CompressorReady, CompressorState
from lonelypsc.ws.handlers.open.compressor_utils import (
    DecompressorReservation,
)
from lonelypsc.ws.state import (
    ReceivedLargeMessage,
    ReceivedMessage,
    ReceivedMessageType,
    ReceivedSmallMessage,
    ReceivingDecompressing,
    ReceivingState,
    StateOpen,
)


def _decompress(
    compressed_body: SyncIOBaseLikeIO,
    first: B2S_ReceiveStreamStartCompressed,
    compressor: CompressorReady,
    spool_size: int,
    decompression_max_window_size: int,
) -> ReceivedMessage:
    try:
        decompressed = tempfile.SpooledTemporaryFile(max_size=spool_size)
        try:
            with DecompressorReservation(
                compressor,
                max_window_size=decompression_max_window_size,
                max_decompressors=5,
            ) as decompressor, decompressor.stream_reader(
                cast(IO[bytes], compressed_body)
            ) as reader:
                read_so_far = 0
                hasher = hashlib.sha512()
                while True:
                    chunk = reader.read(io.DEFAULT_BUFFER_SIZE)
                    if not chunk:
                        break

                    if read_so_far + len(chunk) > first.decompressed_length:
                        raise PubSubError("decompressed data is longer than expected")

                    decompressed.write(chunk)
                    read_so_far += len(chunk)
                    hasher.update(chunk)

                if read_so_far != first.decompressed_length:
                    raise PubSubError("decompressed data is shorter than expected")

                decompressed_sha512 = hasher.digest()

            compressed_body.close()
            decompressed.seek(0, os.SEEK_SET)
            decompressed_msg: ReceivedMessage
            if read_so_far < spool_size:
                decompressed_msg = ReceivedSmallMessage(
                    type=ReceivedMessageType.SMALL,
                    topic=first.topic,
                    data=read_exact(decompressed, first.decompressed_length),
                    sha512=decompressed_sha512,
                )
                decompressed.close()
            else:
                decompressed_msg = ReceivedLargeMessage(
                    type=ReceivedMessageType.LARGE,
                    topic=first.topic,
                    stream=decompressed,
                    sha512=decompressed_sha512,
                )

            return decompressed_msg
        except BaseException:
            decompressed.close()
            raise
    finally:
        compressed_body.close()


def check_receiving_waiting_compressor(state: StateOpen) -> CheckResult:
    """
    Tries to move receiving notify state `WAITING_COMPRESSOR` to `DECOMPRESSING`

    Raises an error if it is not possible to decompress the message; since the
    subscriber is not checking the read_task, it won't parse `DISABLE_ZSTD_CUSTOM`
    that were sent after the `RECEIVE_STREAM`, so if its not decompressable it was
    not decompressible when it was received (i.e., that error is not a race condition)
    """
    if (
        state.receiving is None
        or state.receiving.type != ReceivingState.WAITING_COMPRESSOR
    ):
        return CheckResult.CONTINUE

    try:
        compressor = state.compressors.get_for_decompression(
            state.receiving.first.compressor_id
        )
    except KeyError:
        raise PubSubError(f"unknown compressor {state.receiving.first.compressor_id}")

    if compressor.type == CompressorState.PREPARING:
        return CheckResult.CONTINUE

    state.receiving = ReceivingDecompressing(
        type=ReceivingState.DECOMPRESSING,
        task=asyncio.create_task(
            asyncio.to_thread(
                _decompress,
                state.receiving.compressed_body,
                state.receiving.first,
                compressor,
                state.config.max_websocket_message_size or (2**64 - 1),
                state.config.decompression_max_window_size,
            )
        ),
    )
    return CheckResult.RESTART
