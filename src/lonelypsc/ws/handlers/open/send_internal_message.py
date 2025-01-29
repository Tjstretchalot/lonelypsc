import asyncio
import hashlib
import io
import os
import tempfile
import time
from typing import Optional, Union

from lonelypsp.stateful.constants import (
    BroadcasterToSubscriberStatefulMessageType,
    SubscriberToBroadcasterStatefulMessageType,
)
from lonelypsp.stateful.messages.confirm_notify import B2S_ConfirmNotify
from lonelypsp.stateful.messages.continue_notify import B2S_ContinueNotify
from lonelypsp.stateful.messages.notify import (
    S2B_NotifyCompressed,
    S2B_NotifyUncompressed,
    serialize_s2b_notify,
)
from lonelypsp.stateful.messages.notify_stream import (
    S2B_NotifyStreamContinuation,
    S2B_NotifyStreamStartCompressed,
    S2B_NotifyStreamStartUncompressed,
    serialize_s2b_notify_stream,
)
from lonelypsp.stateful.parser_helpers import read_exact

from lonelypsc.types.sync_io import SyncReadableBytesIO
from lonelypsc.ws.compressor import CompressorReady, CompressorState
from lonelypsc.ws.handlers.open.compressor_utils import reserve_compressor
from lonelypsc.ws.internal_callbacks import (
    inform_internal_message,
    readable_internal_message,
)
from lonelypsc.ws.state import (
    InternalLargeMessage,
    InternalMessage,
    InternalMessageStateSent,
    InternalMessageStateType,
    InternalMessageType,
    InternalSmallMessage,
    StateOpen,
)


async def send_internal_message(
    state: StateOpen, message: Union[InternalSmallMessage, InternalLargeMessage]
) -> None:
    """Expected to be run in the sending.task; sends the given internal
    message over the websocket, adding expected acks as appropriate

    Informs the callback while holding the read lock for the message, and
    queues it to be set to SENT
    """
    async with readable_internal_message(message):
        # will occur only once we exit the context manager
        inform_internal_message(
            message,
            InternalMessageStateSent(
                type=InternalMessageStateType.SENT,
            ),
        )
        if message.type == InternalMessageType.SMALL:
            return await send_internal_small_message(state, message)
        return await send_internal_large_message(state, message)


async def send_internal_small_message(
    state: StateOpen, message: InternalSmallMessage
) -> None:
    """Expected to be run in the sending.task; sends a message which is
    entirely in memory over the websocket, adding expected acks as
    appropriate.

    WARN: does not inform the message callback
    """

    compressor = state.compressors.get_for_compression(len(message.data))
    if compressor is None:
        return await send_internal_small_message_uncompressed(state, message)

    if compressor.type == CompressorState.PREPARING:
        compressor = await compressor.task

    await send_internal_small_message_compressed(state, message, compressor=compressor)


async def send_internal_large_message(
    state: StateOpen, message: InternalLargeMessage
) -> None:
    """Expected to be run in the sending.task; sends a message which is
    too large to hold in memory over the websocket, adding expected acks as
    appropriate.

    Note that this will detect and optimize (reduce python<->c calls) the case
    where the uncompressed payload is too large to hold in memory but the
    compressed payload is small enough

    WARN: does not inform the message callback
    """
    assert message.stream.tell() == 0, "stream must be at start"
    compressor = state.compressors.get_for_compression(message.length)
    if compressor is None:
        return await send_internal_large_message_uncompressed(state, message)

    if compressor.type == CompressorState.PREPARING:
        compressor = await compressor.task

    await send_internal_large_message_compressed(state, message, compressor=compressor)


async def send_internal_small_message_uncompressed(
    state: StateOpen, message: InternalSmallMessage, /
) -> None:
    """Sends the given internal small message uncompressed; will use NOTIFY
    if possible, otherwise NOTIFY_STREAM

    WARN: does not inform the message callback
    """
    tracing = b""  # TODO: tracing

    authorization = await state.config.authorize_notify(
        tracing=tracing,
        topic=message.topic,
        identifier=message.identifier,
        message_sha512=message.sha512,
        now=time.time(),
    )

    if (
        state.config.max_websocket_message_size is None
        or len(message.data) + 128 < state.config.max_websocket_message_size
    ):
        as_simple_message = serialize_s2b_notify(
            S2B_NotifyUncompressed(
                type=SubscriberToBroadcasterStatefulMessageType.NOTIFY,
                authorization=authorization,
                tracing=tracing,
                identifier=message.identifier,
                compressor_id=None,
                topic=message.topic,
                verified_uncompressed_sha512=message.sha512,
                uncompressed_message=message.data,
            ),
            minimal_headers=state.config.websocket_minimal_headers,
        )
        if (
            state.config.max_websocket_message_size is None
            or len(as_simple_message) < state.config.max_websocket_message_size
        ):
            state.expected_acks.append(
                B2S_ConfirmNotify(
                    type=BroadcasterToSubscriberStatefulMessageType.CONFIRM_NOTIFY,
                    identifier=message.identifier,
                    subscribers=-1,
                    tracing=b"",
                    authorization=None,
                )
            )
            state.sent_notifications.append(message)
            await state.websocket.send_bytes(as_simple_message)
            return

    await send_notify_stream_given_first_headers(
        state=state,
        stream=io.BytesIO(message.data),
        length=len(message.data),
        identifier=message.identifier,
        topic=message.topic,
        sha512=message.sha512,
        first_headers=serialize_s2b_notify_stream(
            S2B_NotifyStreamStartUncompressed(
                type=SubscriberToBroadcasterStatefulMessageType.NOTIFY_STREAM,
                authorization=authorization,
                tracing=tracing,
                identifier=message.identifier,
                part_id=None,
                topic=message.topic,
                compressor_id=None,
                uncompressed_length=len(message.data),
                unverified_uncompressed_sha512=message.sha512,
                payload=b"",
            ),
            minimal_headers=state.config.websocket_minimal_headers,
        ),
        msg=message,
    )


async def send_internal_large_message_uncompressed(
    state: StateOpen,
    message: InternalLargeMessage,
    /,
    *,
    authorization: Optional[str] = None,
) -> None:
    """Sends the given large message uncompressed, using NOTIFY_STREAM

    WARN: does not inform the message callback
    """
    if authorization is None:
        authorization = await state.config.authorize_notify(
            tracing=b"",  # TODO: traicng
            topic=message.topic,
            identifier=message.identifier,
            message_sha512=message.sha512,
            now=time.time(),
        )

    await send_notify_stream_given_first_headers(
        state=state,
        stream=message.stream,
        length=message.length,
        identifier=message.identifier,
        topic=message.topic,
        sha512=message.sha512,
        first_headers=serialize_s2b_notify_stream(
            S2B_NotifyStreamStartUncompressed(
                type=SubscriberToBroadcasterStatefulMessageType.NOTIFY_STREAM,
                authorization=authorization,
                tracing=b"",  # TODO: tracing
                identifier=message.identifier,
                part_id=None,
                topic=message.topic,
                compressor_id=None,
                uncompressed_length=message.length,
                unverified_uncompressed_sha512=message.sha512,
                payload=b"",
            ),
            minimal_headers=state.config.websocket_minimal_headers,
        ),
        msg=message,
    )


async def send_internal_small_message_compressed(
    state: StateOpen, message: InternalSmallMessage, /, *, compressor: CompressorReady
) -> None:
    """Sends the given internal small message compressed; will use NOTIFY
    if possible, otherwise NOTIFY_STREAM

    WARN: does not inform the message callback
    """
    with reserve_compressor(state, compressor) as reservation:
        compressed_data = reservation.compress(message.data)

    await send_internal_small_message_compressed_with_compressed_data(
        state,
        message,
        compressor_id=compressor.identifier,
        compressed_data=compressed_data,
    )


async def send_internal_large_message_compressed(
    state: StateOpen,
    message: InternalLargeMessage,
    /,
    *,
    compressor: CompressorReady,
) -> None:
    """Sends the given internal large message compressed; will use NOTIFY if
    possible, otherwise NOTIFY_STREAM

    WARN: does not inform the message callback
    """
    spool_size = state.config.max_websocket_message_size or (2**64 - 1)

    with tempfile.SpooledTemporaryFile(max_size=spool_size) as compressed:
        with reserve_compressor(state, compressor) as reservation:
            chunker = reservation.chunker(
                size=message.length, chunk_size=io.DEFAULT_BUFFER_SIZE
            )

            uncompressed_pos = 0
            compressed_pos = 0
            compressed_hasher = hashlib.sha512()
            while True:
                uncompressed_chunk = read_exact(
                    message.stream,
                    min(io.DEFAULT_BUFFER_SIZE, message.length - uncompressed_pos),
                )
                uncompressed_pos += len(uncompressed_chunk)

                for compressed_chunk in chunker.compress(uncompressed_chunk):
                    compressed.write(compressed_chunk)
                    compressed_pos += len(compressed_chunk)
                    compressed_hasher.update(compressed_chunk)

                if uncompressed_pos >= message.length:
                    break

                await asyncio.sleep(0)

            assert uncompressed_pos == message.length, "read_exact failure"
            for compressed_chunk in chunker.finish():
                compressed.write(compressed_chunk)
                compressed_pos += len(compressed_chunk)
                compressed_hasher.update(compressed_chunk)

        compressed_sha512 = compressed_hasher.digest()
        compressed_length = compressed_pos

        compressed.seek(0, os.SEEK_SET)
        if compressed_length > spool_size:
            authorization = await state.config.authorize_notify(
                tracing=b"",  # TODO: tracing
                topic=message.topic,
                identifier=message.identifier,
                message_sha512=compressed_sha512,
                now=time.time(),
            )
            return await send_notify_stream_given_first_headers(
                state=state,
                stream=compressed,
                length=compressed_length,
                identifier=message.identifier,
                topic=message.topic,
                sha512=compressed_sha512,
                first_headers=serialize_s2b_notify_stream(
                    S2B_NotifyStreamStartCompressed(
                        type=SubscriberToBroadcasterStatefulMessageType.NOTIFY_STREAM,
                        authorization=authorization,
                        tracing=b"",  # TODO: tracing
                        identifier=message.identifier,
                        part_id=None,
                        topic=message.topic,
                        compressor_id=compressor.identifier,
                        compressed_length=compressed_length,
                        decompressed_length=message.length,
                        unverified_compressed_sha512=compressed_sha512,
                        payload=b"",
                    ),
                    minimal_headers=state.config.websocket_minimal_headers,
                ),
                msg=message,
            )

        compressed_data = read_exact(compressed, compressed_length)

    await send_internal_small_message_compressed_with_compressed_data(
        state,
        message,
        compressor_id=compressor.identifier,
        compressed_data=compressed_data,
        compressed_sha512=compressed_sha512,
    )


async def send_internal_small_message_compressed_with_compressed_data(
    state: StateOpen,
    message: Union[InternalSmallMessage, InternalLargeMessage],
    /,
    *,
    compressor_id: int,
    compressed_data: bytes,
    compressed_sha512: Optional[bytes] = None,
) -> None:
    """Sends the given internal message where the uncompressed data may or may
    not be small enough to hold in memory but the compressed data is small enough
    to hold in memory. Uses NOTIFY if possible, otherwise NOTIFY_STREAM

    WARN: does not inform the message callback
    """
    if compressed_sha512 is None:
        compressed_sha512 = hashlib.sha512(compressed_data).digest()

    tracing = b""  # TODO: tracing
    authorization = await state.config.authorize_notify(
        tracing=tracing,
        topic=message.topic,
        identifier=message.identifier,
        message_sha512=compressed_sha512,
        now=time.time(),
    )

    message_length = (
        len(message.data)
        if message.type == InternalMessageType.SMALL
        else message.length
    )

    if (
        state.config.max_websocket_message_size is None
        or message_length + 128 < state.config.max_websocket_message_size
    ):
        as_simple_message = serialize_s2b_notify(
            S2B_NotifyCompressed(
                type=SubscriberToBroadcasterStatefulMessageType.NOTIFY,
                authorization=authorization,
                tracing=b"",  # TODO: tracing
                identifier=message.identifier,
                compressor_id=compressor_id,
                topic=message.topic,
                verified_compressed_sha512=compressed_sha512,
                compressed_message=compressed_data,
                decompressed_length=message_length,
            ),
            minimal_headers=state.config.websocket_minimal_headers,
        )
        if (
            state.config.max_websocket_message_size is None
            or len(as_simple_message) < state.config.max_websocket_message_size
        ):
            state.expected_acks.append(
                B2S_ConfirmNotify(
                    type=BroadcasterToSubscriberStatefulMessageType.CONFIRM_NOTIFY,
                    identifier=message.identifier,
                    subscribers=-1,
                    authorization=None,
                    tracing=b"",
                )
            )
            state.sent_notifications.append(message)
            await state.websocket.send_bytes(as_simple_message)
            return

    await send_notify_stream_given_first_headers(
        state=state,
        stream=io.BytesIO(compressed_data),
        length=len(compressed_data),
        identifier=message.identifier,
        topic=message.topic,
        sha512=message.sha512,
        first_headers=serialize_s2b_notify_stream(
            S2B_NotifyStreamStartCompressed(
                type=SubscriberToBroadcasterStatefulMessageType.NOTIFY_STREAM,
                authorization=authorization,
                tracing=b"",  # TODO: tracing
                identifier=message.identifier,
                part_id=None,
                topic=message.topic,
                compressor_id=compressor_id,
                compressed_length=len(compressed_data),
                decompressed_length=message_length,
                unverified_compressed_sha512=compressed_sha512,
                payload=b"",
            ),
            minimal_headers=state.config.websocket_minimal_headers,
        ),
        msg=message,
    )


async def send_notify_stream_given_first_headers(
    state: StateOpen,
    stream: SyncReadableBytesIO,
    length: int,
    identifier: bytes,
    topic: bytes,
    sha512: bytes,
    first_headers: bytes,
    msg: InternalMessage,
) -> None:
    """Sends the given stream of data to the broadcaster via potentiially multiple
    NOTIFY_STREAM messages, using the given headers for the first message

    WARN: does not inform the message callback

    Args:
        state (StateOpen): the state, for grabbing configuration options
        stream (SyncReadableBytesIO): the stream of data to send
        length (int): the length of the stream
        identifier (bytes): the arbitrary unique identifier assigned by the subscriber
            so the broadcaster can combine the related messages
        topic (bytes): the topic of the message
        sha512 (bytes): the sha512 of the data being sent (so if the stream is
            compressed, the sha512 of the compressed data)
        first_headers (bytes): the headers for the first message which lets the broadcaster
            know how to interpret the stream
        msg (InternalMessage): the message to add to sent_notifications before sending the
            first message
    """
    headers = first_headers
    msg_size = state.config.max_websocket_message_size or (2**64 - 1)

    part_id = 0
    pos = 0
    while True:
        target_read_amount = min(length - pos, max(512, msg_size - len(headers)))
        payload = (
            b"" if target_read_amount == 0 else read_exact(stream, target_read_amount)
        )
        pos += len(payload)
        is_done = pos >= length
        state.expected_acks.append(
            B2S_ConfirmNotify(
                type=BroadcasterToSubscriberStatefulMessageType.CONFIRM_NOTIFY,
                identifier=identifier,
                subscribers=-1,
                authorization=None,
                tracing=b"",
            )
            if is_done
            else B2S_ContinueNotify(
                type=BroadcasterToSubscriberStatefulMessageType.CONTINUE_NOTIFY,
                identifier=identifier,
                part_id=part_id,
                authorization=None,
                tracing=b"",
            )
        )
        if part_id == 0:
            state.sent_notifications.append(msg)
        await state.websocket.send_bytes(headers + payload)

        if is_done:
            return

        part_id += 1
        authorization = await state.config.authorize_notify(
            tracing=b"",
            topic=topic,
            identifier=identifier,
            message_sha512=sha512,
            now=time.time(),
        )
        headers = serialize_s2b_notify_stream(
            S2B_NotifyStreamContinuation(
                type=SubscriberToBroadcasterStatefulMessageType.NOTIFY_STREAM,
                authorization=authorization,
                tracing=b"",  # TODO: tracing
                identifier=identifier,
                part_id=part_id,
                payload=b"",
            ),
            minimal_headers=state.config.websocket_minimal_headers,
        )
