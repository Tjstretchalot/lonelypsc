import asyncio
import dataclasses
import hashlib
import tempfile
import time
from typing import TYPE_CHECKING

from lonelypsp.stateful.constants import SubscriberToBroadcasterStatefulMessageType
from lonelypsp.stateful.messages.confirm_receive import (
    S2B_ConfirmReceive,
    serialize_s2b_confirm_receive,
)
from lonelypsp.stateful.messages.continue_receive import (
    S2B_ContinueReceive,
    serialize_s2b_continue_receive,
)
from lonelypsp.stateful.messages.receive_stream import B2S_ReceiveStream

from lonelypsc.client import PubSubError, PubSubIrrecoverableError
from lonelypsc.ws.handlers.open.handle_authorized_receive import (
    handle_authorized_receive,
)
from lonelypsc.ws.handlers.open.messages.protocol import MessageChecker
from lonelypsc.ws.handlers.open.websocket_url import (
    make_for_receive_websocket_url_and_change_counter,
)
from lonelypsc.ws.state import (
    ReceivingAuthorizing,
    ReceivingIncomplete,
    ReceivingState,
    StateOpen,
)


def check_receive_stream(state: StateOpen, message: B2S_ReceiveStream) -> None:
    """Handles the subscriber receiving the a message from the broadcaster that
    a message was sent to a specific topic that the subscriber is subscribed to;
    if the message is long, this may only contain a part of the message.

    Raises an exception if this is not a proper continuation (either a new message
    after the last one finished normally, or the continuation from the last message)
    """
    spool_size = (
        state.config.max_websocket_message_size
        if state.config.max_websocket_message_size is not None
        else 2**64 - 1
    )
    if message.part_id is None:
        if state.receiving is not None:
            if state.receiving.type != ReceivingState.INCOMPLETE:
                raise PubSubIrrecoverableError(
                    "invariant violated: check_receive_stream while not in INCOMPLETE state"
                )

            raise PubSubError(
                f"expected continnuation of {state.receiving.first.identifier!r}, got new message {message.identifier!r}"
            )

        state.receiving = ReceivingIncomplete(
            type=ReceivingState.INCOMPLETE,
            first=dataclasses.replace(message, payload=b""),
            part_id=-1,
            body_hasher=hashlib.sha512(),
            body=tempfile.SpooledTemporaryFile(max_size=spool_size),
            authorization_task=asyncio.create_task(
                state.config.is_receive_allowed(
                    url=make_for_receive_websocket_url_and_change_counter(state),
                    topic=message.topic,
                    message_sha512=(
                        message.unverified_compressed_sha512
                        if message.compressor_id is not None
                        else message.unverified_uncompressed_sha512
                    ),
                    now=time.time(),
                    authorization=message.authorization,
                )
            ),
        )

    if state.receiving is None:
        raise PubSubError(
            f"expected new message, got continuation of {message.identifier!r}"
        )

    if state.receiving.type != ReceivingState.INCOMPLETE:
        raise PubSubIrrecoverableError(
            "invariant violated: check_receive_stream while not in INCOMPLETE state"
        )

    if message.identifier != state.receiving.first.identifier:
        raise PubSubError(
            f"expected continuation of {state.receiving.first.identifier!r}, got {message.identifier!r}"
        )

    if state.receiving.part_id + 1 != (message.part_id or 0):
        raise PubSubError(
            f"expected part {state.receiving.part_id + 1}, got {message.part_id!r}"
        )

    if (
        state.receiving.authorization_task is not None
        and state.receiving.authorization_task.done()
    ):
        result = state.receiving.authorization_task.result()
        if result != "ok":
            raise PubSubError(f"authorization failed: {result}")
        state.receiving.authorization_task = None

    state.receiving.body.write(message.payload)
    state.receiving.body_hasher.update(message.payload)
    state.receiving.part_id += 1

    received_length = state.receiving.body.tell()
    expected_length = (
        state.receiving.first.compressed_length
        if state.receiving.first.compressor_id is not None
        else state.receiving.first.uncompressed_length
    )

    if received_length > expected_length:
        raise PubSubError(
            f"received more data than expected: {received_length} > {expected_length}"
        )

    if received_length < expected_length:
        state.unsent_acks.append(
            serialize_s2b_continue_receive(
                S2B_ContinueReceive(
                    type=SubscriberToBroadcasterStatefulMessageType.CONTINUE_RECEIVE,
                    identifier=state.receiving.first.identifier,
                    part_id=state.receiving.part_id,
                ),
                minimal_headers=state.config.websocket_minimal_headers,
            )
        )
        return

    received_hash = state.receiving.body_hasher.digest()
    expected_hash = (
        state.receiving.first.unverified_compressed_sha512
        if state.receiving.first.compressor_id is not None
        else state.receiving.first.unverified_uncompressed_sha512
    )

    if received_hash != expected_hash:
        raise PubSubError(
            f"received data hash {received_hash.hex()} does not match expected {expected_hash.hex()}"
        )

    state.unsent_acks.append(
        serialize_s2b_confirm_receive(
            S2B_ConfirmReceive(
                type=SubscriberToBroadcasterStatefulMessageType.CONFIRM_RECEIVE,
                identifier=state.receiving.first.identifier,
            ),
            minimal_headers=state.config.websocket_minimal_headers,
        )
    )

    if state.receiving.authorization_task is not None:
        state.receiving = ReceivingAuthorizing(
            type=ReceivingState.AUTHORIZING,
            first=state.receiving.first,
            body=state.receiving.body,
            authorization_task=state.receiving.authorization_task,
        )
        return

    handle_authorized_receive(state, state.receiving.first, state.receiving.body)


if TYPE_CHECKING:
    _: MessageChecker[B2S_ReceiveStream] = check_receive_stream
