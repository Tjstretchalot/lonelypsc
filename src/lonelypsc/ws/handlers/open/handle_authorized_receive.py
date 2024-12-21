import os
from typing import Union

from lonelypsp.stateful.messages.receive_stream import (
    B2S_ReceiveStreamStartCompressed,
    B2S_ReceiveStreamStartUncompressed,
)
from lonelypsp.stateful.parser_helpers import read_exact

from lonelypsc.types.sync_io import SyncIOBaseLikeIO
from lonelypsc.ws.state import (
    ReceivedLargeMessage,
    ReceivedMessageType,
    ReceivedSmallMessage,
    ReceivingState,
    ReceivingWaitingCompressor,
    StateOpen,
)


def handle_authorized_receive(
    state: StateOpen,
    first: Union[B2S_ReceiveStreamStartUncompressed, B2S_ReceiveStreamStartCompressed],
    body: SyncIOBaseLikeIO,
) -> None:
    """Sets state.receiving as appropriate assuming its either in INCOMPLETE or
    AUTHORIZING state, all the data has been received (and is the right length
    and can be found in body), its authorization task has completed and was ok,
    and the hash of the data matches the expected hash
    """
    spool_size = (
        state.config.max_websocket_message_size
        if state.config.max_websocket_message_size is not None
        else 2**64 - 1
    )

    body.seek(0, os.SEEK_SET)
    if first.compressor_id is None:
        if first.uncompressed_length < spool_size:
            state.received.put_nowait(
                ReceivedSmallMessage(
                    type=ReceivedMessageType.SMALL,
                    topic=first.topic,
                    data=read_exact(body, first.uncompressed_length),
                    sha512=first.unverified_uncompressed_sha512,
                )
            )
            body.close()
            state.receiving = None
            return

        state.received.put_nowait(
            ReceivedLargeMessage(
                type=ReceivedMessageType.LARGE,
                topic=first.topic,
                stream=body,
                sha512=first.unverified_uncompressed_sha512,
            )
        )
        state.receiving = None
        return

    body.seek(0, os.SEEK_SET)
    state.receiving = ReceivingWaitingCompressor(
        type=ReceivingState.WAITING_COMPRESSOR,
        first=first,
        compressed_body=body,
    )
