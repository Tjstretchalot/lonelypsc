from typing import TYPE_CHECKING

from lonelypsp.stateful.messages.disable_zstd_custom import B2S_DisableZstdCustom

from lonelypsc.client import PubSubError
from lonelypsc.ws.handlers.open.messages.protocol import MessageChecker
from lonelypsc.ws.state import (
    StateOpen,
)


def check_disable_zstd_custom(state: StateOpen, message: B2S_DisableZstdCustom) -> None:
    """Handles the subscriber receiving the a message from the broadcaster that
    it has discarded a custom dictionary it sent earlier, so we can discard it
    as well.

    Raises
    """
    try:
        state.compressors.remove_compressor(message.identifier)
    except KeyError:
        raise PubSubError(f"unknown compressor {message.identifier}")


if TYPE_CHECKING:
    _: MessageChecker[B2S_DisableZstdCustom] = check_disable_zstd_custom
