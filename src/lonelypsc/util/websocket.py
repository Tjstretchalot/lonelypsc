from typing import Union, cast

import aiohttp


async def send_bytes_like(
    websocket: aiohttp.ClientWebSocketResponse,
    data: Union[bytes, bytearray, memoryview],
) -> None:
    """Send any bytes-like payload through an aiohttp client websocket.

    aiohttp accepts bytes, bytearray, and memoryview at runtime, but its
    ``send_bytes`` annotation only accepts bytes. The cast keeps this boundary
    zero-copy while documenting and centralizing that compatibility detail.
    """
    await websocket.send_bytes(cast(bytes, data))
