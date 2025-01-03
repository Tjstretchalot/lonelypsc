import asyncio

from aiohttp import ClientWebSocketResponse, WSMsgType

from lonelypsc.types.websocket_message import WSMessage


def make_websocket_read_task(
    websocket: ClientWebSocketResponse,
) -> asyncio.Task[WSMessage]:
    """Creats an asyncio task that provides a better typed version of websocket.receive()"""
    return asyncio.create_task(adapt_websocket_read(websocket))


async def adapt_websocket_read(websocket: ClientWebSocketResponse) -> WSMessage:
    """Adapts the return type of websocket.receive() to WSMessage (the fastapi type)"""

    result = await websocket.receive()
    if result.type == WSMsgType.BINARY:
        return {"type": "websocket.receive", "bytes": result.data}

    if result.type == WSMsgType.CLOSE or result.type == WSMsgType.CLOSED:
        return {
            "type": "websocket.disconnect",
            "code": 1000,
            "reason": result.extra or "",
        }

    if result.type == WSMsgType.ERROR:
        return {
            "type": "websocket.disconnect",
            "code": 1011,
            "reason": result.extra or "",
        }

    if result.type == WSMsgType.TEXT:
        return {"type": "websocket.receive", "text": result.data}

    raise ValueError(f"Unexpected WSMsgType: {result}")
