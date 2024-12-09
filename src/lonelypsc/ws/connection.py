import asyncio
from dataclasses import dataclass
from enum import Enum, auto
from types import TracebackType
from typing import Dict, Literal, Optional, Protocol, Type, Union

import aiohttp

from lonelypsc.config.config import PubSubBroadcasterConfig
from lonelypsc.config.ws_config import WebsocketPubSubConfig

try:
    import zstandard
except ImportError:
    ...


class PubsubWebsocketError(Exception):
    pass


class PubsubWebsocketCloseRequestedError(PubsubWebsocketError):
    pass


class PubsubWebsocketServerDisconnectedError(PubsubWebsocketError):
    pass


class _InternalMessageType(Enum):
    CLOSE = auto()


@dataclass
class _InternalMessageClose:
    type: Literal[_InternalMessageType.CLOSE]


_InternalMessage = _InternalMessageClose


class PubSubWebsocketState(Enum):
    PREPARING = auto()
    """We have not yet tried to open the connection"""
    CONNECTING = auto()
    """We are still trying to connect to the broadcaster"""
    INITIALIZING = auto()
    """We are exchanging handshakes"""
    READY = auto()
    """We are ready to send and receive messages"""
    CLOSING = auto()
    """We are waiting for the broadcaster to gracefully close the connection"""
    CLOSED = auto()
    """The connection is closed"""


@dataclass
class _PubSubWebsocketStatePreparing:
    type: Literal[PubSubWebsocketState.PREPARING]


@dataclass
class _PubSubWebsocketStateConnecting:
    type: Literal[PubSubWebsocketState.CONNECTING]
    session: aiohttp.ClientSession


@dataclass
class _PubSubWebsocketStateInitializing:
    type: Literal[PubSubWebsocketState.INITIALIZING]
    session: aiohttp.ClientSession
    websocket: aiohttp.ClientWebSocketResponse


@dataclass
class _Compressor:
    identifier: int
    """How this compressor is identified over the wire"""
    decompressor: "zstandard.ZstdDecompressor"
    """How to decompress messages compressed with this compressor"""
    compressor: "zstandard.ZstdCompressor"
    """How to compress messages with this compressor"""
    min_size_incl: int
    """The minimum size in bytes where this compressor might be useful (inclusive)"""
    max_size_excl: int
    """The maximum size in bytes where this compressor might be useful (exclusive)"""


@dataclass
class _PubSubWebsocketStateReady:
    type: Literal[PubSubWebsocketState.READY]
    session: aiohttp.ClientSession
    websocket: aiohttp.ClientWebSocketResponse
    compressors: Dict[int, _Compressor]


@dataclass
class _PubSubWebsocketStateClosing:
    type: Literal[PubSubWebsocketState.CLOSING]


@dataclass
class _PubSubWebsocketStateClosed:
    type: Literal[PubSubWebsocketState.CLOSED]


_PubSubWebsocketState = Union[
    _PubSubWebsocketStatePreparing,
    _PubSubWebsocketStateConnecting,
    _PubSubWebsocketStateInitializing,
    _PubSubWebsocketStateReady,
    _PubSubWebsocketStateClosing,
    _PubSubWebsocketStateClosed,
]


class _StateProgresser(Protocol):
    async def __call__(
        self,
        broadcaster: PubSubBroadcasterConfig,
        config: WebsocketPubSubConfig,
        messages: asyncio.Queue[_InternalMessage],
        state: _PubSubWebsocketState,
    ) -> _PubSubWebsocketState: ...


async def _progress_preparing(
    broadcaster: PubSubBroadcasterConfig,
    config: WebsocketPubSubConfig,
    messages: asyncio.Queue[_InternalMessage],
    state: _PubSubWebsocketState,
) -> _PubSubWebsocketState:
    assert state.type == PubSubWebsocketState.PREPARING

    while True:
        try:
            message = messages.get_nowait()
        except asyncio.QueueEmpty:
            break

        if message.type != _InternalMessageType.CLOSE:
            raise PubsubWebsocketError("unknown message while preparing")
        raise PubsubWebsocketCloseRequestedError()

    return _PubSubWebsocketStateConnecting(
        type=PubSubWebsocketState.CONNECTING, session=aiohttp.ClientSession()
    )


async def _progress_connecting(
    broadcaster: PubSubBroadcasterConfig,
    config: WebsocketPubSubConfig,
    messages: asyncio.Queue[_InternalMessage],
    state: _PubSubWebsocketState,
) -> _PubSubWebsocketState:
    assert state.type == PubSubWebsocketState.CONNECTING

    try:
        connect_ws_task = asyncio.create_task(
            state.session.ws_connect(
                broadcaster["host"],
                timeout=aiohttp.ClientWSTimeout(
                    ws_close=config.websocket_close_timeout
                ),
                heartbeat=config.websocket_heartbeat_interval,
            )
        )
        recv_task = asyncio.create_task(messages.get())

        try:
            await asyncio.wait(
                [connect_ws_task, recv_task], return_when=asyncio.FIRST_COMPLETED
            )

            if recv_task.done():
                msg = recv_task.result()
                if msg.type != _InternalMessageType.CLOSE:
                    raise PubsubWebsocketError("unknown message while connecting")
                raise PubsubWebsocketCloseRequestedError()

            socket = connect_ws_task.result()
            return _PubSubWebsocketStateInitializing(
                type=PubSubWebsocketState.INITIALIZING,
                session=state.session,
                websocket=socket,
            )
        except BaseException:
            recv_task.cancel()
            if not connect_ws_task.cancel():
                try:
                    socket = connect_ws_task.result()
                    await socket.close()
                except BaseException:
                    ...
            raise

    except BaseException:
        await state.session.close()
        raise


def _rotate_in_compressor(
    compressors: Dict[int, _Compressor], compressor: _Compressor
) -> None:
    if compressor.identifier in compressors:
        compressors[compressor.identifier] = compressor
        return

    for existing in compressors.values():
        if (
            existing.min_size_incl == compressor.min_size_incl
            and existing.max_size_excl == compressor.max_size_excl
        ):
            del compressors[existing.identifier]
            compressors[compressor.identifier] = compressor
            return

    if len(compressors) > 5:
        for existing in compressors.values():
            if existing.identifier >= 65536:
                del compressors[existing.identifier]
                break

    compressors[compressor.identifier] = compressor


async def _progress_initializing(
    broadcaster: PubSubBroadcasterConfig,
    config: WebsocketPubSubConfig,
    messages: asyncio.Queue[_InternalMessage],
    state: _PubSubWebsocketState,
) -> _PubSubWebsocketState:
    assert state.type == PubSubWebsocketState.INITIALIZING

    compressors: Dict[int, _Compressor] = {}
    raise NotImplementedError


async def _progress_ready(
    broadcaster: PubSubBroadcasterConfig,
    config: WebsocketPubSubConfig,
    messages: asyncio.Queue[_InternalMessage],
    state: _PubSubWebsocketState,
) -> _PubSubWebsocketState:
    raise NotImplementedError


async def _progress_closing(
    broadcaster: PubSubBroadcasterConfig,
    config: WebsocketPubSubConfig,
    messages: asyncio.Queue[_InternalMessage],
    state: _PubSubWebsocketState,
) -> _PubSubWebsocketState:
    raise NotImplementedError


async def _progress_closed(
    broadcaster: PubSubBroadcasterConfig,
    config: WebsocketPubSubConfig,
    messages: asyncio.Queue[_InternalMessage],
    state: _PubSubWebsocketState,
) -> _PubSubWebsocketState:
    raise PubsubWebsocketError("connection is already closed")


_PROGRESSORS: Dict[PubSubWebsocketState, _StateProgresser] = {
    PubSubWebsocketState.PREPARING: _progress_preparing,
    PubSubWebsocketState.CONNECTING: _progress_connecting,
    PubSubWebsocketState.INITIALIZING: _progress_initializing,
    PubSubWebsocketState.READY: _progress_ready,
    PubSubWebsocketState.CLOSING: _progress_closing,
    PubSubWebsocketState.CLOSED: _progress_closed,
}


async def _progress_state(
    broadcaster: PubSubBroadcasterConfig,
    config: WebsocketPubSubConfig,
    messages: asyncio.Queue[_InternalMessage],
    state: _PubSubWebsocketState,
) -> _PubSubWebsocketState:
    return await _PROGRESSORS[state.type](broadcaster, config, messages, state)


class WebsocketPubSubConnection:
    """Acts as an asynchronous context manager for a websocket connection to
    the httppubsub server.

    Usage:

    async with WebsocketPubSubConnection(...) as conn:
        await conn.subscribe(...)
        msg = await conn.receive(...)
    """

    def __init__(
        self, broadcaster: PubSubBroadcasterConfig, config: WebsocketPubSubConfig
    ):
        self.broadcaster = broadcaster
        self.config = config
        self._messages: Optional[asyncio.Queue[_InternalMessage]] = None
        self._state: _PubSubWebsocketState = _PubSubWebsocketStatePreparing(
            type=PubSubWebsocketState.PREPARING
        )
        self._state_lock = asyncio.Lock()
        self._progressor: Optional[asyncio.Task[None]] = None

    async def __aenter__(self) -> "WebsocketPubSubConnection":
        async with self._state_lock:
            assert self._state.type == PubSubWebsocketState.PREPARING, "not reentrant"
            assert self._progressor is None, "not reentrant"
            assert self._messages is None, "not reentrant"

            # to have a cleaner stack trace if there is an error connecting,
            # we will _progress immediately until ready here.

            messages: asyncio.Queue[_InternalMessage] = asyncio.Queue()
            state: _PubSubWebsocketState = self._state

            try:
                while True:
                    if state.type == PubSubWebsocketState.READY:
                        break
                    state = await _progress_state(
                        self.broadcaster, self.config, messages, state
                    )
            except BaseException:
                self._state = _PubSubWebsocketStateClosed(
                    type=PubSubWebsocketState.CLOSED
                )
                raise

            self._messages = messages
            self._state = state
            self._progressor = asyncio.create_task(self._progress_until_error())
        return self

    async def _progress_until_error(self) -> None:
        while True:
            async with self._state_lock:
                if self._messages is None:
                    raise PubsubWebsocketError("connection is already closed")

                self._state = await _progress_state(
                    self.broadcaster, self.config, self._messages, self._state
                )

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        async with self._state_lock:
            if self._state.type == PubSubWebsocketState.CLOSED:
                return

            if self._state.type == PubSubWebsocketState.PREPARING:
                self._state = _PubSubWebsocketStateClosed(
                    type=PubSubWebsocketState.CLOSED
                )
                return

            assert self._messages is not None
            assert self._progressor is not None

            self._messages.put_nowait(
                _InternalMessageClose(type=_InternalMessageType.CLOSE)
            )

            try:
                await self._progressor
            except PubsubWebsocketCloseRequestedError:
                ...
            finally:
                self._state = _PubSubWebsocketStateClosed(
                    type=PubSubWebsocketState.CLOSED
                )
                self._messages = None
                self._progressor = None
