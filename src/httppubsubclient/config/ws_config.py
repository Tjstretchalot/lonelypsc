from typing import TYPE_CHECKING, Dict, List, Literal, Optional, Protocol, Type

from httppubsubclient.config.auth_config import AuthConfig
from httppubsubclient.config.config import PubSubBroadcasterConfig

try:
    import zstandard
except ImportError:
    ...


class WebsocketPubSubConnectConfig(Protocol):
    """Determines how we connect to broadcasters"""

    @property
    def broadcasters(self) -> List[PubSubBroadcasterConfig]:
        """The broadcasters that we can connect to for making subscription requests
        and requesting a message be broadcast
        """

    @property
    def outgoing_initial_connect_retries(self) -> int:
        """The number of times we will retry the initial connection to a broadcaster.
        We will try every broadcaster at least once before retrying the same one
        """

    @property
    def outgoing_min_reconnect_interval(self) -> float:
        """Given that we successfully complete all the initial handshakes with a broadcaster
        and are satisfied the connection is up, how long do we need to then stay alive before
        we consider the connection stable.

        Most easily understood by example:

        Suppose we have broadcasters 1, 2, and 3. We connect to 1, it works, but 12 hours later
        we encounter an error. We reconnect to 2, it stays alive 12 hours, then errors. We connect
        to 3, it stays alive 12 hours, then errors. It seems reasonable that things are working well
        enough that we are ok to just go back to 1, since even if it does die again in 12h it's a
        perfectly manageable amount of overhead.

        Alternatively, if we connected to 1, completed the initial handshake, and died 3s later, then
        connected to 2, same thing, 3, same thing, probably we should stop trying as we're spending a
        lot of time managing connections compared to actually using them.

        Thus, following the above argument, the min reconnect interval should be between 3s and 12h.
        Generally, you should choose a value low enough that if it was reconnecting that often you
        would want to be pinged about it, since this is going to raise errors which will presumably
        trigger your alerting mechanisms.
        """


class WebsocketPubSubConnectConfigFromParts:
    """Convenience class to construct an object fulfilling the WebsocketPubSubConnectConfig
    protocol
    """

    def __init__(
        self,
        broadcasters: List[PubSubBroadcasterConfig],
        outgoing_initial_connect_retries: int,
        outgoing_min_reconnect_interval: float,
    ):
        self.broadcasters = broadcasters
        self.outgoing_initial_connect_retries = outgoing_initial_connect_retries
        self.outgoing_min_reconnect_interval = outgoing_min_reconnect_interval


if TYPE_CHECKING:
    _: Type[WebsocketPubSubConnectConfig] = WebsocketPubSubConnectConfigFromParts


class WebsocketGenericConfig(Protocol):
    @property
    def max_websocket_message_size(self) -> Optional[int]:
        """The maximum size in bytes for outgoing websocket messages. In theory,
        websocket messages are already broken up with websocket frames, which are
        then broken up with TCP packets, so it's redundant to have this limit.
        In practice, intermediaries will either drop large messages or behave
        poorly when they receive them. Thus, for maximum compatibility, set this
        to 16mb or less.
        """

    @property
    def websocket_close_timeout(self) -> Optional[float]:
        """The maximum amount of time to wait after trying to close the websocket
        connection for the acknowledgement from the server
        """

    @property
    def websocket_heartbeat_interval(self) -> float:
        """The interval in seconds between sending websocket ping frames to the
        server. A lower value causes more overhead but more quickly detects
        connection issues.
        """


class WebsocketGenericConfigFromParts:
    """Convenience class to construct an object fulfilling the WebsocketGenericConfig
    protocol
    """

    def __init__(
        self,
        max_websocket_message_size: Optional[int],
        websocket_close_timeout: Optional[float],
        websocket_heartbeat_interval: float,
    ):
        self.max_websocket_message_size = max_websocket_message_size
        self.websocket_close_timeout = websocket_close_timeout
        self.websocket_heartbeat_interval = websocket_heartbeat_interval


if TYPE_CHECKING:
    __: Type[WebsocketGenericConfig] = WebsocketGenericConfigFromParts


class WebsocketCompressionConfig(Protocol):
    @property
    def allow_compression(self) -> bool:
        """True to enable zstandard compression within the websocket connection, False
        to disable it
        """

    async def get_compression_dictionary_by_id(
        self, dictionary_id: int, /
    ) -> "Optional[zstandard.ZstdCompressionDict]":
        """If a precomputed zstandard compression dictionary is available with the
        given id, the corresponding dictionary should be returned. Presets must
        already be available on the broadcaster in order to be used. They
        provide meaningful compression on small messages (where a compression dict
        is too large to send alongside the message) when using short-lived websockets
        (where there isn't enough time to train a dictionary)
        """

    @property
    def allow_training_compression(self) -> bool:
        """True to allow the server to train a custom zstandard dict on the small
        payloads that we receive or send over the websocket connection, then
        send us that custom dictionary so we can reuse it for better
        compression.

        The server may be configured differently, but typically they will train
        on messages between 32 and 16384 bytes, which is large enough that
        compression with a pre-trained dictionary may help, but small enough
        that the the overhead of providing a dictionary alongside each message
        would overwhelm the benefits of compression.

        Generally you should enable this if you expect to send/receive enough
        relevant messages to reach the training thresholds (usually 100kb to 1mb
        of relevant messages), plus enough to make the training overhead worth
        it (typically another 10mb or so). You should disable this if you won't
        send relevant messages or you plan on disconnecting before you send/receive
        enough for it to be useful.

        You should also disable this if you know the message payloads will not be
        meaningfully compressible, e.g., significant parts are random or encrypted data.
        Generally, for encryption, prefer to use a TLS connection so you can still benefit
        from this compression.

        The automatically trained compression means you can generally ignore
        overhead from e.g. field names/whitespace/separators as they will be
        compressed away. To get the most benefit, use sorted keys
        """


class WebsocketCompressionConfigFromParts:
    """Convenience class to construct an object fulfilling the WebsocketCompressionConfig
    protocol
    """

    def __init__(
        self,
        allow_compression: bool,
        compression_dictionary_by_id: "Dict[int, zstandard.ZstdCompressionDict]",
        allow_training_compression: bool,
    ):
        self.allow_compression = allow_compression
        self.compression_dictionary_by_id = compression_dictionary_by_id
        self.allow_training_compression = allow_training_compression

    async def get_compression_dictionary_by_id(
        self, dictionary_id: int, /
    ) -> "Optional[zstandard.ZstdCompressionDict]":
        return self.compression_dictionary_by_id.get(dictionary_id, None)


if TYPE_CHECKING:
    ___: Type[WebsocketCompressionConfig] = WebsocketCompressionConfigFromParts


class WebsocketPubSubConfig(
    WebsocketPubSubConnectConfig,
    WebsocketGenericConfig,
    WebsocketCompressionConfig,
    AuthConfig,
    Protocol,
): ...


class WebsocketPubSubConfigFromParts:
    """Convenience class to construct an object fulfilling the WebsocketPubSubConfig
    protocol from objects which fulfill the various parts
    """

    def __init__(
        self,
        connect: WebsocketPubSubConnectConfig,
        generic: WebsocketGenericConfig,
        compression: WebsocketCompressionConfig,
        auth: AuthConfig,
    ):
        self.connect = connect
        self.generic = generic
        self.compression = compression
        self.auth = auth

    @property
    def broadcasters(self) -> List[PubSubBroadcasterConfig]:
        return self.connect.broadcasters

    @property
    def outgoing_initial_connect_retries(self) -> int:
        return self.connect.outgoing_initial_connect_retries

    @property
    def outgoing_min_reconnect_interval(self) -> float:
        return self.connect.outgoing_min_reconnect_interval

    @property
    def max_websocket_message_size(self) -> Optional[int]:
        return self.generic.max_websocket_message_size

    @property
    def websocket_close_timeout(self) -> Optional[float]:
        return self.generic.websocket_close_timeout

    @property
    def websocket_heartbeat_interval(self) -> float:
        return self.generic.websocket_heartbeat_interval

    @property
    def allow_compression(self) -> bool:
        return self.compression.allow_compression

    async def get_compression_dictionary_by_id(
        self, dictionary_id: int, /
    ) -> "Optional[zstandard.ZstdCompressionDict]":
        return await self.compression.get_compression_dictionary_by_id(dictionary_id)

    @property
    def allow_training_compression(self) -> bool:
        return self.compression.allow_training_compression

    async def setup_incoming_auth(self) -> None:
        await self.auth.setup_incoming_auth()

    async def teardown_incoming_auth(self) -> None:
        await self.auth.teardown_incoming_auth()

    async def setup_outgoing_auth(self) -> None:
        await self.auth.setup_outgoing_auth()

    async def teardown_outgoing_auth(self) -> None:
        await self.auth.teardown_outgoing_auth()

    async def is_receive_allowed(
        self,
        /,
        *,
        url: str,
        topic: bytes,
        message_sha512: bytes,
        now: float,
        authorization: Optional[str],
    ) -> Literal["ok", "unauthorized", "forbidden", "unavailable"]:
        return await self.auth.is_receive_allowed(
            url=url,
            topic=topic,
            message_sha512=message_sha512,
            now=now,
            authorization=authorization,
        )

    async def setup_subscribe_exact_authorization(
        self, /, *, url: str, exact: bytes, now: float
    ) -> Optional[str]:
        return await self.auth.setup_subscribe_exact_authorization(
            url=url,
            exact=exact,
            now=now,
        )

    async def setup_subscribe_glob_authorization(
        self, /, *, url: str, glob: str, now: float
    ) -> Optional[str]:
        return await self.auth.setup_subscribe_glob_authorization(
            url=url,
            glob=glob,
            now=now,
        )

    async def setup_notify_authorization(
        self, /, *, topic: bytes, message_sha512: bytes, now: float
    ) -> Optional[str]:
        return await self.auth.setup_notify_authorization(
            topic=topic,
            message_sha512=message_sha512,
            now=now,
        )


if TYPE_CHECKING:
    ____: Type[WebsocketPubSubConfig] = WebsocketPubSubConfigFromParts


def make_websocket_pub_sub_config(
    broadcasters: List[PubSubBroadcasterConfig],
    outgoing_initial_connect_retries: int,
    outgoing_min_reconnect_interval: float,
    max_websocket_message_size: Optional[int],
    websocket_close_timeout: Optional[float],
    websocket_heartbeat_interval: float,
    allow_compression: bool,
    compression_dictionary_by_id: "Dict[int, zstandard.ZstdCompressionDict]",
    allow_training_compression: bool,
    auth: AuthConfig,
) -> WebsocketPubSubConfig:
    """Convenience function to make a WebsocketPubSubConfig object without excessive nesting
    if you are specifying everything that doesn't need to be synced with the broadcaster
    within code.
    """
    return WebsocketPubSubConfigFromParts(
        connect=WebsocketPubSubConnectConfigFromParts(
            broadcasters=broadcasters,
            outgoing_initial_connect_retries=outgoing_initial_connect_retries,
            outgoing_min_reconnect_interval=outgoing_min_reconnect_interval,
        ),
        generic=WebsocketGenericConfigFromParts(
            max_websocket_message_size=max_websocket_message_size,
            websocket_close_timeout=websocket_close_timeout,
            websocket_heartbeat_interval=websocket_heartbeat_interval,
        ),
        compression=WebsocketCompressionConfigFromParts(
            allow_compression=allow_compression,
            compression_dictionary_by_id=compression_dictionary_by_id,
            allow_training_compression=allow_training_compression,
        ),
        auth=auth,
    )
