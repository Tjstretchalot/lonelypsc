import asyncio
from typing import TYPE_CHECKING, Dict, List, Literal, Optional, Protocol, Tuple, Type

from lonelypsc.config.auth_config import AuthConfig
from lonelypsc.config.config import PubSubBroadcasterConfig

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
        connection for the acknowledgement from the broadcaster
        """

    @property
    def websocket_heartbeat_interval(self) -> float:
        """The interval in seconds between sending websocket ping frames to the
        broadcaster. A lower value causes more overhead but more quickly detects
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
        self, dictionary_id: int, /, *, level: int
    ) -> "Optional[zstandard.ZstdCompressionDict]":
        """If a precomputed zstandard compression dictionary is available with the
        given id, the corresponding dictionary should be returned. Presets must
        already be available on the broadcaster in order to be used. They
        provide meaningful compression on small messages (where a compression dict
        is too large to send alongside the message) when using short-lived websockets
        (where there isn't enough time to train a dictionary)

        The provided compression level is the hint returned from the broadcaster,
        to avoid having to duplicate that configuration here. The returned dict
        should have its data precomputed as if by `precompute_compress`
        """

    @property
    def allow_training_compression(self) -> bool:
        """True to allow the broadcaster to train a custom zstandard dict on the small
        payloads that we receive or send over the websocket connection, then
        send us that custom dictionary so we can reuse it for better
        compression.

        The broadcaster may be configured differently, but typically it will train
        on messages between 32 and 16384 bytes, which is large enough that
        compression with a pre-trained dictionary may help, but small enough
        that the the overhead of providing a dictionary alongside each message
        would overwhelm the benefits of compression.

        Generally the subscriber should enable this if it expects to send/receive enough
        relevant messages to reach the training thresholds (usually 100kb to 1mb
        of relevant messages), plus enough to make the training overhead worth
        it (typically another 10mb or so). The subscriber should disable this if it won't
        send relevant messages or it expects to disconnect before sending/receiving
        enough data for the training to complete or the savings to compensate for
        the work spent building the dictionary.

        The subscriber should also disable this if the message payloads will not be
        meaningfully compressible, e.g., significant parts are random or encrypted data.
        Generally, for encryption, TLS should be used so that compression can still
        occur on the unencrypted payload (i.e., raw -> compressed -> encrypted).

        The automatically trained compression will generally convert a simple
        protocol design, such as json with long key names and extra wrappers for
        extensibility, into the same order of magnitude network size as a more
        compact protocol
        """

    @property
    def decompression_max_window_size(self) -> int:
        """
        Sets an upper limit on the window size for decompression operations
        in kibibytes. This setting can be used to prevent large memory
        allocations for inputs using large compression windows.

        Use 0 for no limit.

        A reasonable value is 0 for no limit. Alternatively, it should be 8mb if
        trying to match the zstandard minimum decoder requirements. The
        remaining alternative would be as high as the subscriber can bear

        WARN:
            This should not be considered a security measure. Authorization
            is already passed prior to decompression, and if that is not enough
            to eliminate adversarial payloads, then disable compression.
        """


class WebsocketCompressionConfigFromParts:
    """Convenience class to construct an object fulfilling the WebsocketCompressionConfig
    protocol
    """

    def __init__(
        self,
        allow_compression: bool,
        compression_dictionary_by_id: "Dict[int, List[Tuple[int, zstandard.ZstdCompressionDict]]]",
        allow_training_compression: bool,
        decompression_max_window_size: int,
    ):
        self.allow_compression = allow_compression
        self.compression_dictionary_by_id = compression_dictionary_by_id
        """
        Maps from dictionary id to a sorted list of (level, precomputed dictionary). You should
        initialize this with a guess for what level the broadcaster will suggest for compression,
        typically 10, and this will automatically fill in remaining levels as needed.
        """
        self.allow_training_compression = allow_training_compression
        self.decompression_max_window_size = decompression_max_window_size

    async def get_compression_dictionary_by_id(
        self, dictionary_id: int, /, *, level: int
    ) -> "Optional[zstandard.ZstdCompressionDict]":
        sorted_choices = self.compression_dictionary_by_id.get(dictionary_id, None)
        if not sorted_choices:
            return None

        for insert_idx, (choice_level, dictionary) in enumerate(sorted_choices):
            if choice_level == level:
                return dictionary
            elif choice_level > level:
                break

        data = sorted_choices[0][1].as_bytes()
        zdict = zstandard.ZstdCompressionDict(data)
        await asyncio.to_thread(zdict.precompute_compress, level)
        sorted_choices.insert(insert_idx, (level, zdict))
        return zdict


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
        self, dictionary_id: int, /, *, level: int
    ) -> "Optional[zstandard.ZstdCompressionDict]":
        return await self.compression.get_compression_dictionary_by_id(
            dictionary_id, level=level
        )

    @property
    def allow_training_compression(self) -> bool:
        return self.compression.allow_training_compression

    @property
    def decompression_max_window_size(self) -> int:
        return self.compression.decompression_max_window_size

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
    compression_dictionary_by_id: "Dict[int, Tuple[zstandard.ZstdCompressionDict, int]]",
    allow_training_compression: bool,
    decompression_max_window_size: int,
    auth: AuthConfig,
) -> WebsocketPubSubConfig:
    """Convenience function to make a WebsocketPubSubConfig object without excessive nesting
    if you are specifying everything that doesn't need to be synced with the broadcaster
    within code.

    The compression dictionary object is inputted in the same form as the broadcaster for
    convenience, and will be converted to the appropriate form for the subscriber
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
            compression_dictionary_by_id=dict(
                (dict_id, [(level, zdict)])
                for (dict_id, (zdict, level)) in compression_dictionary_by_id.items()
            ),
            allow_training_compression=allow_training_compression,
            decompression_max_window_size=decompression_max_window_size,
        ),
        auth=auth,
    )
