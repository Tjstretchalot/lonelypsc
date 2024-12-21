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
    def websocket_open_timeout(self) -> Optional[float]:
        """The maximum amount of time to wait for the websocket connection to be
        established before trying the next broadcaster
        """

    @property
    def websocket_receive_timeout(self) -> Optional[float]:
        """The maximum amount of time to spend waiting for data on the websocket
        before assuming the connection was lost. Note that this should be larger
        than the heartbeat interval
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

    @property
    def websocket_minimal_headers(self) -> bool:
        """True if all messages from the subscriber to the broadcaster should use
        minimal headers, which are faster to parse and more compact but require
        that the subscriber and broadcaster precisely agree on the headers for
        each message. False if all messages from the subscriber to the
        broadcaster use expanded headers, which are more flexible and easier to
        debug but slower to parse and more verbose.

        If you are trying to understand the lonelypss protocol via something
        like wireshark, setting this to False will make messages somewhat easier
        to understand.

        Note that broadcasters and subscribers do not need to agree on this
        setting. It is ok if the broadcaster is sending expanded headers and the
        subscriber is sending minimal headers, or vice versa, as this only
        configures the outgoing messages but they both always accept either
        version for incoming messages.

        Generally, this should be True except when in the process of updating
        the lonelypss/lonelypsc libraries, in which case it should be changed to
        false on the broadcaster and subscribers, then they should be updated
        one at a time, then set to true.
        """

    @property
    def max_sent_notifications(self) -> Optional[int]:
        """The maximum number of unacknowledged notifications before the subscriber
        disconnects because the broadcaster cannot keep up, or None for no limit.

        This is not useful for backpressure; instead, use the fact that notify()
        returns a coroutine that is not complete until the broadcaster has
        acknowledged the notification or the subscriber has dropped it, so e.g.
        a semaphore (or any other concurrency primitive) can be used to limit the
        number of unacknowledged notifications

        This is useful as a better error message if the above mechanism is not
        working as intended, so e.g. if the caller knows that it intends to have
        at most 3 unacknowledged notifications at any time, it can set this to 3
        so that an error is raised if there are 4 unacknowledged notifications
        """

    @property
    def max_unsent_notifications(self) -> Optional[int]:
        """the maximum number of unsent notifications queued up before the subscriber
        disconnects because either the broadcaster or the subscriber cannot keep
        up, or None for no limit.

        This is very similar to `max_sent_notifications` in practice, and should usually
        be set to the same value. This could differ in theory in that it will trigger
        first if trying to send a lot of notifications within the same event loop, or
        the network buffer is actually full (i.e., `send_bytes` is taking a long time)
        """

    @property
    def max_expected_acks(self) -> Optional[int]:
        """The maximum number of unacknowledged management tasks OR notifications;
        this should be at least `max_sent_notifications` plus the number of subscriptions
        (topic or glob) that may be made

        This is not useful for backpressure, and there is no way to implement
        backpressure for subscribe requests as they are sent in bulk at the
        beginning of a retry (since it would be highly unusual to have so many
        that backpressure is required). Instead, if there are a lot of topics
        the subscriber is interested in, use glob patterns to reduce the amount
        of work by the broadcaster and noise when reconnecting

        This is useful as a sanity check that the subscriber is not sending an
        excessive number of subscribe requests at the start of the connection
        (e.g., thousands), which likely means there is a bug or poor pattern in
        the subscriber code

        Note that if you DO have a good reason for subscribing to many topics
        instead of combining them with a pattern, there is no sudden cutoff when
        setting this to a large value (or None). The subscriptions all need to
        be maintained in memory on the subscribers side; so e.g. if there are
        100,000 topics at 64 bytes each it would take on the order of 6mb of
        memory for the topics themselves, plus 2-5x that when reconnecting. It
        may also be helpful, if doing this, to look over the DB config on the
        broadcaster and see if there are possible optimizations on how the
        subscriptions are stored/retrieved for this usecase

        Performance wise, having lots of exact topic subscriptions generally
        doesn't incur excessive overhead (as it's a dict lookup on the subscriber
        side and a btree lookup on the broadcaster side), but having lots of
        glob subscriptions generally does incur linear overhead on both sides.
        If you have a lot of glob subscriptions (>100), but not an excessive
        number of topics, consider at least changing the broadcasters
        implementation to cache topic -> current globs that match to speed up
        that side (this is generally not a good optimization if there are not
        a significant number of glob subscriptions)
        """

    @property
    def max_received(self) -> Optional[int]:
        """The maximum number of messages that have been received by the
        subscriber but not yet sent to message receivers, or None for no
        limit. At this limit the websocket will be disconnected.

        This is not useful for backpressure as the websocket gets disconnected
        when this limit is reached; instead, it is useful as a sanity check
        to ensure the subscriber can keep up with the incoming messages.
        """

    @property
    def max_unsent_acks(self) -> Optional[int]:
        """The maximum number of unsent acknowledgements from the subscriber
        to the broadcaster before disconnecting because connection cannot keep
        up

        This is an unlikely failure point since the broadcaster is expected to
        be waiting for acknowledgements before sending an excessive number of
        messages, and thus this is intended just as a sanity check
        """


class WebsocketGenericConfigFromParts:
    """Convenience class to construct an object fulfilling the WebsocketGenericConfig
    protocol
    """

    def __init__(
        self,
        max_websocket_message_size: Optional[int],
        websocket_open_timeout: Optional[float],
        websocket_receive_timeout: Optional[float],
        websocket_close_timeout: Optional[float],
        websocket_heartbeat_interval: float,
        websocket_minimal_headers: bool,
        max_sent_notifications: Optional[int],
        max_unsent_notifications: Optional[int],
        max_expected_acks: Optional[int],
        max_received: Optional[int],
        max_unsent_acks: Optional[int],
    ):
        self.max_websocket_message_size = max_websocket_message_size
        self.websocket_open_timeout = websocket_open_timeout
        self.websocket_receive_timeout = websocket_receive_timeout
        self.websocket_close_timeout = websocket_close_timeout
        self.websocket_heartbeat_interval = websocket_heartbeat_interval
        self.websocket_minimal_headers = websocket_minimal_headers
        self.max_sent_notifications = max_sent_notifications
        self.max_unsent_notifications = max_unsent_notifications
        self.max_expected_acks = max_expected_acks
        self.max_received = max_received
        self.max_unsent_acks = max_unsent_acks


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
    def initial_compression_dict_id(self) -> Optional[int]:
        """Either None to indicate no initial preset compression dictionary should be
        used, or the id of the preset compression dictionary appropriate for the
        messages that are expected to be sent over the connection. The subscriber will
        send this recommendation in the CONFIGURE packet, but will not use it unless
        the broadcaster agrees with the ENABLE_ZSTD_PRESET packet, which will also
        configure the other metadata (min/max size of messages to use this preset with,
        compression level)
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
        initial_compression_dict_id: Optional[int],
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
        self.initial_compression_dict_id = initial_compression_dict_id
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
    def websocket_open_timeout(self) -> Optional[float]:
        return self.generic.websocket_open_timeout

    @property
    def websocket_receive_timeout(self) -> Optional[float]:
        return self.generic.websocket_open_timeout

    @property
    def websocket_close_timeout(self) -> Optional[float]:
        return self.generic.websocket_close_timeout

    @property
    def websocket_heartbeat_interval(self) -> float:
        return self.generic.websocket_heartbeat_interval

    @property
    def websocket_minimal_headers(self) -> bool:
        return self.generic.websocket_minimal_headers

    @property
    def max_sent_notifications(self) -> Optional[int]:
        return self.generic.max_sent_notifications

    @property
    def max_unsent_notifications(self) -> Optional[int]:
        return self.generic.max_unsent_notifications

    @property
    def max_expected_acks(self) -> Optional[int]:
        return self.generic.max_expected_acks

    @property
    def max_received(self) -> Optional[int]:
        return self.generic.max_received

    @property
    def max_unsent_acks(self) -> Optional[int]:
        return self.generic.max_unsent_acks

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
    def initial_compression_dict_id(self) -> Optional[int]:
        return self.compression.initial_compression_dict_id

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
    websocket_open_timeout: Optional[float],
    websocket_receive_timeout: Optional[float],
    websocket_close_timeout: Optional[float],
    websocket_heartbeat_interval: float,
    websocket_minimal_headers: bool,
    max_sent_notifications: Optional[int],
    max_unsent_notifications: Optional[int],
    max_expected_acks: Optional[int],
    max_received: Optional[int],
    max_unsent_acks: Optional[int],
    allow_compression: bool,
    compression_dictionary_by_id: "Dict[int, Tuple[zstandard.ZstdCompressionDict, int]]",
    initial_compression_dict_id: Optional[int],
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
            websocket_open_timeout=websocket_open_timeout,
            websocket_receive_timeout=websocket_receive_timeout,
            websocket_close_timeout=websocket_close_timeout,
            websocket_heartbeat_interval=websocket_heartbeat_interval,
            websocket_minimal_headers=websocket_minimal_headers,
            max_sent_notifications=max_sent_notifications,
            max_unsent_notifications=max_unsent_notifications,
            max_expected_acks=max_expected_acks,
            max_received=max_received,
            max_unsent_acks=max_unsent_acks,
        ),
        compression=WebsocketCompressionConfigFromParts(
            allow_compression=allow_compression,
            compression_dictionary_by_id=dict(
                (dict_id, [(level, zdict)])
                for (dict_id, (zdict, level)) in compression_dictionary_by_id.items()
            ),
            allow_training_compression=allow_training_compression,
            initial_compression_dict_id=initial_compression_dict_id,
            decompression_max_window_size=decompression_max_window_size,
        ),
        auth=auth,
    )
