from typing import TYPE_CHECKING, Literal, Optional, Protocol, Type

from lonelypsp.stateful.messages.confirm_configure import B2S_ConfirmConfigure
from lonelypsp.stateless.make_strong_etag import StrongEtag


class IncomingAuthConfig(Protocol):
    async def setup_incoming_auth(self) -> None:
        """Prepares this authorization instance for use. If the incoming auth config
        is not re-entrant (i.e., it cannot be used by two clients simultaneously), it
        must detect this and error out.
        """

    async def teardown_incoming_auth(self) -> None:
        """Cleans up this authorization instance after use. This is called when a
        client is done using the auth config, and should release any resources it
        acquired during `setup_incoming_auth`.
        """

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
        """Determines if the given message can be received from the given topic. As
        we support very large messages, for authorization only the SHA-512 of
        the message should be used, which will be fully verified

        Args:
            url (str): the url the broadcaster used to reach us
            topic (bytes): the topic the message claims to be on
            message_sha512 (bytes): the sha512 of the message being received
            now (float): the current time in seconds since the epoch, as if from `time.time()`
            authorization (str, None): the authorization header they provided

        Returns:
            `ok`: if the message is allowed
            `unauthorized`: if the authorization header is required but not provided
            `forbidden`: if the authorization header is provided but invalid
            `unavailable`: if a service is required to check this isn't available.
              the message will be dropped.
        """

    async def is_missed_allowed(
        self,
        /,
        *,
        recovery: str,
        topic: bytes,
        now: float,
        authorization: Optional[str],
    ) -> Literal["ok", "unauthorized", "forbidden", "unavailable"]:
        """Determines if the indication that the subscriber may have missed a
        message from a broadcaster is allowed. This allows the subscriber to
        trigger a recovery mechanism to get back into a consistent state.

        Args:
            recovery (str): the url the missed message was sent to
            topic (bytes): the topic the message was on
            now (float): the current time in seconds since the epoch, as if from `time.time()`
            authorization (str, None): the authorization header they provided

        Returns:
            `ok`: if the message is allowed
            `unauthorized`: if the authorization header is required but not provided
            `forbidden`: if the authorization header is provided but invalid
            `unavailable`: if a service is required to check this isn't available
        """

    async def is_websocket_confirm_configure_allowed(
        self, /, *, message: B2S_ConfirmConfigure, now: float
    ) -> Literal["ok", "unauthorized", "forbidden", "unavailable"]:
        """Determines if the subscriber should continue with a websocket connection
        with a broadcaster who approved our configure message and responded back with
        the given confirm configure message.

        Args:
            message (B2S_ConfirmConfigure): the confirm configure message from the broadcaster
            now (float): the current time in seconds since the epoch, as if from `time.time()`

        Returns:
            `ok`: if the message is allowed
            `unauthorized`: if the authorization header is required but not provided
            `forbidden`: if the authorization header is provided but invalid
            `unavailable`: if a service is required to check this isn't available
        """


class OutgoingAuthConfig(Protocol):
    async def setup_outgoing_auth(self) -> None:
        """Prepares this authorization instance for use. If the outgoing auth config
        is not re-entrant (i.e., it cannot be used by two clients simultaneously), it
        must detect this and error out.
        """

    async def teardown_outgoing_auth(self) -> None:
        """Cleans up this authorization instance after use. This is called when a
        client is done using the auth config, and should release any resources it
        acquired during `setup_outgoing_auth`.
        """

    async def setup_subscribe_exact_authorization(
        self, /, *, url: str, recovery: Optional[str], exact: bytes, now: float
    ) -> Optional[str]:
        """Provides the authorization header that the subscriber should use to
        subscribe to a specific topic at the given url.

        Args:
            url (str): the url the subscriber is subscribing to
            recovery (str, None): the url that will receive MISSED messages for this
                subscription, if any
            exact (bytes): the exact topic they are subscribing to
            now (float): the current time in seconds since the epoch, as if from `time.time()`

        Returns:
            str, None: the authorization header to use, if any
        """

    async def setup_subscribe_glob_authorization(
        self, /, *, url: str, recovery: Optional[str], glob: str, now: float
    ) -> Optional[str]:
        """Provides the authoirzation header that the subscriber should use to subscribe
        to any topic that matches the given glob at the given url.

        Args:
            url (str): the url the subscriber is subscribing to
            recovery (str, None): the url that will receive MISSED messages for this
                subscription, if any
            glob (str): the glob pattern they are subscribing to
            now (float): the current time in seconds since the epoch, as if from `time.time()`

        Returns:
            str, None: the authorization header to use, if any
        """

    async def setup_notify_authorization(
        self, /, *, topic: bytes, message_sha512: bytes, now: float
    ) -> Optional[str]:
        """Provides the authorization header that the subscriber should use to
        ask a broadcaster to notify all subscribers to a topic about a message
        with the given hash. Only the hash of the message is used in authorization
        as the message itself may be very large; the hash will always be checked.

        Args:
            topic (bytes): the topic that the message is being sent to
            message_sha512 (bytes): the sha512 of the message being sent
            now (float): the current time in seconds since the epoch, as if from `time.time()`

        Returns:
            str, None: the authorization header to use, if any
        """

    async def setup_check_subscriptions_authorization(
        self, /, *, url: str, now: float
    ) -> Optional[str]:
        """Provides the authorization header that the subscriber should use to
        check the subscriptions that are currently active for the given url

        Args:
            url (str): the url the subscriber is checking
            now (float): the current time in seconds since the epoch, as if from `time.time()`

        Returns:
            str, None: the authorization header to use, if any
        """

    async def setup_set_subscriptions_authorization(
        self, /, *, url: str, strong_etag: StrongEtag, now: float
    ) -> Optional[str]:
        """Provides the authorization header that the subscriber should use to
        set the subscriptions that are currently active for the given url

        Unlike with the checking side which might compare the user being
        authenticated with vs the topics, there is generally no reason to need
        to view the specific globs/topics that are being subscribed to for
        generating the authorization token, as if they are not valid it will
        be caught by the broadcaster

        Args:
            url (str): the url the subscriber is setting
            strong_etag (StrongEtag): the strong etag of the subscriptions being set
            now (float): the current time in seconds since the epoch, as if from `time.time()`

        Returns:
            str, None: the authorization header to use, if any
        """

    async def setup_websocket_configure(
        self,
        /,
        *,
        subscriber_nonce: bytes,
        enable_zstd: bool,
        enable_training: bool,
        initial_dict: int,
    ) -> Optional[str]:
        """Provides the authorization header that the subscriber should use to
        configure the websocket connection with the broadcaster

        Args:
            subscriber_nonce (bytes): the 32 random bytes the subscriber is
                contributing toward the connection nonce
            enable_zstd (bool): whether to enable zstd compression
            enable_training (bool): whether to enable training mode
            initial_dict (int): the initial dictionary to use

        Returns:
            str, None: the authorization header to use, if any
        """


class AuthConfig(IncomingAuthConfig, OutgoingAuthConfig, Protocol): ...


class AuthConfigFromParts:
    """Convenience class to combine an incoming and outgoing auth config into an
    auth config
    """

    def __init__(self, incoming: IncomingAuthConfig, outgoing: OutgoingAuthConfig):
        self.incoming = incoming
        self.outgoing = outgoing

    async def setup_incoming_auth(self) -> None:
        await self.incoming.setup_incoming_auth()

    async def teardown_incoming_auth(self) -> None:
        await self.incoming.teardown_incoming_auth()

    async def setup_outgoing_auth(self) -> None:
        await self.outgoing.setup_outgoing_auth()

    async def teardown_outgoing_auth(self) -> None:
        await self.outgoing.teardown_outgoing_auth()

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
        return await self.incoming.is_receive_allowed(
            url=url,
            topic=topic,
            message_sha512=message_sha512,
            now=now,
            authorization=authorization,
        )

    async def is_missed_allowed(
        self,
        /,
        *,
        recovery: str,
        topic: bytes,
        now: float,
        authorization: Optional[str],
    ) -> Literal["ok", "unauthorized", "forbidden", "unavailable"]:
        return await self.incoming.is_missed_allowed(
            recovery=recovery,
            topic=topic,
            now=now,
            authorization=authorization,
        )

    async def is_websocket_confirm_configure_allowed(
        self, /, *, message: B2S_ConfirmConfigure, now: float
    ) -> Literal["ok", "unauthorized", "forbidden", "unavailable"]:
        return await self.incoming.is_websocket_confirm_configure_allowed(
            message=message,
            now=now,
        )

    async def setup_subscribe_exact_authorization(
        self, /, *, url: str, recovery: Optional[str], exact: bytes, now: float
    ) -> Optional[str]:
        return await self.outgoing.setup_subscribe_exact_authorization(
            url=url,
            recovery=recovery,
            exact=exact,
            now=now,
        )

    async def setup_subscribe_glob_authorization(
        self, /, *, url: str, recovery: Optional[str], glob: str, now: float
    ) -> Optional[str]:
        return await self.outgoing.setup_subscribe_glob_authorization(
            url=url,
            recovery=recovery,
            glob=glob,
            now=now,
        )

    async def setup_notify_authorization(
        self, /, *, topic: bytes, message_sha512: bytes, now: float
    ) -> Optional[str]:
        return await self.outgoing.setup_notify_authorization(
            topic=topic,
            message_sha512=message_sha512,
            now=now,
        )

    async def setup_check_subscriptions_authorization(
        self, /, *, url: str, now: float
    ) -> Optional[str]:
        return await self.outgoing.setup_check_subscriptions_authorization(
            url=url,
            now=now,
        )

    async def setup_set_subscriptions_authorization(
        self, /, *, url: str, strong_etag: StrongEtag, now: float
    ) -> Optional[str]:
        return await self.outgoing.setup_set_subscriptions_authorization(
            url=url,
            strong_etag=strong_etag,
            now=now,
        )

    async def setup_websocket_configure(
        self,
        /,
        *,
        subscriber_nonce: bytes,
        enable_zstd: bool,
        enable_training: bool,
        initial_dict: int,
    ) -> Optional[str]:
        return await self.outgoing.setup_websocket_configure(
            subscriber_nonce=subscriber_nonce,
            enable_zstd=enable_zstd,
            enable_training=enable_training,
            initial_dict=initial_dict,
        )


if TYPE_CHECKING:
    _: Type[AuthConfig] = AuthConfigFromParts
