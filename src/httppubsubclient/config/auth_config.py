from typing import TYPE_CHECKING, Literal, Optional, Protocol, Type


class IncomingAuthConfig(Protocol):
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


class OutgoingAuthConfig(Protocol):
    async def setup_subscribe_exact_authorization(
        self, /, *, url: str, exact: bytes, now: float
    ) -> Optional[str]:
        """Provides the authorization header that the subscriber should use to
        subscribe to a specific topic at the given url.

        Args:
            url (str): the url the subscriber is subscribing to
            exact (bytes): the exact topic they are subscribing to
            now (float): the current time in seconds since the epoch, as if from `time.time()`

        Returns:
            str, None: the authorization header to use, if any
        """

    async def setup_subscribe_glob_authorization(
        self, /, *, url: str, glob: str, now: float
    ) -> Optional[str]:
        """Provides the authoirzation header that the subscriber should use to subscribe
        to any topic that matches the given glob at the given url.

        Args:
            url (str): the url the subscriber is subscribing to
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


class AuthConfig(IncomingAuthConfig, OutgoingAuthConfig, Protocol): ...


class AuthConfigFromParts:
    """Convenience class to combine an incoming and outgoing auth config into an
    auth config
    """

    def __init__(self, incoming: IncomingAuthConfig, outgoing: OutgoingAuthConfig):
        self.incoming = incoming
        self.outgoing = outgoing

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

    async def setup_subscribe_exact_authorization(
        self, /, *, url: str, exact: bytes, now: float
    ) -> Optional[str]:
        return await self.outgoing.setup_subscribe_exact_authorization(
            url=url,
            exact=exact,
            now=now,
        )

    async def setup_subscribe_glob_authorization(
        self, /, *, url: str, glob: str, now: float
    ) -> Optional[str]:
        return await self.outgoing.setup_subscribe_glob_authorization(
            url=url,
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


if TYPE_CHECKING:
    _: Type[AuthConfig] = AuthConfigFromParts
