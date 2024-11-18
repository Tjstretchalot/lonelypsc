from fastapi import APIRouter
from typing import (
    TYPE_CHECKING,
    List,
    Literal,
    Optional,
    Type,
    TypedDict,
    Protocol,
    Union,
)

from httppubsubclient.config.auth_config import AuthConfig


class HttpPubSubBindUvicornConfig(TypedDict):
    """When used for the `bind` parameter on an `HttpPubSubConfig` object,
    indicates you want to use a relatively standard http server with uvicorn.
    This is provided as the most common option, but if you need to configure
    anything more specific (e.g., middleware for logging, etc), you should
    use `HttpPubSubBindManualConfig` instead.

    Note - this is just converted into a manual config via the module
    `httppubsubclient.config.helpers.uvicorn_bind_config`
    """

    type: Literal["uvicorn"]
    """discriminates the type"""

    host: str
    """What address to bind to, e.g., 0.0.0.0 for all interfaces"""

    port: int
    """What port to bind to. As a very loose guidance, use 3002 for subscribers 
    and 3003 for broadcasters
    """


class HttpPubSubBindManualCallback(Protocol):
    """Describes the callback that binds the http server in manual mode"""

    async def __call__(self, router: APIRouter) -> None:
        """Serves requests continuously with the given router, not returning
        until the server is shutdown. We will cancel the task when the client
        is being exitted. Should never return normally, as then there would be
        no way to know when to shut down the server. The only exception is if
        you are guarranteeing only one client in the lifetime of the application.
        """


class HttpPubSubBindManualConfig(TypedDict):
    """When used for the `bind` parameter on an `HttpPubSubConfig` object,
    indicates you can convert the APIRouter into an HTTP server yourself.
    Often its convenient to use this just because you want to provide more
    parameters to uvicorn (e.g., TLS), or to add middleware, etc.
    """

    type: Literal["manual"]
    """discriminates the type"""

    callback: HttpPubSubBindManualCallback
    """The callback that binds the http server."""


class HttpPubSubBroadcasterConfig(TypedDict):
    """Indicates how to connect to a broadcaster. We leave this type open for
    expansion in the future (compared to a string) in case we want to add e.g.
    optional priority levels
    """

    host: str
    """The host of the broadcaster, e.g., `http://localhost:3003`. Must include
    the schema and port (if not default) and may include a path (if prefixing the
    standard paths), such that f'{host}/v1/subscribe' is the full path to subscribe
    endpoint.
    """


class HttpPubSubBindConfig(Protocol):
    """Determines how the broadcaster can reach us"""

    @property
    def bind(self) -> Union[HttpPubSubBindUvicornConfig, HttpPubSubBindManualConfig]:
        """Determines how the FastAPI APIRouter is converted into an HTTP server"""

    @property
    def host(self) -> str:
        """The schema and address that the broadcaster should use to reach us. This
        can include a path component, which is assumed to be a prefix for the standard
        routes, and a fragment, which is kept (but potentially appended to) for our
        subscriptions. When using multiple httppubsubclients at the same schema and
        adddress they need to be distinguished either by path or fragment
        """


class HttpPubSubBindConfigFromParts:
    """Implementation of HttpPubSubBindConfig from the various parts"""

    def __init__(
        self,
        /,
        *,
        bind: Union[HttpPubSubBindUvicornConfig, HttpPubSubBindManualConfig],
        host: str,
    ):
        self.bind = bind
        self.host = host


class HttpPubSubConnectConfig(Protocol):
    """Determines how we connect to broadcasters"""

    @property
    def broadcasters(self) -> List[HttpPubSubBroadcasterConfig]:
        """The broadcasters that we can connect to for making subscription requests
        and requesting a message be broadcast
        """

    @property
    def outgoing_retries_per_broadcaster(self) -> int:
        """How many times to retry a broadcaster before giving up"""


class HttpPubSubConnectConfigFromParts:
    """Implementation of HttpPubSubConnectConfig from the various parts"""

    def __init__(
        self,
        /,
        *,
        broadcasters: List[HttpPubSubBroadcasterConfig],
        outgoing_retries_per_broadcaster: int,
    ):
        self.broadcasters = broadcasters
        self.outgoing_retries_per_broadcaster = outgoing_retries_per_broadcaster


class HttpPubSubGenericConfig(Protocol):
    """Generic network configuration."""

    @property
    def message_body_spool_size(self) -> int:
        """If the message body exceeds this size we always switch to a temporary file."""

    @property
    def outgoing_http_timeout_total(self) -> Optional[float]:
        """The total timeout for outgoing (to broadcaster) http requests in seconds"""

    @property
    def outgoing_http_timeout_connect(self) -> Optional[float]:
        """The timeout for connecting to the broadcaster in seconds, which may include multiple
        socket attempts
        """

    @property
    def outgoing_http_timeout_sock_read(self) -> Optional[float]:
        """The timeout for reading from a broadcaster socket in seconds before the socket is
        considered dead
        """

    @property
    def outgoing_http_timeout_sock_connect(self) -> Optional[float]:
        """The timeout for a single socket connecting to a broadcaster before we give up, in seconds"""

    @property
    def outgoing_retry_ambiguous(self) -> bool:
        """Determines how to handle when we are unsure if a broadcaster received a message
        and the message is not necessarily idempotent.

        - **True** _(recommended)_: if we are unsure, we will assume the broadcaster
          did NOT receive the message.

        - **False** _(not recommended)_: if we are unsure, we will assume the
          broadcaster DID receive the message.

        Note that this scenario is theoretically guarranteed to be possible;
        this is known as the Two Generals' problem. However, the client may
        treat some scenarios as ambiguous where it could be known it wasn't
        received with a lower-level inspection of the packets.

        Thus, this is a tradeoff between duplicating messages when the guess is
        wrong (True) and dropping messages when the guess is wrong (False).
        """


class HttpPubSubGenericConfigFromParts:
    """Convenience class that allows you to create a GenericConfig protocol
    satisfying object from parts"""

    def __init__(
        self,
        message_body_spool_size: int,
        outgoing_http_timeout_total: Optional[float],
        outgoing_http_timeout_connect: Optional[float],
        outgoing_http_timeout_sock_read: Optional[float],
        outgoing_http_timeout_sock_connect: Optional[float],
        outgoing_retry_ambiguous: bool,
    ):
        self.message_body_spool_size = message_body_spool_size
        self.outgoing_http_timeout_total = outgoing_http_timeout_total
        self.outgoing_http_timeout_connect = outgoing_http_timeout_connect
        self.outgoing_http_timeout_sock_read = outgoing_http_timeout_sock_read
        self.outgoing_http_timeout_sock_connect = outgoing_http_timeout_sock_connect
        self.outgoing_retry_ambiguous = outgoing_retry_ambiguous


class HttpPubSubConfig(
    HttpPubSubBindConfig,
    HttpPubSubConnectConfig,
    HttpPubSubGenericConfig,
    AuthConfig,
    Protocol,
):
    """Configures the library, including how we are reached, how we connect
    to broadcasters, and how we provide/verify authorization
    """


class HttpPubSubConfigFromParts:
    """An implementation of HttpPubSubConfig from the various parts"""

    def __init__(
        self,
        /,
        *,
        bind_config: HttpPubSubBindConfig,
        connect_config: HttpPubSubConnectConfig,
        generic_config: HttpPubSubGenericConfig,
        auth_config: AuthConfig,
    ):
        self.bind_config = bind_config
        self.connect_config = connect_config
        self.generic_config = generic_config
        self.auth_config = auth_config

    @property
    def bind(self) -> Union[HttpPubSubBindUvicornConfig, HttpPubSubBindManualConfig]:
        return self.bind_config.bind

    @property
    def host(self) -> str:
        return self.bind_config.host

    @property
    def broadcasters(self) -> List[HttpPubSubBroadcasterConfig]:
        return self.connect_config.broadcasters

    @property
    def outgoing_retries_per_broadcaster(self) -> int:
        return self.connect_config.outgoing_retries_per_broadcaster

    @property
    def message_body_spool_size(self) -> int:
        return self.generic_config.message_body_spool_size

    @property
    def outgoing_http_timeout_total(self) -> Optional[float]:
        return self.generic_config.outgoing_http_timeout_total

    @property
    def outgoing_http_timeout_connect(self) -> Optional[float]:
        return self.generic_config.outgoing_http_timeout_connect

    @property
    def outgoing_http_timeout_sock_read(self) -> Optional[float]:
        return self.generic_config.outgoing_http_timeout_sock_read

    @property
    def outgoing_http_timeout_sock_connect(self) -> Optional[float]:
        return self.generic_config.outgoing_http_timeout_sock_connect

    @property
    def outgoing_retry_ambiguous(self) -> bool:
        return self.generic_config.outgoing_retry_ambiguous

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
        return await self.auth_config.is_receive_allowed(
            url=url,
            topic=topic,
            message_sha512=message_sha512,
            now=now,
            authorization=authorization,
        )

    async def setup_incoming_auth(self) -> None:
        await self.auth_config.setup_incoming_auth()

    async def teardown_incoming_auth(self) -> None:
        await self.auth_config.teardown_incoming_auth()

    async def setup_outgoing_auth(self) -> None:
        await self.auth_config.setup_outgoing_auth()

    async def teardown_outgoing_auth(self) -> None:
        await self.auth_config.teardown_outgoing_auth()

    async def setup_subscribe_exact_authorization(
        self, /, *, url: str, exact: bytes, now: float
    ) -> Optional[str]:
        return await self.auth_config.setup_subscribe_exact_authorization(
            url=url,
            exact=exact,
            now=now,
        )

    async def setup_subscribe_glob_authorization(
        self, /, *, url: str, glob: str, now: float
    ) -> Optional[str]:
        return await self.auth_config.setup_subscribe_glob_authorization(
            url=url,
            glob=glob,
            now=now,
        )

    async def setup_notify_authorization(
        self, /, *, topic: bytes, message_sha512: bytes, now: float
    ) -> Optional[str]:
        return await self.auth_config.setup_notify_authorization(
            topic=topic,
            message_sha512=message_sha512,
            now=now,
        )


def make_http_pub_sub_config(
    *,
    bind: Union[HttpPubSubBindUvicornConfig, HttpPubSubBindManualConfig],
    host: str,
    broadcasters: List[HttpPubSubBroadcasterConfig],
    outgoing_retries_per_broadcaster: int,
    message_body_spool_size: int,
    outgoing_http_timeout_total: Optional[float],
    outgoing_http_timeout_connect: Optional[float],
    outgoing_http_timeout_sock_read: Optional[float],
    outgoing_http_timeout_sock_connect: Optional[float],
    outgoing_retry_ambiguous: bool,
    auth: AuthConfig,
) -> HttpPubSubConfig:
    """Convenience function to make a HttpPubSubConfig object without excessive nesting
    if you are specifying everything that doesn't need to be synced with the broadcaster
    within code.
    """
    return HttpPubSubConfigFromParts(
        bind_config=HttpPubSubBindConfigFromParts(bind=bind, host=host),
        connect_config=HttpPubSubConnectConfigFromParts(
            broadcasters=broadcasters,
            outgoing_retries_per_broadcaster=outgoing_retries_per_broadcaster,
        ),
        generic_config=HttpPubSubGenericConfigFromParts(
            message_body_spool_size=message_body_spool_size,
            outgoing_http_timeout_total=outgoing_http_timeout_total,
            outgoing_http_timeout_connect=outgoing_http_timeout_connect,
            outgoing_http_timeout_sock_read=outgoing_http_timeout_sock_read,
            outgoing_http_timeout_sock_connect=outgoing_http_timeout_sock_connect,
            outgoing_retry_ambiguous=outgoing_retry_ambiguous,
        ),
        auth_config=auth,
    )


if TYPE_CHECKING:
    _: Type[HttpPubSubBindConfig] = HttpPubSubBindConfigFromParts
    __: Type[HttpPubSubConnectConfig] = HttpPubSubConnectConfigFromParts
    ___: Type[HttpPubSubGenericConfig] = HttpPubSubGenericConfigFromParts
    ____: Type[HttpPubSubConfig] = HttpPubSubConfigFromParts
