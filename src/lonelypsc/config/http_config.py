from typing import (
    TYPE_CHECKING,
    Generic,
    List,
    Literal,
    Optional,
    Protocol,
    Type,
    TypedDict,
    TypeVar,
    Union,
)

from fastapi import APIRouter
from lonelypsp.auth.config import AuthConfig, AuthResult
from lonelypsp.auth.set_subscriptions_info import SetSubscriptionsInfo
from lonelypsp.stateful.messages.configure import S2B_Configure
from lonelypsp.stateful.messages.confirm_configure import B2S_ConfirmConfigure
from lonelypsp.stateful.messages.continue_notify import B2S_ContinueNotify
from lonelypsp.stateful.messages.continue_receive import S2B_ContinueReceive
from lonelypsp.stateful.messages.disable_zstd_custom import B2S_DisableZstdCustom
from lonelypsp.stateful.messages.enable_zstd_custom import B2S_EnableZstdCustom
from lonelypsp.stateful.messages.enable_zstd_preset import B2S_EnableZstdPreset
from lonelypsp.stateless.make_strong_etag import StrongEtag
from lonelypsp.tracing.stateless.root import (
    InitializerTcontra,
    StatelessTracingSubscriberRoot,
)

from lonelypsc.config.config import PubSubBroadcasterConfig


class HttpPubSubBindUvicornConfig(TypedDict):
    """When used for the `bind` parameter on an `HttpPubSubConfig` object,
    indicates you want to use a relatively standard http server with uvicorn.
    This is provided as the most common option, but if you need to configure
    anything more specific (e.g., middleware for logging, etc), you should
    use `HttpPubSubBindManualConfig` instead.

    Note - this is just converted into a manual config via the module
    `lonelypsc.config.helpers.uvicorn_bind_config`
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


class HttpPubSubBindConfig(Protocol):
    """Determines how the broadcaster can reach us"""

    @property
    def bind(self) -> Union[HttpPubSubBindUvicornConfig, HttpPubSubBindManualConfig]:
        """Determines how the FastAPI APIRouter is converted into an HTTP server"""

    @property
    def host(self) -> str:
        """The schema and address that the broadcaster should use to reach us.

        This CAN include a path component, which is assumed to be a prefix for
        the standard routes, and a fragment, which is kept (but potentially
        appended to) for our subscriptions. When using multiple
        lonelypscs at the same schema and adddress they need to be
        distinguished either by path or fragment

        WARN:
            This information is _not_ incorporated into the APIRouter that is passed
            to the bind config. In other words, if the APIRouter is served as is
            (like the "uvicorn" strategy does), then you will be expecting to receive
            requests at the root path, which means that a proxy between the broadcaster
            and us must be rewriting requests to the correct path.

            If you want to use a prefix path but do not want to use a rewriting proxy,
            you can use the "manual" strategy and provide the appropriate arguments to
            include_router, e.g., `app.include_router(router, prefix="/myprefix")`.
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


if TYPE_CHECKING:
    _: Type[HttpPubSubBindConfig] = HttpPubSubBindConfigFromParts


class HttpPubSubConnectConfig(Protocol):
    """Determines how we connect to broadcasters"""

    @property
    def broadcasters(self) -> List[PubSubBroadcasterConfig]:
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
        broadcasters: List[PubSubBroadcasterConfig],
        outgoing_retries_per_broadcaster: int,
    ):
        self.broadcasters = broadcasters
        self.outgoing_retries_per_broadcaster = outgoing_retries_per_broadcaster


if TYPE_CHECKING:
    __: Type[HttpPubSubConnectConfig] = HttpPubSubConnectConfigFromParts


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


if TYPE_CHECKING:
    ___: Type[HttpPubSubGenericConfig] = HttpPubSubGenericConfigFromParts


InitializerT = TypeVar("InitializerT")
InitializerTco = TypeVar("InitializerTco", covariant=True)


class TracingConfig(Generic[InitializerTcontra], Protocol):
    @property
    def tracing(self) -> StatelessTracingSubscriberRoot[InitializerTcontra]:
        """Generates tracing objects for the various requests when given some
        initializer data
        """


class HttpPubSubConfig(
    HttpPubSubBindConfig,
    HttpPubSubConnectConfig,
    HttpPubSubGenericConfig,
    AuthConfig,
    TracingConfig[InitializerTcontra],
    Generic[InitializerTcontra],
    Protocol,
):
    """Configures the library, including how we are reached, how we connect
    to broadcasters, and how we provide/verify authorization
    """


class HttpPubSubConfigFromParts(Generic[InitializerT]):
    """An implementation of HttpPubSubConfig from the various parts"""

    def __init__(
        self,
        /,
        *,
        bind_config: HttpPubSubBindConfig,
        connect_config: HttpPubSubConnectConfig,
        generic_config: HttpPubSubGenericConfig,
        auth_config: AuthConfig,
        tracing: StatelessTracingSubscriberRoot[InitializerT],
    ):
        self.bind_config = bind_config
        self.connect_config = connect_config
        self.generic_config = generic_config
        self.auth = auth_config
        self.tracing = tracing

    @property
    def bind(self) -> Union[HttpPubSubBindUvicornConfig, HttpPubSubBindManualConfig]:
        return self.bind_config.bind

    @property
    def host(self) -> str:
        return self.bind_config.host

    @property
    def broadcasters(self) -> List[PubSubBroadcasterConfig]:
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

    async def setup_to_broadcaster_auth(self) -> None:
        await self.auth.setup_to_broadcaster_auth()

    async def teardown_to_broadcaster_auth(self) -> None:
        await self.auth.teardown_to_broadcaster_auth()

    async def setup_to_subscriber_auth(self) -> None:
        await self.auth.setup_to_subscriber_auth()

    async def teardown_to_subscriber_auth(self) -> None:
        await self.auth.teardown_to_subscriber_auth()

    async def authorize_subscribe_exact(
        self,
        /,
        *,
        tracing: bytes,
        url: str,
        recovery: Optional[str],
        exact: bytes,
        now: float,
    ) -> Optional[str]:
        return await self.auth.authorize_subscribe_exact(
            tracing=tracing, url=url, recovery=recovery, exact=exact, now=now
        )

    async def is_subscribe_exact_allowed(
        self,
        /,
        *,
        tracing: bytes,
        url: str,
        recovery: Optional[str],
        exact: bytes,
        now: float,
        authorization: Optional[str],
    ) -> AuthResult:
        return await self.auth.is_subscribe_exact_allowed(
            tracing=tracing,
            url=url,
            recovery=recovery,
            exact=exact,
            now=now,
            authorization=authorization,
        )

    async def authorize_subscribe_glob(
        self,
        /,
        *,
        tracing: bytes,
        url: str,
        recovery: Optional[str],
        glob: str,
        now: float,
    ) -> Optional[str]:
        return await self.auth.authorize_subscribe_glob(
            tracing=tracing, url=url, recovery=recovery, glob=glob, now=now
        )

    async def is_subscribe_glob_allowed(
        self,
        /,
        *,
        tracing: bytes,
        url: str,
        recovery: Optional[str],
        glob: str,
        now: float,
        authorization: Optional[str],
    ) -> AuthResult:
        return await self.auth.is_subscribe_glob_allowed(
            tracing=tracing,
            url=url,
            recovery=recovery,
            glob=glob,
            now=now,
            authorization=authorization,
        )

    async def authorize_notify(
        self,
        /,
        *,
        tracing: bytes,
        topic: bytes,
        identifier: bytes,
        message_sha512: bytes,
        now: float,
    ) -> Optional[str]:
        return await self.auth.authorize_notify(
            tracing=tracing,
            topic=topic,
            identifier=identifier,
            message_sha512=message_sha512,
            now=now,
        )

    async def is_notify_allowed(
        self,
        /,
        *,
        tracing: bytes,
        topic: bytes,
        identifier: bytes,
        message_sha512: bytes,
        now: float,
        authorization: Optional[str],
    ) -> AuthResult:
        return await self.auth.is_notify_allowed(
            tracing=tracing,
            topic=topic,
            identifier=identifier,
            message_sha512=message_sha512,
            now=now,
            authorization=authorization,
        )

    async def authorize_stateful_configure(
        self,
        /,
        *,
        tracing: bytes,
        subscriber_nonce: bytes,
        enable_zstd: bool,
        enable_training: bool,
        initial_dict: int,
    ) -> Optional[str]:
        return await self.auth.authorize_stateful_configure(
            tracing=tracing,
            subscriber_nonce=subscriber_nonce,
            enable_zstd=enable_zstd,
            enable_training=enable_training,
            initial_dict=initial_dict,
        )

    async def is_stateful_configure_allowed(
        self, /, *, message: S2B_Configure, now: float
    ) -> AuthResult:
        return await self.auth.is_stateful_configure_allowed(message=message, now=now)

    async def authorize_check_subscriptions(
        self, /, *, tracing: bytes, url: str, now: float
    ) -> Optional[str]:
        return await self.auth.authorize_check_subscriptions(
            tracing=tracing, url=url, now=now
        )

    async def is_check_subscriptions_allowed(
        self, /, *, tracing: bytes, url: str, now: float, authorization: Optional[str]
    ) -> AuthResult:
        return await self.auth.is_check_subscriptions_allowed(
            tracing=tracing, url=url, now=now, authorization=authorization
        )

    async def authorize_set_subscriptions(
        self, /, *, tracing: bytes, url: str, strong_etag: StrongEtag, now: float
    ) -> Optional[str]:
        return await self.auth.authorize_set_subscriptions(
            tracing=tracing, url=url, strong_etag=strong_etag, now=now
        )

    async def is_set_subscriptions_allowed(
        self,
        /,
        *,
        tracing: bytes,
        url: str,
        strong_etag: StrongEtag,
        subscriptions: SetSubscriptionsInfo,
        now: float,
        authorization: Optional[str],
    ) -> AuthResult:
        return await self.auth.is_set_subscriptions_allowed(
            tracing=tracing,
            url=url,
            strong_etag=strong_etag,
            subscriptions=subscriptions,
            now=now,
            authorization=authorization,
        )

    async def authorize_stateful_continue_receive(
        self,
        /,
        *,
        tracing: bytes,
        identifier: bytes,
        part_id: int,
        url: str,
        now: float,
    ) -> Optional[str]:
        return await self.auth.authorize_stateful_continue_receive(
            tracing=tracing, identifier=identifier, part_id=part_id, url=url, now=now
        )

    async def is_stateful_continue_receive_allowed(
        self, /, *, url: str, message: S2B_ContinueReceive, now: float
    ) -> AuthResult:
        return await self.auth.is_stateful_continue_receive_allowed(
            url=url, message=message, now=now
        )

    async def authorize_confirm_receive(
        self,
        /,
        *,
        tracing: bytes,
        identifier: bytes,
        num_subscribers: int,
        url: str,
        now: float,
    ) -> Optional[str]:
        return await self.auth.authorize_confirm_receive(
            tracing=tracing,
            identifier=identifier,
            num_subscribers=num_subscribers,
            url=url,
            now=now,
        )

    async def is_confirm_receive_allowed(
        self,
        /,
        *,
        tracing: bytes,
        identifier: bytes,
        num_subscribers: int,
        url: str,
        now: float,
        authorization: Optional[str],
    ) -> AuthResult:
        return await self.auth.is_confirm_receive_allowed(
            tracing=tracing,
            identifier=identifier,
            num_subscribers=num_subscribers,
            url=url,
            now=now,
            authorization=authorization,
        )

    async def authorize_confirm_missed(
        self, /, *, tracing: bytes, topic: bytes, url: str, now: float
    ) -> Optional[str]:
        return await self.auth.authorize_confirm_missed(
            tracing=tracing, topic=topic, url=url, now=now
        )

    async def is_confirm_missed_allowed(
        self,
        /,
        *,
        tracing: bytes,
        topic: bytes,
        url: str,
        now: float,
        authorization: Optional[str],
    ) -> AuthResult:
        return await self.auth.is_confirm_missed_allowed(
            tracing=tracing, topic=topic, url=url, now=now, authorization=authorization
        )

    async def authorize_receive(
        self,
        /,
        *,
        tracing: bytes,
        url: str,
        topic: bytes,
        message_sha512: bytes,
        identifier: bytes,
        now: float,
    ) -> Optional[str]:
        return await self.auth.authorize_receive(
            tracing=tracing,
            url=url,
            topic=topic,
            message_sha512=message_sha512,
            identifier=identifier,
            now=now,
        )

    async def is_receive_allowed(
        self,
        /,
        *,
        tracing: bytes,
        url: str,
        topic: bytes,
        message_sha512: bytes,
        identifier: bytes,
        now: float,
        authorization: Optional[str],
    ) -> AuthResult:
        return await self.auth.is_receive_allowed(
            tracing=tracing,
            url=url,
            topic=topic,
            message_sha512=message_sha512,
            identifier=identifier,
            now=now,
            authorization=authorization,
        )

    async def authorize_missed(
        self, /, *, tracing: bytes, recovery: str, topic: bytes, now: float
    ) -> Optional[str]:
        return await self.auth.authorize_missed(
            tracing=tracing, recovery=recovery, topic=topic, now=now
        )

    async def is_missed_allowed(
        self,
        /,
        *,
        tracing: bytes,
        recovery: str,
        topic: bytes,
        now: float,
        authorization: Optional[str],
    ) -> AuthResult:
        return await self.auth.is_missed_allowed(
            tracing=tracing,
            recovery=recovery,
            topic=topic,
            now=now,
            authorization=authorization,
        )

    async def authorize_confirm_subscribe_exact(
        self,
        /,
        *,
        tracing: bytes,
        url: str,
        recovery: Optional[str],
        exact: bytes,
        now: float,
    ) -> Optional[str]:
        return await self.auth.authorize_confirm_subscribe_exact(
            tracing=tracing, url=url, recovery=recovery, exact=exact, now=now
        )

    async def is_confirm_subscribe_exact_allowed(
        self,
        /,
        *,
        tracing: bytes,
        url: str,
        recovery: Optional[str],
        exact: bytes,
        now: float,
        authorization: Optional[str],
    ) -> AuthResult:
        return await self.auth.is_confirm_subscribe_exact_allowed(
            tracing=tracing,
            url=url,
            recovery=recovery,
            exact=exact,
            now=now,
            authorization=authorization,
        )

    async def authorize_confirm_subscribe_glob(
        self,
        /,
        *,
        tracing: bytes,
        url: str,
        recovery: Optional[str],
        glob: str,
        now: float,
    ) -> Optional[str]:
        return await self.auth.authorize_confirm_subscribe_glob(
            tracing=tracing, url=url, recovery=recovery, glob=glob, now=now
        )

    async def is_confirm_subscribe_glob_allowed(
        self,
        /,
        *,
        tracing: bytes,
        url: str,
        recovery: Optional[str],
        glob: str,
        now: float,
        authorization: Optional[str],
    ) -> AuthResult:
        return await self.auth.is_confirm_subscribe_glob_allowed(
            tracing=tracing,
            url=url,
            recovery=recovery,
            glob=glob,
            now=now,
            authorization=authorization,
        )

    async def authorize_confirm_notify(
        self,
        /,
        *,
        tracing: bytes,
        identifier: bytes,
        subscribers: int,
        topic: bytes,
        message_sha512: bytes,
        now: float,
    ) -> Optional[str]:
        return await self.auth.authorize_confirm_notify(
            tracing=tracing,
            identifier=identifier,
            subscribers=subscribers,
            topic=topic,
            message_sha512=message_sha512,
            now=now,
        )

    async def is_confirm_notify_allowed(
        self,
        /,
        *,
        tracing: bytes,
        identifier: bytes,
        subscribers: int,
        topic: bytes,
        message_sha512: bytes,
        authorization: Optional[str],
        now: float,
    ) -> AuthResult:
        return await self.auth.is_confirm_notify_allowed(
            tracing=tracing,
            identifier=identifier,
            subscribers=subscribers,
            topic=topic,
            message_sha512=message_sha512,
            authorization=authorization,
            now=now,
        )

    async def authorize_check_subscriptions_response(
        self,
        /,
        *,
        tracing: bytes,
        strong_etag: StrongEtag,
        now: float,
    ) -> Optional[str]:
        return await self.auth.authorize_check_subscriptions_response(
            tracing=tracing, strong_etag=strong_etag, now=now
        )

    async def is_check_subscription_response_allowed(
        self,
        /,
        *,
        tracing: bytes,
        strong_etag: StrongEtag,
        authorization: Optional[str],
        now: float,
    ) -> AuthResult:
        return await self.auth.is_check_subscription_response_allowed(
            tracing=tracing,
            strong_etag=strong_etag,
            authorization=authorization,
            now=now,
        )

    async def authorize_set_subscriptions_response(
        self, /, *, tracing: bytes, strong_etag: StrongEtag, now: float
    ) -> Optional[str]:
        return await self.auth.authorize_set_subscriptions_response(
            tracing=tracing, strong_etag=strong_etag, now=now
        )

    async def is_set_subscription_response_allowed(
        self,
        /,
        *,
        tracing: bytes,
        strong_etag: StrongEtag,
        authorization: Optional[str],
        now: float,
    ) -> AuthResult:
        return await self.auth.is_set_subscription_response_allowed(
            tracing=tracing,
            strong_etag=strong_etag,
            authorization=authorization,
            now=now,
        )

    async def authorize_stateful_confirm_configure(
        self, /, *, broadcaster_nonce: bytes, tracing: bytes, now: float
    ) -> Optional[str]:
        return await self.auth.authorize_stateful_confirm_configure(
            broadcaster_nonce=broadcaster_nonce, tracing=tracing, now=now
        )

    async def is_stateful_confirm_configure_allowed(
        self, /, *, message: B2S_ConfirmConfigure, now: float
    ) -> AuthResult:
        return await self.auth.is_stateful_confirm_configure_allowed(
            message=message, now=now
        )

    async def authorize_stateful_enable_zstd_preset(
        self,
        /,
        *,
        tracing: bytes,
        url: str,
        compressor_identifier: int,
        compression_level: int,
        min_size: int,
        max_size: int,
        now: float,
    ) -> Optional[str]:
        return await self.auth.authorize_stateful_enable_zstd_preset(
            tracing=tracing,
            url=url,
            compressor_identifier=compressor_identifier,
            compression_level=compression_level,
            min_size=min_size,
            max_size=max_size,
            now=now,
        )

    async def is_stateful_enable_zstd_preset_allowed(
        self, /, *, url: str, message: B2S_EnableZstdPreset, now: float
    ) -> AuthResult:
        return await self.auth.is_stateful_enable_zstd_preset_allowed(
            url=url, message=message, now=now
        )

    async def authorize_stateful_enable_zstd_custom(
        self,
        /,
        *,
        tracing: bytes,
        url: str,
        compressor_identifier: int,
        compression_level: int,
        min_size: int,
        max_size: int,
        sha512: bytes,
        now: float,
    ) -> Optional[str]:
        return await self.auth.authorize_stateful_enable_zstd_custom(
            tracing=tracing,
            url=url,
            compressor_identifier=compressor_identifier,
            compression_level=compression_level,
            min_size=min_size,
            max_size=max_size,
            sha512=sha512,
            now=now,
        )

    async def is_stateful_enable_zstd_custom_allowed(
        self, /, *, url: str, message: B2S_EnableZstdCustom, now: float
    ) -> AuthResult:
        return await self.auth.is_stateful_enable_zstd_custom_allowed(
            url=url, message=message, now=now
        )

    async def authorize_stateful_disable_zstd_custom(
        self, /, *, tracing: bytes, compressor_identifier: int, url: str, now: float
    ) -> Optional[str]:
        return await self.auth.authorize_stateful_disable_zstd_custom(
            tracing=tracing,
            compressor_identifier=compressor_identifier,
            url=url,
            now=now,
        )

    async def is_stateful_disable_zstd_custom_allowed(
        self, /, *, url: str, message: B2S_DisableZstdCustom, now: float
    ) -> AuthResult:
        return await self.auth.is_stateful_disable_zstd_custom_allowed(
            url=url, message=message, now=now
        )

    async def authorize_stateful_continue_notify(
        self, /, *, tracing: bytes, identifier: bytes, part_id: int, now: float
    ) -> Optional[str]:
        return await self.auth.authorize_stateful_continue_notify(
            tracing=tracing, identifier=identifier, part_id=part_id, now=now
        )

    async def is_stateful_continue_notify_allowed(
        self, /, *, message: B2S_ContinueNotify, now: float
    ) -> AuthResult:
        return await self.auth.is_stateful_continue_notify_allowed(
            message=message, now=now
        )


if TYPE_CHECKING:
    ____: Type[HttpPubSubConfig] = HttpPubSubConfigFromParts


def make_http_pub_sub_config(
    *,
    bind: Union[HttpPubSubBindUvicornConfig, HttpPubSubBindManualConfig],
    host: str,
    broadcasters: List[PubSubBroadcasterConfig],
    outgoing_retries_per_broadcaster: int,
    message_body_spool_size: int,
    outgoing_http_timeout_total: Optional[float],
    outgoing_http_timeout_connect: Optional[float],
    outgoing_http_timeout_sock_read: Optional[float],
    outgoing_http_timeout_sock_connect: Optional[float],
    outgoing_retry_ambiguous: bool,
    auth: AuthConfig,
    tracing: StatelessTracingSubscriberRoot[InitializerT],
) -> HttpPubSubConfig[InitializerT]:
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
        tracing=tracing,
    )
