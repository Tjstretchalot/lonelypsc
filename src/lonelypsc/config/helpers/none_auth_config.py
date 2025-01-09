from typing import TYPE_CHECKING, Literal, Optional, Type

from lonelypsp.stateful.messages.confirm_configure import B2S_ConfirmConfigure
from lonelypsp.stateless.make_strong_etag import StrongEtag

from lonelypsc.config.auth_config import IncomingAuthConfig, OutgoingAuthConfig


class IncomingNoneAuth:
    """Implements the IncomingAuthConfig protocol with no-ops. Generally, use HMAC instead
    if you just want minimal setup, as it only requires syncing a single secret
    (hmac is still effective without https)
    """

    async def setup_incoming_auth(self) -> None: ...
    async def teardown_incoming_auth(self) -> None: ...

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
        return "ok"

    async def is_missed_allowed(
        self,
        /,
        *,
        recovery: str,
        topic: bytes,
        now: float,
        authorization: Optional[str],
    ) -> Literal["ok", "unauthorized", "forbidden", "unavailable"]:
        return "ok"

    async def is_websocket_confirm_configure_allowed(
        self, /, *, message: B2S_ConfirmConfigure, now: float
    ) -> Literal["ok", "unauthorized", "forbidden", "unavailable"]:
        return "ok"


class OutgoingNoneAuth:
    """Implements the OutgoingAuthConfig protocol with no-ops. Generally, use HMAC instead
    if you just want minimal setup, as it only requires syncing a single secret
    (hmac is still effective without https)
    """

    async def setup_outgoing_auth(self) -> None: ...
    async def teardown_outgoing_auth(self) -> None: ...

    async def setup_subscribe_exact_authorization(
        self, /, *, url: str, recovery: Optional[str], exact: bytes, now: float
    ) -> Optional[str]:
        return None

    async def setup_subscribe_glob_authorization(
        self, /, *, url: str, recovery: Optional[str], glob: str, now: float
    ) -> Optional[str]:
        return None

    async def setup_notify_authorization(
        self, /, *, topic: bytes, message_sha512: bytes, now: float
    ) -> Optional[str]:
        return None

    async def setup_check_subscriptions_authorization(
        self, /, *, url: str, now: float
    ) -> Optional[str]:
        return None

    async def setup_set_subscriptions_authorization(
        self, /, *, url: str, strong_etag: StrongEtag, now: float
    ) -> Optional[str]:
        return None

    async def setup_websocket_configure(
        self,
        /,
        *,
        subscriber_nonce: bytes,
        enable_zstd: bool,
        enable_training: bool,
        initial_dict: int,
    ) -> Optional[str]:
        return None


if TYPE_CHECKING:
    _: Type[IncomingAuthConfig] = IncomingNoneAuth
    __: Type[OutgoingAuthConfig] = OutgoingNoneAuth
