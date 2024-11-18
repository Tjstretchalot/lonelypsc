from typing import TYPE_CHECKING, Literal, Optional, Type

from httppubsubclient.config.auth_config import IncomingAuthConfig, OutgoingAuthConfig


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


class OutgoingNoneAuth:
    """Implements the OutgoingAuthConfig protocol with no-ops. Generally, use HMAC instead
    if you just want minimal setup, as it only requires syncing a single secret
    (hmac is still effective without https)
    """

    async def setup_outgoing_auth(self) -> None: ...
    async def teardown_outgoing_auth(self) -> None: ...

    async def setup_subscribe_exact_authorization(
        self, /, *, url: str, exact: bytes, now: float
    ) -> Optional[str]:
        return None

    async def setup_subscribe_glob_authorization(
        self, /, *, url: str, glob: str, now: float
    ) -> Optional[str]:
        return None

    async def setup_notify_authorization(
        self, /, *, topic: bytes, message_sha512: bytes, now: float
    ) -> Optional[str]:
        return None


if TYPE_CHECKING:
    _: Type[IncomingAuthConfig] = IncomingNoneAuth
    __: Type[OutgoingAuthConfig] = OutgoingNoneAuth
