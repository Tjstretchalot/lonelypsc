import hmac
from typing import TYPE_CHECKING, Literal, Optional, Type

from lonelypsp.stateless.make_strong_etag import StrongEtag

from lonelypsc.config.auth_config import IncomingAuthConfig, OutgoingAuthConfig


class IncomingTokenAuth:
    """Implements the IncomingAuthConfig protocol by requiring the authorization
    header matches a specific value (in the form `Bearer <token>`). In order for
    this to be useful, the headers must be encrypted, typically via HTTPS.
    """

    def __init__(self, token: str) -> None:
        self.authorization = f"Bearer {token}"
        """The exact authorization header we expect to receive"""

    async def setup_incoming_auth(self) -> None: ...
    async def teardown_incoming_auth(self) -> None: ...

    def _check(
        self, authorization: Optional[str]
    ) -> Literal["ok", "unauthorized", "forbidden", "unavailable"]:
        if authorization is None:
            return "unauthorized"

        if not hmac.compare_digest(authorization, self.authorization):
            return "forbidden"

        return "ok"

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
        return self._check(authorization)

    async def is_missed_allowed(
        self,
        /,
        *,
        recovery: str,
        topic: bytes,
        now: float,
        authorization: Optional[str],
    ) -> Literal["ok", "unauthorized", "forbidden", "unavailable"]:
        return self._check(authorization)


class OutgoingTokenAuth:
    """Implements the OutgoingAuthConfig protocol by setting the authorization header
    to a specific value, of the form `Bearer <token>`. In order for this to be useful,
    the headers must be encrypted, typically via HTTPS.
    """

    def __init__(self, token: str) -> None:
        self.authorization = f"Bearer {token}"
        """The exact authorization header we will send"""

    async def setup_outgoing_auth(self) -> None: ...
    async def teardown_outgoing_auth(self) -> None: ...

    async def setup_subscribe_exact_authorization(
        self, /, *, url: str, recovery: Optional[str], exact: bytes, now: float
    ) -> Optional[str]:
        return self.authorization

    async def setup_subscribe_glob_authorization(
        self, /, *, url: str, recovery: Optional[str], glob: str, now: float
    ) -> Optional[str]:
        return self.authorization

    async def setup_notify_authorization(
        self, /, *, topic: bytes, message_sha512: bytes, now: float
    ) -> Optional[str]:
        return self.authorization

    async def setup_check_subscriptions_authorization(
        self, /, *, url: str, now: float
    ) -> Optional[str]:
        return self.authorization

    async def setup_set_subscriptions_authorization(
        self, /, *, url: str, strong_etag: StrongEtag, now: float
    ) -> Optional[str]:
        return self.authorization


if TYPE_CHECKING:
    _: Type[IncomingAuthConfig] = IncomingTokenAuth
    __: Type[OutgoingAuthConfig] = OutgoingTokenAuth
