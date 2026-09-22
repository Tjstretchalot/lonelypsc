import asyncio
from typing import Any

from lonelypsp.stateless.constants import BroadcasterToSubscriberStatelessMessageType

from lonelypsc.http_client import HttpPubSubClientConnector


class _Config:
    host = "https://subscriber.example"

    async def authorize_check_subscriptions(self, **kwargs: Any) -> None:
        return None

    async def is_check_subscription_response_allowed(self, **kwargs: Any) -> bool:
        self.response_checked = True
        return True


class _Connector(HttpPubSubClientConnector[_Config]):
    async def _make_small_request(self, **kwargs: Any) -> bytes:
        return (
            int(
                BroadcasterToSubscriberStatelessMessageType.RESPONSE_CHECK_SUBSCRIPTIONS
            ).to_bytes(2, "big")
            + b"\x00\x00"  # authorization length
            b"\x00\x00"  # tracing length
            b"\x00"  # strong etag format
            + b"etag".ljust(64, b"\x00")
        )


async def _check_response_authorization() -> None:
    config = _Config()
    connector = _Connector(config)
    connector._session = object()  # type: ignore[assignment]

    await connector.check_subscriptions()

    assert config.response_checked is True


def test_check_subscription_response_authorization_is_awaited() -> None:
    asyncio.run(_check_response_authorization())
