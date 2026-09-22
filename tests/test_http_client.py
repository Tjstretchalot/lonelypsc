import asyncio
from typing import Any, List, cast

from lonelypsp.stateless.constants import BroadcasterToSubscriberStatelessMessageType
from lonelypsp.stateless.make_strong_etag import (
    GlobAndRecovery,
    StrongEtag,
    TopicAndRecovery,
    make_strong_etag,
)

from lonelypsc.config.http_config import HttpPubSubGenericConfigFromParts
from lonelypsc.http_client import HttpPubSubClientConnector


class _Config:
    host = "https://subscriber.example"
    resubscribe_interval = 60.0

    async def authorize_check_subscriptions(self, **kwargs: Any) -> None:
        return None

    async def is_check_subscription_response_allowed(self, **kwargs: Any) -> bool:
        self.response_checked = True
        return True


class _Connector(HttpPubSubClientConnector[Any]):
    async def _make_small_request(self, **kwargs: Any) -> bytes:
        return (
            int(
                BroadcasterToSubscriberStatelessMessageType.RESPONSE_CHECK_SUBSCRIPTIONS
            ).to_bytes(2, "big")
            + b"\x00\x00"  # authorization length
            b"\x00\x00"  # tracing length
            b"\x00" + b"etag".ljust(64, b"\x00")  # strong etag format
        )


class _ReconcileConnector(HttpPubSubClientConnector[Any]):
    def __init__(self, config: _Config) -> None:
        super().__init__(cast(Any, config))
        self.actual = StrongEtag(format=0, etag=b"wrong")
        self.check_calls = 0
        self.set_calls: List[tuple[List[bytes], List[str]]] = []

    async def _check_subscriptions(self) -> StrongEtag:
        self.check_calls += 1
        return self.actual

    async def _set_subscriptions(
        self, /, *, exact: List[bytes], globs: List[str]
    ) -> None:
        self.set_calls.append((exact, globs))


class _LifecycleConfig:
    host = "https://subscriber.example"
    broadcasters = [{"host": "https://broadcaster.example"}]
    outgoing_retries_per_broadcaster = 1
    resubscribe_interval = 60.0
    outgoing_http_timeout_total = None
    outgoing_http_timeout_connect = None
    outgoing_http_timeout_sock_read = None
    outgoing_http_timeout_sock_connect = None


async def _check_response_authorization() -> None:
    config = _Config()
    connector = _Connector(cast(Any, config))
    connector._session = object()  # type: ignore[assignment]

    await connector.check_subscriptions()

    assert config.response_checked is True


def test_check_subscription_response_authorization_is_awaited() -> None:
    asyncio.run(_check_response_authorization())


def test_generic_config_defaults_resubscribe_interval() -> None:
    config = HttpPubSubGenericConfigFromParts(1, None, None, None, None, True)

    assert config.resubscribe_interval == 300.0


async def _resubscribe_repairs_outdated_subscriptions() -> None:
    config = _Config()
    connector = _ReconcileConnector(cast(Any, config))
    connector._desired_exact.add(b"topic")
    connector._desired_globs.add("events/*")

    await connector._check_and_resubscribe()

    assert connector.check_calls == 1
    assert connector.set_calls == [([b"topic"], ["events/*"])]


async def _resubscribe_skips_matching_subscriptions() -> None:
    config = _Config()
    connector = _ReconcileConnector(cast(Any, config))
    connector._desired_exact.add(b"topic")
    connector._desired_globs.add("events/*")
    connector.actual = make_strong_etag(
        connector._receive_url,
        [TopicAndRecovery(b"topic", connector._recovery_url)],
        [GlobAndRecovery("events/*", connector._recovery_url)],
    )

    await connector._check_and_resubscribe()

    assert connector.check_calls == 1
    assert connector.set_calls == []


async def _resubscribe_skips_without_subscriptions() -> None:
    connector = _ReconcileConnector(cast(Any, _Config()))

    await connector._check_and_resubscribe()

    assert connector.check_calls == 0
    assert connector.set_calls == []


async def _resubscribe_task_is_cancelled_on_teardown() -> None:
    connector: HttpPubSubClientConnector[Any] = HttpPubSubClientConnector(
        cast(Any, _LifecycleConfig())
    )

    await connector.setup_connector()
    assert connector._resubscribe_task is not None

    await connector.teardown_connector()

    assert connector._resubscribe_task is None


def test_resubscribe_repairs_outdated_subscriptions() -> None:
    asyncio.run(_resubscribe_repairs_outdated_subscriptions())


def test_resubscribe_skips_matching_subscriptions() -> None:
    asyncio.run(_resubscribe_skips_matching_subscriptions())


def test_resubscribe_skips_without_subscriptions() -> None:
    asyncio.run(_resubscribe_skips_without_subscriptions())


def test_resubscribe_task_is_cancelled_on_teardown() -> None:
    asyncio.run(_resubscribe_task_is_cancelled_on_teardown())
