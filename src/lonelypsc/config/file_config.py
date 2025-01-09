import json
from typing import Literal, Optional, Tuple, cast

from lonelypsp.auth.config import ToBroadcasterAuthConfig, ToSubscriberAuthConfig
from lonelypsp.auth.helpers.hmac_auth_config import (
    IncomingHmacAuthDBConfig,
    IncomingHmacAuthDBReentrantConfig,
    IncomingHmacAuthSqliteDBConfig,
    ToBroadcasterHmacAuth,
    ToSubscriberHmacAuth,
)
from lonelypsp.auth.helpers.none_auth_config import (
    ToBroadcasterNoneAuth,
    ToSubscriberNoneAuth,
)
from lonelypsp.auth.helpers.token_auth_config import (
    ToBroadcasterTokenAuth,
    ToSubscriberTokenAuth,
)


def get_auth_config_from_file(
    file_path: str,
) -> Tuple[ToSubscriberAuthConfig, ToBroadcasterAuthConfig]:
    """Reads the incoming/outgoing authorization specified in the file path,
    conventionally called `subscriber-secrets.json`, that was dumped
    from `httppubsubserver --setup`
    """
    with open(file_path, "r") as f:
        raw = json.load(f)

    if raw.get("version") != "2":
        raise ValueError(f"Unknown version {raw['version']}")

    to_subscriber_type = cast(
        Literal["hmac", "token", "none"],
        "none" if "to-subscriber" not in raw else raw["to-subscriber"]["type"],
    )
    to_subscriber_secret = cast(
        Optional[str],
        raw["to-subscriber"]["secret"] if to_subscriber_type != "none" else None,
    )

    to_broadcaster_type = cast(
        Literal["hmac", "token", "none"],
        "none" if "to-broadcaster" not in raw else raw["to-broadcaster"]["type"],
    )
    to_broadcaster_secret = cast(
        Optional[str],
        raw["to-broadcaster"]["secret"] if to_broadcaster_type != "none" else None,
    )

    to_subscriber = cast(Optional[ToSubscriberAuthConfig], None)
    to_broadcaster = cast(Optional[ToBroadcasterAuthConfig], None)

    hmac_db_config: Optional[IncomingHmacAuthDBConfig] = None

    if to_subscriber_type == "none":
        to_subscriber = ToSubscriberNoneAuth()
    elif to_subscriber_type == "token":
        assert to_subscriber_secret is not None, "impossible"
        to_subscriber = ToSubscriberTokenAuth(token=to_subscriber_secret)
    elif to_subscriber_type == "hmac":
        assert to_subscriber_secret is not None, "impossible"
        hmac_db_config = IncomingHmacAuthDBReentrantConfig(
            IncomingHmacAuthSqliteDBConfig(":memory:")
        )
        to_subscriber = ToSubscriberHmacAuth(
            secret=to_subscriber_secret, db_config=hmac_db_config
        )

    if to_broadcaster_type == "none":
        to_broadcaster = ToBroadcasterNoneAuth()
    elif to_broadcaster_type == "token":
        assert to_broadcaster_secret is not None, "impossible"
        to_broadcaster = ToBroadcasterTokenAuth(token=to_broadcaster_secret)
    elif to_broadcaster_type == "hmac":
        assert to_broadcaster_secret is not None, "impossible"
        if hmac_db_config is None:
            hmac_db_config = IncomingHmacAuthDBReentrantConfig(
                IncomingHmacAuthSqliteDBConfig(":memory:")
            )
        to_broadcaster = ToBroadcasterHmacAuth(
            secret=to_broadcaster_secret, db_config=hmac_db_config
        )

    assert (
        to_subscriber is not None
    ), f"unknown or unsupported incoming auth type {to_subscriber_type}"
    assert (
        to_broadcaster is not None
    ), f"unknown or unsupported outgoing auth type {to_broadcaster_type}"

    return (to_subscriber, to_broadcaster)
