import json
from typing import Literal, Optional, Tuple, cast

from lonelypsc.config.auth_config import IncomingAuthConfig, OutgoingAuthConfig
from lonelypsc.config.helpers.hmac_auth_config import (
    IncomingHmacAuth,
    IncomingHmacAuthSqliteDBConfig,
    OutgoingHmacAuth,
)
from lonelypsc.config.helpers.none_auth_config import (
    IncomingNoneAuth,
    OutgoingNoneAuth,
)
from lonelypsc.config.helpers.token_auth_config import (
    IncomingTokenAuth,
    OutgoingTokenAuth,
)


def get_auth_config_from_file(
    file_path: str,
) -> Tuple[IncomingAuthConfig, OutgoingAuthConfig]:
    """Reads the incoming/outgoing authorization specified in the file path,
    conventionally called `subscriber-secrets.json`, that was dumped
    from `httppubsubserver --setup`
    """
    with open(file_path, "r") as f:
        raw = json.load(f)

    if raw.get("version") != "1":
        raise ValueError(f"Unknown version {raw['version']}")

    incoming_type = cast(
        Literal["hmac", "token", "none"],
        "none" if "incoming" not in raw else raw["incoming"]["type"],
    )
    incoming_secret = cast(
        Optional[str], raw["incoming"]["secret"] if incoming_type != "none" else None
    )

    outgoing_type = cast(
        Literal["hmac", "token", "none"],
        "none" if "outgoing" not in raw else raw["outgoing"]["type"],
    )
    outgoing_secret = cast(
        Optional[str], raw["outgoing"]["secret"] if outgoing_type != "none" else None
    )

    incoming = cast(Optional[IncomingAuthConfig], None)
    outgoing = cast(Optional[OutgoingAuthConfig], None)

    if incoming_type == "none":
        incoming = IncomingNoneAuth()
    elif incoming_type == "token":
        assert incoming_secret is not None, "impossible"
        incoming = IncomingTokenAuth(incoming_secret)
    elif incoming_type == "hmac":
        assert incoming_secret is not None, "impossible"
        incoming = IncomingHmacAuth(
            incoming_secret, db_config=IncomingHmacAuthSqliteDBConfig(":memory:")
        )

    if outgoing_type == "none":
        outgoing = OutgoingNoneAuth()
    elif outgoing_type == "token":
        assert outgoing_secret is not None, "impossible"
        outgoing = OutgoingTokenAuth(outgoing_secret)
    elif outgoing_type == "hmac":
        assert outgoing_secret is not None, "impossible"
        outgoing = OutgoingHmacAuth(outgoing_secret)

    assert (
        incoming is not None
    ), f"unknown or unsupported incoming auth type {incoming_type}"
    assert (
        outgoing is not None
    ), f"unknown or unsupported outgoing auth type {outgoing_type}"

    return (incoming, outgoing)
