import json
from typing import Literal, Optional, Tuple, cast

from httppubsubclient.config.auth_config import IncomingAuthConfig, OutgoingAuthConfig
from httppubsubclient.config.helpers.none_auth_config import (
    IncomingNoneAuth,
    OutgoingNoneAuth,
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

    if outgoing_type == "none":
        outgoing = OutgoingNoneAuth()

    assert (
        incoming is not None
    ), f"unknown or unsupported incoming auth type {incoming_type}"
    assert (
        outgoing is not None
    ), f"unknown or unsupported outgoing auth type {outgoing_type}"

    return (incoming, outgoing)
