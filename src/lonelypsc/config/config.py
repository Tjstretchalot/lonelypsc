from typing import TypedDict


class PubSubBroadcasterConfig(TypedDict):
    """Indicates how to connect to a broadcaster. We leave this type open for
    expansion in the future (compared to a string) in case we want to add e.g.
    optional priority levels
    """

    host: str
    """The host of the broadcaster, e.g., `http://localhost:3003`. Must include
    the schema and port (if not default) and may include a path (if prefixing the
    standard paths), such that f'{host}/v1/subscribe' is the full path to subscribe
    endpoint.

    For websocket clients, the schema must be a ws or wss. For http clients,
    the schema must be http or https.
    """
