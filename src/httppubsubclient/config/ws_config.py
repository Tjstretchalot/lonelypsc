from typing import TYPE_CHECKING, Protocol, Type


class WebsocketPubSubConfig(Protocol): ...


class WebsocketPubSubConfigFromParts: ...


if TYPE_CHECKING:
    _: Type[WebsocketPubSubConfig] = WebsocketPubSubConfigFromParts


def make_websocket_pub_sub_config() -> WebsocketPubSubConfig:
    """Convenience function to make a WebsocketPubSubConfig object without excessive nesting
    if you are specifying everything that doesn't need to be synced with the broadcaster
    within code.
    """
    return WebsocketPubSubConfigFromParts()
