from fastapi import APIRouter
from typing import List, Literal, Optional, TypedDict, Protocol, Union
from dataclasses import dataclass


class HttpPubSubBindUvicornConfig(TypedDict):
    """When used for the `bind` parameter on an `HttpPubSubConfig` object,
    indicates you want to use a relatively standard http server with uvicorn.
    This is provided as the most common option, but if you need to configure
    anything more specific (e.g., middleware for logging, etc), you should
    use `HttpPubSubBindManualConfig` instead.
    """

    type: Literal["uvicorn"]
    """discriminates the type"""

    address: str
    """What address to bind to, e.g., 0.0.0.0 for all interfaces"""

    port: int
    """What port to bind to. As a very loose guidance, use 3002 for subscribers 
    and 3003 for broadcasters
    """


class HttpPubSubBindManualCallback(Protocol):
    """Describes the callback that binds the http server in manual mode"""

    async def __call__(self, router: APIRouter) -> None: ...


class HttpPubSubBindManualConfig(TypedDict):
    """When used for the `bind` parameter on an `HttpPubSubConfig` object,
    indicates you can convert the APIRouter into an HTTP server yourself.
    Often its convenient to use this just because you want to provide more
    parameters to uvicorn (e.g., TLS), or to add middleware, etc.
    """

    type: Literal["manual"]
    """discriminates the type"""

    callback: HttpPubSubBindManualCallback
    """The callback that binds the http server"""


class HttpPubSubBroadcasterConfig(TypedDict):
    """Indicates how to connect to a broadcaster. We leave this type open for
    expansion in the future (compared to a string) in case we want to add e.g.
    optional priority levels
    """

    host: str
    """The host of the broadcaster, e.g., `http://localhost:3003`. Must include
    the schema and port (if not default) and may include a path (if prefixing the
    standard paths), such that f'{host}/v1/subscribe' is the full path to subscribe
    endpoint.
    """


# TODO -> auth, docs
@dataclass
class HttpPubSubConfig:
    bind: Union[HttpPubSubBindUvicornConfig, HttpPubSubBindManualConfig]
    host: str
    broadcasters: List[HttpPubSubBroadcasterConfig]
    send_auth: Literal["TODO"]
    receive_auth: Literal["TODO"]
    message_body_spool_size: int
    outgoing_http_timeout_total: Optional[float]
    outgoing_http_timeout_connect: Optional[float]
    outgoing_http_timeout_sock_read: Optional[float]
    outgoing_http_timeout_sock_connect: Optional[float]
    outgoing_retry_network_errors: bool
    outgoing_retries_per_broadcaster: int
