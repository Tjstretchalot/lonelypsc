import asyncio
import base64
from dataclasses import dataclass
from functools import partial
import hashlib
import io
import json
import tempfile
import time
from typing import (
    TYPE_CHECKING,
    Annotated,
    Dict,
    List,
    Literal,
    Optional,
    Protocol,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
)

from fastapi import APIRouter, Header
from fastapi.requests import Request
from fastapi.responses import Response
from httppubsubclient.client import (
    PubSubClient,
    PubSubClientConnector,
    PubSubClientMessage,
    PubSubClientReceiver,
    PubSubDirectOnMessageReceiver,
    PubSubRequestAmbiguousError,
    PubSubRequestRefusedError,
    PubSubRequestRetriesExhaustedError,
    PubSubNotifyResult,
)
from httppubsubclient.config.config import HttpPubSubBroadcasterConfig, HttpPubSubConfig
import aiohttp
from aiohttp.typedefs import LooseHeaders
import random

from httppubsubclient.config.helpers.uvicorn_bind_config import handle_bind_with_uvicorn
from httppubsubclient.types.sync_readable_bytes_io import SyncStandardIO
from httppubsubclient.util.io_helpers import (
    PositionedSyncStandardIO,
    PrefixedSyncStandardIO,
)


# We can return T, or a subset of T
T_co = TypeVar("T_co", covariant=True)

# We will return a T
T = TypeVar("T")


class _BroadcasterCallable(Protocol[T_co]):
    async def __call__(self, /, *, broadcaster: HttpPubSubBroadcasterConfig) -> T_co:
        raise NotImplementedError


@dataclass
class HttpPubSubNotifyResult:
    notified: int


if TYPE_CHECKING:
    _: Type[PubSubNotifyResult] = HttpPubSubNotifyResult


class HttpPubSubClientConnector:
    def __init__(self, config: HttpPubSubConfig) -> None:
        self.config = config
        """The configuration that dictates how we behave"""

        self._session: Optional[aiohttp.ClientSession] = None
        """The client session to use for making requests, if entered, otherwise None"""

    async def setup_connector(self) -> None:
        assert self._session is None, "already set up"
        sess = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(
                total=self.config.outgoing_http_timeout_total,
                connect=self.config.outgoing_http_timeout_connect,
                sock_read=self.config.outgoing_http_timeout_sock_read,
                sock_connect=self.config.outgoing_http_timeout_sock_connect,
            ),
            skip_auto_headers=("User-Agent",),
            auto_decompress=False,
        )
        await sess.__aenter__()
        self._session = sess

    async def teardown_connector(self) -> None:
        assert self._session is not None, "not set up"
        sess = self._session
        self._session = None
        await sess.__aexit__(None, None, None)
        return None

    async def _try_large_post_request(
        self,
        /,
        *,
        broadcaster: HttpPubSubBroadcasterConfig,
        headers: LooseHeaders,
        path: str,
        body: SyncStandardIO,
        body_starts_at: int,
    ) -> Union[
        aiohttp.ClientResponse,
        Literal["ambiguous", "refused", "retry"],
    ]:
        """Not async, thread, or process safe when reusing body.

        - MUST specify content-length
        - MUST specify content-type
        """
        assert self._session is not None, "not set up"

        body.seek(body_starts_at)
        try:
            result = await self._session.post(
                broadcaster["host"] + path,
                data=body,
                headers=headers,
                allow_redirects=False,
                read_until_eof=False,
            )
        except aiohttp.ClientError:
            return "ambiguous"

        await result.__aenter__()
        if result.status in (502, 503, 504):
            await result.__aexit__(None, None, None)
            return "retry"

        if result.status < 200 or result.status >= 300:
            await result.__aexit__(None, None, None)
            return "refused"

        return result

    async def _retry_with_broadcasters(
        self, /, *, broadcaster_callable: _BroadcasterCallable[T]
    ) -> T:
        """Calls the given function with each broadcaster in a random order up to the
        configured number of retries. async safe iff broadcaster_callable is async safe.

        `T` should include `Literal["retry", "refused", "ambiguous"]` to indicate that the
        request was not received by the broadcaster and should be retried, and anything
        else to indicate success.
        """
        assert self._session is not None, "not set up"

        start_idx = random.randint(0, len(self.config.broadcasters) - 1)
        start_broadcaster = self.config.broadcasters[start_idx]

        result = await broadcaster_callable(broadcaster=start_broadcaster)
        if result != "ambiguous" and result != "retry":
            return result
        if result == "ambiguous" and not self.config.outgoing_retry_ambiguous:
            return result

        # this could be a boolean, but doing it this way helps static
        # analysis understand we don't return ambiguous unless T is a superset
        # of Literal["ambiguous"]
        seen_ambiguous: Optional[T] = None
        if result == "ambiguous":
            seen_ambiguous = result

        iteration = 0
        remaining = list(self.config.broadcasters)
        remaining.pop(start_idx)
        random.shuffle(remaining)

        while True:
            if not remaining:
                iteration += 1
                if iteration >= self.config.outgoing_retries_per_broadcaster:
                    if seen_ambiguous is not None:
                        return seen_ambiguous
                    return result
                remaining = list(self.config.broadcasters)
                random.shuffle(remaining)
                await asyncio.sleep(2 ** (iteration - 1) + random.random())

            broadcaster = remaining.pop()
            result = await broadcaster_callable(broadcaster=broadcaster)
            if result != "ambiguous" and result != "retry":
                return result
            if result == "ambiguous" and not self.config.outgoing_retry_ambiguous:
                return result
            if result == "ambiguous":
                seen_ambiguous = result

    async def _make_large_post_request(
        self, /, *, path: str, headers: LooseHeaders, body: SyncStandardIO
    ) -> Union[aiohttp.ClientResponse, Literal["retry", "refused", "ambiguous"]]:
        """Not async, thread, or process safe when reusing body. Assumes the body and/or
        response may be very large; if the request body is large, it should already be
        spooled if necessary and this will rewind when required

        Returned client response is already entered but not released

        - MUST specify content-length
        - MUST specify content-type

        Result is a response which may not indicate success, but should definitely not
        be retried
        """
        assert self._session is not None, "not set up"

        body_starts_at = body.tell()

        async def broadcaster_callable(
            *,
            broadcaster: HttpPubSubBroadcasterConfig,
        ) -> Union[aiohttp.ClientResponse, Literal["retry", "refused", "ambiguous"]]:
            return await self._try_large_post_request(
                broadcaster=broadcaster,
                headers=headers,
                path=path,
                body=body,
                body_starts_at=body_starts_at,
            )

        return await self._retry_with_broadcasters(
            broadcaster_callable=broadcaster_callable
        )

    async def _try_small_request(
        self,
        /,
        *,
        method: Literal["GET", "POST"],
        broadcaster: HttpPubSubBroadcasterConfig,
        headers: LooseHeaders,
        path: str,
        body: Optional[bytes],
        special_ok_codes: Set[int],
    ) -> Union[bytes, Literal["ambiguous", "retry", "refused"]]:
        """Tries the given broadcaster with a post/get request, assuming everything
        can be held in memory. async safe.
        """
        assert self._session is not None, "not set up"

        try:
            async with self._session.request(
                method,
                broadcaster["host"] + path,
                headers=headers,
                data=body,
                allow_redirects=False,
            ) as resp:
                if resp.status not in special_ok_codes:
                    if resp.status in (502, 503, 504):
                        return "retry"
                    if resp.status < 200 or resp.status >= 300:
                        return "refused"
                return await resp.read()
        except aiohttp.ClientError:
            return "ambiguous"

    async def _make_small_request(
        self,
        /,
        *,
        method: Literal["GET", "POST"],
        headers: LooseHeaders,
        path: str,
        body: Optional[bytes],
        special_ok_codes: Set[int],
    ) -> Union[bytes, Literal["ambiguous", "refused", "retry"]]:
        """Makes a small request, trying broadcasters in a random order up to
        the configured number of retries. async safe.
        """
        assert self._session is not None, "not set up"

        async def broadcaster_callable(
            *,
            broadcaster: HttpPubSubBroadcasterConfig,
        ) -> Union[Literal["ambiguous", "retry", "refused"], bytes]:
            return await self._try_small_request(
                method=method,
                broadcaster=broadcaster,
                headers=headers,
                path=path,
                body=body,
                special_ok_codes=special_ok_codes,
            )

        return await self._retry_with_broadcasters(
            broadcaster_callable=broadcaster_callable
        )

    @property
    def _receive_url(self) -> str:
        host_url = self.config.host
        host_fragment_starts_at = host_url.index("#")
        host_fragment = ""
        if host_fragment_starts_at != -1:
            host_fragment = host_url[host_fragment_starts_at:]
            host_url = host_url[:host_fragment_starts_at]

        return host_url + "/v1/receive" + host_fragment

    def _raise_for_error(
        self, /, result: Union[Literal["ambiguous", "retry", "refused"], T]
    ) -> None:
        if result == "ambiguous":
            raise PubSubRequestAmbiguousError()
        if result == "retry":
            raise PubSubRequestRetriesExhaustedError()
        if result == "refused":
            raise PubSubRequestRefusedError()

    async def subscribe_exact(self, /, *, topic: bytes) -> None:
        assert self._session is not None, "not set up"
        receive_url = self._receive_url

        auth_at = time.time()
        authorization = await self.config.setup_subscribe_exact_authorization(
            url=receive_url, exact=topic, now=auth_at
        )
        headers: Dict[str, str] = {
            "Content-Type": "application/octet-stream",
        }
        if authorization is not None:
            headers["Authorization"] = authorization

        encoded_receive_url = receive_url.encode("utf-8")

        body = io.BytesIO()
        body.write(len(encoded_receive_url).to_bytes(2, "big", signed=False))
        body.write(encoded_receive_url)
        body.write(b"\x00")
        body.write(len(topic).to_bytes(2, "big", signed=False))
        body.write(topic)

        result = await self._make_small_request(
            method="POST",
            headers=headers,
            path="/v1/subscribe",
            body=body.getvalue(),
            special_ok_codes={409},
        )
        self._raise_for_error(result)

    async def subscribe_glob(self, /, *, glob: str) -> None:
        assert self._session is not None, "not set up"
        receive_url = self._receive_url

        auth_at = time.time()
        authorization = await self.config.setup_subscribe_glob_authorization(
            url=receive_url, glob=glob, now=auth_at
        )
        headers: Dict[str, str] = {
            "Content-Type": "application/octet-stream",
        }
        if authorization is not None:
            headers["Authorization"] = authorization

        encoded_receive_url = receive_url.encode("utf-8")
        encoded_glob = glob.encode("utf-8")

        body = io.BytesIO()
        body.write(len(encoded_receive_url).to_bytes(2, "big", signed=False))
        body.write(encoded_receive_url)
        body.write(b"\x01")
        body.write(len(encoded_glob).to_bytes(2, "big", signed=False))
        body.write(encoded_glob)

        result = await self._make_small_request(
            method="POST",
            headers=headers,
            path="/v1/subscribe",
            body=body.getvalue(),
            special_ok_codes={409},
        )
        self._raise_for_error(result)

    async def unsubscribe_exact(self, /, *, topic: bytes) -> None:
        assert self._session is not None, "not set up"
        receive_url = self._receive_url

        auth_at = time.time()
        authorization = await self.config.setup_subscribe_exact_authorization(
            url=receive_url, exact=topic, now=auth_at
        )
        headers: Dict[str, str] = {
            "Content-Type": "application/octet-stream",
        }
        if authorization is not None:
            headers["Authorization"] = authorization

        encoded_receive_url = receive_url.encode("utf-8")

        body = io.BytesIO()
        body.write(len(encoded_receive_url).to_bytes(2, "big", signed=False))
        body.write(encoded_receive_url)
        body.write(b"\x00")
        body.write(len(topic).to_bytes(2, "big", signed=False))
        body.write(topic)

        result = await self._make_small_request(
            method="POST",
            headers=headers,
            path="/v1/unsubscribe",
            body=body.getvalue(),
            special_ok_codes={409},
        )
        self._raise_for_error(result)

    async def unsubscribe_glob(self, /, *, glob: str) -> None:
        assert self._session is not None, "not set up"
        receive_url = self._receive_url

        auth_at = time.time()
        authorization = await self.config.setup_subscribe_glob_authorization(
            url=receive_url, glob=glob, now=auth_at
        )
        headers: Dict[str, str] = {
            "Content-Type": "application/octet-stream",
        }
        if authorization is not None:
            headers["Authorization"] = authorization

        encoded_receive_url = receive_url.encode("utf-8")
        encoded_glob = glob.encode("utf-8")

        body = io.BytesIO()
        body.write(len(encoded_receive_url).to_bytes(2, "big", signed=False))
        body.write(encoded_receive_url)
        body.write(b"\x01")
        body.write(len(encoded_glob).to_bytes(2, "big", signed=False))
        body.write(encoded_glob)

        result = await self._make_small_request(
            method="POST",
            headers=headers,
            path="/v1/unsubscribe",
            body=body.getvalue(),
            special_ok_codes={409},
        )
        self._raise_for_error(result)

    async def notify(
        self,
        /,
        *,
        topic: bytes,
        message: SyncStandardIO,
        length: int,
        message_sha512: bytes,
    ) -> HttpPubSubNotifyResult:
        assert self._session is not None, "not set up"

        auth_at = time.time()
        authorization = await self.config.setup_notify_authorization(
            topic=topic, message_sha512=message_sha512, now=auth_at
        )

        initial_message_tell = message.tell()
        normalized_message = PositionedSyncStandardIO(
            message,
            start_idx=initial_message_tell,
            end_idx=initial_message_tell + length,
        )

        message_prefix = io.BytesIO()
        message_prefix.write(len(topic).to_bytes(2, "big", signed=False))
        message_prefix.write(topic)
        message_prefix.write(message_sha512)
        message_prefix.write(length.to_bytes(8, "big", signed=False))

        body = PrefixedSyncStandardIO(
            PositionedSyncStandardIO(message_prefix, 0, message_prefix.tell()),
            normalized_message,
        )
        headers: Dict[str, str] = {
            "Content-Type": "application/octet-stream",
            "Content-Length": str(len(body)),
        }
        if authorization is not None:
            headers["Authorization"] = authorization

        result = await self._make_large_post_request(
            path="/v1/notify",
            headers=headers,
            body=body,
        )
        try:
            self._raise_for_error(result)
            assert not isinstance(result, str), "impossible"
            result_json = await result.json()
        finally:
            if result != "ambiguous" and result != "refused" and result != "retry":
                await result.__aexit__(None, None, None)

        return HttpPubSubNotifyResult(notified=result_json["notified"])


if TYPE_CHECKING:
    __: Type[PubSubClientConnector] = HttpPubSubClientConnector


class HttpPubSubClientReceiver:
    def __init__(self, config: HttpPubSubConfig) -> None:
        self.config = config
        self.handlers: List[Tuple[int, PubSubDirectOnMessageReceiver]] = []
        """The registered on_message receivers"""
        self.bind_task: Optional[asyncio.Task] = None

    async def setup_receiver(self) -> None:
        assert self.bind_task is None, "already setup & not re-entrant"
        bind_config = self.config.bind

        if bind_config["type"] == "uvicorn":
            bind_config = await handle_bind_with_uvicorn(bind_config)

        router = APIRouter()
        router.add_api_route("/v1/receive", self._receive, methods=["POST"])
        self.bind_task = asyncio.create_task(bind_config["callback"](router))

    async def teardown_receiver(self) -> None:
        assert self.bind_task is not None, "not set up"
        bind_task = self.bind_task
        self.bind_task = None
        bind_task.cancel()
        await asyncio.wait([bind_task])

    async def _receive(
        self,
        request: Request,
        authorization: Annotated[Optional[str], Header()] = None,
        repr_digest: Annotated[Optional[str], Header()] = None,
        x_topic: Annotated[Optional[str], Header()] = None,
    ):
        """HttpPubSubClientReceiver primary endpoint

        The authorization header provided shows that the request came from a broadcaster,
        and is validated according to the `auth` mechanism configured.

        The `Repr-Digest` header MUST include the sha-512 digest of the message. The repr
        digest is used to bail out early if the request is not authorized, but is rechecked
        before processing. It MAY include additional digests in any order.

        The `X-Topic` header MUST be set to the topic name, base64 encoded.
        """
        if repr_digest is None:
            return Response(
                status_code=400,
                headers={"Content-Type": "application/json; charset=utf-8"},
                content=b'{"unsubscribe": true, "reason": "missing repr-digest header"}',
            )

        if x_topic is None:
            return Response(
                status_code=400,
                headers={"Content-Type": "application/json; charset=utf-8"},
                content=b'{"unsubscribe": true, "reason": "missing x-topic header"}',
            )

        try:
            topic = base64.b64decode(x_topic)
        except BaseException:
            return Response(
                status_code=400,
                headers={"Content-Type": "application/json; charset=utf-8"},
                content=b'{"unsubscribe": true, "reason": "invalid x-topic header"}',
            )

        expected_digest_b64: Optional[str] = None
        for digest_pair in repr_digest.split(","):
            split_digest_pair = digest_pair.split("=", 1)
            if len(split_digest_pair) != 2:
                continue
            digest_type, digest_value = split_digest_pair
            if digest_type != "sha-512":
                continue

            expected_digest_b64 = digest_value

        if expected_digest_b64 is None:
            return Response(
                status_code=400,
                headers={"Content-Type": "application/json; charset=utf-8"},
                content=b'{"unsubscribe": true, "reason": "missing sha-512 repr-digest"}',
            )

        try:
            expected_digest = base64.b64decode(expected_digest_b64)
        except BaseException:
            return Response(
                status_code=400,
                headers={"Content-Type": "application/json; charset=utf-8"},
                content=b'{"unsubscribe": true, "reason": "unparseable sha-512 repr-digest (not base64)"}',
            )

        auth_result = await self.config.is_receive_allowed(
            url=request.url.path,
            topic=topic,
            message_sha512=expected_digest,
            now=time.time(),
            authorization=authorization,
        )
        if auth_result == "unavailable":
            return Response(status_code=503)
        if auth_result != "ok":
            return Response(
                status_code=403,
                headers={"Content-Type": "application/json; charset=utf-8"},
                content=b'{"unsubscribe": true, "reason": '
                + json.dumps(auth_result).encode("utf-8")
                + b"}",
            )

        with tempfile.SpooledTemporaryFile(
            max_size=self.config.message_body_spool_size, mode="w+b"
        ) as request_body:
            read_length = 0
            hasher = hashlib.sha512()
            stream_iter = request.stream().__aiter__()
            while True:
                try:
                    chunk = await stream_iter.__anext__()
                except StopAsyncIteration:
                    break
                hasher.update(chunk)
                read_length += len(chunk)
                request_body.write(chunk)

            real_digest = hasher.digest()
            if real_digest != expected_digest_b64:
                return Response(
                    status_code=403,
                    headers={"Content-Type": "application/json; charset=utf-8"},
                    content=b'{"unsubscribe": true, "reason": "incorrect sha-512 repr-digest"}',
                )

            message = PubSubClientMessage(
                topic=topic,
                sha512=real_digest,
                data=request_body,
            )

            # not doing a for loop to be clear what we want to happen if
            # handlers is mutated during iteration
            idx = 0
            while idx < len(self.handlers):
                handler = self.handlers[idx][1]
                request_body.seek(0)
                await handler.on_message(message)
                idx += 1

    async def register_on_message(
        self, /, *, receiver: PubSubDirectOnMessageReceiver
    ) -> int:
        new_id = 1 if not self.handlers else self.handlers[-1][0] + 1
        self.handlers.append((new_id, receiver))
        return new_id

    async def unregister_on_message(self, /, *, registration_id: int) -> None:
        # seems more likely a more recent handler is being removed, hence search
        # from tail
        idx = len(self.handlers) - 1
        while idx >= 0:
            if self.handlers[idx][0] == registration_id:
                self.handlers.pop(idx)
                return
            idx -= 1


if TYPE_CHECKING:
    ___: Type[PubSubClientReceiver] = HttpPubSubClientReceiver


def HttpPubSubClient(config: HttpPubSubConfig) -> PubSubClient:
    return PubSubClient(
        HttpPubSubClientConnector(config), HttpPubSubClientReceiver(config)
    )
