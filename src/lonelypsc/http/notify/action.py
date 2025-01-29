import io
import secrets
import time
from typing import TYPE_CHECKING, Dict, Generic, Literal, Type, TypeVar, Union

import aiohttp
from lonelypsp.auth.config import AuthResult
from lonelypsp.compat import fast_dataclass
from lonelypsp.stateless.constants import BroadcasterToSubscriberStatelessMessageType
from lonelypsp.tracing.stateless.notify import (
    StatelessTracingNotifyOnResponseReceived,
    StatelessTracingNotifyOnRetryDetermined,
    StatelessTracingNotifyOnSending,
)

from lonelypsc.config.config import PubSubBroadcasterConfig
from lonelypsc.config.http_config import HttpPubSubConfig
from lonelypsc.http.notify.result import HttpPubSubNotifyResult
from lonelypsc.http.retrier import (
    BroadcasterRetryableAction,
    BroadcasterRetryableActionResult,
    BroadcasterRetryableActionResultFailure,
    BroadcasterRetryableActionResultRetry,
    BroadcasterRetryableActionResultSuccess,
    BroadcasterRetryableActionRetryFollowup,
)
from lonelypsc.util.async_io import async_read_exact
from lonelypsc.util.io_helpers import (
    PositionedSyncStandardIO,
    PrefixedSyncStandardIO,
    SyncStandardWithLengthIO,
)

InitializerT = TypeVar("InitializerT")


@fast_dataclass
class HttpNotifyInfo:
    """The information about the underlying notification"""

    topic: bytes
    """the topic the message is being posted to"""
    normalized_message: SyncStandardWithLengthIO
    """the message to send where offset 0 is the start and length is the end"""
    sha512: bytes
    """the sha512 of the message"""


class HttpNotifyAction(Generic[InitializerT]):
    """The action taken to post a new message to a topic"""

    def __init__(
        self,
        /,
        *,
        config: HttpPubSubConfig[InitializerT],
        info: HttpNotifyInfo,
        session: aiohttp.ClientSession,
        tracer: StatelessTracingNotifyOnSending,
        seen_ambiguous: bool,
    ):
        self.config = config
        self.info = info
        self.session = session
        self.tracer = tracer
        self.seen_ambiguous = seen_ambiguous

    async def attempt(self, /, *, broadcaster: PubSubBroadcasterConfig) -> Union[
        BroadcasterRetryableActionResultSuccess[HttpPubSubNotifyResult],
        BroadcasterRetryableActionResultFailure[Literal["ambiguous", "refused"]],
        BroadcasterRetryableActionResultRetry[
            HttpPubSubNotifyResult, Literal["ambiguous", "refused"]
        ],
    ]:
        identifier = secrets.token_bytes(4)
        tracing_and_followup = self.tracer.on_sending_request(
            broadcaster=broadcaster["host"], identifier=identifier
        )
        tracing = tracing_and_followup.tracing
        on_response_received_tracer = tracing_and_followup.followup

        auth_at = time.time()
        authorization = await self.config.authorize_notify(
            tracing=tracing,
            topic=self.info.topic,
            identifier=identifier,
            message_sha512=self.info.sha512,
            now=auth_at,
        )

        message_prefix = io.BytesIO()
        message_prefix.write(len(self.info.topic).to_bytes(2, "big", signed=False))
        message_prefix.write(self.info.topic)
        message_prefix.write(self.info.sha512)
        message_prefix.write(len(tracing).to_bytes(2, "big", signed=False))
        message_prefix.write(tracing)
        message_prefix.write(len(identifier).to_bytes(1, "big", signed=False))
        message_prefix.write(identifier)
        message_prefix.write(
            len(self.info.normalized_message).to_bytes(8, "big", signed=False)
        )

        body = PrefixedSyncStandardIO(
            PositionedSyncStandardIO(message_prefix, 0, message_prefix.tell()),
            self.info.normalized_message,
        )
        headers: Dict[str, str] = {
            "Content-Type": "application/octet-stream",
            "Content-Length": str(len(body)),
        }
        if authorization is not None:
            headers["Authorization"] = authorization

        try:
            result = await self.session.post(
                broadcaster["host"] + "/v1/notify",
                data=body,
                headers=headers,
                allow_redirects=False,
                read_until_eof=False,
            )
        except aiohttp.ClientError:
            return await self._on_network_error(
                on_response_received_tracer=on_response_received_tracer
            )

        try:
            await result.__aenter__()
        except BaseException as e:
            await result.__aexit__(type(e), e, None)
            await self._on_network_error(
                on_response_received_tracer=on_response_received_tracer
            )
            raise

        try:
            on_response_auth_result = on_response_received_tracer.on_response_received(
                status_code=result.status
            )
            del on_response_received_tracer
        except BaseException as e:
            await result.__aexit__(type(e), e, None)
            raise

        try:
            type_bytes = await async_read_exact(result.content, 2)
            type_int = int.from_bytes(type_bytes, "big", signed=False)
            if type_int != int(
                BroadcasterToSubscriberStatelessMessageType.RESPONSE_NOTIFY
            ):
                raise ValueError

            resp_authorization_length_bytes = await async_read_exact(result.content, 2)
            resp_authorization_length = int.from_bytes(
                resp_authorization_length_bytes, "big", signed=False
            )
            resp_authorization_bytes = await async_read_exact(
                result.content, resp_authorization_length
            )
            resp_authorization = (
                None
                if resp_authorization_bytes == b""
                else resp_authorization_bytes.decode("utf-8")
            )

            resp_tracing_length_bytes = await async_read_exact(result.content, 2)
            resp_tracing_length = int.from_bytes(
                resp_tracing_length_bytes, "big", signed=False
            )
            resp_tracing = await async_read_exact(result.content, resp_tracing_length)

            num_subscribers_bytes = await async_read_exact(result.content, 4)
            num_subscribers = int.from_bytes(num_subscribers_bytes, "big", signed=False)

            resp_identifier_length_bytes = await async_read_exact(result.content, 1)
            resp_identifier_length = int.from_bytes(
                resp_identifier_length_bytes, "big", signed=False
            )
            if resp_identifier_length != len(identifier):
                raise ValueError

            resp_identifier = await async_read_exact(
                result.content, resp_identifier_length
            )
            if resp_identifier != identifier:
                raise ValueError
        except ValueError:
            # also raised by async_read_exact if not enough bytes are read;
            # i don't think this will happen if the connection is interrupted
            # (it should result in a aiohttp.ClientError)
            return BroadcasterRetryableActionResultRetry(
                type=BroadcasterRetryableActionResult.RETRY,
                followup=HttpNotifyActionRetryFollowup(
                    config=self.config,
                    info=self.info,
                    session=self.session,
                    tracer=on_response_auth_result.on_bad_response(),
                    seen_ambiguous=self.seen_ambiguous,
                ),
            )
        except aiohttp.ClientError:
            # connection was interrupted; might have contained a valid
            # response
            return await self._on_ambiguous_retryable(
                retry_tracer=on_response_auth_result.on_bad_response()
            )
        finally:
            await result.__aexit__(None, None, None)

        auth_result = await self.config.is_confirm_notify_allowed(
            tracing=resp_tracing,
            identifier=resp_identifier,
            subscribers=num_subscribers,
            topic=self.info.topic,
            message_sha512=self.info.sha512,
            authorization=resp_authorization,
            now=time.time(),
        )
        if auth_result != AuthResult.OK:
            return BroadcasterRetryableActionResultRetry(
                type=BroadcasterRetryableActionResult.RETRY,
                followup=HttpNotifyActionRetryFollowup(
                    config=self.config,
                    info=self.info,
                    session=self.session,
                    tracer=on_response_auth_result.on_bad_auth_result(
                        result=auth_result
                    ),
                    seen_ambiguous=self.seen_ambiguous,
                ),
            )

        on_response_auth_result.on_response_notify_accepted(
            tracing=resp_tracing, num_subscribers=num_subscribers
        )
        return BroadcasterRetryableActionResultSuccess(
            type=BroadcasterRetryableActionResult.SUCCESS,
            result=HttpPubSubNotifyResult(notified=num_subscribers),
        )

    async def _on_network_error(
        self,
        /,
        *,
        on_response_received_tracer: StatelessTracingNotifyOnResponseReceived,
    ) -> Union[
        BroadcasterRetryableActionResultFailure[Literal["ambiguous", "refused"]],
        BroadcasterRetryableActionResultRetry[
            HttpPubSubNotifyResult, Literal["ambiguous", "refused"]
        ],
    ]:
        return await self._on_ambiguous_retryable(
            retry_tracer=on_response_received_tracer.on_network_error()
        )

    async def _on_ambiguous_retryable(
        self, /, *, retry_tracer: StatelessTracingNotifyOnRetryDetermined
    ) -> Union[
        BroadcasterRetryableActionResultFailure[Literal["ambiguous", "refused"]],
        BroadcasterRetryableActionResultRetry[
            HttpPubSubNotifyResult, Literal["ambiguous", "refused"]
        ],
    ]:
        if self.config.outgoing_retry_ambiguous:
            return BroadcasterRetryableActionResultRetry(
                type=BroadcasterRetryableActionResult.RETRY,
                followup=HttpNotifyActionRetryFollowup(
                    config=self.config,
                    info=self.info,
                    session=self.session,
                    tracer=retry_tracer,
                    seen_ambiguous=True,
                ),
            )

        retry_tracer.on_retry_prevented()
        return BroadcasterRetryableActionResultFailure(
            type=BroadcasterRetryableActionResult.FAILURE,
            result="ambiguous",
        )


class HttpNotifyActionRetryFollowup(Generic[InitializerT]):
    """Retry followup after a request fails in a retryable manner for notify"""

    def __init__(
        self,
        config: HttpPubSubConfig[InitializerT],
        info: HttpNotifyInfo,
        session: aiohttp.ClientSession,
        tracer: StatelessTracingNotifyOnRetryDetermined,
        seen_ambiguous: bool,
    ):
        self.config = config
        self.info = info
        self.session = session
        self.tracer = tracer
        self.seen_ambiguous = seen_ambiguous

    async def on_retries_exhausted(
        self, /
    ) -> BroadcasterRetryableActionResultFailure[Literal["ambiguous", "refused"]]:
        self.tracer.on_retries_exhausted()
        return BroadcasterRetryableActionResultFailure(
            type=BroadcasterRetryableActionResult.FAILURE,
            result="ambiguous" if self.seen_ambiguous else "refused",
        )

    async def on_waiting_to_retry(
        self, /
    ) -> BroadcasterRetryableActionRetryFollowup[
        HttpPubSubNotifyResult, Literal["ambiguous", "refused"]
    ]:
        self.tracer = self.tracer.on_waiting_to_retry()
        return self

    async def on_retrying(
        self, /
    ) -> BroadcasterRetryableAction[
        HttpPubSubNotifyResult, Literal["ambiguous", "refused"]
    ]:
        tracer = self.tracer.on_retrying()
        return HttpNotifyAction(
            config=self.config,
            info=self.info,
            session=self.session,
            tracer=tracer,
            seen_ambiguous=self.seen_ambiguous,
        )


if TYPE_CHECKING:
    _a: Type[
        BroadcasterRetryableAction[
            HttpPubSubNotifyResult, Literal["ambiguous", "refused"]
        ]
    ] = HttpNotifyAction
    _b: Type[
        BroadcasterRetryableActionRetryFollowup[
            HttpPubSubNotifyResult, Literal["ambiguous", "refused"]
        ]
    ] = HttpNotifyActionRetryFollowup
