import asyncio
import hashlib
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any, Iterator, List, Literal, Optional, Protocol, Set, Union

import aiohttp
from aiohttp import ClientSession, ClientWebSocketResponse
from lonelypsp.auth.config import AuthResult
from lonelypsp.compat import fast_dataclass
from lonelypsp.stateful.constants import SubscriberToBroadcasterStatefulMessageType
from lonelypsp.stateful.messages.confirm_notify import B2S_ConfirmNotify
from lonelypsp.stateful.messages.confirm_subscribe import (
    B2S_ConfirmSubscribeExact,
    B2S_ConfirmSubscribeGlob,
)
from lonelypsp.stateful.messages.confirm_unsubscribe import (
    B2S_ConfirmUnsubscribeExact,
    B2S_ConfirmUnsubscribeGlob,
)
from lonelypsp.stateful.messages.continue_notify import B2S_ContinueNotify
from lonelypsp.stateful.messages.missed import B2S_Missed
from lonelypsp.stateful.messages.receive_stream import (
    B2S_ReceiveStreamStartCompressed,
    B2S_ReceiveStreamStartUncompressed,
)
from lonelypsp.util.bounded_deque import BoundedDeque
from lonelypsp.util.drainable_asyncio_queue import DrainableAsyncioQueue

from lonelypsc.config.config import BroadcastersShuffler, PubSubBroadcasterConfig
from lonelypsc.config.ws_config import WebsocketPubSubConfig
from lonelypsc.types.sync_io import SyncIOBaseLikeIO, SyncStandardIO
from lonelypsc.types.websocket_message import WSMessage
from lonelypsc.ws.compressor import CompressorStore


class StateType(Enum):
    """Discriminator value for the state the websocket is in"""

    CONNECTING = auto()
    """The necessary websocket/tcp handshakes for communicating with a specific
    broadcaster are in progress. This is the initial state, and the state that
    the websocket will return to if the connection is closed unexpectedly and there
    are still retries available
    """

    CONFIGURING = auto()
    """A websocket connection is open to a specific broadcaster and either the configure
    message is being sent or the subscriber is waiting to receive the configure confirmation
    message from the broadcaster
    """

    OPEN = auto()
    """The standard state where a websocket connection is open with the broadcaster
    and the configuration handshake is already complete.
    """

    WAITING_RETRY = auto()
    """No websocket connection is open or opening but the subscriber plans to try 
    connecting to broadcasters again after a retry period
    """

    CLOSING = auto()
    """Trying to close the websocket normally, then possibly raising an exception"""

    CLOSED = auto()
    """There is no websocket connection open and no plans to retry connecting to
    broadcasters, either because the subscriber chose to disconnect or because
    all broadcasters and retries have been exhausted
    """


@dataclass
class RetryInformation:
    """Information required to progress through other broadcasters and
    retries after a failure to connect to a broadcaster
    """

    shuffler: BroadcastersShuffler
    """The shuffler that is being used to produce iterators of broadcasters"""

    iteration: int
    """Starts at 0 indicating that there are some broadcasters that have
    not been tried at all, then after all broadcasters have been attempted
    increases to 1, then 2, etc.
    """

    iterator: Iterator[PubSubBroadcasterConfig]
    """The iterator for this iteration of the shuffler"""


class OpenRetryInformationType(Enum):
    """discriminator value for OpenRetryInformation.type"""

    STABLE = auto()
    """the connection has been stable long enough that if disconnected the
    subscriber will restart the retry process from the beginning
    """

    TENTATIVE = auto()
    """the connection was recently opened, so if disconnected the subscriber
    will continue the retry process from where it left off
    """


@fast_dataclass
class OpenRetryInformationStable:
    type: Literal[OpenRetryInformationType.STABLE]
    """discriminator value"""


@fast_dataclass
class OpenRetryInformationTentative:
    type: Literal[OpenRetryInformationType.TENTATIVE]
    """discriminator value"""

    stable_at: float
    """if the connection is still live at this time in fractional seconds since
    the unix epoch, then the connection should be considered stable
    """

    continuation: RetryInformation
    """how to continue the retry process if disconnected"""


OpenRetryInformation = Union[OpenRetryInformationStable, OpenRetryInformationTentative]


class ClosingRetryInformationType(Enum):
    """discriminator value for ClosingRetryInformation.type"""

    CANNOT_RETRY = auto()
    """There is an exception that cannot be caught; it must be raised
    after closing the websocket
    """

    WANT_RETRY = auto()
    """the websocket is closing unexpectedly and there will be an attempt to
    re-establish a connection. move to WAITING_RETRY state, move to the
    CONNECTING state, or raise that all retries have been exhausted based on the
    result of next() on the iterator
    """


@fast_dataclass
class ClosingRetryInformationCannotRetry:
    type: Literal[ClosingRetryInformationType.CANNOT_RETRY]
    """discriminator value"""

    tasks: Optional["TasksOnceOpen"]
    """the tasks that need to be cleaned up, if any"""

    exception: BaseException
    """the exception to raise once the websocket is closed"""


@fast_dataclass
class ClosingRetryInformationWantRetry:
    type: Literal[ClosingRetryInformationType.WANT_RETRY]
    """discriminator value"""

    retry: RetryInformation
    """how to continue the retry process"""

    tasks: "TasksOnceOpen"
    """the tasks that need to be performed after re-establishing the
    connection
    """

    exception: BaseException
    """context for the exception that indicated the connection needed to
    be closed
    """


ClosingRetryInformation = Union[
    ClosingRetryInformationCannotRetry, ClosingRetryInformationWantRetry
]


class InternalMessageType(Enum):
    """Discriminator value for the type of message that is being sent internally"""

    SMALL = auto()
    """The message is small enough to hold in memory"""

    LARGE = auto()
    """The message can be read in parts via a stream"""


class InternalMessageStateType(Enum):
    """Describes what state an internal message can be in"""

    UNSENT = auto()
    """
    - the subscriber HAS NOT sent the message
    - the broadcaster HAS NOT processed the message
    - the state WILL change again
    - read() MAY be called on the stream (for large messages)
    """

    SENT = auto()
    """
    - the subscriber MAY HAVE sent the message ANY NUMBER OF TIMES
    - the broadcaster MAY HAVE processed the message ANY NUMBER OF TIMES
    - the state WILL change again
    - read() WILL NOT be called on the stream (for large messages)
    """

    RESENDING = auto()
    """
    - the subscriber MAY HAVE sent the message ANY NUMBER OF TIMES
    - the broadcaster MAY HAVE processed the message ANY NUMBER OF TIMES
    - the state WILL change again
    - read() MAY be called on the stream (for large messages)
    """

    ACKNOWLEDGED = auto()
    """
    - the subscriber HAS sent the message AT LEAST ONCE
    - the broadcaster HAS processed the message AT LEAST ONCE
    - the state WILL NOT change again
    - read() WILL NOT be called on the stream (for large messages)
    """

    DROPPED_UNSENT = auto()
    """
    - the subscriber MAY HAVE sent the message ANY NUMBER OF TIMES
    - the broadcaster HAS NOT processed the message
    - the state WILL NOT change again
    - read() WILL NOT be called on the stream (for large messages)

    note: the subscriber MAY move the message from `SENT` to `DROPPED_UNSENT`
    if the broadcaster explicitly refuses the message, though this is not
    currently implemented. this is because its more important for most callers
    to know if the broadcaster may have processed the message vs the message 
    was sent
    """

    DROPPED_SENT = auto()
    """
    - the subscriber MAY HAVE sent the message ANY NUMBER OF TIMES
    - the broadcaster MAY HAVE processed the message ANY NUMBER OF TIMES
    - the state WILL NOT change again
    - read() WILL NOT be called on the stream (for large messages)
    """


@fast_dataclass
class InternalMessageStateUnsent:
    type: Literal[InternalMessageStateType.UNSENT]
    """discriminator value"""


@fast_dataclass
class InternalMessageStateSent:
    type: Literal[InternalMessageStateType.SENT]
    """discriminator value"""


@fast_dataclass
class InternalMessageStateResending:
    type: Literal[InternalMessageStateType.RESENDING]
    """discriminator value"""


@fast_dataclass
class InternalMessageStateAcknowledged:
    type: Literal[InternalMessageStateType.ACKNOWLEDGED]
    """discriminator value"""

    notified: int
    """a lower bound for the number of unique subscribers that received the
    message
    """


@fast_dataclass
class InternalMessageStateDroppedUnsent:
    type: Literal[InternalMessageStateType.DROPPED_UNSENT]
    """discriminator value"""


@fast_dataclass
class InternalMessageStateDroppedSent:
    type: Literal[InternalMessageStateType.DROPPED_SENT]
    """discriminator value"""


InternalMessageState = Union[
    InternalMessageStateUnsent,
    InternalMessageStateSent,
    InternalMessageStateResending,
    InternalMessageStateAcknowledged,
    InternalMessageStateDroppedUnsent,
    InternalMessageStateDroppedSent,
]


class InternalMessageStateCallback(Protocol):
    """Describes an function that is called when a message changes state
    (e.g., from unsent to sent, from sent to acknowledged, from any to dropped,
    etc). See the documentation for the `InternalMessageStateType` enum for
    details on what each state means

    It is guarranteed that within a single message this callback is not invoked
    concurrently, so e.g. the following implementation will always produce a
    valid ordering of internal message states:

    ```python
    async def my_callback(state: InternalMessageState) -> None:
        await asyncio.sleep(random.random())  # simulates network io
        print(f"state: {state.type}")  # stdout ordered correctly
    ```

    Note that for the most part a valid ordering just means that a final state
    will be printed last, i.e., `UNSENT -> SENT -> ACKNOWLEDGED`, or possibly
    `UNSENT -> ACKNOWLEDGED` (skipping an intermediate state), but definitely
    *not* `UNSENT -> ACKNOWLEDGED -> SENT`
    """

    async def __call__(self, state: InternalMessageState, /) -> None:
        pass


@dataclass
class InternalMessageStateAndCallback:
    """Keeps track of what state a callback was last scheduled for on the
    event loop, the running task for the callback (if any), and the the queued
    state for the callback

    By not weaving multiple calls to the same callback together it is easier
    to reason about recovery when using this library; the alternative is that
    this library only promises the order that the callbacks are scheduled is
    consistent, but if e.g. logging is async this easily leads to subtle bugs
    that are hard to distinguish from errors within the library
    """

    state: InternalMessageState
    """the last state passed to the callback; if task is set, this is the
    state sent to the callback that the task is tracking
    """

    callback: InternalMessageStateCallback
    """the actual callback function"""

    task: Optional[asyncio.Task[None]]
    """the task that is running the callback right now, if any. the state can only
    be changed if this task is None or from within this task
    """

    queued: Optional[
        Union[
            InternalMessageStateUnsent,
            InternalMessageStateSent,
            InternalMessageStateResending,
        ]
    ]
    """the most recent intermediate state for the callback, or None if the
    callback knows about the most recent intermediate state.

    NOTE: the final state is handled differently and, at that point there are
    assumed to be no lingering references to this object
    """


@fast_dataclass
class InternalSmallMessage:
    """
    A message to be sent that is entirely in memory. although this could be
    converted to an InternalLargeMessage via wrapping the data with BytesIO, if
    the whole message is in memory it is often possible to be more efficient
    than generic stream processing, so this distinction is still useful
    """

    type: Literal[InternalMessageType.SMALL]
    """discriminator value"""

    identifier: bytes
    """the arbitrary, unique identifier the subscriber assigned to this message"""

    topic: bytes
    """the topic the message was sent to"""

    data: bytes
    """the uncompressed message data"""

    sha512: bytes
    """the trusted 64-byte hash of the data"""

    callback: InternalMessageStateAndCallback
    """the state of the callback and the callback for this message"""


@fast_dataclass
class InternalLargeMessage:
    """A message that can be read in parts via a stream"""

    type: Literal[InternalMessageType.LARGE]
    """discriminator value"""

    identifier: bytes
    """the arbitrary, unique identifier the subscriber assigned to this message"""

    topic: bytes
    """the topic the message was sent to"""

    stream: SyncStandardIO
    """the readable, seekable, tellable stream that the message data is read from

    guarranteed to have data starting at tell() == 0

    no read() calls that would read past the indicated length from the stream will 
    be made

    this stream is never closed by the state machine, but the caller is alerted
    to if its functions will be called via the callback field (for
    example, if this stream can be reopened the caller could e.g. close in SENT
    then reopen in RESENDING).
    """

    length: int
    """the total length that can be read from the stream; the stream may over-read
    if read(n) is called with an n too large, which would be an error in lonelypsc
    not the caller
    """

    sha512: bytes
    """the trusted 64-byte SHA512 hash of the data"""

    callback: InternalMessageStateAndCallback
    """the state of the callback and the callback for this message"""


InternalMessage = Union[InternalSmallMessage, InternalLargeMessage]


class ManagementTaskType(Enum):
    """discriminator value for `ManagementTask.type`"""

    SUBSCRIBE_EXACT = auto()
    """need to subscribe to an exact topic"""

    SUBSCRIBE_GLOB = auto()
    """need to subscribe to a glob pattern"""

    UNSUBSCRIBE_EXACT = auto()
    """need to unsubscribe from an exact topic"""

    UNSUBSCRIBE_GLOB = auto()
    """need to unsubscribe from a glob pattern"""

    GRACEFUL_DISCONNECT = auto()
    """graceful disconnect requested"""


@fast_dataclass
class ManagementTaskSubscribeExact:
    type: Literal[ManagementTaskType.SUBSCRIBE_EXACT]
    """discriminator value"""

    topic: bytes
    """the topic to subscribe to"""


@fast_dataclass
class ManagementTaskSubscribeGlob:
    type: Literal[ManagementTaskType.SUBSCRIBE_GLOB]
    """discriminator value"""

    glob: str
    """the glob pattern to subscribe to"""


@fast_dataclass
class ManagementTaskUnsubscribeExact:
    type: Literal[ManagementTaskType.UNSUBSCRIBE_EXACT]
    """discriminator value"""

    topic: bytes
    """the topic to unsubscribe from"""


@fast_dataclass
class ManagementTaskUnsubscribeGlob:
    type: Literal[ManagementTaskType.UNSUBSCRIBE_GLOB]
    """discriminator value"""

    glob: str
    """the glob pattern to unsubscribe from"""


ManagementTask = Union[
    ManagementTaskSubscribeExact,
    ManagementTaskSubscribeGlob,
    ManagementTaskUnsubscribeExact,
    ManagementTaskUnsubscribeGlob,
]


@fast_dataclass
class TasksOnceOpen:
    """When not in the OPEN state the client can still receive requests to
    perform operations (e.g., subscribe, notify). This object keeps track
    of those operations that need to be performed until the OPEN state, where
    they are transformed into a different form for actually being sent across
    the websocket
    """

    exact_subscriptions: Set[bytes]
    """The topics which should be subscribed to"""

    glob_subscriptions: Set[str]
    """The glob patterns which should be subscribed to"""

    unsorted: DrainableAsyncioQueue[ManagementTask]
    """management tasks which have been sent from other asyncio coroutines and not applied yet"""

    unsent_notifications: DrainableAsyncioQueue[InternalMessage]
    """The unsent messages that should be sent to the broadcaster via NOTIFY / NOTIFY STREAM."""

    resending_notifications: List[InternalMessage]
    """The resending messages that should be sent to the broadcaster via NOTIFY / NOTIFY STREAM"""


Acknowledgement = Union[
    B2S_ConfirmSubscribeExact,
    B2S_ConfirmSubscribeGlob,
    B2S_ConfirmUnsubscribeExact,
    B2S_ConfirmUnsubscribeGlob,
    B2S_ContinueNotify,
    B2S_ConfirmNotify,
]


class ReceivedMessageType(Enum):
    """Discriminator value for `ReceivedMessage.type`"""

    SMALL = auto()
    """The message is entirely in memory"""

    MISSED = auto()
    """The actual message was never sent"""

    LARGE = auto()
    """The message is in a stream"""


@fast_dataclass
class ReceivedSmallMessage:
    """A received message which is entirely in memory"""

    type: Literal[ReceivedMessageType.SMALL]
    """discriminator value"""

    topic: bytes
    """the topic the message was sent to"""

    data: bytes
    """the uncompressed message data"""

    sha512: bytes
    """the trusted 64-byte hash of the data"""


@fast_dataclass
class ReceivedMissedMessage:
    """indicates that the broadcaster may not have sent a message it should have"""

    type: Literal[ReceivedMessageType.MISSED]
    """discriminator value"""

    topic: bytes
    """the topic the missed message may have been on"""


@fast_dataclass
class ReceivedLargeMessage:
    """A received message which is not entirely in memory; closing the
    stream will delete the data. must close the stream once it is
    consumed
    """

    type: Literal[ReceivedMessageType.LARGE]
    """discriminator value"""

    topic: bytes
    """the topic the message was sent to"""

    stream: SyncIOBaseLikeIO
    """the readable, seekable, tellable, closeable stream that the message data can
    be read from
    """

    sha512: bytes
    """the trusted 64-byte hash of the data"""


ReceivedMessage = Union[
    ReceivedSmallMessage, ReceivedMissedMessage, ReceivedLargeMessage
]


class ReceivingState(Enum):
    """Discriminator value for `StateOpen.receiving`"""

    INCOMPLETE = auto()
    """Waiting for the rest of the message to come in"""

    AUTHORIZING_MISSED = auto()
    """Waiting for the authorization task to complete on a missed message"""

    AUTHORIZING_SIMPLE = auto()
    """Waiting for the authorization task to complete on a message which just
    configures this connection (i.e., isn't important for retries)
    """

    AUTHORIZING = auto()
    """Waiting for the authorization task to complete"""

    WAITING_COMPRESSOR = auto()
    """Waiting for the compressor to be ready"""

    DECOMPRESSING = auto()
    """Decompressing the message"""


@dataclass
class ReceivingIncomplete:
    """
    The subscriber has received part of a message via a RECEIVE_STREAM and is
    waiting for more to come in. The subscriber is hashing the message as it
    comes in, and may have not finished verifying the provided authorization.
    The subscriber hasn't confirmed the hash from the first message yet, since
    that requires the entire message to be received, so the authorization check
    is tentative anyway
    """

    type: Literal[ReceivingState.INCOMPLETE]
    """discriminator value"""

    first: Union[B2S_ReceiveStreamStartUncompressed, B2S_ReceiveStreamStartCompressed]
    """The first stream message with this id, with the payload stripped out"""

    part_id: int
    """The last part id that the subscriber received"""

    body_hasher: "hashlib._Hash"
    """the hash object that is producing the sha512 hash of the body as it comes in"""

    body: SyncIOBaseLikeIO
    """a writable, seekable, tellable, closeable file-like object where the
    subscriber is storing the potentially compressed body of the message as it
    comes in. closing this file will delete the data
    """

    authorization_task: Optional[asyncio.Task[AuthResult]]
    """the task checking if the authorization on the first message is valid or None
    if the task completed, was already checked, and was "ok"
    """


@fast_dataclass
class ReceivingAuthorizingMissed:
    """The subscriber has received a MISSED message and is waiting to verify that
    the authorization is valid before proceeding
    """

    type: Literal[ReceivingState.AUTHORIZING_MISSED]
    """discriminator value"""

    message: B2S_Missed
    """the message that was received"""

    authorization_task: asyncio.Task[AuthResult]
    """the task the subscriber is waiting on to finish checking the messages authorization"""


@fast_dataclass
class ReceivingAuthorizingSimple:
    """The subscriber has received a configuration style message (e.g. DISABLE_ZSTD_PRESET)
    and is waiting to verify that the authorization is valid and/or applying the change
    """

    type: Literal[ReceivingState.AUTHORIZING_SIMPLE]
    """discriminator value"""

    task: asyncio.Task[None]
    """the task that is applying the change"""


@fast_dataclass
class ReceivingAuthorizing:
    """The subscriber has received the entire RECEIVE via RECEIVE_STREAM calls, verified that the
    hash matches what was provided, and are waiting for the authorization task to complete
    before proceeding

    the message has already been ack'd (or at least the ack has been queued) at this point
    """

    type: Literal[ReceivingState.AUTHORIZING]
    """discriminator value"""

    first: Union[B2S_ReceiveStreamStartUncompressed, B2S_ReceiveStreamStartCompressed]
    """The first stream message with this id, with the payload stripped out"""

    body: SyncIOBaseLikeIO
    """a readable, seekable, tellable, closeable file-like object where the
    subscriber stored the potentially compressed body of the message as it came
    in. closing this file will delete the data
    """

    authorization_task: asyncio.Task[AuthResult]
    """the task the subscriber is waiting on to finish checking the messages authorization"""


@fast_dataclass
class ReceivingWaitingCompressor:
    """
    The subscriber has received the entire message via one or more
    RECEIVE_STREAM calls, verified that the hash matches what was provided,
    verified the authorization provided, and is waiting for the compressor to be
    ready
    """

    type: Literal[ReceivingState.WAITING_COMPRESSOR]
    """discriminator value"""

    first: B2S_ReceiveStreamStartCompressed
    """The first stream message with this id, with the payload stripped out"""

    compressed_body: SyncIOBaseLikeIO
    """a readable, seekable, tellable, closeable file-like object where the subscriber
    stored the compressed body of the message as it came in. closing this file will delete
    the data
    """


@fast_dataclass
class ReceivingDecompressing:
    """
    The subscriber has received the entire message via one or more
    RECEIVE_STREAM calls, verified that the hash matches what was provided,
    verified the authorization, and is decompressing the message
    """

    type: Literal[ReceivingState.DECOMPRESSING]
    """discriminator value"""

    task: asyncio.Task[ReceivedMessage]
    """the task that will produce the received message"""


Receiving = Union[
    ReceivingIncomplete,
    ReceivingAuthorizingMissed,
    ReceivingAuthorizingSimple,
    ReceivingAuthorizing,
    ReceivingWaitingCompressor,
    ReceivingDecompressing,
]


class SendingState(Enum):
    """Discriminator value for `StateOpen.sending`"""

    SIMPLE = auto()
    """The subscriber is sending a simple message and no additional cleanup work
    is needed
    """

    MANAGEMENT_TASK = auto()
    """The subscriber is sending a management task; this may take multiple event
    loops to build the authorization part of the message
    """

    INTERNAL_MESSAGE = auto()
    """The subscriber is sending an internal message which MIGHT be in the ack queue,
    but it also might not be. When cleaning up, if the message isn't found in the ack
    queue (by identifier), it should be treated as a SENT notification and have its
    status callback called
    """


@fast_dataclass
class SendingSimple:
    """The subscriber is sending a message that was entirely deduced before being sent
    and did not involve acks
    """

    type: Literal[SendingState.SIMPLE]
    """discriminator value"""
    task: asyncio.Task[None]
    """the task that is sending the message"""


@fast_dataclass
class SendingManagementTask:
    """The subscriber is performing a management task; the management task is
    added to `expected_acks` before the task is started, meaning it can be
    acknowledged normally before this completes, without a need to delay as
    the state is not ever ambiguous
    """

    type: Literal[SendingState.MANAGEMENT_TASK]
    """discriminator value"""
    management_task: ManagementTask
    """the management task that is being sent"""
    task: asyncio.Task[None]
    """the task that is sending the message"""


@fast_dataclass
class SendingInternalMessage:
    """The subscriber is sending an internal message that may or may not be in the
    ack queue
    """

    type: Literal[SendingState.INTERNAL_MESSAGE]
    """discriminator value"""
    task: asyncio.Task[None]
    """the task that is sending the message"""
    internal_message: InternalMessage
    """the message that is being sent

    ## callback

    if the task is not done or failed, the callback could think it is in any of these states:
    - UNSENT
    - RESENDING
    - SENT

    if the task is done and successful, the callback will think it is in SENT. to ensure
    this is the case, the read task will not progress if it receives an acknowledgement
    for the message that is still in sending, as otherwise it would be possible the callback
    thinks the message is in ACKNOWLEDGED, which would be an issue when recovering (ACKNOWLEDGED
    must never change state to RESENDING)

    ## location in state

    if the task is not done or raised an error, the message may be nowhere. otherwise, it
    will be in expected_acks and sent_notifications. this can only be guarranteed because
    the read task will not progress if it receives an acknowledgement for the message that
    is still in sending
    """


Sending = Union[SendingSimple, SendingManagementTask, SendingInternalMessage]


@dataclass
class UnsentAckContinueReceive:
    """Request the broadcaster continue sending RECEIVE_STREAM"""

    type: Literal[SubscriberToBroadcasterStatefulMessageType.CONTINUE_RECEIVE]
    """discriminator value"""

    identifier: bytes
    """the message being received"""

    part_id: int
    """the last part id received"""


@dataclass
class UnsentAckConfirmReceive:
    """Confirms receipt of a message from the broadcaster"""

    type: Literal[SubscriberToBroadcasterStatefulMessageType.CONFIRM_RECEIVE]
    """discriminator value"""

    identifier: bytes
    """the message received"""


UnsentAck = Union[UnsentAckContinueReceive, UnsentAckConfirmReceive]


@dataclass
class StateConnecting:
    """the variables when in the CONNECTING state"""

    type: Literal[StateType.CONNECTING]
    """discriminator value"""

    config: WebsocketPubSubConfig
    """how the subscriber is configured"""

    client_session: aiohttp.ClientSession
    """the client session the websocket is being connected by means of"""

    websocket_task: asyncio.Task[ClientWebSocketResponse]
    """the task that is connecting to the broadcaster"""

    cancel_requested: asyncio.Event
    """if set, the state machine should move to closed as soon as possible"""

    broadcaster: PubSubBroadcasterConfig
    """the broadcaster that is being connected to"""

    retry: RetryInformation
    """information required for proceeding in the retry process"""

    tasks: TasksOnceOpen
    """the tasks that need to be performed after configuring the stream"""

    backgrounded: Set[asyncio.Task[Any]]
    """
    tasks that have been scheduled and if they error it's not recoverable, but
    the result isnt otherwise important. the most prominent example is informing
    callbacks on internal messages

    these shouldnt be canceled unless the state machine is moving to CLOSED; i.e.,
    retries should not cause these tasks to be canceled
    """


@dataclass
class StateConfiguring:
    """the variables when in the CONFIGURING state"""

    type: Literal[StateType.CONFIGURING]
    """discriminator value"""

    client_session: ClientSession
    """the client session the websocket is part of"""

    config: WebsocketPubSubConfig
    """how the subscriber is configured"""

    cancel_requested: asyncio.Event
    """if set, the state machine should move to closed as soon as possible"""

    broadcaster: PubSubBroadcasterConfig
    """the broadcaster that the websocket goes to"""

    websocket: ClientWebSocketResponse
    """the live websocket to the broadcaster"""

    retry: RetryInformation
    """information required for proceeding in the retry process"""

    tasks: TasksOnceOpen
    """the tasks that need to be performed after configuring the stream"""

    subscriber_nonce: bytes
    """the 32 bytes that the subscriber is contributing to the connection nonce"""

    send_task: Optional[asyncio.Task[None]]
    """if still trying to send the configure message, the task for sending it"""

    read_task: asyncio.Task[WSMessage]
    """the task for reading the next message from the websocket"""

    backgrounded: Set[asyncio.Task[Any]]
    """
    tasks that have been scheduled and if they error it's not recoverable, but
    the result isnt otherwise important. the most prominent example is informing
    callbacks on internal messages

    these shouldnt be canceled unless the state machine is moving to CLOSED; i.e.,
    retries should not cause these tasks to be canceled
    """


@dataclass
class StateOpen:
    """the variables when in the OPEN state"""

    type: Literal[StateType.OPEN]
    """discriminator value"""

    client_session: ClientSession
    """the client session the websocket is part of"""

    config: WebsocketPubSubConfig
    """how the subscriber is configured"""

    cancel_requested: asyncio.Event
    """if set, the state machine should move to closed as soon as possible"""

    broadcaster: PubSubBroadcasterConfig
    """the broadcaster that the websocket is connected to"""

    broadcaster_counter: int
    """A counter that is incremented whenever the broadcaster could use a url to
    generate an authorization code
    """

    subscriber_counter: int
    """A counter that is decremented whenever the subscriber could use a url to
    generate an authorization code
    """

    nonce_b64: str
    """the agreed unique identifier for this connection, that combines the
    subscribers contribution and the broadcasters contribution, both of which
    are asked to produce 32 random bytes

    base64 encoded since thats how it is used
    """

    websocket: ClientWebSocketResponse
    """the live websocket to the broadcaster"""

    retry: OpenRetryInformation
    """information required for proceeding in the retry process"""

    compressors: CompressorStore
    """the available compressors for decompressing incoming messages or compressing
    outgoing messages
    """

    unsent_notifications: DrainableAsyncioQueue[InternalMessage]
    """the unsent messages that should be sent to the broadcaster via NOTIFY / NOTIFY STREAM
    in this order. Since these notifications are in UNSENT with nothing queued it's not
    necessary to sweep the internal message state callbacks for these messages
    """

    resending_notifications: List[InternalMessage]
    """the resending messages that should be sent to the broadcaster via NOTIFY / NOTIFY STREAM
    in this order. the internal message state callbacks need to be swept consistently

    NOTE: the subscriber never retries a message within the same websocket connection,
    so it cannot receive acknowledgements for these messages. if the broadcaster does
    not acknowledge a notification in time the connection is closed. thus, the distinction
    between unsent and resending here is only to distinguish which need to be swept and
    which don't

    NOTE: resending notifications are always handled before sent notifications, so this
    empties out at the beginning of the connection and never refills, meaning the linear
    pass through this list is generally a no-op
    """

    sent_notifications: BoundedDeque[InternalMessage]
    """the messages that have been sent to the broadcaster but not acknowledged,
    in the order they are expected to be acknowledged. the last message in this
    list may only have been partially sent. these need to be swept regularly

    NOTE: this list is just a subset of `expected_acks` in the following sense:
        - `len(sent_notifications) <= len(expected_acks)`
        - for every message in `sent_notifications`, there is a corresponding
          acknowledgement in `expected_acks`
        - the order of the messages in `sent_notifications` is the same as the order
          of the corresponding acknowledgements in `expected_acks`

    NOTE: the broadcaster is given until `expected_acks` reaches its max size or
    the websocket ping determines the connection is dead to acknowledge a message;
    there is otherwise no timeout. thus in theory a malicious broadcaster could
    cause a DoS for the client, though this scenario seems unlikely
    """

    exact_subscriptions: Set[bytes]
    """the topics the subscriber is subscribed to BEFORE all management tasks/acks; 
    this is used for restoring the state if the connection is lost
    """

    glob_subscriptions: Set[str]
    """the glob patterns the subscriber is susbcribed to BEFORE all management tasks/acks
    are completed; this is used for restoring the state if the connection is lost
    """

    management_tasks: DrainableAsyncioQueue[ManagementTask]
    """the management tasks that need to be performed in the order they need to be
    performed; these have not had their ack added to expected_acks and have not
    been applied to `exact_subscriptions`/`glob_subscriptions` yet
    """

    expected_acks: BoundedDeque[Acknowledgement]
    """the acknowledgements the subscriber expects to receive in the order they
    are expected to be received; receiving an acknowledgement out of order 
    is an error.
    """

    receiving: Optional[Receiving]
    """if the subscriber has received some part of a notification
    but knows theres more to come or hasn't finished processing it, the state of
    that receiving notification, otherwise None. the broadcaster MUST always
    finish the last notification before sending a new one, and the subscriber
    does not try to read additional messages while processing the previous
    receive stream, so only one notification can be in this state at a time.
    """

    unsent_acks: BoundedDeque[UnsentAck]
    """Outgoing acknowledgements that need to be sent"""

    received: DrainableAsyncioQueue[ReceivedMessage]
    """the messages that have been received from the broadcaster but not yet
    consumed; it's expected that this is consumed from outside the state machine
    """

    sending: Optional[Sending]
    """the task which has exclusive access to `send_bytes`, if any, otherwise
    None
    """

    read_task: asyncio.Task[WSMessage]
    """the task responsible for reading the next message from the websocket"""

    backgrounded: Set[asyncio.Task[Any]]
    """
    tasks that have been scheduled and if they error it's not recoverable, but
    the result isnt otherwise important. the most prominent example is informing
    callbacks on internal messages

    these shouldnt be canceled unless the state machine is moving to CLOSED; i.e.,
    retries should not cause these tasks to be canceled
    """


@dataclass
class StateWaitingRetry:
    """the variables in the WAITING_RETRY state"""

    type: Literal[StateType.WAITING_RETRY]
    """discriminator value"""

    config: WebsocketPubSubConfig
    """how the subscriber is configured"""

    cancel_requested: asyncio.Event
    """if set, the state machine should move to closed as soon as possible"""

    retry: RetryInformation
    """information required for proceeding in the retry process"""

    tasks: TasksOnceOpen
    """the tasks that need to be performed after configuring the stream"""

    retry_at: float
    """the time in fractional seconds from the epoch (as if by `time.time()`)
    before proceeding to the next attempt
    """

    backgrounded: Set[asyncio.Task[Any]]
    """
    tasks that have been scheduled and if they error it's not recoverable, but
    the result isnt otherwise important. the most prominent example is informing
    callbacks on internal messages

    these shouldnt be canceled unless the state machine is moving to CLOSED; i.e.,
    retries should not cause these tasks to be canceled
    """


@dataclass
class StateClosing:
    """the variables in the CLOSING state"""

    type: Literal[StateType.CLOSING]
    """discriminator value"""

    config: WebsocketPubSubConfig
    """how the subscriber is configured"""

    cancel_requested: asyncio.Event
    """if set, the state machine should move to closed as soon as possible"""

    broadcaster: PubSubBroadcasterConfig
    """the broadcaster that the websocket was connected to"""

    client_session: ClientSession
    """the client session the websocket is part of"""

    websocket: ClientWebSocketResponse
    """the potentially live websocket to the broadcaster"""

    retry: ClosingRetryInformation
    """determines if and how the subscriber will retry connecting to
    a broadcaster once the websocket is done closing
    """

    backgrounded: Set[asyncio.Task[Any]]
    """
    tasks that have been scheduled and if they error it's not recoverable, but
    the result isnt otherwise important. the most prominent example is informing
    callbacks on internal messages

    these shouldnt be canceled unless the state machine is moving to CLOSED; i.e.,
    retries should not cause these tasks to be canceled
    """


@fast_dataclass
class StateClosed:
    """the variables in the CLOSED state"""

    type: Literal[StateType.CLOSED]
    """discriminator value"""


State = Union[
    StateConnecting,
    StateConfiguring,
    StateOpen,
    StateWaitingRetry,
    StateClosing,
    StateClosed,
]
