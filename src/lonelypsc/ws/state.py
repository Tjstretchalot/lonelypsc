import asyncio
import hashlib
from dataclasses import dataclass
from enum import Enum, auto
from typing import Iterator, List, Literal, Optional, Protocol, Set, Union

from aiohttp import ClientSession, ClientWebSocketResponse
from lonelypsp.compat import fast_dataclass
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
    """

    async def __call__(self, state: InternalMessageState, /) -> None:
        pass


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

    callback: InternalMessageStateCallback
    """the callback when the state changes; the current state of the message is
    assumed to be stored separately (which allows this dataclass to be frozen)
    """


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

    callback: InternalMessageStateCallback
    """the callback when the state changes; the current state of the message is
    assumed to be stored separately (which allows this dataclass to be frozen)
    """


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


@fast_dataclass
class ReceiveStreamState:
    """Keeps track of the combined state of the last related RECEIVE_STREAM messages
    from the broadcaster that have been processed.
    """

    identifier: bytes
    """the message identifier chosen arbitrarily by the broadcaster"""

    first: Union[B2S_ReceiveStreamStartUncompressed, B2S_ReceiveStreamStartCompressed]
    """The first stream message with this id, with the payload stripped out"""

    part_id: int
    """The last part id that the subscriber received"""

    body_hasher: "hashlib._Hash"
    """the hash object that is producing the sha512 hash of the body as it comes in"""

    body: SyncIOBaseLikeIO
    """a writable, seekable, tellable, closeable file-like object where the subscriber
    is storing the body of the message as it comes in. closing this file will delete 
    the data
    """


class ReceivedMessageType(Enum):
    """Discriminator value for `ReceivedMessage.type`"""

    SMALL = auto()
    """The message is entirely in memory"""

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


ReceivedMessage = Union[ReceivedSmallMessage, ReceivedLargeMessage]


@fast_dataclass
class StateConnecting:
    """the variables when in the CONNECTING state"""

    type: Literal[StateType.CONNECTING]
    """discriminator value"""

    config: WebsocketPubSubConfig
    """how the subscriber is configured"""

    cancel_requested: asyncio.Event
    """if set, the state machine should move to closed as soon as possible"""

    broadcaster: PubSubBroadcasterConfig
    """the broadcaster that is being connected to"""

    retry: RetryInformation
    """information required for proceeding in the retry process"""

    tasks: TasksOnceOpen
    """the tasks that need to be performed after configuring the stream"""


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
    in this order
    """

    resending_notifications: List[InternalMessage]
    """the resending messages that should be sent to the broadcaster via NOTIFY / NOTIFY STREAM
    in this order

    NOTE: the subscriber never retries a message within the same websocket connection,
    so it cannot receive acknowledgements for these messages. if the broadcaster does
    not acknowledge a notification in time the connection is closed. thus, the distinction
    between unsent and resending here is only to handle if a message was sent in a previous
    connection, then is not sent in this connection before disconnecting, and then no
    more retries are attempted, in which case the message should be to `DROPPED_SENT` for
    resending_notifications (instead of `DROPPED_UNSENT` for unsent_notifications) so that
    the caller knows the message _may_ have been processed

    NOTE: resending notifications are always handled before sent notifications, so this
    empties out at the beginning of the connection and never refills
    """

    sent_notifications: BoundedDeque[InternalMessage]
    """the messages that have been sent to the broadcaster but not acknowledged,
    in the order they are expected to be acknowledged.

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
    """the topics the subscriber is subscribed to BEFORE all management tasks are
    completed; this is used for restoring the state if the connection is lost
    """

    glob_subscriptions: Set[str]
    """the glob patterns the subscriber is susbcribed to BEFORE all management tasks
    are completed; this is used for restoring the state if the connection is lost
    """

    management_tasks: DrainableAsyncioQueue[ManagementTask]
    """the management tasks that need to be performed in the order they need to be
    performed
    """

    expected_acks: BoundedDeque[Acknowledgement]
    """the acknowledgements the subscriber expects to receive in the order they
    are expected to be received; receiving an acknowledgement out of order 
    is an error.
    """

    received: DrainableAsyncioQueue[ReceivedMessage]
    """the messages that have been received from the broadcaster but not yet
    consumed; it's expected that this is consumed from outside the state machine
    """

    send_task: Optional[asyncio.Task[None]]
    """the task which has exclusive access to `send_bytes`, if any, otherwise
    None
    """

    read_task: asyncio.Task[WSMessage]
    """the task responsible for reading the next message from the websocket"""


@fast_dataclass
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
