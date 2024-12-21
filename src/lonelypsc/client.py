import asyncio
import hashlib
import os
import re
from dataclasses import dataclass
from enum import Enum, auto
from io import BytesIO
from types import TracebackType
from typing import (
    TYPE_CHECKING,
    Awaitable,
    Callable,
    Dict,
    Iterable,
    List,
    Literal,
    Optional,
    Protocol,
    Set,
    Type,
    Union,
    overload,
)

from lonelypsp.util.drainable_asyncio_queue import DrainableAsyncioQueue

from lonelypsc.types.sync_io import (
    SyncReadableBytesIO,
    SyncStandardIO,
)
from lonelypsc.util.io_helpers import PositionedSyncStandardIO

try:
    from glob import translate as _glob_translate  # type: ignore

    def translate(pat: str) -> str:
        return _glob_translate(pat, recursive=True, include_hidden=True)

except ImportError:
    from fnmatch import translate


class _Cleanup(Protocol):
    async def __call__(self) -> None: ...


@dataclass
class PubSubClientMessageWithCleanup:
    """A message received on a topic plus the function to cleanup any associated
    resources
    """

    topic: bytes
    """The topic the message was sent to"""
    sha512: bytes
    """The sha512 hash of the message"""
    data: SyncReadableBytesIO
    """The message data"""
    cleanup: _Cleanup
    """The function to cleanup the message once it's removed from the buffer and done with"""


class PubSubClientMessage(Protocol):
    """A message received on a topic where cleanup is already being handled"""

    @property
    def topic(self) -> bytes:
        """The topic the message was sent to"""

    @property
    def sha512(self) -> bytes:
        """The sha512 hash of the message"""

    @property
    def data(self) -> SyncReadableBytesIO:
        """The message data"""


if TYPE_CHECKING:
    _: Type[PubSubClientMessage] = PubSubClientMessageWithCleanup


class PubSubClientSubscriptionIterator:
    def __init__(
        self,
        state: "_PubSubClientSubscriptionStateEnteredBuffering",
    ) -> None:
        self.state = state

    def __aiter__(self) -> "PubSubClientSubscriptionIterator":
        return self

    async def __anext__(self) -> PubSubClientMessage:
        """Explicitly expects cancellation"""
        while True:
            try:
                item_to_cleanup = self.state.cleanup.get_nowait()
            except asyncio.QueueEmpty:
                break
            await item_to_cleanup.cleanup()

        while True:
            seen_lost = self.state.status == PubSubClientConnectionStatus.LOST
            while True:
                try:
                    new_status = self.state.status_queue.get_nowait()
                except asyncio.QueueEmpty:
                    if self.state.status != PubSubClientConnectionStatus.LOST:
                        break
                    else:
                        new_status = await self.state.status_queue.get()

                seen_lost = seen_lost or new_status == PubSubClientConnectionStatus.LOST
                self.state.status = new_status

            if self.state.status == PubSubClientConnectionStatus.ABANDONED:
                raise PubSubRequestConnectionAbandonedError("connection abandoned")

            assert self.state.status == PubSubClientConnectionStatus.OK, "impossible"
            if seen_lost and self.state.on_receiving is not None:
                await self.state.on_receiving()

            buffer_task = asyncio.create_task(self.state.buffer.get())
            status_task = asyncio.create_task(self.state.status_queue.get())
            try:
                await asyncio.wait(
                    [buffer_task, status_task], return_when=asyncio.FIRST_COMPLETED
                )
            except asyncio.CancelledError:
                buffer_task.cancel()
                status_task.cancel()
                raise

            if not status_task.cancel():
                canceled_buffer_task = buffer_task.cancel()
                self.state.status = status_task.result()
                if canceled_buffer_task:
                    continue

            result = buffer_task.result()
            try:
                self.state.cleanup.put_nowait(result)
            except asyncio.QueueFull:
                await result.cleanup()
                raise
            return result


class PubSubClientSubscriptionWithTimeoutIterator:
    """Wrapper around PubSubClientSubscriptionIterator that yields None if no message
    is received within the timeout (seconds). Note the timeout starts with `__anext__` is
    called, not when the last message was received.
    """

    def __init__(
        self,
        raw_iter: "PubSubClientSubscriptionIterator",
        timeout: float,
    ) -> None:
        self.raw_iter = raw_iter
        self.timeout = timeout

    def __aiter__(self) -> "PubSubClientSubscriptionWithTimeoutIterator":
        return self

    async def __anext__(self) -> Optional[PubSubClientMessage]:
        """Explicitly expects cancellation"""
        timeout_task = asyncio.create_task(asyncio.sleep(self.timeout))
        message_task = asyncio.create_task(self.raw_iter.__anext__())
        try:
            await asyncio.wait(
                (timeout_task, message_task), return_when=asyncio.FIRST_COMPLETED
            )
        except asyncio.CancelledError:
            timeout_task.cancel()
            message_task.cancel()
            raise

        if not message_task.cancel():
            timeout_task.cancel()
            return await message_task

        message_task.cancel()
        return None


_STATE_NOT_ENTERED: Literal[1] = 1
_STATE_ENTERED_NOT_BUFFERING: Literal[2] = 2
_STATE_ENTERED_BUFFERING: Literal[3] = 3
_STATE_DISPOSED: Literal[4] = 4


class OnReceiving(Protocol):
    async def __call__(self) -> None: ...


@dataclass
class _PubSubClientSubscriptionStateNotEntered:
    """State when the subscription has not yet been entered"""

    type: Literal[1]
    """Type discriminator (_STATE_NOT_ENTERED)"""

    client: "PubSubClient"
    """The client we are connected to"""

    exact: Set[bytes]
    """The exact topics that have been queued up to subscribe to when entered"""

    glob: Set[str]
    """The glob topics that have been queued up to subscribe to when entered"""

    on_receiving: Optional[OnReceiving]
    """The function to call at the beginning of what we believe to be a continuous
    stream of messages
    """


@dataclass
class _PubSubClientSubscriptionStateEnteredNotBuffering:
    """State when we have been entered but an iterator hasn't been created yet;
    we have subscribed to the topics but are not yet receiving/buffering messages
    """

    type: Literal[2]
    """Type discriminator (_STATE_ENTERED_NOT_BUFFERING)"""

    client: "PubSubClient"
    """The client we are connected to"""

    exact: Dict[bytes, int]
    """The topic -> subscription id pairs we are subscribed to"""

    glob: Dict[str, int]
    """The glob -> subscription id pairs we are subscribed to"""

    on_receiving: Optional[OnReceiving]
    """The function to call at the beginning of what we believe to be a continuous
    stream of messages
    """


@dataclass
class _PubSubClientSubscriptionStateEnteredBuffering:
    """State when we have been entered and an iterator has been created; we
    are subscribed and buffering messages
    """

    type: Literal[3]
    """Type discriminator (_STATE_ENTERED_BUFFERING)"""

    client: "PubSubClient"
    """The client we are connected to"""

    on_message_subscription_id: int
    """The registration id for the on_message callback"""

    on_status_subscription_id: int
    """The registration id for the connection status listeners"""

    status: "PubSubClientConnectionStatus"
    """The last connection status that we handled"""

    status_queue: DrainableAsyncioQueue["PubSubClientConnectionStatus"]
    """When the status changes we push the new status to this queue as we need
    to process them in order.
    """

    exact: Dict[bytes, int]
    """The topic -> subscription_id mapping for exact subscriptions. We care
    about a message because its an exact match if its a key in this dict.
    """

    glob_regexes: List[re.Pattern]
    """The list of regexes we are subscribed to, in the exact order of `glob_list`. We
    care about a message because its a glob match if any of these regexes match.
    """

    glob_list: List[str]
    """The globs that made the regexes in glob_regexes"""

    glob: Dict[str, int]
    """The glob -> subscription id pairs we are subscribed to"""

    buffer: DrainableAsyncioQueue[PubSubClientMessageWithCleanup]
    """the buffer that we push matching messages to such that they are read by anext"""

    cleanup: DrainableAsyncioQueue[PubSubClientMessageWithCleanup]
    """the messages that haven't been cleaned up yet but need to be; this is normally cleared
    out when calling anext on the iterator, but the last item has to be cleaned out when the
    subscription is exited
    """

    on_receiving: Optional[OnReceiving]
    """The function to call at the beginning of what we believe to be a continuous
    stream of messages
    """


@dataclass
class _PubSubClientSubscriptionStateDisposed:
    type: Literal[4]
    """Type discriminator (_STATE_DISPOSED)"""


_PubSubClientSubscriptionState = Union[
    _PubSubClientSubscriptionStateNotEntered,
    _PubSubClientSubscriptionStateEnteredNotBuffering,
    _PubSubClientSubscriptionStateEnteredBuffering,
    _PubSubClientSubscriptionStateDisposed,
]


class PubSubClientSubscription:
    """Describes a subscription to one or more topic/globs within a single
    client. When the client exits it will exit all subscriptions, but exiting
    a subscription does not exit the client.

    ## Usage

    Using the http client or you don't care about missed notifications:

    ```python
    async with client.subscribe_exact(b"topic") as subscription:
        async for message in await subscription.with_timeout(5):
            if message is None:
                print("still waiting")
            print(f"received message: {message.data.read()}")
    ```

    Using the websocket client and you care about missed notifications:

    ```python
    async def on_receiving():
        # reset your state here via polling. while you're doing this, any
        # messages received will be buffered and then played after.
        # usecases:
        #  - if incoming messages bust the cache, bust the cache here in case you
        #    missed a message to do so
        #  - if you are maintaining state, set the current state here (make sure to
        #    have some kind of counter in the log so you can discard messages already
        #    incorporated later)
        ...

    async with client.subscribe_exact(
        b"topic",
        on_receiving=on_receiving
    ) as subscription:
        async for message in await subscription.with_timeout(5):
            if message is None:
                print("still waiting")
            print(f"received message: {message.data.read()}")
    ```

    ## Details

    You can subscribe before aenter, which will queue up commands to be executed
    while aenter'ing. This facilitates generating methods for subscriptions that
    haven't been aenter'd yet.

    This interface is mostly to automatically pair subscribe/unsubscribes and, when
    creating an iterator, to handle buffering of messages between `anext` calls

    You can use this as an asynchronous iterable,, but you can only make one
    async iterator or you will get an error. This is because, since PEP 533 was
    not accepted, we cannot reliably detect when an iterator has been closed.
    However, there is a period of time while you are processing a message
    between `__anext__` calls where any message received needs to be buffered
    for the next `__anext__` call.

    While this is fine so long as the `__anext__` call is actually coming, if an
    iterator is leaked (i.e., no longer getting __anext__ calls) then the buffer
    will "quietly" grow.

    Thus, a safe way to use this is as follows:

    ```
    async with client.subscribe_exact(b"topic") as subscription: # subscribes
        async for message in await subscription.messages():  # starts buffering
            ...
    # exiting the subscription unsubscribes and stops buffering
    ```

    but this would be extremely error-prone if we allowed it:

    ```
    async with client.subscribe_exact(b"topic") as subscription:  # subscribes
        async for message in await subscription.messages():  # starts buffering
            break

        # still buffering! furthermore, lonelypsc doesn't
        # know if the other iter is still going or not! thus,
        # this is ambiguous!
        async for message in await subscription.messages():  # ERROR
            ...
    # exiting the subscription unsubscribes and stops buffering
    ```

    Note the buffering process does not start until the aiter is created, so if
    you want to only have the subscribe endpoint called once but create multiple
    iterators, and you are ok dropping messages between the iterators, this
    pattern will work:

    ```
    async with client.subscribe_exact(b"topic"):  # subscribes
        async with client.subscribe_exact(b"topic") as subscription: # no-op
            async for message in await subscription.messages(): # starts buffering
                ...
        # exiting the subscription stops buffering

        # any messages created before the next iterator is created are dropped

        async with client.subscribe_exact(b"topic") as subscription:  # no-op
            async for message in await subscription.messages():  # starts buffering
                ...
        # exiting the subscription stops buffering
    # exiting the last subscription to `topic` unsubscribes
    ```

    and if the potential dropped messages needs to be avoided but you cannot use
    a single `async for` loop, then don't use the `async for` syntax at all:
    instead use the `__anext__` method directly:

    ```
    async with client.subscribe_exact(b"topic") as subscription:  # subscribes
        my_iter = await subscription.messages()  # starts buffering

        # whenever you want...
        message = await my_iter.__anext__()  # gets a message, blocking if required
    # exiting the subscription unsubscribes and stops buffering
    ```
    """

    def __init__(
        self,
        client: "PubSubClient",
        /,
        *,
        exact: Set[bytes],
        glob: Set[str],
        on_receiving: Optional[OnReceiving],
    ) -> None:
        self.state: _PubSubClientSubscriptionState = (
            _PubSubClientSubscriptionStateNotEntered(
                _STATE_NOT_ENTERED, client, exact, glob, on_receiving
            )
        )
        self._state_lock: asyncio.Lock = asyncio.Lock()
        """Protects changing self.state"""

    async def __aenter__(self) -> "PubSubClientSubscription":
        async with self._state_lock:
            state = self.state
            assert state.type == _STATE_NOT_ENTERED, "already entered"
            exact: Dict[bytes, int] = dict()
            glob: Dict[str, int] = dict()
            try:
                for topic in state.exact:
                    exact[topic] = await state.client.direct_subscribe_exact(
                        topic=topic
                    )
                for gb in state.glob:
                    glob[gb] = await state.client.direct_subscribe_glob(glob=gb)
            except BaseException:
                for sub_id in exact.values():
                    try:
                        await state.client.direct_unsubscribe_exact(
                            subscription_id=sub_id
                        )
                    except BaseException:
                        ...
                for sub_id in glob.values():
                    try:
                        await state.client.direct_unsubscribe_glob(
                            subscription_id=sub_id
                        )
                    except BaseException:
                        ...
                raise

            self.state = _PubSubClientSubscriptionStateEnteredNotBuffering(
                type=_STATE_ENTERED_NOT_BUFFERING,
                client=state.client,
                exact=exact,
                glob=glob,
                on_receiving=state.on_receiving,
            )

        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        async with self._state_lock:
            state = self.state
            if (
                state.type != _STATE_ENTERED_NOT_BUFFERING
                and state.type != _STATE_ENTERED_BUFFERING
            ):
                return

            exc: Optional[BaseException] = None

            if state.type == _STATE_ENTERED_BUFFERING:
                try:
                    await state.client.direct_unregister_on_message(
                        registration_id=state.on_message_subscription_id
                    )
                except BaseException as e:
                    if exc is None:
                        exc = e

                try:
                    await state.client.direct_unregister_status_handler(
                        registration_id=state.on_status_subscription_id
                    )
                except BaseException as e:
                    if exc is None:
                        exc = e

                for message in state.buffer.drain():
                    try:
                        await message.cleanup()
                    except BaseException as e:
                        if exc is None:
                            exc = e

                for message in state.cleanup.drain():
                    try:
                        await message.cleanup()
                    except BaseException as e:
                        if exc is None:
                            exc = e

            for sub_id in state.exact.values():
                try:
                    await state.client.direct_unsubscribe_exact(subscription_id=sub_id)
                except BaseException as e:
                    if exc is None:
                        exc = e

            for sub_id in state.glob.values():
                try:
                    await state.client.direct_unsubscribe_glob(subscription_id=sub_id)
                except BaseException as e:
                    if exc is None:
                        exc = e

            self.state = _PubSubClientSubscriptionStateDisposed(_STATE_DISPOSED)
            if exc is not None:
                raise exc

    async def on_connection_lost(self) -> None:
        async with self._state_lock:
            if self.state.type == _STATE_ENTERED_BUFFERING:
                self.state.status_queue.put_nowait(PubSubClientConnectionStatus.LOST)

    async def on_connection_established(self) -> None:
        async with self._state_lock:
            if self.state.type == _STATE_ENTERED_BUFFERING:
                self.state.status_queue.put_nowait(PubSubClientConnectionStatus.OK)

    async def on_connection_abandoned(self) -> None:
        async with self._state_lock:
            if self.state.type == _STATE_ENTERED_BUFFERING:
                self.state.status_queue.put_nowait(
                    PubSubClientConnectionStatus.ABANDONED
                )

    async def subscribe_exact(self, topic: bytes) -> None:
        async with self._state_lock:
            state = self.state
            if state.type == _STATE_NOT_ENTERED:
                state.exact.add(topic)
                return

            if state.type == _STATE_DISPOSED:
                raise RuntimeError("subscription has been disposed")

            if topic in state.exact:
                return

            sub_id = await state.client.direct_subscribe_exact(topic=topic)
            state.exact[topic] = sub_id

    async def subscribe_glob(self, glob: str) -> None:
        async with self._state_lock:
            state = self.state
            if state.type == _STATE_NOT_ENTERED:
                state.glob.add(glob)
                return

            if state.type == _STATE_DISPOSED:
                raise RuntimeError("subscription has been disposed")

            if glob in state.glob:
                return

            glob_regex = (
                None
                if state.type != _STATE_ENTERED_BUFFERING
                else re.compile(translate(glob))
            )

            sub_id = await state.client.direct_subscribe_glob(glob=glob)
            state.glob[glob] = sub_id

            if state.type == _STATE_ENTERED_BUFFERING:
                assert glob_regex is not None
                state.glob_list.append(glob)
                state.glob_regexes.append(glob_regex)

    async def unsubscribe_exact(self, topic: bytes) -> None:
        """Unsubscribes from the given exact topic subscription. This is included
        for completeness, but is not necessarily particularly fast.

        Raises ValueError if not subscribed to the given topic, RuntimeError if already
        disposed
        """
        async with self._state_lock:
            state = self.state
            if state.type == _STATE_NOT_ENTERED:
                try:
                    state.exact.remove(topic)
                except KeyError:
                    raise ValueError(f"not subscribed to {topic!r}")
                return

            if state.type == _STATE_DISPOSED:
                raise RuntimeError("subscription has been disposed")

            try:
                sub_id = state.exact.pop(topic)
            except KeyError:
                raise ValueError(f"not subscribed to {topic!r}")

            await state.client.direct_unsubscribe_exact(subscription_id=sub_id)

    async def unsubscribe_glob(self, glob: str) -> None:
        """Unsubscribes from the given glob subscription. This is included
        for completeness, but is not necessarily particularly fast.

        Raises ValueError if not subscribed to the given glob, RuntimeError if already
        disposed
        """
        async with self._state_lock:
            state = self.state
            if state.type == _STATE_NOT_ENTERED:
                try:
                    state.glob.remove(glob)
                except KeyError:
                    raise ValueError(f"not subscribed to {glob!r}")
                return

            if state.type == _STATE_DISPOSED:
                raise RuntimeError("subscription has been disposed")

            try:
                sub_id = state.glob.pop(glob)
            except KeyError:
                raise ValueError(f"not subscribed to {glob!r}")

            exc: Optional[BaseException] = None
            if state.type == _STATE_ENTERED_BUFFERING:
                try:
                    idx = state.glob_list.index(glob)
                    state.glob_list.pop(idx)
                    state.glob_regexes.pop(idx)
                except (IndexError, ValueError) as e:
                    exc = e

            await state.client.direct_unsubscribe_glob(subscription_id=sub_id)

            if exc is not None:
                raise exc

    async def on_message(self, message: PubSubClientMessageWithCleanup) -> None:
        # can avoid a lock by using put_nowait, and raising instead of blocking is
        # preferred in the unlikely event that the queue has a max size AND we reach it
        state = self.state
        if state.type != _STATE_ENTERED_BUFFERING:
            await message.cleanup()
            return

        found = message.topic in state.exact
        if not found:
            try:
                topic_str = message.topic.decode("utf-8")
            except UnicodeDecodeError:
                topic_str = None

            if topic_str is not None:
                for regex in state.glob_regexes:
                    if regex.match(topic_str):
                        found = True
                        break

        if found:
            state.buffer.put_nowait(message)
        else:
            await message.cleanup()

    async def messages(self) -> PubSubClientSubscriptionIterator:
        async with self._state_lock:
            state = self.state
            if state.type == _STATE_NOT_ENTERED:
                raise RuntimeError("not entered")
            if state.type == _STATE_ENTERED_BUFFERING:
                raise RuntimeError("already iterating")
            if state.type == _STATE_DISPOSED:
                raise RuntimeError("subscription has been disposed")
            assert state.type == _STATE_ENTERED_NOT_BUFFERING, "unknown state"

            if (
                state.client.receiver.connection_status
                == PubSubClientConnectionStatus.ABANDONED
            ):
                raise PubSubRequestConnectionAbandonedError("connection abandoned")

            glob_list: List[str] = []
            glob_regexes: List[re.Pattern] = []
            for glob in state.glob.keys():
                glob_list.append(glob)
                glob_regexes.append(re.compile(translate(glob)))

            on_message_subscription_id = await state.client.direct_register_on_message(
                receiver=self
            )
            on_status_subscription_id = (
                await state.client.direct_register_status_handler(receiver=self)
            )

            status = PubSubClientConnectionStatus.LOST
            status_queue: DrainableAsyncioQueue[PubSubClientConnectionStatus] = (
                DrainableAsyncioQueue()
            )
            if state.client.receiver.connection_status != status:
                status_queue.put_nowait(state.client.receiver.connection_status)

            buffer: DrainableAsyncioQueue[PubSubClientMessageWithCleanup] = (
                DrainableAsyncioQueue()
            )
            cleanup: DrainableAsyncioQueue[PubSubClientMessageWithCleanup] = (
                DrainableAsyncioQueue()
            )
            self.state = _PubSubClientSubscriptionStateEnteredBuffering(
                type=_STATE_ENTERED_BUFFERING,
                client=state.client,
                on_message_subscription_id=on_message_subscription_id,
                on_status_subscription_id=on_status_subscription_id,
                status=status,
                status_queue=status_queue,
                exact=state.exact,
                glob_regexes=glob_regexes,
                glob_list=glob_list,
                glob=state.glob,
                buffer=buffer,
                cleanup=cleanup,
                on_receiving=state.on_receiving,
            )
            return PubSubClientSubscriptionIterator(self.state)

    async def with_timeout(
        self, seconds: float
    ) -> PubSubClientSubscriptionWithTimeoutIterator:
        return PubSubClientSubscriptionWithTimeoutIterator(
            await self.messages(), seconds
        )


class PubSubDirectOnMessageWithCleanupReceiver(Protocol):
    async def on_message(self, message: PubSubClientMessageWithCleanup) -> None: ...


class PubSubDirectConnectionStatusReceiver(Protocol):
    """Describes an object that wants to receive feedback about the state of the
    connection, if any information is known. This is most directly interpretable
    with active connections (e.g., websockets)
    """

    async def on_connection_lost(self) -> None:
        """
        Called to indicate that the subscriber knows it's possible that we are
        missing some notifications right now and moving forward. Generally, the
        only thing an implementation can do with this is switch to polling, or in
        practice, set a timeout for an alert if the connection is not established
        again in time.

        NOTE: this will be called under normal circumstances if registering
        status receivers prior to setting up the receiver
        """

    async def on_connection_established(self) -> None:
        """Indicates we (re-)established a connection and now expect that we are
        receiving messages without interruption. This is the most meaningful event
        that implementations can use as it will definitely be called in normal operations
        (when the connection is first established, which, assuming the service regularly
        restarts for e.g. updates, will happen regularly), and by implementating will
        naturally take care of small interruptions in the connection

        There are a few general operations that implementations would perform
        - if using these notifications to fill a local cache, which on misses checks
          the source of truth, just clear the local cache
        - if there is an external log of messages, check for and replay messages
          that weren't processed (taking care to ensure this completes, e.g., by
          marking the current position before starting)
        """

    async def on_connection_abandoned(self) -> None:
        """Indicates the subscriber has given up trying to re-establish the connection and
        will raise errors when trying to receive messages.

        This will happen in normal operation when exiting the context manager for
        the websocket client (i.e., closing the websocket by subscriber request),
        and normally doesn't have any useful recovery. If this was not expected
        and error is going to be raised in the notify/receive methods which can
        be handled with better context
        """


class PubSubError(Exception):
    """Base class for pub/sub exceptions"""


class PubSubIrrecoverableError(BaseException):
    """Base class for pub/sub exceptions that should prevent retries but aren't one
    of the standard base exceptions
    """


class PubSubRequestError(PubSubError):
    """An error occurred while making a request to the pub/sub server"""


class PubSubRequestAmbiguousError(PubSubRequestError):
    """We failed to confirm the server received the request, and we also
    failed to confirm they did not
    """


class PubSubRequestRetriesExhaustedError(PubSubRequestError):
    """Every attempt we made was met with the server explicitly indicating
    we should retry (502, 503, or 504 status code), but we have reached the
    maximum number of retries
    """


class PubSubRequestRefusedError(PubSubRequestError):
    """The server refused the request and indicated we should not retry"""


class PubSubRequestConnectionAbandonedError(PubSubRequestError):
    """We do not have a connection to the broadcaster and we have given up
    trying to establish one
    """


class PubSubNotifyResult(Protocol):
    @property
    def notified(self) -> int:
        """The number of subscribers that were successfully notified. Success means
        either an HTTP 200 response, or, for websocket subscribers, an acknowledgement.
        Ambiguous attempts, such as a connection close after posting the data,
        are never included in this value even though the subscriber may have
        received them
        """


class PubSubClientConnector(Protocol):
    """Something capable of making subscribe/unsubscribe requests"""

    async def setup_connector(self) -> None:
        """Performs any necessary setup of the connector methods. Must raise an
        error if re-entry is not supported but is attempted
        """

    async def teardown_connector(self) -> None:
        """If called after a setup, must tear down resources created in that setup.
        Otherwise, SHOULD ensure all resources are teared down and MAY raise an
        error.

        If this is implemented such that all resources are as torn down as possible
        after the first call, MAY simply error on subsequent calls
        """

    async def subscribe_exact(self, /, *, topic: bytes) -> None:
        """
        Subscribe to the given topic, such that the corresponding receiver will
        receive one additional notification when a message is posted on that
        topic. MUST raise an error unless this confirmed that during the
        execution of this function the broadcaster had our client subscribed to
        the given topic

        Specifically, this means repeating calls to this function with the same
        topic should typically result in no errors and the same effect as a
        single call.
        """

    async def subscribe_glob(self, /, *, glob: str) -> None:
        """
        Subscribe to the given topic, such that the corresponding receiver will
        receive one additional notification when a message is posted to a matching
        toic. MUST raise an error unless this confirmed that during the
        execution of this function the broadcaster had our client subscribed to
        the given glob

        Specifically, this means repeating calls to this function with the same
        glob should typically result in no errors and the same effect as a
        single call.
        """

    async def unsubscribe_exact(self, /, *, topic: bytes) -> None:
        """
        Unsubscribe from the given topic, such that the corresponding receiver will
        receive one fewer notification when a message is posted on that topic. MUST
        raise an error unless this confirmed that during the execution of this
        function the broadcaster had our client unsubscribed from the given topic

        Specifically, this means repeating calls to this function with the same
        topic should typically result in no errors and the same effect as a
        single call.
        """

    async def unsubscribe_glob(self, /, *, glob: str) -> None:
        """
        Unsubscribe from the given topic, such that the corresponding receiver will
        receive one fewer notification when a message is posted to a matching topic.
        MUST raise an error unless this confirmed that during the execution of this
        function the broadcaster had our client unsubscribed from the given glob

        Specifically, this means repeating calls to this function with the same
        glob should typically result in no errors and the same effect as a
        single call.
        """

    async def notify(
        self,
        /,
        *,
        topic: bytes,
        message: SyncStandardIO,
        length: int,
        message_sha512: bytes,
    ) -> PubSubNotifyResult:
        """Sends a message, which is composed of the next length bytes on the given
        seekable synchronous io object, to all subscribers of the given topic.

        MUST raise an error unless this confirmed that during the execution of this
        function the broadcaster received, accepted, and processed the message

        Args:
            topic (bytes): the topic to post the message to
            message (SyncStandardIO): the message to post; the current position is as indicated
                via tell(), and only the next length bytes are part of the message
            length (int): the number of bytes in the message
            message_sha512 (bytes): the sha512 hash of the message (64 bytes)
        """


class PubSubClientConnectionStatus(Enum):
    OK = auto()
    """Indicates that we believe we are receiving messages"""

    LOST = auto()
    """Indicates that we believe we may not be receiving messages and are
    trying to re-establish a stable connection
    """

    ABANDONED = auto()
    """Indicates that we believe we may not be receiving messages and are
    not making attempts to rectify the situation
    """


class PubSubClientReceiver(Protocol):
    """Something capable of registering additional callbacks when messages are received"""

    @property
    def connection_status(self) -> PubSubClientConnectionStatus:
        """The subscribers best belief on the current state of the connection. This
        is generally for debugging. See `register_status_handler` for the more useful
        interface programmatically
        """

    async def setup_receiver(self) -> None:
        """Performs any necessary work to prepare to receive messages from the
        broadcaster
        """

    async def teardown_receiver(self) -> None:
        """Called to notify that this object is no longer expected to call status
        handlers or message handlers. Generally, tears down any work done in
        setup_receiver. Implementations MAY assume that after teardown the object
        will not be used again, though in that case they SHOULD raise an error on
        unsupported setup calls.
        """

    async def register_on_message(
        self, /, *, receiver: PubSubDirectOnMessageWithCleanupReceiver
    ) -> int:
        """Registers the given receiver to be called when a message on one of the
        subscribed topics (either via an exact match or a glob match) is received,
        and returns an id that can be used to unregister the receiver.
        """

    async def unregister_on_message(self, /, *, registration_id: int) -> None:
        """Unregisters the receiver with the given id. The implementation MAY assume
        that the registration id is valid (returned from register_on_message from
        this object and not invalidated), and if it is not, may arbitrarily do any
        of the following:
        - corrupt its state
        - unregister an unrelated receiver
        - raise an error
        - do nothing

        Regardless of if this raises an error, afterwards the registration id MUST
        be considered invalidated by the caller
        """

    async def register_status_handler(
        self, /, *, receiver: PubSubDirectConnectionStatusReceiver
    ) -> int:
        """Registers the given receiver to be called when the connection status changes,
        and returns an id that can be used to unregister the receiver.

        The core purpose of the receiver is so that the caller can perform some
        operation when it may have missed messages but is now receiving messages.
        The most general thing it could do is look at a log of the messages sent
        over the topic stored elsewhere and replay any it hasn't seen, but most of
        the time there is a simpler alternative.

        For example, if the caller does the same idempotent operation regardless
        of the contents/topic of the message, then they can simply do that operation
        on `on_connec
        """

    async def unregister_status_handler(self, /, *, registration_id: int) -> None:
        """Unregisters the receiver with the given id. The implementation MAY assume
        that the registration id is valid (returned from register_status_handler from
        this object and not invalidated), and if it is not, may arbitrarily do any
        of the following:
        - corrupt its state
        - unregister an unrelated receiver
        - raise an error
        - do nothing

        Regardless of if this raises an error, afterwards the registration id MUST
        be considered invalidated by the caller
        """


class PubSubClient:
    def __init__(
        self,
        connector: PubSubClientConnector,
        receiver: PubSubClientReceiver,
        *,
        setup: Callable[[], Awaitable[None]],
        teardown: Callable[[], Awaitable[None]],
    ) -> None:
        self.connector: PubSubClientConnector = connector
        """The connector that can make subscribe/unsubscribe requests. We assume that we
        need to setup this when we are entered and teardown when we are exited.
        """

        self.receiver: PubSubClientReceiver = receiver
        """The receiver that can register additional callbacks when messages are received.
        We assume that we need to setup this when we are entered and teardown when we are exited.
        """

        self._setup = setup
        """A function to call when we are entered"""

        self._teardown = teardown
        """A function to call when we are exited"""

        self.exact_subscriptions: Dict[bytes, int] = {}
        """Maps from topic we've subscribed to to the number of requests to subscribe to it"""

        self.active_exact_subscriptions: Dict[int, bytes] = {}
        """Maps from subscription_id to the corresponding exact topic"""

        self.glob_subscriptions: Dict[str, int] = {}
        """Maps from glob we've subscribed to to the number of requests to subscribe to it"""

        self.active_glob_subscriptions: Dict[int, str] = {}
        """Maps from subscription_id to the corresponding glob"""

        self._entered: bool = False
        """True if we are active (aenter without aexit), False otherwise"""

        self._counter: int = 0
        """The counter for generating unique subscription ids"""

        self._subscribing_lock: asyncio.Lock = asyncio.Lock()
        """A lock while actively subscribing/unsubscribing; managed by the direct_*
        methods
        """

    async def __aenter__(self) -> "PubSubClient":
        assert not self._entered, "already entered (not re-entrant)"
        await self._setup()
        self._entered = True
        try:
            await self.connector.setup_connector()
            try:
                await self.receiver.setup_receiver()
            except BaseException:
                await self.connector.teardown_connector()
                raise
        except BaseException:
            try:
                await self._teardown()
            finally:
                self._entered = False

            raise

        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        assert self._entered, "not entered"

        self._entered = False
        excs: List[BaseException] = []

        try:
            await self.receiver.teardown_receiver()
        except BaseException as e:
            excs.append(e)

        async with self._subscribing_lock:
            for topic in self.exact_subscriptions.keys():
                try:
                    await self.connector.unsubscribe_exact(topic=topic)
                except BaseException as e:
                    excs.append(e)
            for glob in self.glob_subscriptions.keys():
                try:
                    await self.connector.unsubscribe_glob(glob=glob)
                except BaseException as e:
                    excs.append(e)

            self.exact_subscriptions = {}
            self.active_exact_subscriptions = {}
            self.glob_subscriptions = {}
            self.active_glob_subscriptions = {}

        try:
            await self.connector.teardown_connector()
        except BaseException as e:
            excs.append(e)

        try:
            await self._teardown()
        except BaseException as e:
            excs.append(e)

        if excs:
            raise excs[0]

    def _reserve_subscription_id(self) -> int:
        """An asyncio-safe (since it doesn't yield) way to reserve a subscription
        id. Not thread-safe
        """
        result = self._counter
        self._counter += 1
        return result

    async def _try_direct_subscribe_exact(
        self, /, *, topic: bytes, my_id: int, have_lock: bool
    ) -> Literal["ok", "need_lock"]:
        requests_so_far = self.exact_subscriptions.get(topic, 0)
        if requests_so_far <= 0:
            if not have_lock:
                return "need_lock"

            await self.connector.subscribe_exact(topic=topic)

        self.exact_subscriptions[topic] = max(1, requests_so_far + 1)
        self.active_exact_subscriptions[my_id] = topic
        return "ok"

    async def _try_direct_subscribe_glob(
        self, /, *, glob: str, my_id: int, have_lock: bool
    ) -> Literal["ok", "need_lock"]:
        requests_so_far = self.glob_subscriptions.get(glob, 0)
        if requests_so_far == 0:
            if not have_lock:
                return "need_lock"

            await self.connector.subscribe_glob(glob=glob)

        self.glob_subscriptions[glob] = max(1, requests_so_far + 1)
        self.active_glob_subscriptions[my_id] = glob
        return "ok"

    async def _try_direct_unsubscribe_exact(
        self, /, *, subscription_id: int, have_lock: bool
    ) -> Literal["ok", "need_lock"]:
        topic = self.active_exact_subscriptions.get(subscription_id)
        if topic is None:
            return "ok"

        requests_so_far = self.exact_subscriptions[topic]
        need_unsubscribe = requests_so_far <= 1
        if need_unsubscribe and not have_lock:
            return "need_lock"

        # If we're unsubscribing we'll set the value in exact_subscriptions to 0
        # temporarily, then remove it if we're successful. The only effect of this
        # is that we might want to use this information when we aexit
        del self.active_exact_subscriptions[subscription_id]
        self.exact_subscriptions[topic] = requests_so_far - 1

        if need_unsubscribe:
            await self.connector.unsubscribe_exact(topic=topic)
            # we hold the lock so it shouldn't have changed, but we can recheck anyway
            requests_so_far = self.exact_subscriptions[topic]
            if requests_so_far <= 0:
                del self.exact_subscriptions[topic]

        return "ok"

    async def _try_direct_unsubscribe_glob(
        self, /, *, subscription_id: int, have_lock: bool
    ) -> Literal["ok", "need_lock"]:
        glob = self.active_glob_subscriptions.get(subscription_id)
        if glob is None:
            return "ok"

        requests_so_far = self.glob_subscriptions[glob]
        need_unsubscribe = requests_so_far <= 1
        if need_unsubscribe and not have_lock:
            return "need_lock"

        del self.active_glob_subscriptions[subscription_id]
        self.glob_subscriptions[glob] = requests_so_far - 1

        if need_unsubscribe:
            await self.connector.unsubscribe_glob(glob=glob)
            requests_so_far = self.glob_subscriptions[glob]
            if requests_so_far <= 0:
                del self.glob_subscriptions[glob]

        return "ok"

    async def direct_subscribe_exact(self, /, *, topic: bytes) -> int:
        """If we are not already subscribed to the given topic, subscribe to it.
        Returns an id that must be provided to `direct_unsubscribe_exact` when
        the caller is no longer interested in the topic. The caller should register
        with `direct_register_on_message` to receive messages, filtering to
        those it cares about.

        WARN:
            prefer using the `subscribe*` methods instead, which will handle
            unsubscribing via an async context manager. otherwise, cleanup is
            both tedious and error-prone

        WARN:
            the returned id is only guarranteed to be unique to other _active_
            subscriptions. we may reuse values once they are no longer active
            (via `direct_unsubscribe_exact`)

        WARN:
            on error it may be ambiguous if we are subscribed or not
        """
        assert self._entered, "not entered"
        my_id = self._reserve_subscription_id()
        result = await self._try_direct_subscribe_exact(
            topic=topic, my_id=my_id, have_lock=False
        )
        if result == "need_lock":
            async with self._subscribing_lock:
                result = await self._try_direct_subscribe_exact(
                    topic=topic, my_id=my_id, have_lock=True
                )
        assert result == "ok"
        return my_id

    async def direct_subscribe_glob(self, /, *, glob: str) -> int:
        """If we are not already subscribed to the given glob, subscribe to it.
        Returns an id that must be provided to `direct_unsubscribe_glob` when
        the caller is no longer interested in the topic. The caller should register
        with `direct_register_on_message` to receive messages, filtering to
        those it cares about.

        WARN:
            prefer using the `subscribe*` methods instead, which will handle
            unsubscribing via an async context manager. otherwise, cleanup is
            both tedious and error-prone

        WARN:
            the returned id is only guarranteed to be unique to other _active_
            subscriptions. we may reuse values once they are no longer active
            (via `direct_unsubscribe_glob`)

        WARN:
            if the glob overlaps with another glob or exact subscription, we
            will receive multiple messages for the same topic with no way to
            deduplicate them (you can put a message uid in the body to detect
            them). this is an intentional limitation as deduplicating can be
            very expensive, this type of duplication can usually be designed
            around, and duplication from network errors needs to be handled
            anyway so should not cause logic errors
        """
        assert self._entered, "not entered"
        my_id = self._reserve_subscription_id()
        result = await self._try_direct_subscribe_glob(
            glob=glob, my_id=my_id, have_lock=False
        )
        if result == "need_lock":
            async with self._subscribing_lock:
                result = await self._try_direct_subscribe_glob(
                    glob=glob, my_id=my_id, have_lock=True
                )
        assert result == "ok"
        return my_id

    async def direct_unsubscribe_exact(self, /, *, subscription_id: int) -> None:
        """If the subscription id was returned from `direct_subscribe_exact`, and
        it has not already been unsubscribed via this method, then unsubscribe
        from the topic. If the subscription id is not as indicated, the behavior
        is undefined:
        - it may do nothing
        - it may raise an error
        - it may unsubscribe an unrelated subscription
        - it may corrupt the state of the client

        WARN:
            prefer using the `subscribe*` methods instead, which will handle
            unsubscribing via an async context manager. otherwise, cleanup is
            both tedious and error-prone
        """
        assert self._entered, "not entered"
        result = await self._try_direct_unsubscribe_exact(
            subscription_id=subscription_id, have_lock=False
        )
        if result == "need_lock":
            async with self._subscribing_lock:
                result = await self._try_direct_unsubscribe_exact(
                    subscription_id=subscription_id, have_lock=True
                )
        assert result == "ok"

    async def direct_unsubscribe_glob(self, /, *, subscription_id: int) -> None:
        """If the subscription id was returned from `direct_subscribe_glob`, and
        it has not already been unsubscribed via this method, then unsubscribe
        from the topic. If the subscription id is not as indicated, the behavior
        is undefined:
        - it may do nothing
        - it may raise an error
        - it may unsubscribe an unrelated subscription
        - it may corrupt the state of the client

        WARN:
            prefer using the `subscribe*` methods instead, which will handle
            unsubscribing via an async context manager. otherwise, cleanup is
            both tedious and error-prone
        """
        assert self._entered, "not entered"
        result = await self._try_direct_unsubscribe_glob(
            subscription_id=subscription_id, have_lock=False
        )
        if result == "need_lock":
            async with self._subscribing_lock:
                result = await self._try_direct_unsubscribe_glob(
                    subscription_id=subscription_id, have_lock=True
                )
        assert result == "ok"

    async def direct_register_on_message(
        self, /, *, receiver: PubSubDirectOnMessageWithCleanupReceiver
    ) -> int:
        """Registers the given callback to be invoked whenever we receive a message
        for any topic. Returns a registration id that must be provided to
        `direct_unregister_on_message` when the caller is no longer interested in
        the messages.

        WARN:
            prefer using the `subscribe*` methods instead, which will handle
            unsubscribing via an async context manager. otherwise, cleanup is
            both tedious and error-prone
        """
        assert self._entered, "not entered"
        return await self.receiver.register_on_message(receiver=receiver)

    async def direct_unregister_on_message(self, /, *, registration_id: int) -> None:
        """If the registration id was returned from `direct_register_on_message`, and
        it has not already been unregistered via this method, then unregister the
        callback. If the registration id is not as indicated, the behavior is undefined:
        - it may do nothing
        - it may raise an error
        - it may unregister an unrelated callback
        - it may corrupt the state of the client

        WARN:
            prefer using the `subscribe*` methods instead, which will handle
            unsubscribing via an async context manager. otherwise, cleanup is
            both tedious and error-prone
        """
        assert self._entered, "not entered"
        return await self.receiver.unregister_on_message(
            registration_id=registration_id
        )

    async def direct_register_status_handler(
        self, /, *, receiver: PubSubDirectConnectionStatusReceiver
    ) -> int:
        """Registers the given callback to be invoked whenever we receive a connection
        status update. Returns a registration id that must be provided to
        `direct_unregister_status_handler` when the caller is no longer interested in
        the messages.

        WARN:
            the subscribe* methods will handle registering a status handler for you
            while also checking the state of the connection, and thus should be preferred
        """
        assert self._entered, "not entered"
        return await self.receiver.register_status_handler(receiver=receiver)

    async def direct_unregister_status_handler(
        self, /, *, registration_id: int
    ) -> None:
        """If the registration id was returned from `direct_register_status_handler`, and
        it has not already been unregistered via this method, then unregister the
        callback. If the registration id is not as indicated, the behavior is undefined:
        - it may do nothing
        - it may raise an error
        - it may unregister an unrelated callback
        - it may corrupt the state of the client

        WARN:
            prefer using the `subscribe*` methods instead, which will handle
            unsubscribing via an async context manager. otherwise, cleanup is
            both tedious and error-prone
        """
        assert self._entered, "not entered"
        return await self.receiver.unregister_status_handler(
            registration_id=registration_id
        )

    def subscribe_multi(
        self, *, on_receiving: Optional[OnReceiving] = None
    ) -> PubSubClientSubscription:
        """Returns a new async context manager within which you can
        register multiple subscriptions (exact or glob). When exiting,
        the subscriptions will be removed.

        If `on_receiving` is provided, it will be called after any period where
        we may not have been receiving messages and we are likely now receiving
        messages, after buffering begins. This is always called at least once when
        making the iterator (assuming no errors).

        Re-entrant subscriptions are supported and avoid duplicate subscribe/
        unsubscribe requests to the broadcaster

        All the other `subscribe_*` methods behave similarly to this but with
        a bit of setup in advance.
        """
        assert self._entered, "not entered"
        return PubSubClientSubscription(
            self, exact=set(), glob=set(), on_receiving=on_receiving
        )

    def subscribe_exact(
        self, topic: bytes, *rest: bytes, on_receiving: Optional[OnReceiving] = None
    ) -> PubSubClientSubscription:
        """Subscribe to one or more topics by exact match. The result is an
        async context manager which, when exited, will unsubscribe from the
        topic(s)

        If `on_receiving` is provided, it will be called after any period where
        we may not have been receiving messages and we are likely now receiving
        messages, after buffering begins. This is always called at least once when
        making the iterator (assuming no errors).

        Re-entrant subscriptions are supported and avoid duplicate subscribe/
        unsubscribe requests to the broadcaster
        """
        assert self._entered, "not entered"
        return PubSubClientSubscription(
            self, exact={topic, *rest}, glob=set(), on_receiving=on_receiving
        )

    def subscribe_glob(
        self, glob: str, *rest: str, on_receiving: Optional[OnReceiving] = None
    ) -> PubSubClientSubscription:
        """Subscribe to one or more topics by glob match. The result is an
        async context manager which, when exited, will unsubscribe from the
        topic(s)

        If `on_receiving` is provided, it will be called after any period where
        we may not have been receiving messages and we are likely now receiving
        messages, after buffering begins. This is always called at least once when
        making the iterator (assuming no errors).

        Re-entrant subscriptions are supported and avoid duplicate subscribe/
        unsubscribe requests to the broadcaster
        """
        assert self._entered, "not entered"
        return PubSubClientSubscription(
            self, exact=set(), glob={glob, *rest}, on_receiving=on_receiving
        )

    def subscribe(
        self,
        /,
        *,
        glob: Optional[Iterable[str]] = None,
        exact: Optional[Iterable[bytes]] = None,
        on_receiving: Optional[OnReceiving] = None,
    ) -> PubSubClientSubscription:
        """Subscribe to a combination of glob and/or exact topics. The result is
        an async context manager which, when exited, will unsubscribe from the
        topic(s)

        If `on_receiving` is provided, it will be called after any period where
        we may not have been receiving messages and we are likely now receiving
        messages, after buffering begins. This is always called at least once when
        making the iterator (assuming no errors).

        Re-entrant subscriptions are supported and avoid duplicate subscribe/
        unsubscribe requests to the broadcaster
        """
        assert self._entered, "not entered"
        return PubSubClientSubscription(
            self,
            exact=set(exact or ()),
            glob=set(glob or ()),
            on_receiving=on_receiving,
        )

    @overload
    async def notify(
        self, /, *, topic: bytes, data: bytes, sha512: Optional[bytes] = None
    ) -> PubSubNotifyResult: ...

    @overload
    async def notify(
        self,
        /,
        *,
        topic: bytes,
        sync_file: SyncStandardIO,
        length: Optional[int] = None,
        sha512: Optional[bytes] = None,
    ) -> PubSubNotifyResult: ...

    async def notify(
        self,
        /,
        *,
        topic: bytes,
        data: Optional[bytes] = None,
        sync_file: Optional[SyncStandardIO] = None,
        length: Optional[int] = None,
        sha512: Optional[bytes] = None,
    ) -> PubSubNotifyResult:
        """Notifies all subscribers of the given topic of the message. The message
        may be provided as bytes or a readable synchronous file-like object.

        If the sha512 is not provided it will be calculated from the message, which
        will incidentally discover the length of the message (via seeking to EOF)
        if not provided. If the length and sha512 are provided then the file
        will only be read once.

        If the message is provided as bytes and length is set, then the length must
        be equal to len(message).
        """
        assert (
            data is None or sync_file is None
        ), "only one of data or sync_file may be provided"
        assert (
            data is not None or sync_file is not None
        ), "either data or sync_file must be provided"
        assert (
            data is None or length is None or len(data) == length
        ), "if data is provided, length must be None or len(data)"
        assert len(topic) <= 65535, "topic too long"
        assert self._entered, "not entered"

        if sync_file is not None:
            file_starts_at = sync_file.tell()
            if length is None:
                length = sync_file.seek(0, os.SEEK_END) - file_starts_at
            sync_file = PositionedSyncStandardIO(sync_file, file_starts_at, length)
            del file_starts_at
            sync_file.seek(0, os.SEEK_SET)
        else:
            assert data is not None, "impossible"
            length = len(data)
            sync_file = BytesIO(data)

        # message is used to make it clear to the type checker that either
        # data or sync_file is set

        if sha512 is None:
            if data is not None:
                sha512 = hashlib.sha512(data).digest()
            else:
                hasher = hashlib.sha512()
                while True:
                    chunk = sync_file.read(8192)
                    if not chunk:
                        break
                    hasher.update(chunk)
                    await asyncio.sleep(0)
                sha512 = hasher.digest()
                sync_file.seek(0, os.SEEK_SET)

        return await self.connector.notify(
            topic=topic, message=sync_file, length=length, message_sha512=sha512
        )
