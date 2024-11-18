from typing import Dict, Iterable, List, Literal, Optional, Protocol, Tuple, Union
from httppubsubclient.config.config import HttpPubSubConfig
from httppubsubclient.types.sync_readable_bytes_io import SyncReadableBytesIO
from dataclasses import dataclass
import asyncio
import re


@dataclass
class HttpPubSubClientMessage:
    """A message received on a topic"""

    topic: bytes
    """The topic the message was sent to"""
    sha512: str
    """The sha512 hash of the message"""
    data: SyncReadableBytesIO
    """The message data"""


class HttpPubSubClientSubscriptionIterator:
    def __init__(self, parent: "HttpPubSubClientSubscription") -> None:
        self.parent = parent

    async def __aiter__(self) -> "HttpPubSubClientSubscriptionIterator":
        return await self.parent.__aiter__()

    async def __anext__(self) -> HttpPubSubClientMessage:
        """Explicitly expects cancellation"""
        raise NotImplementedError


class HttpPubSubClientSubscriptionWithTimeoutIterator:
    """Wrapper around HttpPubSubClientSubscriptionIterator that yields None if no message
    is received within the timeout (seconds). Note the timeout starts with `__anext__` is
    called, not when the last message was received.
    """

    def __init__(
        self,
        parent: "HttpPubSubClientSubscription",
        raw_iter: "HttpPubSubClientSubscriptionIterator",
        timeout: float,
    ) -> None:
        self.parent = parent
        self.raw_iter = raw_iter
        self.timeout = timeout

    async def __aiter__(self) -> "HttpPubSubClientSubscriptionWithTimeoutIterator":
        return await self.parent.with_timeout(self.timeout)

    async def __anext__(self) -> Optional[HttpPubSubClientMessage]:
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


_STATE_NOT_ENTERED = 1
_STATE_ENTERED_NOT_BUFFERING = 2
_STATE_ENTERED_BUFFERING = 3
_STATE_DISPOSED = 4


@dataclass
class _HttpPubSubClientSubscriptionStateNotEntered:
    """State when the subscription has not yet been entered"""

    type: Literal[1]
    """Type discriminator (_STATE_NOT_ENTERED)"""

    client: "HttpPubSubClient"
    """The client we are connected to"""

    exact: List[bytes]
    """The exact topics that have been queued up to subscribe to when entered"""

    glob: List[str]
    """The glob topics that have been queued up to subscribe to when entered"""


@dataclass
class _HttpPubSubClientSubscriptionStateEnteredNotBuffering:
    """State when we have been entered but an iterator hasn't been created yet;
    we have subscribed to the topics but are not yet receiving/buffering messages
    """

    type: Literal[2]
    """Type discriminator (_STATE_ENTERED_NOT_BUFFERING)"""

    client: "HttpPubSubClient"
    """The client we are connected to"""

    exact: List[Tuple[int, bytes]]
    """The (subscription_id, topic) pairs we are subscribed to"""

    glob: List[Tuple[int, str]]
    """The (subscription_id, glob) pairs we are subscribed to"""


@dataclass
class _HttpPubSubClientSubscriptionStateEnteredBuffering:
    """State when we have been entered and an iterator has been created; we
    are subscribed and buffering messages
    """

    type: Literal[3]
    """Type discriminator (_STATE_ENTERED_BUFFERING)"""

    client: "HttpPubSubClient"
    """The client we are connected to"""

    exact: Dict[bytes, int]
    """The topic -> subscription_id mapping for exact subscriptions. We care
    about a message because its an exact match if its a key in this dict.
    """

    glob_regexes: List[re.Pattern]
    """The list of regexes we are subscribed to, in the exact order of glob_list. We
    care about a message because its a glob match if any of these regexes match.
    """

    glob: List[Tuple[int, str]]
    """The (subscription_id, glob) pairs we are subscribed to, with index correspondance
    with glob_regexes.
    """

    buffer: asyncio.Queue[HttpPubSubClientMessage]
    """the buffer that we push matching messages to such that they are read by anext"""


@dataclass
class _HttpPubSubClientSubscriptionStateDisposed:
    type: Literal[4]
    """Type discriminator (_STATE_DISPOSED)"""


_HttpPubSubClientSubscriptionState = Union[
    _HttpPubSubClientSubscriptionStateNotEntered,
    _HttpPubSubClientSubscriptionStateEnteredNotBuffering,
    _HttpPubSubClientSubscriptionStateEnteredBuffering,
    _HttpPubSubClientSubscriptionStateDisposed,
]


class HttpPubSubClientSubscription:
    """Describes a subscription to one or more topic/globs within a single
    client. When the client exits it will exit all subscriptions, but exiting
    a subscription does not exit the client.

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
        async for message in subscription:  # starts buffering
            ...
    # exiting the subscription unsubscribes and stops buffering
    ```

    but this would be extremely error-prone if we allowed it:

    ```
    async with client.subscribe_exact(b"topic") as subscription:  # subscribes
        async for message in subscription:  # starts buffering
            break

        # still buffering! furthermore, httppubsubclient doesn't
        # know if the other iter is still going or not! thus,
        # this is ambiguous!
        async for message in subscription:  # ERROR
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
            async for message in subscription: # starts buffering
                ...
        # exiting the subscription stops buffering

        # any messages created before the next iterator is created are dropped

        async with client.subscribe_exact(b"topic") as subscription:  # no-op
            async for message in subscription:  # starts buffering
                ...
        # exiting the subscription stops buffering
    # exiting the last subscription to `topic` unsubscribes
    ```

    and if the potential dropped messages needs to be avoided but you cannot use
    a single `async for` loop, then don't use the `async for` syntax at all:
    instead use the `__anext__` method directly:

    ```
    async with client.subscribe_exact(b"topic") as subscription:  # subscribes
        my_iter = await subscription.__aiter__()  # starts buffering

        # whenever you want...
        message = await my_iter.__anext__()  # gets a message, blocking if required
    # exiting the subscription unsubscribes and stops buffering
    ```
    """

    def __init__(self, client: "HttpPubSubClient") -> None:
        self.state: _HttpPubSubClientSubscriptionState = (
            _HttpPubSubClientSubscriptionStateNotEntered(
                _STATE_NOT_ENTERED, client, [], []
            )
        )

    async def subscribe_exact(self, topic: bytes) -> None:
        if self.state.type == _STATE_NOT_ENTERED:
            ...

    async def subscribe_glob(self, glob: str) -> None: ...
    async def unsubscribe_exact(self, topic: bytes) -> None: ...
    async def unsubscribe_glob(self, glob: str) -> None: ...
    async def on_message(self, message: HttpPubSubClientMessage) -> None: ...

    async def __aenter__(self) -> "HttpPubSubClientSubscription":
        return self

    async def __aexit__(self, exc_type, exc_value, traceback) -> None: ...

    async def __aiter__(self) -> HttpPubSubClientSubscriptionIterator:
        return HttpPubSubClientSubscriptionIterator(self)

    async def with_timeout(
        self, seconds: float
    ) -> HttpPubSubClientSubscriptionWithTimeoutIterator:
        return HttpPubSubClientSubscriptionWithTimeoutIterator(self, seconds)


class HttpPubSubDirectOnMessageReceiver(Protocol):
    async def on_message(self, message: HttpPubSubClientMessage) -> None: ...


class HttpPubSubClient:
    def __init__(self, config: HttpPubSubConfig):
        self.exact_subscriptions: Dict[bytes, int] = {}
        """Maps from topic we've subscribed to to the number of requests to subscribe to it"""

        self.active_exact_subscriptions: Dict[int, bytes] = {}
        """Maps from subscription_id to the corresponding exact topic"""

        self.glob_subscriptions: Dict[str, int] = {}
        """Maps from glob we've subscribed to to the number of requests to subscribe to it"""

        self.active_glob_subscriptions: Dict[int, str] = {}
        """Maps from subscription_id to the corresponding glob"""

        self._counter: int = 0
        """The counter for generating unique subscription ids"""

    async def __aenter__(self) -> "HttpPubSubClient": ...
    async def __aexit__(self, exc_type, exc_value, traceback) -> None: ...

    async def _do_subscribe_exact(self, /, *, topic: bytes) -> None: ...

    async def _do_subscribe_glob(self, /, *, glob: str) -> None: ...

    async def _do_unsubscribe_exact(self, /, *, subscription_id: int) -> None: ...

    async def _do_unsubscribe_glob(self, /, *, subscription_id: int) -> None: ...

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
        # TODO: concurrency
        my_id = self._counter
        self._counter += 1
        requests_so_far = self.exact_subscriptions.get(topic, 0)
        self.exact_subscriptions[topic] = requests_so_far + 1
        self.active_exact_subscriptions[my_id] = topic
        if requests_so_far == 0:
            # TODO: at this point an unsubscribe request should be blocked until we finish
            # subscribing
            try:
                await self._do_subscribe_exact(topic=topic)
            except:
                del self.exact_subscriptions[topic]
                requests_so_far = self.exact_subscriptions[topic]
                if requests_so_far == 0:
                    del self.exact_subscriptions[topic]
                else:
                    self.exact_subscriptions[topic] = requests_so_far - 1
                raise

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
        # TODO: concurrency
        my_id = self._counter
        self._counter += 1
        requests_so_far = self.glob_subscriptions.get(glob, 0)
        self.glob_subscriptions[glob] = requests_so_far + 1
        self.active_glob_subscriptions[my_id] = glob
        if requests_so_far == 0:
            # TODO: at this point an unsubscribe request should be blocked until we finish
            # subscribing
            try:
                await self._do_subscribe_glob(glob=glob)
            except:
                del self.glob_subscriptions[glob]
                requests_so_far = self.glob_subscriptions[glob]
                if requests_so_far == 0:
                    del self.glob_subscriptions[glob]
                else:
                    self.glob_subscriptions[glob] = requests_so_far - 1
                raise

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
        # TODO: concurrency
        topic = self.active_exact_subscriptions.get(subscription_id)
        if topic is None:
            return

        del self.active_exact_subscriptions[subscription_id]
        requests_so_far = self.exact_subscriptions[topic]
        if requests_so_far == 1:
            del self.exact_subscriptions[topic]
            # TODO:at this point a subscribe request should be blocked until we finish
            # unsubscribing
            await self._do_unsubscribe_exact(subscription_id=subscription_id)
        else:
            self.exact_subscriptions[topic] = requests_so_far - 1

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
        # TODO: concurrency
        glob = self.active_glob_subscriptions.get(subscription_id)
        if glob is None:
            return

        del self.active_glob_subscriptions[subscription_id]
        requests_so_far = self.glob_subscriptions[glob]
        if requests_so_far == 1:
            del self.glob_subscriptions[glob]
            # TODO: at this point a subscribe request should be blocked until we finish
            # unsubscribing
            await self._do_unsubscribe_glob(subscription_id=subscription_id)
        else:
            self.glob_subscriptions[glob] = requests_so_far - 1

    async def direct_register_on_message(
        self, /, *, receiver: HttpPubSubDirectOnMessageReceiver
    ) -> int: ...

    async def direct_unregister_on_message(
        self, /, *, registration_id: int
    ) -> None: ...

    async def subscribe_multi() -> HttpPubSubClientSubscription:
        """Returns a new async context manager within which you can
        register multiple subscriptions (exact or glob). When exiting,
        the subscriptions will be removed.

        Re-entrant subscriptions are supported and avoid duplicate subscribe/
        unsubscribe requests to the broadcaster

        All the other `subscribe_*` methods just delegate to this plus
        a little setup
        """
        raise NotImplementedError

    async def subscribe_exact(
        self, topic: bytes, *rest: bytes
    ) -> HttpPubSubClientSubscription:
        """Subscribe to one or more topics by exact match. The result is an
        async context manager which, when exited, will unsubscribe from the
        topic(s)

        Re-entrant subscriptions are supported and avoid duplicate subscribe/
        unsubscribe requests to the broadcaster
        """
        result = await self.subscribe_multi()
        await result.subscribe_exact(topic)
        for t in rest:
            await result.subscribe_exact(t)
        return result

    async def subscribe_glob(
        self, glob: str, *rest: str
    ) -> HttpPubSubClientSubscription:
        """Subscribe to one or more topics by glob match. The result is an
        async context manager which, when exited, will unsubscribe from the
        topic(s)

        Re-entrant subscriptions are supported and avoid duplicate subscribe/
        unsubscribe requests to the broadcaster
        """
        result = await self.subscribe_multi()
        await result.subscribe_glob(glob)
        for t in rest:
            await result.subscribe_glob(t)
        return result

    async def subscribe(
        self,
        /,
        *,
        glob: Optional[Iterable[str]] = None,
        exact: Optional[Iterable[bytes]] = None,
    ):
        """Subscribe to a combination of glob and/or exact topics. The result is
        an async context manager which, when exited, will unsubscribe from the
        topic(s)

        Re-entrant subscriptions are supported and avoid duplicate subscribe/
        unsubscribe requests to the broadcaster
        """
        result = await self.subscribe_multi()
        if glob:
            for t in glob:
                await result.subscribe_glob(t)
        if exact:
            for t in exact:
                await result.subscribe_exact(t)
        return result
