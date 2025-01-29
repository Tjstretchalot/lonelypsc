import asyncio
import io
import os
import secrets
import sys
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum, auto
from typing import (
    TYPE_CHECKING,
    ContextManager,
    Dict,
    Iterable,
    Iterator,
    List,
    Literal,
    NoReturn,
    Optional,
    Tuple,
    Type,
    Union,
    cast,
)

import aiohttp
from lonelypsp.compat import fast_dataclass
from lonelypsp.stateful.parser_helpers import read_exact
from lonelypsp.util.async_queue_like import AsyncQueueLike
from lonelypsp.util.drainable_asyncio_queue import DrainableAsyncioQueue, QueueDrained

from lonelypsc.client import (
    PubSubClient,
    PubSubClientBulkSubscriptionConnector,
    PubSubClientConnectionStatus,
    PubSubClientConnector,
    PubSubClientMessageWithCleanup,
    PubSubClientReceiver,
    PubSubClientTracingNotifyOnHashed,
    PubSubClientTracingNotifyStart,
    PubSubDirectConnectionStatusReceiver,
    PubSubDirectOnMessageWithCleanupReceiver,
    PubSubNotifyResult,
    PubSubRequestAmbiguousError,
    PubSubRequestConnectionAbandonedError,
)
from lonelypsc.config.config import BroadcastersShuffler
from lonelypsc.config.ws_config import WebsocketPubSubConfig
from lonelypsc.types.sync_io import (
    SyncStandardIO,
)
from lonelypsc.util.errors import combine_multiple_exceptions
from lonelypsc.util.io_helpers import PositionedSyncStandardIO
from lonelypsc.ws.handlers.handler import handle_any
from lonelypsc.ws.state import (
    ClosingRetryInformationType,
    InternalLargeMessage,
    InternalMessage,
    InternalMessageState,
    InternalMessageStateAndCallback,
    InternalMessageStateType,
    InternalMessageStateUnsent,
    InternalMessageType,
    InternalSmallMessage,
    ManagementTask,
    ManagementTaskSubscribeExact,
    ManagementTaskSubscribeGlob,
    ManagementTaskType,
    ManagementTaskUnsubscribeExact,
    ManagementTaskUnsubscribeGlob,
    ReceivedLargeMessage,
    ReceivedMessage,
    ReceivedMessageType,
    ReceivedSmallMessage,
    RetryInformation,
    State,
    StateClosed,
    StateConnecting,
    StateType,
    TasksOnceOpen,
)
from lonelypsc.ws.websocket_connect_task import make_websocket_connect_task

if sys.version_info >= (3, 11):
    from typing import Never
else:
    from typing import NoReturn as Never


class NoopStatelessTracingNotifyStart:
    def on_start_without_hash(
        self, /, *, topic: bytes, length: int, filelike: bool
    ) -> PubSubClientTracingNotifyOnHashed[Literal[None]]:
        return self

    def on_hashed(self) -> None: ...

    def on_start_with_hash(
        self, /, *, topic: bytes, length: int, filelike: bool
    ) -> None: ...


@contextmanager
def _noop_notify_tracer() -> Iterator[NoopStatelessTracingNotifyStart]:
    yield NoopStatelessTracingNotifyStart()


@fast_dataclass
class WsPubSubNotifyResult:
    notified: int


class CRStateType(Enum):
    NOT_SETUP = auto()
    SETUP = auto()
    ERRORED = auto()
    TORN_DOWN = auto()


@dataclass
class CRStateNotSetup:
    """The state variables before a setup call"""

    type: Literal[CRStateType.NOT_SETUP]
    """discriminator value"""

    config: WebsocketPubSubConfig
    """how the subscriber is configured"""

    receiver_counter: int
    """the id to assign to the next message/status receiver"""

    message_receivers: Dict[int, PubSubDirectOnMessageWithCleanupReceiver]
    """who to inform when the subscriber receives a message on a topic

    since the subscriber is not yet setup, these are not called yet
    """

    status_receivers: Dict[int, PubSubDirectConnectionStatusReceiver]
    """who to inform if the connection status changes. these are expected to be
    under the assumption the connection status is currently LOST.
    
    since the subscriber is not yet setup, these do not need to be called until
    at least setup
    """


@dataclass
class CRStateSetup:
    """The state variables after a setup call and before any teardown call"""

    type: Literal[CRStateType.SETUP]
    """discriminator value"""

    config: WebsocketPubSubConfig
    """how the subscriber is configured"""

    receiver_counter: int
    """the id to assign to the next message/status receiver"""

    message_receivers: Dict[int, PubSubDirectOnMessageWithCleanupReceiver]
    """who to inform when the subscriber receives a message on a topic
    
    called within the state task

    if mutated during a call: every entry that is in this for the entire
    iteration will be called, and no entry will be called more than once
    """

    new_status_receivers: DrainableAsyncioQueue[
        Tuple[int, PubSubDirectConnectionStatusReceiver]
    ]
    """status receivers which have not yet been put into status_receivers because they
    have not yet been informed about the current connection status, which needs to be
    done within the state_task
    """

    removed_status_receivers: DrainableAsyncioQueue[int]
    """status receivers which may be in new_status_receivers or status_receivers but
    need to be removed from there, which must be done within the state_task for
    simplicity and symmetry
    """

    status_receivers: Dict[int, PubSubDirectConnectionStatusReceiver]
    """who to inform if the connection status changes.
    
    called within the state task

    if mutated during a call: every entry that is in this for the entire
    iteration will be called, and no entry will be called more than once
    """

    connection_lost_flag: bool
    """A flag that can be set to true to indicate the state task should
    consider the connection LOST if it were to be considered OK in the next
    iteration
    """

    ws_state: State
    """the state of the connection to the broadcasters, mutated by the state task.
    unless otherwise noted it should be assumed all inner variables are not asyncio
    safe
    """

    state_task: asyncio.Task[None]
    """the task that is mutating ws_state"""


@fast_dataclass
class CRStateErrored:
    """the state variables if the state task errors/finishes unexpectedly and before
    any teardown call
    """

    type: Literal[CRStateType.ERRORED]
    """discriminator value"""

    exception: BaseException
    """the exception to raise when trying to use the connection"""


@fast_dataclass
class CRStateTornDown:
    """the state variables after a teardown call"""

    type: Literal[CRStateType.TORN_DOWN]
    """discriminator value"""


CRState = Union[
    CRStateNotSetup,
    CRStateSetup,
    CRStateErrored,
    CRStateTornDown,
]


class WSPubSubConnectorReceiver:
    """The connector/receiver for the websocket client; unlike with the
    http client, both outgoing and incoming messages are handled within
    the same connection, so splitting the connector and receiver is less
    useful

    In order to maintain the same signature as the standard connector/receiver,
    this will treat setting up the connector or receiver as setting up this
    object, then tearing down either as tearing down this object, and ignore
    repeated setups/teardowns02
    """

    def __init__(self, config: WebsocketPubSubConfig) -> None:
        self.state: CRState = CRStateNotSetup(
            type=CRStateType.NOT_SETUP,
            config=config,
            receiver_counter=1,
            message_receivers=dict(),
            status_receivers=dict(),
        )
        """our state; we add a layer of indirection to make the relationships between
        the state variables more clear
        """

    async def _setup(self) -> None:
        """The implementation for setup connector / setup receiver"""
        if self.state.type != CRStateType.NOT_SETUP:
            if self.state.type == CRStateType.ERRORED:
                raise self.state.exception
            if self.state.type == CRStateType.TORN_DOWN:
                raise Exception("cannot setup after teardown")
            return

        shuffler = BroadcastersShuffler(self.state.config.broadcasters)
        retry = RetryInformation(
            shuffler=shuffler,
            iteration=0,
            iterator=iter(shuffler),
        )
        broadcaster = next(retry.iterator)

        client_session = aiohttp.ClientSession()
        self.state = CRStateSetup(
            type=CRStateType.SETUP,
            config=self.state.config,
            receiver_counter=self.state.receiver_counter,
            message_receivers=self.state.message_receivers,
            status_receivers=self.state.status_receivers,
            new_status_receivers=DrainableAsyncioQueue(),
            removed_status_receivers=DrainableAsyncioQueue(),
            connection_lost_flag=False,
            ws_state=StateConnecting(
                type=StateType.CONNECTING,
                config=self.state.config,
                client_session=client_session,
                websocket_task=make_websocket_connect_task(
                    config=self.state.config,
                    broadcaster=broadcaster,
                    client_session=client_session,
                ),
                cancel_requested=asyncio.Event(),
                broadcaster=broadcaster,
                retry=retry,
                tasks=TasksOnceOpen(
                    exact_subscriptions=set(),
                    glob_subscriptions=set(),
                    unsorted=DrainableAsyncioQueue(
                        max_size=self.state.config.max_expected_acks
                    ),
                    unsent_notifications=DrainableAsyncioQueue(
                        max_size=self.state.config.max_unsent_notifications
                    ),
                    resending_notifications=[],
                ),
                backgrounded=set(),
            ),
            state_task=cast(asyncio.Task[None], None),  # order of initialization
        )
        self.state.state_task = asyncio.create_task(self._state_task_target())

    async def _teardown(self) -> None:
        """The implementation for teardown connector / teardown receiver"""
        if (
            self.state.type == CRStateType.NOT_SETUP
            or self.state.type == CRStateType.ERRORED
            or self.state.type == CRStateType.TORN_DOWN
        ):
            self.state = CRStateTornDown(type=CRStateType.TORN_DOWN)
            return

        if self.state.ws_state.type != StateType.CLOSED:
            self.state.ws_state.cancel_requested.set()
        try:
            await self.state.state_task
        finally:
            self.state = CRStateTornDown(type=CRStateType.TORN_DOWN)

    async def _state_task_target(self) -> None:
        """the target for state_task when in StateType.SETUP; manages actually looping the
        websocket state and notifying message/status receivers based on changes to that state

        this will complete when the state moves away from SETUP or the websocket state
        moves to closed. Thus, to cancel this task, do not use standard cancellation, instead,
        set cancel_requested on the websocket state which will tell the underlying handler to
        gracefully shutdown
        """
        state = self.state
        if state.type != CRStateType.SETUP:
            return
        receivers_expect = PubSubClientConnectionStatus.LOST
        receiver_errors: List[BaseException] = []
        handler_error: Optional[BaseException] = None
        received_queue: DrainableAsyncioQueue[ReceivedMessage] = DrainableAsyncioQueue(
            max_size=state.config.max_received
        )
        received_missed: asyncio.Event = asyncio.Event()
        received_task = asyncio.create_task(
            self._handle_received_task_target(received_queue, received_missed)
        )

        while True:
            state = self.state
            if state.type != CRStateType.SETUP:
                break

            new_receivers = dict(state.new_status_receivers.drain())
            state.new_status_receivers = DrainableAsyncioQueue()

            state.status_receivers.update(new_receivers)

            removed_receivers = state.removed_status_receivers.drain()
            state.removed_status_receivers = DrainableAsyncioQueue()

            for receiver_id in removed_receivers:
                state.status_receivers.pop(receiver_id, None)
                new_receivers.pop(receiver_id, None)

            if receivers_expect != PubSubClientConnectionStatus.LOST:
                await self._inform_status_receivers(
                    receivers_expect, new_receivers.values(), receiver_errors
                )

                if receiver_errors and state.ws_state.type != StateType.CLOSED:
                    state.ws_state.cancel_requested.set()

            old_received = (
                None
                if state.ws_state.type != StateType.OPEN
                else state.ws_state.received
            )
            try:
                state.ws_state = await handle_any(state.ws_state)
            except BaseException as e:
                handler_error = e
                state.ws_state = StateClosed(type=StateType.CLOSED)

            connection_statuses: List[PubSubClientConnectionStatus] = [
                PubSubClientConnectionStatus.LOST
            ]

            if received_task.done():
                connection_statuses = [PubSubClientConnectionStatus.ABANDONED]
                exc = received_task.exception()
                if exc is not None:
                    receiver_errors.append(exc)
                else:
                    receiver_errors.append(
                        Exception("received_task finished unexpectedly")
                    )
            elif (
                state.ws_state.type != StateType.CLOSED
                and state.ws_state.cancel_requested.is_set()
            ):
                connection_statuses = [PubSubClientConnectionStatus.ABANDONED]
            elif (
                state.ws_state.type == StateType.OPEN
                # if there are no unsent management tasks and all the acks can be explained by sent
                # notifications, then all subscriptions/unsubscriptions have been ack'd
                and state.ws_state.management_tasks.qsize() == 0
                and len(state.ws_state.expected_acks)
                <= len(state.ws_state.sent_notifications)
            ):
                if state.connection_lost_flag or received_missed.is_set():
                    connection_statuses = [
                        PubSubClientConnectionStatus.LOST,
                        PubSubClientConnectionStatus.OK,
                    ]
                else:
                    connection_statuses = [PubSubClientConnectionStatus.OK]
            elif state.ws_state.type == StateType.CLOSED:
                connection_statuses = [PubSubClientConnectionStatus.ABANDONED]

            state.connection_lost_flag = False
            received_missed.clear()

            for connection_status in connection_statuses:
                if connection_status == receivers_expect:
                    continue
                await self._inform_status_receivers(
                    connection_status,
                    state.status_receivers.values(),
                    receiver_errors,
                )
                receivers_expect = connection_status

                if not receiver_errors:
                    continue

                if receivers_expect != PubSubClientConnectionStatus.ABANDONED:
                    await self._inform_status_receivers(
                        PubSubClientConnectionStatus.ABANDONED,
                        state.status_receivers.values(),
                        receiver_errors,
                    )

                if state.ws_state.type != StateType.CLOSED:
                    state.ws_state.cancel_requested.set()

                receivers_expect = PubSubClientConnectionStatus.ABANDONED
                break

            if state.ws_state.type != StateType.OPEN:
                if old_received is not None:
                    for message in old_received.drain():
                        received_queue.put_nowait(message)
            else:
                while True:
                    try:
                        message = state.ws_state.received.get_nowait()
                    except asyncio.QueueEmpty:
                        break

                    received_queue.put_nowait(message)

            if state.ws_state.type == StateType.CLOSED:
                should_process = not received_task.done()
                unprocessed = received_queue.drain()
                try:
                    await received_task
                except BaseException as e:
                    receiver_errors.append(e)

                if should_process:
                    old_error_count = len(receiver_errors)
                    for message in unprocessed:
                        if message.type == ReceivedMessageType.MISSED:
                            continue

                        await self._handle_received_message(
                            message, state.message_receivers.values(), receiver_errors
                        )
                        if len(receiver_errors) > old_error_count:
                            break
                break

        if receiver_errors:
            raise combine_multiple_exceptions(
                "handlers raised error", receiver_errors, context=handler_error
            )

        if handler_error:
            raise handler_error

    async def _inform_status_receivers(
        self,
        status: PubSubClientConnectionStatus,
        receivers: Iterable[PubSubDirectConnectionStatusReceiver],
        errors: List[BaseException],
    ) -> None:
        """informs each of the status receivers about the new status; if any of them
        raise errors (including asyncio.CancelledError), puts those errors in the given
        list to be merged and raised at the appropriate point. also, critically, this
        ensures that a receiver that is later in the list is still informed if an earlier
        one raises an exception
        """
        for receiver in receivers:
            try:
                if status == PubSubClientConnectionStatus.OK:
                    await receiver.on_connection_established()
                elif status == PubSubClientConnectionStatus.LOST:
                    await receiver.on_connection_lost()
                else:
                    # verify type
                    _: Literal[PubSubClientConnectionStatus.ABANDONED] = status
                    await receiver.on_connection_abandoned()
            except BaseException as e:
                errors.append(e)

    async def _handle_received_task_target(
        self, queue: AsyncQueueLike[ReceivedMessage], received_missed: asyncio.Event
    ) -> None:
        """target for received_task within _state_task_target; constantly pulls received
        messages from the queue and handles them

        assumes that message_receivers may be mutated, but the reference will not change

        can be canceled by draining the queue, if the queue is drainable, otherwise regular
        cancellation will work (but might interrupt a receiver, which is not ideal)
        """
        if (
            self.state.type == CRStateType.TORN_DOWN
            or self.state.type == CRStateType.ERRORED
        ):
            return
        receivers_dict = self.state.message_receivers
        errors: List[BaseException] = []
        while True:
            try:
                message = await queue.get()
            except QueueDrained:
                return
            if message.type == ReceivedMessageType.SMALL:
                await self._handle_received_small_message(
                    message, receivers_dict.values(), errors
                )
            elif message.type == ReceivedMessageType.LARGE:
                await self._handle_received_large_message(
                    message, receivers_dict.values(), errors
                )
            elif message.type == ReceivedMessageType.MISSED:
                received_missed.set()
            else:
                _assert_never(message)

            if errors:
                raise combine_multiple_exceptions(
                    "message handlers raised error", errors
                )

    async def _handle_received_message(
        self,
        message: Union[ReceivedSmallMessage, ReceivedLargeMessage],
        receivers: Iterable[PubSubDirectOnMessageWithCleanupReceiver],
        errors: List[BaseException],
    ) -> None:
        """normally received messages are handled in _handle_received_task_target,
        and if that is confirmed done(), this is called by _state_task_target
        with the final messages, such that this is never called concurrently
        with itself

        tells every receiver about the message, and if any of them raise an error,
        puts that error in the given list to be merged and raised at the appropriate
        point

        NOTE: the behavior that is targeted is that:
        - if one receiver is notified about a message, then every receiver is notified
        - receivers are notified one by one
        - once any message receiver errors, no future messages are sent to receivers and
          the connection is gracefully shutdown, then the error continues to bubble
          (the shutdown will cause the status handler to be informed of ABANDONED, which
          the caller will then see and teardown, which will have the error raised). the
          idea is that is perhaps the best stack trace experience: both the connection
          that was setup that errored and the receiver that errored will be included

        in some situations its recoverable if one message fails to process; in that case,
        just swallow errors inside the receiver (presumably, logging it where appropriate)
        """
        if message.type == ReceivedMessageType.SMALL:
            return await self._handle_received_small_message(message, receivers, errors)

        return await self._handle_received_large_message(message, receivers, errors)

    async def _handle_received_small_message(
        self,
        message: ReceivedSmallMessage,
        receivers: Iterable[PubSubDirectOnMessageWithCleanupReceiver],
        errors: List[BaseException],
    ) -> None:
        """informs receivers about a small message; see _handle_received_message for
        more information
        """
        for recv in list(receivers):
            try:
                await recv.on_message(
                    PubSubClientMessageWithCleanup(
                        topic=message.topic,
                        sha512=message.sha512,
                        data=io.BytesIO(message.data),
                        cleanup=_noop,
                    )
                )
            except BaseException as e:
                errors.append(e)

    async def _handle_received_large_message(
        self,
        message: ReceivedLargeMessage,
        receivers: Iterable[PubSubDirectOnMessageWithCleanupReceiver],
        errors: List[BaseException],
    ) -> None:
        """informs receivers about a large message; see _handle_received_message for
        more information
        """
        try:
            data_starts_at = message.stream.tell()
            for recv in list(receivers):
                try:
                    event = asyncio.Event()

                    async def _set_event() -> None:
                        event.set()

                    message.stream.seek(data_starts_at, os.SEEK_SET)
                    await recv.on_message(
                        PubSubClientMessageWithCleanup(
                            topic=message.topic,
                            sha512=message.sha512,
                            data=message.stream,
                            cleanup=_set_event,
                        )
                    )
                    await event.wait()
                except BaseException as e:
                    errors.append(e)
        finally:
            try:
                message.stream.close()
            except BaseException as e:
                errors.append(e)

    async def _check_errored(self) -> None:
        """Verifies we are in the SETUP state and the state task is still running.
        If closing and not retrying, waits for the state task to finish
        """
        if self.state.type == CRStateType.ERRORED:
            raise self.state.exception
        if self.state.type == CRStateType.TORN_DOWN:
            raise Exception("cannot use connection after teardown")

        assert self.state.type == CRStateType.SETUP, "call setup_connector first"
        if (
            self.state.ws_state.type == StateType.CLOSED
            or (
                self.state.ws_state.type == StateType.CLOSING
                and self.state.ws_state.retry.type
                == ClosingRetryInformationType.CANNOT_RETRY
            )
            or self.state.state_task.done()
        ):
            try:
                await self.state.state_task
                self.state = cast(CRState, self.state)  # tell mypy it may have changed
                if self.state.type == CRStateType.SETUP:
                    self.state = CRStateErrored(
                        type=CRStateType.ERRORED,
                        exception=Exception("state task finished unexpectedly"),
                    )
            except BaseException as e:
                self.state = cast(CRState, self.state)  # tell mypy it may have changed
                if self.state.type == CRStateType.SETUP:
                    self.state = CRStateErrored(type=CRStateType.ERRORED, exception=e)

            if self.state.type == CRStateType.ERRORED:
                raise self.state.exception

            assert self.state.type == CRStateType.TORN_DOWN
            raise Exception("cannot use connection after teardown")

    def _put_management_task(self, task: ManagementTask) -> None:
        """Puts a management task into the appropriate queue based on the current state
        to ensure it eventually gets performed

        MUST call `_check_errored` before calling this

        If it is a subscribe task then also ensures that the status receivers are
        told that the connection is LOST; this can be a little unintuitive, but the
        idea is that `on_connection_established` should be used if there was a period
        of time where messages may have been sent and not received, which is trivially
        true if the subscriber was previously not subscribed
        """
        assert self.state.type == CRStateType.SETUP, "_check_errored?"

        self.state.connection_lost_flag = (
            self.state.connection_lost_flag
            or task.type == ManagementTaskType.SUBSCRIBE_EXACT
            or task.type == ManagementTaskType.SUBSCRIBE_GLOB
        )

        if self.state.ws_state.type == StateType.OPEN:
            self.state.ws_state.management_tasks.put_nowait(task)
            return

        if self.state.ws_state.type == StateType.CLOSING:
            assert (
                self.state.ws_state.retry.type == ClosingRetryInformationType.WANT_RETRY
            )
            self.state.ws_state.retry.tasks.unsorted.put_nowait(task)
            return

        assert self.state.ws_state.type != StateType.CLOSED, "_check_errored?"
        self.state.ws_state.tasks.unsorted.put_nowait(task)

    async def setup_connector(self) -> None:
        await self._setup()

    async def teardown_connector(self) -> None:
        await self._teardown()

    async def subscribe_exact(self, /, *, topic: bytes) -> None:
        await self._check_errored()
        self._put_management_task(
            ManagementTaskSubscribeExact(
                type=ManagementTaskType.SUBSCRIBE_EXACT, topic=topic
            )
        )

    async def subscribe_glob(self, /, *, glob: str) -> None:
        await self._check_errored()
        self._put_management_task(
            ManagementTaskSubscribeGlob(
                type=ManagementTaskType.SUBSCRIBE_GLOB, glob=glob
            )
        )

    async def unsubscribe_exact(self, /, *, topic: bytes) -> None:
        await self._check_errored()
        self._put_management_task(
            ManagementTaskUnsubscribeExact(
                type=ManagementTaskType.UNSUBSCRIBE_EXACT, topic=topic
            )
        )

    async def unsubscribe_glob(self, /, *, glob: str) -> None:
        await self._check_errored()
        self._put_management_task(
            ManagementTaskUnsubscribeGlob(
                type=ManagementTaskType.UNSUBSCRIBE_GLOB, glob=glob
            )
        )

    def get_bulk(self) -> Optional[PubSubClientBulkSubscriptionConnector]:
        """Returns a bulk subscription connector if supported, otherwise None"""
        return None

    def prepare_notifier_trace(
        self, initializer: Literal[None], /
    ) -> ContextManager[PubSubClientTracingNotifyStart[Literal[None]]]:
        return _noop_notify_tracer()

    async def notify(
        self,
        /,
        *,
        topic: bytes,
        message: SyncStandardIO,
        length: int,
        message_sha512: bytes,
        tracer: Literal[None],
    ) -> PubSubNotifyResult:
        await self._check_errored()
        assert self.state.type == CRStateType.SETUP, "_check_errored?"

        state_queue: DrainableAsyncioQueue[InternalMessageState] = (
            DrainableAsyncioQueue(max_size=1)
        )
        callback = InternalMessageStateAndCallback(
            state=InternalMessageStateUnsent(
                type=InternalMessageStateType.UNSENT,
            ),
            callback=state_queue.put,
            task=None,
            queued=None,
        )

        identifier = secrets.token_bytes(4)
        msg: InternalMessage
        if (
            self.state.config.max_websocket_message_size is None
            or length < self.state.config.max_websocket_message_size
        ):
            msg = InternalSmallMessage(
                type=InternalMessageType.SMALL,
                identifier=identifier,
                topic=topic,
                data=read_exact(message, length),
                sha512=message_sha512,
                callback=callback,
            )
        else:
            message_start = message.tell()
            if message.tell() != 0:
                message = PositionedSyncStandardIO(
                    message,
                    start_idx=message_start,
                    end_idx=message_start + length,
                )
            msg = InternalLargeMessage(
                type=InternalMessageType.LARGE,
                identifier=identifier,
                topic=topic,
                stream=message,
                length=length,
                sha512=message_sha512,
                callback=callback,
            )

        if self.state.ws_state.type == StateType.OPEN:
            self.state.ws_state.unsent_notifications.put_nowait(msg)
        elif self.state.ws_state.type == StateType.CLOSING:
            assert (
                self.state.ws_state.retry.type == ClosingRetryInformationType.WANT_RETRY
            ), "_check_errored?"
            self.state.ws_state.retry.tasks.unsent_notifications.put_nowait(msg)
        elif self.state.ws_state.type == StateType.CLOSED:
            raise AssertionError("_check_errored?")
        else:
            self.state.ws_state.tasks.unsent_notifications.put_nowait(msg)

        async with state_queue:
            while True:
                state = await state_queue.get()
                if state.type == InternalMessageStateType.ACKNOWLEDGED:
                    return WsPubSubNotifyResult(state.notified)
                if state.type == InternalMessageStateType.DROPPED_UNSENT:
                    raise PubSubRequestConnectionAbandonedError(
                        "message was not sent before connection was lost"
                    )
                elif state.type == InternalMessageStateType.DROPPED_SENT:
                    raise PubSubRequestAmbiguousError(
                        "message was sent but not acknowledged before the connection was abandoned"
                    )

    @property
    def connection_status(self) -> PubSubClientConnectionStatus:
        if self.state.type == CRStateType.NOT_SETUP:
            return PubSubClientConnectionStatus.LOST
        if self.state.type != CRStateType.SETUP:
            return PubSubClientConnectionStatus.ABANDONED

        if (
            self.state.ws_state.type == StateType.OPEN
            # if there are no unsent management tasks and all the acks can be explained by sent
            # notifications, then all subscriptions/unsubscriptions have been ack'd
            and self.state.ws_state.management_tasks.qsize() == 0
            and len(self.state.ws_state.expected_acks)
            <= len(self.state.ws_state.sent_notifications)
            and not self.state.connection_lost_flag
        ):
            return PubSubClientConnectionStatus.OK

        if self.state.ws_state.type == StateType.CLOSED:
            return PubSubClientConnectionStatus.ABANDONED

        if (
            self.state.ws_state.type == StateType.CLOSING
            and self.state.ws_state.retry.type
            == ClosingRetryInformationType.CANNOT_RETRY
        ):
            return PubSubClientConnectionStatus.ABANDONED

        return PubSubClientConnectionStatus.LOST

    async def setup_receiver(self) -> None:
        await self._setup()

    async def teardown_receiver(self) -> None:
        await self._teardown()

    async def register_on_message(
        self, /, *, receiver: PubSubDirectOnMessageWithCleanupReceiver
    ) -> int:
        if self.state.type != CRStateType.NOT_SETUP:
            await self._check_errored()

        assert (
            self.state.type == CRStateType.SETUP
            or self.state.type == CRStateType.NOT_SETUP
        ), "_check_errored?"

        recv_id = self.state.receiver_counter
        self.state.receiver_counter += 1
        self.state.message_receivers[recv_id] = receiver
        return recv_id

    async def unregister_on_message(self, /, *, registration_id: int) -> None:
        if self.state.type != CRStateType.NOT_SETUP:
            await self._check_errored()

        assert (
            self.state.type == CRStateType.SETUP
            or self.state.type == CRStateType.NOT_SETUP
        ), "_check_errored?"

        self.state.message_receivers.pop(registration_id, None)

    async def register_status_handler(
        self, /, *, receiver: PubSubDirectConnectionStatusReceiver
    ) -> int:
        if self.state.type == CRStateType.NOT_SETUP:
            recv_id = self.state.receiver_counter
            self.state.receiver_counter += 1
            self.state.status_receivers[recv_id] = receiver
            return recv_id

        await self._check_errored()
        assert self.state.type == CRStateType.SETUP, "_check_errored?"
        recv_id = self.state.receiver_counter
        self.state.receiver_counter += 1
        self.state.new_status_receivers.put_nowait((recv_id, receiver))
        return recv_id

    async def unregister_status_handler(self, /, *, registration_id: int) -> None:
        if self.state.type == CRStateType.NOT_SETUP:
            self.state.status_receivers.pop(registration_id, None)
            return

        await self._check_errored()
        assert self.state.type == CRStateType.SETUP, "_check_errored?"
        self.state.removed_status_receivers.put_nowait(registration_id)


def WebsocketPubSubClient(config: WebsocketPubSubConfig) -> PubSubClient[None, None]:
    """A constructor-like function that creates a PubSubClient that communicates
    over a websocket, handling retries and reconnections as necessary. It is
    typically important when using this to pass `on_receiving` when subscribing,
    as while reconnecting to broadcasters no messages will be received
    """

    async def setup() -> None:
        await config.setup_to_subscriber_auth()
        try:
            await config.setup_to_broadcaster_auth()
        except BaseException:
            await config.teardown_to_subscriber_auth()
            raise

    async def teardown() -> None:
        try:
            await config.teardown_to_broadcaster_auth()
        finally:
            await config.teardown_to_subscriber_auth()

    connector_receiver = WSPubSubConnectorReceiver(config)

    return PubSubClient(
        connector_receiver,
        connector_receiver,
        setup=setup,
        teardown=teardown,
    )


async def _noop() -> None:
    pass


def _assert_never(_: Never) -> NoReturn:
    raise AssertionError("unreachable")


if TYPE_CHECKING:
    __: Type[PubSubClientConnector] = WSPubSubConnectorReceiver
    ___: Type[PubSubClientReceiver] = WSPubSubConnectorReceiver
