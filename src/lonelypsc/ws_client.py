import asyncio
import secrets
from dataclasses import dataclass
from enum import Enum, auto
from typing import (
    TYPE_CHECKING,
    Dict,
    List,
    Literal,
    Optional,
    Tuple,
    Type,
    Union,
    cast,
)

from lonelypsp.compat import fast_dataclass
from lonelypsp.stateful.parser_helpers import read_exact
from lonelypsp.util.drainable_asyncio_queue import DrainableAsyncioQueue

from lonelypsc.client import (
    PubSubClient,
    PubSubClientConnectionStatus,
    PubSubClientConnector,
    PubSubClientReceiver,
    PubSubDirectConnectionStatusReceiver,
    PubSubDirectOnMessageWithCleanupReceiver,
    PubSubNotifyResult,
)
from lonelypsc.config.config import BroadcastersShuffler
from lonelypsc.config.ws_config import WebsocketPubSubConfig
from lonelypsc.http_client import HttpPubSubNotifyResult
from lonelypsc.types.sync_io import (
    SyncStandardIO,
)
from lonelypsc.util.errors import combine_multiple_exceptions
from lonelypsc.ws.handlers.handler import handle_any
from lonelypsc.ws.state import (
    ClosingRetryInformationType,
    InternalLargeMessage,
    InternalMessage,
    InternalMessageState,
    InternalMessageStateType,
    InternalMessageType,
    InternalSmallMessage,
    ManagementTask,
    ManagementTaskSubscribeExact,
    ManagementTaskSubscribeGlob,
    ManagementTaskType,
    ManagementTaskUnsubscribeExact,
    ManagementTaskUnsubscribeGlob,
    RetryInformation,
    State,
    StateClosed,
    StateConnecting,
    StateType,
    TasksOnceOpen,
)


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
        receivers_expect = PubSubClientConnectionStatus.LOST
        receiver_errors: List[BaseException] = []
        handler_error: Optional[BaseException] = None

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
                for receiver in new_receivers.values():
                    try:
                        if receivers_expect == PubSubClientConnectionStatus.OK:
                            await receiver.on_connection_established()
                        else:
                            # verify type
                            _: Literal[PubSubClientConnectionStatus.ABANDONED] = (
                                receivers_expect
                            )
                            await receiver.on_connection_abandoned()
                    except BaseException as e:
                        receiver_errors.append(e)

                if receiver_errors and state.ws_state.type != StateType.CLOSED:
                    state.ws_state.cancel_requested.set()

            try:
                state.ws_state = await handle_any(state.ws_state)
            except BaseException as e:
                handler_error = e
                state.ws_state = StateClosed(type=StateType.CLOSED)

            connection_status = PubSubClientConnectionStatus.LOST

            if (
                state.ws_state.type != StateType.CLOSED
                and state.ws_state.cancel_requested.is_set()
            ):
                connection_status = PubSubClientConnectionStatus.ABANDONED
            elif (
                state.ws_state.type == StateType.OPEN
                and not state.ws_state.management_tasks
                and not state.connection_lost_flag
            ):
                connection_status = PubSubClientConnectionStatus.OK
            elif state.ws_state.type == StateType.CLOSED:
                connection_status = PubSubClientConnectionStatus.ABANDONED

            state.connection_lost_flag = False

            if connection_status != receivers_expect:
                for receiver in state.status_receivers.values():
                    try:
                        if connection_status == PubSubClientConnectionStatus.OK:
                            await receiver.on_connection_established()
                        elif connection_status == PubSubClientConnectionStatus.LOST:
                            await receiver.on_connection_lost()
                        else:
                            # verify type
                            _ab: Literal[PubSubClientConnectionStatus.ABANDONED] = (
                                connection_status
                            )
                            await receiver.on_connection_abandoned()
                    except BaseException as e:
                        receiver_errors.append(e)
                receivers_expect = connection_status

                if receiver_errors:
                    if receivers_expect != PubSubClientConnectionStatus.ABANDONED:
                        for receiver in state.status_receivers.values():
                            try:
                                await receiver.on_connection_abandoned()
                            except BaseException as e:
                                receiver_errors.append(e)

                    if state.ws_state.type != StateType.CLOSED:
                        state.ws_state.cancel_requested.set()

                    receivers_expect = PubSubClientConnectionStatus.ABANDONED

            if state.ws_state.type == StateType.CLOSED:
                break

        if receiver_errors:
            raise combine_multiple_exceptions(
                "status handlers raised error", receiver_errors, context=handler_error
            )

        if handler_error:
            raise handler_error

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

    def _put_management_task(self, task: ManagementTask, *, is_subscribe: bool) -> None:
        assert self.state.type == CRStateType.SETUP, "_check_errored?"

        self.state.connection_lost_flag = (
            self.state.connection_lost_flag or is_subscribe
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
            ),
            is_subscribe=True,
        )

    async def subscribe_glob(self, /, *, glob: str) -> None:
        await self._check_errored()
        self._put_management_task(
            ManagementTaskSubscribeGlob(
                type=ManagementTaskType.SUBSCRIBE_GLOB, glob=glob
            ),
            is_subscribe=True,
        )

    async def unsubscribe_exact(self, /, *, topic: bytes) -> None:
        await self._check_errored()
        self._put_management_task(
            ManagementTaskUnsubscribeExact(
                type=ManagementTaskType.UNSUBSCRIBE_EXACT, topic=topic
            ),
            is_subscribe=False,
        )

    async def unsubscribe_glob(self, /, *, glob: str) -> None:
        await self._check_errored()
        self._put_management_task(
            ManagementTaskUnsubscribeGlob(
                type=ManagementTaskType.UNSUBSCRIBE_GLOB, glob=glob
            ),
            is_subscribe=False,
        )

    async def notify(
        self,
        /,
        *,
        topic: bytes,
        message: SyncStandardIO,
        length: int,
        message_sha512: bytes,
    ) -> PubSubNotifyResult:
        await self._check_errored()
        assert self.state.type == CRStateType.SETUP, "_check_errored?"

        state_queue: DrainableAsyncioQueue[InternalMessageState] = (
            DrainableAsyncioQueue(max_size=1)
        )

        async def _callback(state: InternalMessageState) -> None:
            await state_queue.put(state)

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
                callback=_callback,
            )
        else:
            msg = InternalLargeMessage(
                type=InternalMessageType.LARGE,
                identifier=identifier,
                topic=topic,
                stream=message,
                length=length,
                sha512=message_sha512,
                callback=_callback,
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
                    raise Exception("message was not sent before connection was lost")
                elif state.type == InternalMessageStateType.DROPPED_SENT:
                    raise Exception(
                        "message was sent but not acknowledged before connection was lost"
                    )

    @property
    def connection_status(self) -> PubSubClientConnectionStatus:
        if self.state.type == CRStateType.NOT_SETUP:
            return PubSubClientConnectionStatus.LOST
        if self.state.type != CRStateType.SETUP:
            return PubSubClientConnectionStatus.ABANDONED

        if (
            self.state.ws_state.type == StateType.OPEN
            and not self.state.ws_state.management_tasks
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


def WebsocketPubSubClient(config: WebsocketPubSubConfig) -> PubSubClient:
    async def setup() -> None:
        await config.setup_incoming_auth()
        try:
            await config.setup_outgoing_auth()
        except BaseException:
            await config.teardown_incoming_auth()
            raise

    async def teardown() -> None:
        try:
            await config.teardown_outgoing_auth()
        finally:
            await config.teardown_incoming_auth()

    connector_receiver = WSPubSubConnectorReceiver(config)

    return PubSubClient(
        connector_receiver,
        connector_receiver,
        setup=setup,
        teardown=teardown,
    )


if TYPE_CHECKING:
    _: Type[PubSubNotifyResult] = HttpPubSubNotifyResult
    __: Type[PubSubClientConnector] = WSPubSubConnectorReceiver
    ___: Type[PubSubClientReceiver] = WSPubSubConnectorReceiver
