import asyncio
import time

from lonelypsp.stateful.constants import (
    BroadcasterToSubscriberStatefulMessageType,
    SubscriberToBroadcasterStatefulMessageType,
)
from lonelypsp.stateful.messages.confirm_subscribe import (
    B2S_ConfirmSubscribeExact,
    B2S_ConfirmSubscribeGlob,
)
from lonelypsp.stateful.messages.confirm_unsubscribe import (
    B2S_ConfirmUnsubscribeExact,
    B2S_ConfirmUnsubscribeGlob,
)
from lonelypsp.stateful.messages.subscribe import (
    S2B_SubscribeExact,
    S2B_SubscribeGlob,
    serialize_s2b_subscribe_exact,
    serialize_s2b_subscribe_glob,
)
from lonelypsp.stateful.messages.unsubscribe import (
    S2B_UnsubscribeExact,
    S2B_UnsubscribeGlob,
    serialize_s2b_unsubscribe_exact,
    serialize_s2b_unsubscribe_glob,
)

from lonelypsc.ws.check_result import CheckResult
from lonelypsc.ws.handlers.open.websocket_url import (
    make_for_send_websocket_url_and_change_counter,
)
from lonelypsc.ws.state import (
    ManagementTask,
    ManagementTaskType,
    SendingManagementTask,
    SendingState,
    StateOpen,
)


def check_management_tasks(state: StateOpen) -> CheckResult:
    """Checks if the left item from management_tasks can be moved to send_task,
    doing so if possible.

    Critically this does not return successfully if it is possible the management
    task cannot be recovered. Specifically, the management task is immediately
    moved into expected_acks where it can be recovered in cleanup_open, only
    possibly failing with irrecoverable errors

    (Note: cleanup_open using sending.management_task instead would be difficult
    given it would be ambiguous if it had been added to expected_acks or not if it
    was added there in the sending.task)
    """
    if state.sending is not None:
        return CheckResult.CONTINUE

    try:
        task = state.management_tasks.get_nowait()
    except asyncio.QueueEmpty:
        return CheckResult.CONTINUE

    if task.type == ManagementTaskType.SUBSCRIBE_EXACT:
        state.expected_acks.append(
            B2S_ConfirmSubscribeExact(
                type=BroadcasterToSubscriberStatefulMessageType.CONFIRM_SUBSCRIBE_EXACT,
                topic=task.topic,
                tracing=b"",
                authorization=None,
            )
        )
    elif task.type == ManagementTaskType.SUBSCRIBE_GLOB:
        state.expected_acks.append(
            B2S_ConfirmSubscribeGlob(
                type=BroadcasterToSubscriberStatefulMessageType.CONFIRM_SUBSCRIBE_GLOB,
                glob=task.glob,
                tracing=b"",
                authorization=None,
            )
        )
    elif task.type == ManagementTaskType.UNSUBSCRIBE_EXACT:
        state.expected_acks.append(
            B2S_ConfirmUnsubscribeExact(
                type=BroadcasterToSubscriberStatefulMessageType.CONFIRM_UNSUBSCRIBE_EXACT,
                topic=task.topic,
                tracing=b"",
                authorization=None,
            )
        )
    elif task.type == ManagementTaskType.UNSUBSCRIBE_GLOB:
        state.expected_acks.append(
            B2S_ConfirmUnsubscribeGlob(
                type=BroadcasterToSubscriberStatefulMessageType.CONFIRM_UNSUBSCRIBE_GLOB,
                glob=task.glob,
                authorization=None,
                tracing=b"",
            )
        )
    else:
        raise NotImplementedError(f"unknown management task type {task}")

    state.sending = SendingManagementTask(
        type=SendingState.MANAGEMENT_TASK,
        management_task=task,
        task=asyncio.create_task(send_management_task(state, task)),
    )
    return CheckResult.RESTART


async def send_management_task(state: StateOpen, task: ManagementTask) -> None:
    """Target for state.sending.task when using SendingManagementTask; this
    potentially needs multiple event loops to build the authorization value,
    during which time nothing else can send on the websocket since the url
    included in the authorization is order dependent
    """
    url = make_for_send_websocket_url_and_change_counter(state)

    if task.type == ManagementTaskType.SUBSCRIBE_EXACT:
        authorization = await state.config.authorize_subscribe_exact(
            tracing=b"",  # TODO: tracing
            url=url,
            recovery=None,
            exact=task.topic,
            now=time.time(),
        )
        await state.websocket.send_bytes(
            serialize_s2b_subscribe_exact(
                S2B_SubscribeExact(
                    type=SubscriberToBroadcasterStatefulMessageType.SUBSCRIBE_EXACT,
                    authorization=authorization,
                    topic=task.topic,
                    tracing=b"",  # TODO: tracing
                ),
                minimal_headers=state.config.websocket_minimal_headers,
            )
        )
        return

    if task.type == ManagementTaskType.SUBSCRIBE_GLOB:
        authorization = await state.config.authorize_subscribe_glob(
            tracing=b"", url=url, recovery=None, glob=task.glob, now=time.time()
        )
        await state.websocket.send_bytes(
            serialize_s2b_subscribe_glob(
                S2B_SubscribeGlob(
                    type=SubscriberToBroadcasterStatefulMessageType.SUBSCRIBE_GLOB,
                    authorization=authorization,
                    tracing=b"",  # TODO: traicng
                    glob=task.glob,
                ),
                minimal_headers=state.config.websocket_minimal_headers,
            )
        )
        return

    if task.type == ManagementTaskType.UNSUBSCRIBE_EXACT:
        authorization = await state.config.authorize_subscribe_exact(
            tracing=b"", url=url, recovery=None, exact=task.topic, now=time.time()
        )
        await state.websocket.send_bytes(
            serialize_s2b_unsubscribe_exact(
                S2B_UnsubscribeExact(
                    type=SubscriberToBroadcasterStatefulMessageType.UNSUBSCRIBE_EXACT,
                    authorization=authorization,
                    tracing=b"",  # TODO: tracing
                    topic=task.topic,
                ),
                minimal_headers=state.config.websocket_minimal_headers,
            )
        )
        return

    if task.type == ManagementTaskType.UNSUBSCRIBE_GLOB:
        authorization = await state.config.authorize_subscribe_glob(
            tracing=b"", url=url, recovery=None, glob=task.glob, now=time.time()
        )
        await state.websocket.send_bytes(
            serialize_s2b_unsubscribe_glob(
                S2B_UnsubscribeGlob(
                    type=SubscriberToBroadcasterStatefulMessageType.UNSUBSCRIBE_GLOB,
                    authorization=authorization,
                    tracing=b"",  # TODO: tracing
                    glob=task.glob,
                ),
                minimal_headers=state.config.websocket_minimal_headers,
            )
        )
        return

    raise NotImplementedError(f"unknown management task type {task}")
