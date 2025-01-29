import asyncio
import random
from enum import Enum, auto
from typing import Generic, Literal, Optional, Protocol, TypeVar, Union, cast

from lonelypsp.compat import assert_never, fast_dataclass

from lonelypsc.config.config import BroadcastersShuffler, PubSubBroadcasterConfig
from lonelypsc.config.http_config import HttpPubSubConfig

SuccessT = TypeVar("SuccessT")
FailureT = TypeVar("FailureT")
InitializerT = TypeVar("InitializerT")


class BroadcasterRetryableActionResult(Enum):
    SUCCESS = auto()
    """don't retry: have a SuccessT"""

    FAILURE = auto()
    """don't retry: have a FailureT"""

    RETRY = auto()
    """prefer to retry but otherwise have a FailureT"""


@fast_dataclass
class BroadcasterRetryableActionResultSuccess(Generic[SuccessT]):
    type: Literal[BroadcasterRetryableActionResult.SUCCESS]
    """discriminator value"""

    result: SuccessT
    """the result of the action"""


@fast_dataclass
class BroadcasterRetryableActionResultFailure(Generic[FailureT]):
    type: Literal[BroadcasterRetryableActionResult.FAILURE]
    """discriminator value"""

    result: FailureT
    """the result of the action"""


@fast_dataclass
class BroadcasterRetryableActionResultRetry(Generic[SuccessT, FailureT]):
    type: Literal[BroadcasterRetryableActionResult.RETRY]
    """discriminator value"""

    followup: "BroadcasterRetryableActionRetryFollowup[SuccessT, FailureT]"
    """the callback once the next course of action is determined"""


class BroadcasterRetryableAction(Generic[SuccessT, FailureT], Protocol):
    """Describes some action that performed against a broadcaster
    by the http connector
    """

    async def attempt(self, /, *, broadcaster: PubSubBroadcasterConfig) -> Union[
        BroadcasterRetryableActionResultSuccess[SuccessT],
        BroadcasterRetryableActionResultFailure[FailureT],
        BroadcasterRetryableActionResultRetry[SuccessT, FailureT],
    ]:
        """Attempts the given broadcaster. Only called once per logical
        instance (ie., if it is called again, it means this returned retry
        and the retry function returned this instance again)
        """


class BroadcasterRetryableActionRetryFollowup(Generic[SuccessT, FailureT], Protocol):
    async def on_retries_exhausted(
        self, /
    ) -> BroadcasterRetryableActionResultFailure[FailureT]: ...

    async def on_waiting_to_retry(
        self, /
    ) -> "BroadcasterRetryableActionRetryFollowup[SuccessT, FailureT]": ...

    async def on_retrying(
        self, /
    ) -> "BroadcasterRetryableAction[SuccessT, FailureT]": ...


async def attempt_broadcasters(
    *,
    config: HttpPubSubConfig[InitializerT],
    shuffler: BroadcastersShuffler,
    action: BroadcasterRetryableAction[SuccessT, FailureT],
) -> Union[
    BroadcasterRetryableActionResultSuccess[SuccessT],
    BroadcasterRetryableActionResultFailure[FailureT],
]:
    """Attempts broadcasters one at a time until all retries are exhausted
    or a success is found, with callbacks for all the relevant points so that
    tracing data can be collected.
    """

    next_action = cast(Optional[BroadcasterRetryableAction[SuccessT, FailureT]], action)
    next_retry = cast(
        Optional[BroadcasterRetryableActionRetryFollowup[SuccessT, FailureT]], None
    )
    del action

    for iteration in range(config.outgoing_retries_per_broadcaster):
        if iteration > 0:
            assert next_retry is not None
            next_retry = await next_retry.on_waiting_to_retry()
            await asyncio.sleep(2 ** (iteration - 1) + random.random())

        for broadcaster in shuffler:
            if next_retry is not None:
                next_action = await next_retry.on_retrying()
                next_retry = None

            assert next_action is not None
            result = await next_action.attempt(broadcaster=broadcaster)
            if result.type == BroadcasterRetryableActionResult.SUCCESS:
                return result
            if result.type == BroadcasterRetryableActionResult.FAILURE:
                return result
            if result.type != BroadcasterRetryableActionResult.RETRY:
                assert_never(result)

            next_retry = result.followup
            next_action = None

    if next_action is None:
        raise Exception("no broadcasters to try!")

    assert next_retry is not None
    return await next_retry.on_retries_exhausted()
