import random
from typing import (
    TYPE_CHECKING,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Type,
    TypedDict,
    Union,
    cast,
)


class SimplePubSubBroadcasterConfig(TypedDict):
    """Indicates how to connect to a broadcaster. We leave this type open for
    expansion in the future (compared to a string) in case we want to add e.g.
    optional priority levels
    """

    host: str
    """The host of the broadcaster, e.g., `http://localhost:3003`. Must include
    the schema and port (if not default) and may include a path (if prefixing the
    standard paths), such that f'{host}/v1/subscribe/exact' is the full path to 
    subscribe exact endpoint.

    For websocket clients, the schema must be a ws or wss. For http clients,
    the schema must be http or https.
    """


class PrioritizedPubSubBroadcasterConfig(SimplePubSubBroadcasterConfig):
    """Indicates how to connect to a broadcaster with a priority level. This is
    primarily recommended for websocket connections
    """

    priority: int
    """The priority level of the broadcaster. Lower values are higher priority.
    Ties are broken randomly. All specific priority values are considered higher
    priority (will be used first) than an unset priority value.

    Priority is most important when using websockets as a bad choice of host has
    much longer lasting effects compared to http, and websockets are generally
    used where there is more volume and thus the latency is more important
    """


PubSubBroadcasterConfig = Union[
    SimplePubSubBroadcasterConfig, PrioritizedPubSubBroadcasterConfig
]


class _BroadcasterShufflerIterator:
    """The iterator produced by BroadcastersShuffler.

    There are two design goals:
    1. Correctness; this should be a fair shuffling of the broadcasters,
       and should respect priority
    2. Efficiency in happy path; this should have essentially no overhead when
       it's used with only a single next() call then discarded, as is expected
       most of the time
    3. Reasonable efficiency in unhappy path; this should be able to handle
       iterating through the entire list in no worse than O(n (log n)^2) time
    """

    def __init__(self, parent: "BroadcastersShuffler") -> None:
        self.parent = parent
        self._level_start: int = 0
        self._level_end: Optional[int] = None
        self._level_first_used_idx: Optional[int] = None
        self._level_remaining_idxs: Optional[List[int]] = None

    def __next__(self) -> PubSubBroadcasterConfig:
        if self._level_start >= len(self.parent.sorted_broadcasters):
            raise StopIteration

        if self._level_end is None:
            # typically there will be either be very few options (e.g., 1) at
            # this level (when using priority to prefer connecting to the nearest
            # instance) or the entire remaining list will be at this level
            #
            # example 1: using multiple availability zones within the same region
            # [{"priority": 0, "host": "same az"}, {"host": "other az"}, {"host": "other az"}, ...]
            #
            # example 2: using multiple availablity zones and multiple regions
            # [{"priority": 0, "host": "same region, same az"}, {"priority": 1, "host": "same region, other az"}, {"host": "other region"}, {"host": "other region"}, ...]

            # over all next() calls this never repeats the same index, so requires O(n)
            # time
            level = self.parent.sorted_broadcasters[self._level_start].get("priority")
            if self.parent.sorted_broadcasters[-1].get("priority") == level:
                self._level_end = len(self.parent.sorted_broadcasters)
            else:
                self._level_end = self._level_start + 1
                while (
                    self._level_end < len(self.parent.sorted_broadcasters)
                    and self.parent.sorted_broadcasters[self._level_end].get("priority")
                    == level
                ):
                    self._level_end += 1

        if self._level_first_used_idx is None:
            # most common case: first pluck from this level is very easy
            # if we don't end up iterating through the rest, we can avoid
            # building the remaining list
            # happens at most once per level, which is at most once per index.
            # random.choice is constant time in this case, so requires at most
            # O(n) time
            choice = random.choice(range(self._level_start, self._level_end))

            if self._level_start == self._level_end - 1:
                self._level_start += 1
                self._level_end = None
            else:
                self._level_first_used_idx = choice

            return self.parent.sorted_broadcasters[choice]

        if self._level_remaining_idxs is None:
            # second pluck from this level
            # this never repeats the same index and is equally performant
            # regardless of the number of levels as building the indexes +
            # random.shuffle takes O(n) time
            # this takes O(n) time
            self._level_remaining_idxs = list(
                i
                for i in range(self._level_start, self._level_end)
                if i != self._level_first_used_idx
            )
            random.shuffle(self._level_remaining_idxs)

        # constant time pop up to n-1 times; requires O(n) time
        choice = self._level_remaining_idxs.pop()
        if not self._level_remaining_idxs:
            self._level_start = self._level_end
            self._level_end = None
            self._level_first_used_idx = None
            self._level_remaining_idxs = None

        return self.parent.sorted_broadcasters[choice]

    def __iter__(self) -> "_BroadcasterShufflerIterator":
        return self


class BroadcastersShuffler:
    """An object that can precompute whatever internal data structure is
    appropriate such that it can quickly build new iterators for the broadcasters
    that match the desired semantics

    Usage:

    ```python
    from lonelypsc.config.config import BroadcastersShuffler

    shuffler = BroadcastersShuffler(
        [
            {"host": "http://localhost:3003"},
            {"host": "http://localhost:3004"},
            {"host": "http://localhost:3005", "priority": 0},
        ]
    )
    for broadcaster in shuffler:
        print(broadcaster)
    ```
    """

    def __init__(self, broadcasters: List[PubSubBroadcasterConfig]) -> None:
        self.sorted_broadcasters = sorted(
            broadcasters,
            key=lambda x: cast(Dict[str, int], x).get("priority", float("+inf")),
        )

    def __iter__(self) -> _BroadcasterShufflerIterator:
        return _BroadcasterShufflerIterator(self)


if TYPE_CHECKING:
    _: Type[Iterator[PubSubBroadcasterConfig]] = _BroadcasterShufflerIterator
    __: Type[Iterable[PubSubBroadcasterConfig]] = BroadcastersShuffler
    ___: PubSubBroadcasterConfig = {"host": "http://localhost:3003"}
    ____: PubSubBroadcasterConfig = {
        "host": "http://localhost:3003",
        "priority": 0,
    }
