import asyncio
from enum import Enum, auto
from typing import TYPE_CHECKING, Dict, List, Literal, Optional, Protocol, Union

from lonelypsp.compat import fast_dataclass

from lonelypsc.util.errors import combine_multiple_exceptions
from lonelypsc.ws.check_result import CheckResult

try:
    import zstandard
except ImportError:
    ...


class CompressorState(Enum):
    """Describes the state that a zstandard compressor is in"""

    PREPARING = auto()
    """We are in the process of preparing the compressor for use"""

    READY = auto()
    """The compressor is ready to use right now; the individual zstandard compressors/
    decompressors are not async or thread safe, but the pool of them is async safe
    """


@fast_dataclass
class CompressorReady:
    """A compressor which is ready to use"""

    type: Literal[CompressorState.READY]
    """discriminator value"""

    identifier: int
    """the integer identifier for this compressor; positive integers only."""

    level: int
    """the compression level the compressor is set to"""

    min_size: int
    """the minimum size, in bytes, inclusive, that a message can be for the subscriber
    to choose this compressor for the message. 0 means no minimum size. Note that the
    broadcaster may use this compressor for messages smaller than this size and the
    subscriber will still decompress it.
    """

    max_size: Optional[int]
    """the maximum size, if any, in bytes, exclusive, that a message can be for the subscriber
    to choose this compressor for the message. None means no maximum size. Note that the
    broadcaster may use this compressor for messages larger than this size and the
    subscriber will still decompress it.
    """

    data: "Optional[zstandard.ZstdCompressionDict]"
    """if there is a custom compression dictionary, that dictionary, otherwise None"""

    compressors: "List[zstandard.ZstdCompressor]"
    """the zstandard compressor objects that are not in use. pulled LIFO as it is
    preferable to reuse the same object as much as possible.
    
    WARN: individual compressors are not asyncio safe
    """

    decompressors: "List[zstandard.ZstdDecompressor]"
    """the zstandard decompressors that are not in use. pulled LIFO as it is
    preferable to reuse the same object as much as possible.
    
    WARN: decompressors are not asyncio-safe
    """


@fast_dataclass
class CompressorPreparing:
    """A compressor that isn't ready to use yet"""

    type: Literal[CompressorState.PREPARING]
    """discriminator value"""

    identifier: int
    """the integer identifier for this compressor; positive integers only.

    1 is reserved for the compressor with no special compression dictionary,
    i.e., data is compressed by building a compression dictionary for each
    message. this works best on moderate sized messages (e.g., at least 16kb)
    as otherwise the overhead of the dictionary size may outweigh the benefits
    of using it

    2-65535 are reserved for preset compression dictionaries, where the compression
    dictionary is distributed out of band

    65536 and above are custom dictionaries built for the connection
    """

    task: asyncio.Task[CompressorReady]
    """The task that is working on preparing the compressor for use"""


Compressor = Union[CompressorReady, CompressorPreparing]


class CompressorStore(Protocol):
    """Describes something that can store compressors and retrieve them
    either by identifier or find the appropriate one for a given message
    length. Also needs to be able to sweep preparing compressors and check
    if any are ready
    """

    def add_compressor(self, compressor: Compressor) -> None:
        """Add a compressor to the store.

        Raises ValueError if the identifier is already in the store
        """

    def remove_compressor(self, identifier: int) -> None:
        """Remove a compressor from the store.

        Raises KeyError if the identifier is not in the store
        """

    def get_for_compression(self, length: int) -> Optional[Compressor]:
        """Get the compressor to use for a message of the given length, or
        None to not use compression
        """

    def get_for_decompression(self, identifier: int) -> Compressor:
        """Get the compressor to use to decompress a message compressed
        with the given identifier

        Raises KeyError if the identifier is not in the store
        """

    def get_compressor_tasks(self) -> List[asyncio.Task[CompressorReady]]:
        """Get the tasks that are preparing compressors, or an empty list
        if there are none.
        """

    def check_compressor_tasks(self) -> CheckResult:
        """Checks all tasks to see if any are done. For finished tasks,
        replace the compressor internally with the ready compressor.

        Raises any errors that occurred in the tasks

        Returns `CheckResult.RESTART` if any tasks were done, otherwise
        `CheckResult.CONTINUE`
        """


class CompressorStoreImpl:
    """Satisfies CompressorStore using separate lists; intended for not that many
    compressors and using them is much more common than mutating them

    This is the only expected implementation of CompressorStore right now, and
    normally it's unhelpful to make a protocol if there is only one
    implementation and its not easy for the caller to swap it out, but in this
    case it feels better than exposing the data structure chosen here in the
    type hints (even if underscored), since it might be an important part of the
    hot path (i.e., might need to change it later for performance reasons) while
    at the same time should not be particularly usecase dependent (so letting
    callers swap the implementation just bloats the API)
    """

    def __init__(self) -> None:
        self.preparing: List[CompressorPreparing] = []
        """Compressors that are not ready to use yet, in insertion order"""

        self.ready: List[CompressorReady] = []
        """Compressors that are ready to use, sorted by ascending minimum size"""

        self.by_compressor_id: Dict[int, Compressor] = {}
        """Lookup table for compressors by identifier"""

    def add_compressor(self, compressor: Compressor) -> None:
        if compressor.identifier in self.by_compressor_id:
            raise ValueError(
                f"compressor with identifier {compressor.identifier} already exists"
            )

        if compressor.type == CompressorState.PREPARING:
            self.preparing.append(compressor)
        else:
            self._insert_ready(compressor)

        self.by_compressor_id[compressor.identifier] = compressor

    def remove_compressor(self, identifier: int) -> None:
        compressor = self.by_compressor_id.pop(identifier)
        if compressor.type == CompressorState.PREPARING:
            self.preparing.remove(compressor)
        else:
            self.ready.remove(compressor)

    def get_for_compression(self, length: int) -> Optional[Compressor]:
        for compressor in self.ready:
            if compressor.min_size <= length < (compressor.max_size or float("+inf")):
                return compressor

        return None

    def get_for_decompression(self, identifier: int) -> Compressor:
        return self.by_compressor_id[identifier]

    def get_compressor_tasks(self) -> List[asyncio.Task[CompressorReady]]:
        return [compressor.task for compressor in self.preparing]

    def check_compressor_tasks(self) -> CheckResult:
        if not self.preparing:
            return CheckResult.CONTINUE

        new_preparing: List[CompressorPreparing] = []
        excs: List[BaseException] = []
        for compressor in self.preparing:
            if not compressor.task.done():
                new_preparing.append(compressor)
                continue

            try:
                ready = compressor.task.result()
                if excs:
                    continue
                self._insert_ready(ready)
                self.by_compressor_id[ready.identifier] = ready
            except BaseException as e:
                excs.append(e)

        if excs:
            raise combine_multiple_exceptions("failed to prepare compressors", excs)

        if len(new_preparing) == len(self.preparing):
            return CheckResult.CONTINUE

        self.preparing = new_preparing
        return CheckResult.RESTART

    def _insert_ready(self, compressor: CompressorReady) -> None:
        for i, existing in enumerate(self.ready):
            if existing.min_size > compressor.min_size:
                self.ready.insert(i, compressor)
                break
        else:
            self.ready.append(compressor)


if TYPE_CHECKING:
    _: CompressorStore = CompressorStoreImpl()
