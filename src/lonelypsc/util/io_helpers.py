import io
import os
from types import TracebackType
from typing import TYPE_CHECKING, Any, Iterator, Optional, Type

from lonelypsc.types.sync_io import (
    SyncStandardIO,
    SyncStandardWithLengthIO,
)


class PositionedSyncStandardIO(io.IOBase):
    """Implements the SyncStandardWithLengthIO interface using the given part of the underlying
    stream. Also looks like an io.IOBase object to satisfy external callers, like aiohttp,
    which check against that rather than duck-typing
    """

    def __init__(self, stream: SyncStandardIO, start_idx: int, end_idx: int):
        assert 0 <= start_idx <= end_idx
        self.stream = stream
        self.start_idx = start_idx
        self.end_idx = end_idx
        self.index = min(stream.tell() - start_idx, end_idx - start_idx)

    def __iter__(self) -> Iterator[bytes]:
        return self

    def __next__(self) -> bytes:
        return self.read(io.DEFAULT_BUFFER_SIZE)

    def __enter__(self) -> "PositionedSyncStandardIO":
        raise NotImplementedError

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        raise NotImplementedError

    def close(self) -> None: ...

    def fileno(self) -> int:
        raise OSError("not implemented")

    def flush(self) -> None:
        raise OSError("not implemented")

    def isatty(self) -> bool:
        raise OSError("not implemented")

    def readable(self) -> bool:
        return True

    def readlines(self, hint: int = -1, /) -> list[bytes]:
        raise OSError("not implemented")

    def seekable(self) -> bool:
        return True

    def truncate(self, size: Optional[int] = None, /) -> int:
        raise OSError("not implemented")

    def writable(self) -> bool:
        return False

    def write(self, b: bytes) -> int:
        raise OSError("not implemented")

    def writelines(self, lines: Any, /) -> None:
        raise OSError("not implemented")

    def readline(self, size: Optional[int] = -1, /) -> bytes:
        raise OSError("not implemented")

    def __del__(self) -> None: ...

    @property
    def closed(self) -> bool:
        raise OSError("not implemented")

    def read(self, n: int) -> bytes:
        if n < 0:
            n = len(self) - self.index

        amount_to_read = min(len(self) - self.index, n)
        if amount_to_read > 0:
            actually_read = self.stream.read(amount_to_read)
            assert len(actually_read) <= amount_to_read
            self.index += len(actually_read)
            return actually_read
        return b""

    def tell(self) -> int:
        return self.index

    def seek(self, offset: int, whence: int = os.SEEK_SET) -> int:
        if whence == os.SEEK_SET:
            assert offset >= 0, "offset must be non-negative for SEEK_SET"
            target_notional_index = min(offset, len(self))
            target_real_index = target_notional_index + self.start_idx

            self.stream.seek(target_real_index, os.SEEK_SET)
            self.index = target_notional_index
            return self.index
        elif whence == os.SEEK_CUR:
            return self.seek(max(0, self.index + offset), os.SEEK_SET)
        elif whence == os.SEEK_END:
            return self.seek(max(0, len(self) + offset), os.SEEK_SET)
        raise OSError(f"unsupported whence: {whence}")

    def __len__(self) -> int:
        return self.end_idx - self.start_idx


if TYPE_CHECKING:
    _: Type[SyncStandardWithLengthIO] = PositionedSyncStandardIO


class PrefixedSyncStandardIO(io.IOBase):
    """Implements the SyncStandardWithLengthIO interface by acting like the entire prefix + entire child
    Also looks like an io.IOBase object to satisfy external callers, like aiohttp,
    which check against that rather than duck-typing
    """

    def __init__(
        self,
        prefix: SyncStandardWithLengthIO,
        child: SyncStandardWithLengthIO,
    ):
        self.prefix = prefix
        self.child = child
        self.index = 0
        self.prefix.seek(0)
        if len(self.prefix) == 0:
            self.child.seek(0)

    def __iter__(self) -> Iterator[bytes]:
        return self

    def __next__(self) -> bytes:
        return self.read(io.DEFAULT_BUFFER_SIZE)

    def __enter__(self) -> "PrefixedSyncStandardIO":
        raise NotImplementedError

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        raise NotImplementedError

    def close(self) -> None: ...

    def fileno(self) -> int:
        raise OSError("not implemented")

    def flush(self) -> None:
        raise OSError("not implemented")

    def isatty(self) -> bool:
        raise OSError("not implemented")

    def readable(self) -> bool:
        return True

    def readlines(self, hint: int = -1, /) -> list[bytes]:
        raise OSError("not implemented")

    def seekable(self) -> bool:
        return True

    def truncate(self, size: Optional[int] = None, /) -> int:
        raise OSError("not implemented")

    def writable(self) -> bool:
        return False

    def write(self, b: bytes) -> int:
        raise OSError("not implemented")

    def writelines(self, lines: Any, /) -> None:
        raise OSError("not implemented")

    def readline(self, size: Optional[int] = -1, /) -> bytes:
        raise OSError("not implemented")

    def __del__(self) -> None: ...

    @property
    def closed(self) -> bool:
        raise OSError("not implemented")

    def read(self, n: int) -> bytes:
        if n < 0:
            n = len(self) - self.index

        result = b""

        amount_to_read_from_prefix = min(len(self.prefix) - self.index, n)
        if amount_to_read_from_prefix > 0:
            actually_read = self.prefix.read(amount_to_read_from_prefix)
            assert len(actually_read) <= amount_to_read_from_prefix
            self.index += len(actually_read)
            n -= len(actually_read)
            if len(actually_read) < amount_to_read_from_prefix:
                return actually_read
            result = actually_read

        amount_to_read = min(len(self) - self.index, n)
        if amount_to_read > 0:
            actually_read = self.child.read(amount_to_read)
            assert len(actually_read) <= amount_to_read
            self.index += len(actually_read)
            result += actually_read

        return result

    def tell(self) -> int:
        return self.index

    def seek(self, offset: int, whence: int = os.SEEK_SET) -> int:
        if whence == os.SEEK_SET:
            assert offset >= 0, "offset must be non-negative for SEEK_SET"
            if offset < len(self.prefix):
                self.prefix.seek(offset)
                self.index = offset
                return offset

            if offset >= len(self):
                self.index = len(self)
                return self.index

            offset_in_child = offset - len(self.prefix)
            self.child.seek(offset_in_child)
            self.index = offset
            return self.index
        elif whence == os.SEEK_CUR:
            return self.seek(self.index + offset, os.SEEK_SET)
        elif whence == os.SEEK_END:
            return self.seek(len(self) + offset, os.SEEK_SET)
        raise OSError(f"unsupported whence: {whence}")

    def __len__(self) -> int:
        return len(self.prefix) + len(self.child)


if TYPE_CHECKING:
    __: Type[SyncStandardWithLengthIO] = PrefixedSyncStandardIO
