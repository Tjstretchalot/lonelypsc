from typing import TYPE_CHECKING, Type
from httppubsubclient.types.sync_readable_bytes_io import (
    SyncStandardWithLengthIO,
    SyncStandardIO,
)
import os


class PositionedSyncStandardIO:
    """Implements the SyncStandardWithLengthIO interface using the given part of the underlying
    stream
    """

    def __init__(self, stream: SyncStandardIO, start_idx: int, end_idx: int):
        assert 0 <= start_idx <= end_idx
        self.stream = stream
        self.start_idx = start_idx
        self.end_idx = end_idx

    def read(self, n: int) -> bytes:
        return self.stream.read(n)

    def tell(self) -> int:
        return self.stream.tell() - self.start_idx

    def seek(self, offset: int, whence: int = os.SEEK_SET) -> int:
        if whence == os.SEEK_SET:
            return self.stream.seek(offset + self.start_idx, whence)
        elif whence == os.SEEK_END:
            return self.stream.seek(offset + self.end_idx, whence)
        return self.stream.seek(offset, whence)

    def __len__(self) -> int:
        return self.end_idx - self.start_idx


if TYPE_CHECKING:
    _: Type[SyncStandardWithLengthIO] = PositionedSyncStandardIO


class PrefixedSyncStandardIO:
    """Implements the SyncStandardWithLengthIO interface by acting like the entire prefix + entire child"""

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

    def read(self, n: int) -> bytes:
        if self.index < len(self.prefix):
            remaining = len(self.prefix) - self.index
            if n <= remaining:
                return self.prefix.read(n)
            self.prefix.read(remaining)
            n -= remaining
            self.index += remaining
            self.child.seek(0)

        remaining = n - len(self.child)
        if remaining > 0:
            self.index += remaining
            return self.child.read(n - remaining)
        return b""

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
