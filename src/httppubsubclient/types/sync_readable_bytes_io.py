from typing import Protocol, Union


class SyncReadableBytesIOA(Protocol):
    """A type that represents a stream that can be read synchronously"""

    def read(self, n: int) -> bytes:
        """Reads n bytes from the file-like object"""
        raise NotImplementedError()


class SyncReadableBytesIOB(Protocol):
    """A type that represents a stream that can be read synchronously"""

    def read(self, n: int, /) -> bytes:
        """Reads n bytes from the file-like object"""
        raise NotImplementedError()


SyncReadableBytesIO = Union[SyncReadableBytesIOA, SyncReadableBytesIOB]
