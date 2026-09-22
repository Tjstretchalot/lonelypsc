"""Ownership-aware helpers for asyncio tasks.

Every task created by lonelypsc is wrapped in :class:`TaskHandle`.  A handle
has exactly one terminal operation: ``wait``, ``result``, or
``cancel_and_check``.  This makes task ownership explicit and turns accidental
second awaits/cancellations into immediate errors.
"""

import asyncio
from dataclasses import dataclass, field
from typing import Any, Coroutine, Generic, Optional, TypeVar

from lonelypsp.util.cancel_and_check import cancel_and_check as _cancel_and_check

T = TypeVar("T")
D = TypeVar("D")


@dataclass(eq=False)
class TaskHandle(Generic[T]):
    """Own one asyncio task until its single terminal observation."""

    _task: asyncio.Task[T]
    _terminal_operation: Optional[str] = field(default=None, init=False)

    @property
    def task(self) -> asyncio.Task[T]:
        """The underlying task, for APIs such as ``asyncio.wait``.

        Passing the task to ``asyncio.wait`` does not consume it.  The owner
        must still call one of this handle's terminal methods afterwards.
        """
        return self._task

    def done(self) -> bool:
        return self._task.done()

    @property
    def consumed(self) -> bool:
        return self._terminal_operation is not None

    async def __aenter__(self) -> "TaskHandle[T]":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if self._terminal_operation is None:
            await self.cancel_and_check()

    def _claim(self, operation: str) -> None:
        if self._terminal_operation is not None:
            raise RuntimeError(
                f"task terminal operation repeated: "
                f"{self._terminal_operation} then {operation}"
            )
        self._terminal_operation = operation

    def result(self) -> T:
        """Consume a task known to be complete."""
        self._claim("result")
        return self._task.result()

    @property
    def cached_result(self) -> T:
        """Return a completed task's result without claiming another operation.

        ``result()`` is deliberately safe to call repeatedly after completion;
        this property is for the cases where another owner has already
        consumed the handle but a shared data structure still needs the value.
        """
        if not self.consumed or not self._task.done():
            raise RuntimeError("task has not been consumed successfully")
        return self._task.result()

    async def wait(self, *, shield: bool = False) -> T:
        """Await and consume this task exactly once."""
        if shield:
            self._claim("wait")
            try:
                result = await asyncio.shield(self._task)
            except BaseException:
                if not self._task.done():
                    self._terminal_operation = None
                raise
            return result

        self._claim("wait")
        return await self._task

    async def cancel_and_check(self, default: Optional[D] = None) -> Optional[T | D]:
        """Cancel and consume this task exactly once."""
        self._claim("cancel_and_check")
        return await _cancel_and_check(self._task, default)


def create_task(coro: Coroutine[Any, Any, T]) -> TaskHandle[T]:
    """Create an owned task.

    Keeping task creation in one function makes it possible to audit that all
    tasks have an owner and gives tests a single seam for task instrumentation.
    """
    return TaskHandle(asyncio.create_task(coro))
