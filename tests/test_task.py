import asyncio

import pytest

from lonelypsc.util.task import create_task


async def _never() -> None:
    await asyncio.Event().wait()


async def _test_task_handle_wait_is_the_only_terminal_operation() -> None:
    task = create_task(asyncio.sleep(0, result=42))

    assert await task.wait() == 42
    assert task.cached_result == 42
    assert task.cached_result == 42
    assert task.consumed
    with pytest.raises(RuntimeError, match="terminal operation repeated"):
        task.result()


def test_task_handle_wait_is_the_only_terminal_operation() -> None:
    asyncio.run(_test_task_handle_wait_is_the_only_terminal_operation())


async def _test_task_handle_cancel_and_check_is_the_only_terminal_operation() -> None:
    task = create_task(_never())

    assert await task.cancel_and_check("default") == "default"
    assert task.consumed
    with pytest.raises(RuntimeError, match="terminal operation repeated"):
        await task.cancel_and_check()


def test_task_handle_cancel_and_check_is_the_only_terminal_operation() -> None:
    asyncio.run(_test_task_handle_cancel_and_check_is_the_only_terminal_operation())


async def _test_task_handle_context_manager_cleans_up_pending_task() -> None:
    task = create_task(_never())

    async with task:
        assert not task.done()

    assert task.done()
    assert task.consumed


def test_task_handle_context_manager_cleans_up_pending_task() -> None:
    asyncio.run(_test_task_handle_context_manager_cleans_up_pending_task())


async def _test_task_handle_context_manager_does_not_cancel_consumed_task() -> None:
    task = create_task(asyncio.sleep(0, result=None))

    async with task:
        await asyncio.sleep(0)
        await task.wait()

    assert task.consumed


def test_task_handle_context_manager_does_not_cancel_consumed_task() -> None:
    asyncio.run(_test_task_handle_context_manager_does_not_cancel_consumed_task())
