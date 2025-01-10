import asyncio
import warnings
from typing import (
    Any,
    Generic,
    Iterable,
    List,
    Set,
    TypeVar,
    cast,
)

import uvicorn
from fastapi import APIRouter, FastAPI
from lonelypsp.util.cancel_and_check import cancel_and_check

from lonelypsc.config.http_config import (
    HttpPubSubBindManualConfig,
    HttpPubSubBindUvicornConfig,
)

T = TypeVar("T")


class _EmptyEventSet(Generic[T]):
    """Looks like a set(), but when it empties it sets the event"""

    def __init__(self, raw: Set[T]) -> None:
        self.raw = raw
        self.event = asyncio.Event()

        if not raw:
            self.event.set()

    # in order from https://docs.python.org/3/library/stdtypes.html#set
    def __len__(self) -> int:
        return len(self.raw)

    def __contains__(self, item: T) -> bool:
        return item in self.raw

    def isdisjoint(self, other: Iterable[T]) -> bool:
        return self.raw.isdisjoint(other)

    def issubset(self, other: Iterable[T]) -> bool:
        return self.raw.issubset(other)

    # order
    def __eq__(self, other: Any) -> bool:
        return self.raw == other

    def __ne__(self, other: Any) -> bool:
        return self.raw != other

    def __lt__(self, other: Set[T]) -> bool:
        return self.raw < other

    def __le__(self, other: Set[T]) -> bool:
        return self.raw <= other

    def __gt__(self, other: Set[T]) -> bool:
        return self.raw > other

    def __ge__(self, other: Set[T]) -> bool:
        return self.raw >= other

    def union(self, *others: Iterable[T]) -> Set[T]:
        return self.raw.union(*others)

    def __or__(self, other: Set[T]) -> Set[T]:
        return self.raw | other

    def intersection(self, *others: Iterable[T]) -> Set[T]:
        return self.raw.intersection(*others)

    def __and__(self, other: Set[T]) -> Set[T]:
        return self.raw & other

    def difference(self, *others: Iterable[T]) -> Set[T]:
        return self.raw.difference(*others)

    def __sub__(self, other: Set[T]) -> Set[T]:
        return self.raw - other

    def symmetric_difference(self, other: Iterable[T]) -> Set[T]:
        return self.raw.symmetric_difference(other)

    def __xor__(self, other: Set[T]) -> Set[T]:
        return self.raw ^ other

    def copy(self) -> Set[T]:
        return self.raw.copy()

    def update(self, *others: Iterable[T]) -> None:
        self.raw.update(*others)
        if not self.raw:
            self.event.set()

    def __ior__(self, other: Set[T]) -> None:
        self.raw |= other
        if not self.raw:
            self.event.set()

    def intersection_update(self, *others: Iterable[T]) -> None:
        self.raw.intersection_update(*others)
        if not self.raw:
            self.event.set()

    def __iand__(self, other: Set[T]) -> None:
        self.raw &= other
        if not self.raw:
            self.event.set()

    def difference_update(self, *others: Iterable[T]) -> None:
        self.raw.difference_update(*others)
        if not self.raw:
            self.event.set()

    def __isub__(self, other: Set[T]) -> None:
        self.raw -= other
        if not self.raw:
            self.event.set()

    def symmetric_difference_update(self, other: Iterable[T]) -> None:
        self.raw.symmetric_difference_update(other)
        if not self.raw:
            self.event.set()

    def __ixor__(self, other: Set[T]) -> None:
        self.raw ^= other
        if not self.raw:
            self.event.set()

    def add(self, elem: T) -> None:
        self.raw.add(elem)

    def remove(self, elem: T) -> None:
        self.raw.remove(elem)
        if not self.raw:
            self.event.set()

    def discard(self, elem: T) -> None:
        self.raw.discard(elem)
        if not self.raw:
            self.event.set()

    def pop(self) -> T:
        elem = self.raw.pop()
        if not self.raw:
            self.event.set()
        return elem

    def clear(self) -> None:
        self.raw.clear()
        self.event.set()


class BindWithUvicornCallback:
    """Fulfills the HttpPubSubBindManualCallback using uvicorn as the runner"""

    def __init__(self, settings: HttpPubSubBindUvicornConfig):
        self.settings = settings

    async def _shutdown(self, uv_server: uvicorn.Server) -> None:
        """Gracefully shutdown the server without polling; must faster than
        uv_server.shutdown()
        """
        for server in uv_server.servers:
            server.close()

        for connection in list(uv_server.server_state.connections):
            connection.shutdown()

        timeout_task = (
            asyncio.create_task(
                asyncio.sleep(uv_server.config.timeout_graceful_shutdown)
            )
            if uv_server.config.timeout_graceful_shutdown is not None
            else None
        )

        connection_done_tasks: List[asyncio.Task[Any]] = []

        if uv_server.server_state.connections:
            replacer = _EmptyEventSet(uv_server.server_state.connections)
            for conn in replacer.raw:
                conn.connections = cast(Any, replacer)
            uv_server.server_state.connections = cast(Any, replacer)
            connection_done_tasks.append(asyncio.create_task(replacer.event.wait()))

        if connection_done_tasks or uv_server.server_state.tasks:
            graceful_shutdown_complete_task = asyncio.create_task(
                asyncio.wait(
                    [
                        *connection_done_tasks,
                        *uv_server.server_state.tasks,
                    ],
                    return_when=asyncio.ALL_COMPLETED,
                )
            )
            if timeout_task is not None:
                await asyncio.wait(
                    [
                        timeout_task,
                        graceful_shutdown_complete_task,
                    ],
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if await cancel_and_check(timeout_task, True):
                    warnings.warn_explicit(
                        "graceful shutdown timed out", ResourceWarning, "", 0
                    )
                await cancel_and_check(graceful_shutdown_complete_task)
            else:
                await graceful_shutdown_complete_task
                assert not uv_server.server_state.connections
                assert not uv_server.server_state.tasks

            for task in uv_server.server_state.tasks:
                await cancel_and_check(task)

            for task in connection_done_tasks:
                await cancel_and_check(task)

        if not uv_server.force_exit:
            await uv_server.lifespan.shutdown()

    async def _serve(self, server: uvicorn.Server, cancel_event: asyncio.Event) -> None:
        # if canceled before starting, nothing to do
        if cancel_event.is_set():
            return

        # setup lifespan
        config = server.config
        if not config.loaded:
            config.load()

        server.lifespan = config.lifespan_class(config)

        # don't interrupt startup; too likely to leak
        await server.startup()

        # lifespan events aborted
        if server.should_exit:
            return

        # if canceled, go straight to shutdown without starting main loop
        if cancel_event.is_set():
            # don't interrupt shutdown; too likely to leak
            await self._shutdown(server)
            return

        # server.main_loop relies on sleeping but isn't safe to cancel because
        # it might be in on_tick if unlucky; so we reimplement here
        cancel_event_wait_task = asyncio.create_task(cancel_event.wait())

        counter = 0
        while not await server.on_tick(counter):
            counter += 1
            if counter == 864000:
                counter = 0

            sleep_task = asyncio.create_task(asyncio.sleep(0.1))
            await asyncio.wait(
                [sleep_task, cancel_event_wait_task],
                return_when=asyncio.FIRST_COMPLETED,
            )
            if await cancel_and_check(sleep_task, True):
                # since sleep_task wasn't done, must have been canceled
                assert cancel_event_wait_task.done()
                break

        await self._shutdown(server)

    async def __call__(self, router: APIRouter) -> None:
        app = FastAPI()
        app.include_router(router)
        app.router.redirect_slashes = False
        uv_config = uvicorn.Config(
            app,
            host=self.settings["host"],
            port=self.settings["port"],
            lifespan="off",
            # prevents spurious cancellation errors
            log_level="warning",
            # reduce default logging since this isn't the main deal for the process
        )
        uv_server = uvicorn.Server(uv_config)
        cancel_event = asyncio.Event()
        serve_task = asyncio.create_task(self._serve(uv_server, cancel_event))

        try:
            await asyncio.shield(serve_task)
        finally:
            cancel_event.set()
            await serve_task


async def handle_bind_with_uvicorn(
    settings: HttpPubSubBindUvicornConfig,
) -> HttpPubSubBindManualConfig:
    """Converts the bind with uvicorn settings into the generic manual config"""
    return {
        "type": "manual",
        "callback": BindWithUvicornCallback(settings),
    }
