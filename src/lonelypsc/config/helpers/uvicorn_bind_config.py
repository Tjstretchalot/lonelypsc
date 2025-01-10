import asyncio

import uvicorn
from fastapi import APIRouter, FastAPI
from lonelypsp.util.cancel_and_check import cancel_and_check

from lonelypsc.config.http_config import (
    HttpPubSubBindManualConfig,
    HttpPubSubBindUvicornConfig,
)


class BindWithUvicornCallback:
    """Fulfills the HttpPubSubBindManualCallback using uvicorn as the runner"""

    def __init__(self, settings: HttpPubSubBindUvicornConfig):
        self.settings = settings

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
            await server.shutdown()
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

        await server.shutdown()

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
