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

        main_loop_task = asyncio.create_task(server.main_loop())
        canceled_task = asyncio.create_task(cancel_event.wait())
        await asyncio.wait(
            [main_loop_task, canceled_task], return_when=asyncio.FIRST_COMPLETED
        )

        if await cancel_and_check(canceled_task, False) is False:
            # canceled_task was canceled, meaning main loop must have ended!
            assert main_loop_task.done()
        else:
            # canceled_task completed normally, request shutdown in main loop
            server.should_exit = True

        # raise any errors and shutdown
        try:
            await main_loop_task
        finally:
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
