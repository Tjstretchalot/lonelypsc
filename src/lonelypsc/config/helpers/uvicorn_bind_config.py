import asyncio

import uvicorn
from fastapi import APIRouter, FastAPI

from lonelypsc.config.http_config import (
    HttpPubSubBindManualConfig,
    HttpPubSubBindUvicornConfig,
)


class BindWithUvicornCallback:
    """Fulfills the HttpPubSubBindManualCallback using uvicorn as the runner"""

    def __init__(self, settings: HttpPubSubBindUvicornConfig):
        self.settings = settings

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
        serve_task = asyncio.create_task(uv_server.serve())

        try:
            await asyncio.shield(serve_task)
        finally:
            uv_server.should_exit = True
            await serve_task


async def handle_bind_with_uvicorn(
    settings: HttpPubSubBindUvicornConfig,
) -> HttpPubSubBindManualConfig:
    """Converts the bind with uvicorn settings into the generic manual config"""
    return {
        "type": "manual",
        "callback": BindWithUvicornCallback(settings),
    }
