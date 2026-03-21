from __future__ import annotations

import asyncio
import contextlib
import logging
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from seedr_tg.db.models import JobRecord

LOGGER = logging.getLogger(__name__)

_MAGNET_PATTERN = re.compile(r"^magnet:\?[^\s]+$", re.IGNORECASE)
_WEB_SOURCE_CHAT_ID = 0
_WEB_SOURCE_MESSAGE_ID = 0


class MagnetCreateRequest(BaseModel):
    magnet_link: str = Field(min_length=8)


class MagnetCreateResponse(BaseModel):
    accepted: bool
    duplicate: bool
    job_id: int | None
    phase: str | None
    message: str


class QueueJobResponse(BaseModel):
    id: int
    phase: str
    progress_percent: float
    current_step: str


@dataclass(slots=True)
class WebApiConfig:
    host: str = "127.0.0.1"
    port: int = 8787
    allowed_origins: tuple[str, ...] = (
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    )


class WebApiServer:
    def __init__(
        self,
        *,
        enqueue_callback: Callable[[str, int, int], Awaitable[JobRecord | None]],
        list_jobs_callback: Callable[[], Awaitable[list[JobRecord]]],
        config: WebApiConfig | None = None,
    ) -> None:
        self._enqueue_callback = enqueue_callback
        self._list_jobs_callback = list_jobs_callback
        self._config = config or WebApiConfig()
        self._server: uvicorn.Server | None = None
        self._server_task: asyncio.Task[None] | None = None
        self._app = self._create_app()

    def _create_app(self) -> FastAPI:
        app = FastAPI(title="seedr-tg web api", version="1.0.0")
        app.add_middleware(
            CORSMiddleware,
            allow_origins=list(self._config.allowed_origins),
            allow_credentials=False,
            allow_methods=["GET", "POST", "OPTIONS"],
            allow_headers=["*"],
        )

        @app.get("/api/health")
        async def health() -> dict[str, bool]:
            return {"ok": True}

        @app.get("/api/jobs")
        async def jobs() -> list[QueueJobResponse]:
            queue = await self._list_jobs_callback()
            return [
                QueueJobResponse(
                    id=job.id,
                    phase=job.phase.value,
                    progress_percent=float(job.progress_percent),
                    current_step=job.current_step,
                )
                for job in queue
            ]

        @app.post("/api/magnets", response_model=MagnetCreateResponse)
        async def submit_magnet(payload: MagnetCreateRequest) -> MagnetCreateResponse:
            magnet = payload.magnet_link.strip()
            if not _MAGNET_PATTERN.match(magnet):
                raise HTTPException(status_code=400, detail="Invalid magnet link")
            job = await self._enqueue_callback(
                magnet,
                source_chat_id=_WEB_SOURCE_CHAT_ID,
                source_message_id=_WEB_SOURCE_MESSAGE_ID,
            )
            if job is None:
                return MagnetCreateResponse(
                    accepted=False,
                    duplicate=True,
                    job_id=None,
                    phase=None,
                    message="Duplicate magnet ignored",
                )
            return MagnetCreateResponse(
                accepted=True,
                duplicate=False,
                job_id=job.id,
                phase=job.phase.value,
                message="Magnet queued",
            )

        return app

    async def start(self) -> None:
        if self._server_task is not None:
            return
        config = uvicorn.Config(
            app=self._app,
            host=self._config.host,
            port=self._config.port,
            log_level="warning",
            access_log=False,
        )
        self._server = uvicorn.Server(config)
        self._server_task = asyncio.create_task(self._server.serve())
        LOGGER.info(
            "Web API started at http://%s:%s",
            self._config.host,
            self._config.port,
        )
        await asyncio.sleep(0)

    async def stop(self) -> None:
        if self._server is not None:
            self._server.should_exit = True
        if self._server_task is not None:
            with contextlib.suppress(asyncio.CancelledError):
                await self._server_task
        self._server = None
        self._server_task = None
