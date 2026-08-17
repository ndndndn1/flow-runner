from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from . import __version__
from .config import Settings
from .models import RunRequest
from .runner import LineageBlocked
from .services import Services, build_services


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = Settings.from_env()
    services = await build_services(settings)
    app.state.services = services
    yield
    await services.close()


app = FastAPI(title="flow-runner", version=__version__, lifespan=lifespan)


@app.middleware("http")
async def reject_oversized_body(request: Request, call_next):
    limit = int(os.environ.get("FLOW_MAX_PAYLOAD_BYTES", str(10 * 1024 * 1024)))
    content_length = request.headers.get("content-length")
    if content_length and int(content_length) > limit:
        return JSONResponse(status_code=413, content={"detail": "request body exceeds limit"})
    return await call_next(request)


def _services(request: Request) -> Services:
    return request.app.state.services


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok", "service": "flow-runner"}


@app.post("/v1/runs")
async def create_run(body: RunRequest, request: Request) -> dict[str, Any]:
    try:
        return await _services(request).runner.run(body.workflow, body.input)
    except LineageBlocked as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@app.get("/v1/runs/{run_id}")
async def get_run(run_id: str, request: Request) -> dict[str, Any]:
    value = await _services(request).store.get_run(run_id)
    if value is None:
        raise HTTPException(status_code=404, detail="run not found")
    return value
