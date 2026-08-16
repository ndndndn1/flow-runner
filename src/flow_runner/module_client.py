from __future__ import annotations

import asyncio
from typing import Any

import httpx

from .models import Deployment
from .registry import _headers


class ModuleCallError(RuntimeError):
    pass


async def call_module(
    client: httpx.AsyncClient,
    deployment: Deployment,
    module_ref: str,
    run_id: str,
    value: Any,
    *,
    retries: int = 2,
) -> Any:
    endpoint = f"{deployment.base_url.rstrip('/')}/v1/run"
    envelope = {"schema_version": "1.0.0", "run_id": run_id, "module": module_ref.split("/")[-1].split("@")[0], "input": value}
    last_error: Exception | None = None
    for attempt in range(retries + 1):
        try:
            response = await client.post(endpoint, headers=_headers(deployment), json=envelope)
            if response.status_code >= 500:
                response.raise_for_status()
            if response.status_code >= 400:
                raise ModuleCallError(
                    f"module {module_ref} returned HTTP {response.status_code}"
                )
            body = response.json()
            if not isinstance(body, dict) or "output" not in body:
                raise ModuleCallError(f"module {module_ref} response has no output")
            return body["output"]
        except ModuleCallError:
            raise
        except (httpx.TransportError, httpx.HTTPStatusError) as exc:
            last_error = exc
            if attempt == retries:
                break
            await asyncio.sleep(0.05 * (2**attempt))
    raise ModuleCallError(f"module {module_ref} failed after {retries + 1} attempts: {last_error}")
