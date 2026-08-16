from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import httpx
import yaml

from .models import Deployment, ModuleContract, RegistrySpec


class RegistryError(RuntimeError):
    pass


class DeploymentRegistry:
    def __init__(self, spec: RegistrySpec, client: httpx.AsyncClient) -> None:
        self.spec = spec
        self.client = client
        self._contracts: dict[str, ModuleContract] = {}

    @classmethod
    def from_path(cls, path: str | Path, client: httpx.AsyncClient) -> DeploymentRegistry:
        source = Path(path)
        text = source.read_text(encoding="utf-8")
        raw = json.loads(text) if source.suffix.lower() == ".json" else yaml.safe_load(text)
        return cls(RegistrySpec.model_validate(raw), client)

    def deployment(self, module_ref: str) -> Deployment:
        try:
            return self.spec.deployments[module_ref]
        except KeyError as exc:
            raise RegistryError(f"module is not deployed: {module_ref}") from exc

    async def contract(self, module_ref: str, *, refresh: bool = False) -> ModuleContract:
        if not refresh and module_ref in self._contracts:
            return self._contracts[module_ref]
        deployment = self.deployment(module_ref)
        headers = _headers(deployment)
        response = await self.client.get(
            f"{deployment.base_url.rstrip('/')}/v1/modules", headers=headers
        )
        response.raise_for_status()
        body = response.json()
        entries: list[dict[str, Any]] = body.get("modules", []) if isinstance(body, dict) else []
        for entry in entries:
            contract = ModuleContract.model_validate(_normalize_contract(entry))
            self._contracts[contract.ref] = contract
        if module_ref not in self._contracts:
            raise RegistryError(f"deployment did not advertise module: {module_ref}")
        return self._contracts[module_ref]


def _headers(deployment: Deployment) -> dict[str, str]:
    if not deployment.token_env:
        return {}
    token = os.environ.get(deployment.token_env)
    if not token:
        raise RegistryError(f"deployment token environment variable is unset: {deployment.token_env}")
    return {"Authorization": f"Bearer {token}"}


def _normalize_contract(entry: dict[str, Any]) -> dict[str, Any]:
    """Accept both the public module catalog and the runner's compact contract form."""
    return {
        "ref": entry.get("ref") or entry.get("module_ref"),
        "input_schema": entry.get("input_schema") or entry.get("input_ports"),
        "output_schema": entry.get("output_schema") or entry.get("output_ports"),
        "schema_version": entry.get("schema_version", "1.0"),
    }
