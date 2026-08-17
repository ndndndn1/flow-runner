from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime
from typing import Any

import httpx

from .store import RunStore

PRODUCER = "https://github.com/ndndndn1/flow-runner"
OPENLINEAGE_SCHEMA = "https://openlineage.io/spec/2-0-2/OpenLineage.json"
FACET_BASE = "https://github.com/ndndndn1/flow-runner/schemas"
EVENT_NAMESPACE = uuid.UUID("688ee129-0192-46d5-a234-36c9a25cf152")


def _facet(facet_name: str, **values: Any) -> dict[str, Any]:
    return {
        "_producer": PRODUCER,
        "_schemaURL": f"{FACET_BASE}/{facet_name}.json",
        **values,
    }


def dataset(name: str, metadata: dict[str, Any]) -> dict[str, Any]:
    return {
        "namespace": "flow/runtime",
        "name": name,
        "facets": {"flow_data": _facet("flow_data", **metadata)},
    }


def dataset_reference(value: dict[str, Any]) -> str:
    facet = value["facets"]["flow_data"]
    return f"dataset_version:{value['namespace']}/{value['name']}@{facet['hash']}"


def module_facet(module_ref: str) -> dict[str, Any]:
    repository, versioned_name = module_ref.split("/", 1)
    name, version = versioned_name.rsplit("@", 1)
    return _facet(
        "flow_module",
        repository=repository,
        namespace=repository,
        name=name,
        version=version,
        contract=module_ref,
    )


def event_id(run_id: str, step_id: str, event_type: str) -> str:
    return str(uuid.uuid5(EVENT_NAMESPACE, f"{run_id}:{step_id}:{event_type}"))


def build_event(
    event_type: str,
    *,
    run_id: str,
    root_run_id: str,
    workflow_id: str,
    workflow_version: str,
    step_id: str,
    module_ref: str,
    inputs: list[dict[str, Any]],
    outputs: list[dict[str, Any]],
    upstream_steps: list[str] | None = None,
    dependency_steps: list[str] | None = None,
    upstream_modules: list[str] | None = None,
    upstream_datasets: list[str] | None = None,
    sources: list[dict[str, Any]] | None = None,
    bindings: dict[str, str] | None = None,
    error: str | None = None,
) -> dict[str, Any]:
    run_facets: dict[str, Any] = {
        "parent": {
            "_producer": PRODUCER,
            "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/ParentRunFacet.json",
            "run": {"runId": root_run_id},
            "job": {"namespace": "flow/workflows", "name": workflow_id},
        },
        "flow_step": _facet(
            "flow_step", workflowId=workflow_id, stepId=step_id, moduleRef=module_ref
        ),
        "flow_workflow": _facet(
            "flow_workflow",
            namespace="flow/workflows",
            name=workflow_id,
            version=workflow_version,
            workflowId=workflow_id,
            workflowVersion=workflow_version,
        ),
        "flow_transfer": _facet(
            "flow_transfer",
            fromSteps=upstream_steps or [],
            dependencySteps=dependency_steps or [],
            fromModules=upstream_modules or [],
            fromDatasets=upstream_datasets or [],
            sources=sources or [],
            bindings=bindings or {},
            toStep=step_id,
            toModule=module_ref,
        ),
    }
    if error:
        run_facets["flow_error"] = _facet("flow_error", message=error[:1000])
    return {
        "eventType": event_type,
        "eventTime": datetime.now(UTC).isoformat(),
        "run": {"runId": run_id, "facets": run_facets},
        "job": {
            "namespace": "flow/modules",
            "name": module_ref,
            "facets": {"flow_module": module_facet(module_ref)},
        },
        "inputs": inputs,
        "outputs": outputs,
        "producer": PRODUCER,
        "schemaURL": OPENLINEAGE_SCHEMA,
    }


def build_workflow_event(
    event_type: str,
    *,
    run_id: str,
    workflow_id: str,
    workflow_version: str,
    inputs: list[dict[str, Any]] | None = None,
    outputs: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        "eventType": event_type,
        "eventTime": datetime.now(UTC).isoformat(),
        "run": {
            "runId": run_id,
            "facets": {
                "flow_workflow": _facet(
                    "flow_workflow",
                    namespace="flow/workflows",
                    name=workflow_id,
                    version=workflow_version,
                    workflowId=workflow_id,
                    workflowVersion=workflow_version,
                )
            },
        },
        "job": {"namespace": "flow/workflows", "name": workflow_id},
        "inputs": inputs or [],
        "outputs": outputs or [],
        "producer": PRODUCER,
        "schemaURL": OPENLINEAGE_SCHEMA,
    }


class LineageSink:
    def __init__(self, client: httpx.AsyncClient, url: str, token: str | None = None) -> None:
        self.client = client
        self.url = url.rstrip("/") + "/api/v1/lineage"
        self.token = token

    async def emit(self, event: dict[str, Any]) -> None:
        headers = {"Authorization": f"Bearer {self.token}"} if self.token else {}
        response = await self.client.post(self.url, json=event, headers=headers)
        response.raise_for_status()


class OutboxPublisher:
    def __init__(self, store: RunStore, sink: LineageSink, retries: int = 2) -> None:
        self.store = store
        self.sink = sink
        self.retries = retries

    async def publish(self, identifier: str, event: dict[str, Any]) -> None:
        item = await self.store.enqueue(identifier, event)
        if item.get("state") == "acked":
            return
        last_error: Exception | None = None
        for attempt in range(self.retries + 1):
            try:
                await self.sink.emit(event)
                await self.store.ack(identifier)
                return
            except Exception as exc:  # noqa: BLE001 - sink/store failures are uniformly durable
                last_error = exc
                await self.store.outbox_error(identifier, str(exc))
                if attempt < self.retries:
                    await asyncio.sleep(0.05 * (2**attempt))
        raise RuntimeError(f"lineage acknowledgment failed: {last_error}")

    async def replay_pending(self, limit: int = 1000) -> int:
        """Deliver durable events left pending by a prior process before accepting new work."""
        items = await self.store.pending_outbox(limit)
        for item in items:
            await self.publish(item["event_id"], item["event"])
        return len(items)


class FlowprintAdapter:
    def __init__(self, client: httpx.AsyncClient, url: str | None) -> None:
        self.client = client
        self.url = url.rstrip("/") if url else None

    async def emit(self, run_id: str, source: str, destination: str) -> None:
        if not self.url:
            return
        try:
            await self.client.post(
                f"{self.url}/events",
                json={
                    "run_id": run_id,
                    "system": "flow-runner",
                    "from": source,
                    "step": destination,
                },
            )
        except httpx.HTTPError:
            # Flowprint augments, but does not own, the fail-closed lineage record.
            return
