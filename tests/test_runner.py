from __future__ import annotations

import json
from typing import Any

import httpx
import pytest

from flow_runner.lineage import LineageSink, OutboxPublisher, dataset_reference
from flow_runner.models import RegistrySpec
from flow_runner.registry import DeploymentRegistry
from flow_runner.runner import FlowRunner, LineageBlocked
from flow_runner.store import MemoryStore

MODULES = {
    "a.local": "module-a/source@1.0.0",
    "b.local": "module-b/left@1.0.0",
    "c.local": "module-c/right@1.0.0",
    "d.local": "module-d/merge@1.0.0",
}


def contract(ref: str) -> dict[str, Any]:
    return {
        "ref": ref,
        "input_schema": {"type": "object", "additionalProperties": True},
        "output_schema": {"type": "object", "additionalProperties": True},
    }


class Harness:
    def __init__(self) -> None:
        self.module_calls: list[str] = []
        self.events: list[dict[str, Any]] = []
        self.fail_start = False
        self.fail_module_complete = False
        self.module_statuses: list[int] = []

    async def handler(self, request: httpx.Request) -> httpx.Response:
        if request.url.host == "lineage.local":
            event = json.loads(request.content)
            if self.fail_start and event["eventType"] == "START":
                return httpx.Response(503)
            if (
                self.fail_module_complete
                and event["eventType"] == "COMPLETE"
                and event["job"]["namespace"] == "flow/modules"
            ):
                return httpx.Response(503)
            self.events.append(event)
            return httpx.Response(201, json={"accepted": True})
        if request.method == "GET":
            return httpx.Response(200, json={"modules": [contract(MODULES[request.url.host])]})
        ref = MODULES[request.url.host]
        self.module_calls.append(ref)
        if self.module_statuses:
            status = self.module_statuses.pop(0)
            if status != 200:
                return httpx.Response(status, text="private response must not be stored")
        body = json.loads(request.content)
        value = body["input"]
        if ref.endswith("source@1.0.0"):
            output = {"value": value.get("value")}
        elif ref.endswith("merge@1.0.0"):
            output = {"value": f"{value.get('left')}+{value.get('right')}"}
        else:
            output = {"value": f"{value.get('value')}:{ref.split('/')[1].split('@')[0]}"}
        return httpx.Response(200, json={"output": output})


def branch_workflow() -> dict[str, Any]:
    return {
        "schema_version": "1.0",
        "workflow": {"id": "branch", "version": "1.0.0", "max_parallel": 4},
        "steps": [
            {"id": "source", "module": MODULES["a.local"], "bindings": {"value": "workflow.input.value"}},
            {"id": "left", "module": MODULES["b.local"], "needs": ["source"], "bindings": {"value": "steps.source.output.value"}},
            {"id": "right", "module": MODULES["c.local"], "needs": ["source"], "bindings": {"value": "steps.source.output.value"}},
            {"id": "merge", "module": MODULES["d.local"], "needs": ["left", "right"], "bindings": {"left": "steps.left.output.value", "right": "steps.right.output.value"}},
        ],
        "outputs": {"result": "steps.merge.output.value"},
    }


def linear(first: str, second: str) -> dict[str, Any]:
    return {
        "schema_version": "1.0",
        "workflow": {"id": "linear", "version": "1.0.0"},
        "steps": [
            {"id": "first", "module": first, "bindings": {"value": "workflow.input.value"}},
            {"id": "second", "module": second, "needs": ["first"], "bindings": {"value": "steps.first.output.value"}},
        ],
        "outputs": {"result": "steps.second.output.value"},
    }


def make_runner(harness: Harness) -> tuple[FlowRunner, MemoryStore, httpx.AsyncClient]:
    client = httpx.AsyncClient(transport=httpx.MockTransport(harness.handler))
    spec = RegistrySpec.model_validate(
        {
            "schema_version": "1.0",
            "deployments": {ref: {"base_url": f"http://{host}"} for host, ref in MODULES.items()},
        }
    )
    store = MemoryStore()
    registry = DeploymentRegistry(spec, client)
    publisher = OutboxPublisher(store, LineageSink(client, "http://lineage.local"))
    return FlowRunner(store=store, registry=registry, publisher=publisher, client=client), store, client


async def test_branch_merge_and_metadata_only_persistence():
    harness = Harness()
    runner, store, client = make_runner(harness)
    try:
        result = await runner.run(branch_workflow(), {"value": "classified-secret"})
    finally:
        await client.aclose()
    assert result["status"] == "complete"
    assert result["output"] == {"result": "classified-secret:left+classified-secret:right"}
    assert len(harness.module_calls) == 4
    assert {event["eventType"] for event in harness.events} == {"START", "COMPLETE"}
    assert "classified-secret" not in json.dumps(store.runs)
    assert "classified-secret" not in json.dumps(store.outbox)
    merge_start = next(
        event for event in harness.events if event["eventType"] == "START" and event["job"]["name"] == MODULES["d.local"]
    )
    transfer = merge_start["run"]["facets"]["flow_transfer"]
    assert transfer["fromModules"] == [MODULES["b.local"], MODULES["c.local"]]
    assert transfer["bindings"] == {
        "left": "steps.left.output.value",
        "right": "steps.right.output.value",
    }
    upstream_outputs = {
        dataset_reference(item)
        for event in harness.events
        if event["eventType"] == "COMPLETE"
        and event["job"]["name"] in {MODULES["b.local"], MODULES["c.local"]}
        for item in event["outputs"]
    }
    assert set(transfer["fromDatasets"]) == upstream_outputs
    assert transfer["sources"] == [
        {
            "step": "left",
            "module": MODULES["b.local"],
            "dataset": next(
                item
                for item in transfer["fromDatasets"]
                if "/left/output/" in item
            ),
        },
        {
            "step": "right",
            "module": MODULES["c.local"],
            "dataset": next(
                item
                for item in transfer["fromDatasets"]
                if "/right/output/" in item
            ),
        },
    ]
    module_facet = merge_start["job"]["facets"]["flow_module"]
    assert module_facet == {
        "_producer": "https://github.com/ndndndn1/flow-runner",
        "_schemaURL": "https://github.com/ndndndn1/flow-runner/schemas/flow_module.json",
        "repository": "module-d",
        "namespace": "module-d",
        "name": "merge",
        "version": "1.0.0",
        "contract": MODULES["d.local"],
    }
    root_complete = next(
        event
        for event in harness.events
        if event["eventType"] == "COMPLETE"
        and event["job"]["namespace"] == "flow/workflows"
    )
    merge_output = next(
        event["outputs"][0]
        for event in harness.events
        if event["eventType"] == "COMPLETE" and event["job"]["name"] == MODULES["d.local"]
    )
    assert root_complete["inputs"] == [merge_output]


async def test_same_modules_can_run_in_both_directions():
    harness = Harness()
    runner, _, client = make_runner(harness)
    try:
        await runner.run(linear(MODULES["a.local"], MODULES["b.local"]), {"value": "x"})
        forward = list(harness.module_calls)
        harness.module_calls.clear()
        await runner.run(linear(MODULES["b.local"], MODULES["a.local"]), {"value": "x"})
        reverse = list(harness.module_calls)
    finally:
        await client.aclose()
    assert forward == [MODULES["a.local"], MODULES["b.local"]]
    assert reverse == [MODULES["b.local"], MODULES["a.local"]]
    transfers = [
        event["run"]["facets"]["flow_transfer"]["fromModules"]
        for event in harness.events
        if event["eventType"] == "START"
        and event["job"]["namespace"] == "flow/modules"
        and event["run"]["facets"]["flow_transfer"]["fromModules"]
    ]
    assert [MODULES["a.local"]] in transfers
    assert [MODULES["b.local"]] in transfers


async def test_lineage_start_failure_prevents_all_module_calls():
    harness = Harness()
    harness.fail_start = True
    runner, store, client = make_runner(harness)
    try:
        with pytest.raises(LineageBlocked):
            await runner.run(linear(MODULES["a.local"], MODULES["b.local"]), {"value": "x"})
    finally:
        await client.aclose()
    assert harness.module_calls == []
    assert next(iter(store.runs.values()))["status"] == "lineage_blocked"


async def test_lineage_complete_failure_prevents_downstream_call():
    harness = Harness()
    harness.fail_module_complete = True
    runner, _, client = make_runner(harness)
    try:
        result = await runner.run(
            linear(MODULES["a.local"], MODULES["b.local"]), {"value": "x"}
        )
    finally:
        await client.aclose()
    assert result["status"] == "lineage_blocked"
    assert harness.module_calls == [MODULES["a.local"]]


async def test_retries_5xx_but_not_4xx():
    harness = Harness()
    harness.module_statuses = [503, 503, 200]
    runner, _, client = make_runner(harness)
    try:
        result = await runner.run(linear(MODULES["a.local"], MODULES["b.local"]), {"value": "x"})
        assert result["status"] == "complete"
    finally:
        await client.aclose()
    assert harness.module_calls.count(MODULES["a.local"]) == 3

    harness = Harness()
    harness.module_statuses = [400, 200]
    runner, store, client = make_runner(harness)
    try:
        result = await runner.run(linear(MODULES["a.local"], MODULES["b.local"]), {"value": "sensitive"})
    finally:
        await client.aclose()
    assert result["status"] == "failed"
    assert harness.module_calls == [MODULES["a.local"]]
    assert "sensitive" not in json.dumps(store.outbox)
    assert "private response" not in json.dumps(store.outbox)


async def test_pending_outbox_is_replayed_and_acked_after_restart():
    harness = Harness()
    client = httpx.AsyncClient(transport=httpx.MockTransport(harness.handler))
    store = MemoryStore()
    event = {
        "eventType": "START",
        "eventTime": "2026-08-17T00:00:00Z",
        "run": {"runId": "restart-run"},
        "job": {"namespace": "flow/workflows", "name": "restart"},
        "inputs": [],
        "outputs": [],
        "producer": "https://github.com/ndndndn1/flow-runner",
    }
    await store.enqueue("pending-restart-event", event)
    publisher = OutboxPublisher(store, LineageSink(client, "http://lineage.local"))
    try:
        replayed = await publisher.replay_pending()
    finally:
        await client.aclose()

    assert replayed == 1
    assert store.outbox["pending-restart-event"]["state"] == "acked"
    assert harness.events == [event]
