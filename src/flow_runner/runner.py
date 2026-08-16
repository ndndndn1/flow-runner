from __future__ import annotations

import asyncio
import re
import uuid
from dataclasses import dataclass
from typing import Any

import httpx
from jsonschema import ValidationError, validate

from .lineage import (
    FlowprintAdapter,
    OutboxPublisher,
    build_event,
    build_workflow_event,
    dataset,
    dataset_reference,
    event_id,
)
from .metadata import describe
from .models import ModuleContract, WorkflowSpec
from .module_client import call_module
from .registry import DeploymentRegistry
from .store import RunStore, now
from .workflow import (
    MISSING,
    WorkflowError,
    bind_input,
    evaluate,
    topological_order,
    validate_workflow,
)


@dataclass
class StepExecution:
    output: Any
    output_metadata: dict[str, Any]
    output_dataset: dict[str, Any]


class FlowRunner:
    def __init__(
        self,
        *,
        store: RunStore,
        registry: DeploymentRegistry,
        publisher: OutboxPublisher,
        client: httpx.AsyncClient,
        flowprint: FlowprintAdapter | None = None,
        max_payload_bytes: int = 10 * 1024 * 1024,
        module_retries: int = 2,
    ) -> None:
        self.store = store
        self.registry = registry
        self.publisher = publisher
        self.client = client
        self.flowprint = flowprint or FlowprintAdapter(client, None)
        self.max_payload_bytes = max_payload_bytes
        self.module_retries = module_retries

    async def plan(self, document: dict[str, Any]) -> dict[str, Any]:
        spec = validate_workflow(document)
        contracts = await self._contracts(spec)
        dependencies = {step.id: set(step.needs) for step in spec.steps}
        order = topological_order(dependencies)
        levels: list[list[str]] = []
        completed: set[str] = set()
        while len(completed) < len(order):
            level = [name for name in order if name not in completed and dependencies[name] <= completed]
            levels.append(level)
            completed.update(level)
        return {
            "workflow": spec.workflow.model_dump(),
            "order": order,
            "levels": levels,
            "modules": {
                ref: {
                    "schema_version": contract.schema_version,
                    "input_schema": contract.input_schema,
                    "output_schema": contract.output_schema,
                }
                for ref, contract in contracts.items()
            },
        }

    async def run(self, document: dict[str, Any], workflow_input: Any) -> dict[str, Any]:
        spec = validate_workflow(document)
        contracts = await self._contracts(spec)
        root_run_id = str(uuid.uuid4())
        created = now()
        root_input_meta = describe(
            workflow_input,
            f"flow/workflows/{spec.workflow.id}@{spec.workflow.version}#input",
            max_bytes=self.max_payload_bytes,
        )
        root_input_dataset = dataset(
            f"{root_run_id}/workflow/input/{root_input_meta['hash'].split(':')[1]}",
            root_input_meta,
        )
        step_state = {step.id: {"status": "pending", "module": step.module} for step in spec.steps}
        await self.store.create_run(
            {
                "run_id": root_run_id,
                "workflow_id": spec.workflow.id,
                "workflow_version": spec.workflow.version,
                "status": "running",
                "steps": step_state,
                "created_at": created,
                "updated_at": created,
            }
        )
        root_start = build_workflow_event(
            "START",
            run_id=root_run_id,
            workflow_id=spec.workflow.id,
            workflow_version=spec.workflow.version,
            inputs=[root_input_dataset],
        )
        try:
            await self.publisher.publish(
                event_id(root_run_id, "__workflow__", "START"), root_start
            )
        except Exception as exc:
            await self.store.update_run(root_run_id, {"status": "lineage_blocked"})
            raise LineageBlocked(str(exc)) from exc
        context: dict[str, Any] = {
            "workflow": {"input": workflow_input, "input_dataset": root_input_dataset},
            "steps": {},
        }
        step_map = {step.id: step for step in spec.steps}
        pending = set(step_map)
        semaphore = asyncio.Semaphore(spec.workflow.max_parallel)

        while pending:
            progress = False
            for step_id in list(pending):
                step = step_map[step_id]
                need_statuses = [step_state[need]["status"] for need in step.needs]
                if any(status in {"failed", "skipped", "lineage_blocked"} for status in need_statuses):
                    step_state[step_id] = {**step_state[step_id], "status": "skipped", "reason": "upstream"}
                    context["steps"][step_id] = {"status": "skipped"}
                    pending.remove(step_id)
                    progress = True
            ready = [
                step_map[name]
                for name in list(pending)
                if all(step_state[need]["status"] == "complete" for need in step_map[name].needs)
            ]
            if ready:
                progress = True
                results = await asyncio.gather(
                    *[
                        self._run_one(
                            semaphore, spec, step, contracts[step.module], context, root_run_id
                        )
                        for step in ready
                    ],
                    return_exceptions=True,
                )
                for step, result in zip(ready, results, strict=True):
                    pending.remove(step.id)
                    if isinstance(result, SkippedStep):
                        step_state[step.id] = {**step_state[step.id], "status": "skipped", "reason": "condition"}
                        context["steps"][step.id] = {"status": "skipped"}
                    elif isinstance(result, LineageBlocked):
                        step_state[step.id] = {**step_state[step.id], "status": "lineage_blocked", "error": str(result)}
                        context["steps"][step.id] = {"status": "lineage_blocked"}
                    elif isinstance(result, Exception):
                        step_state[step.id] = {
                            **step_state[step.id],
                            "status": "failed",
                            "error": _safe_error(result),
                        }
                        context["steps"][step.id] = {"status": "failed"}
                    else:
                        step_state[step.id] = {
                            **step_state[step.id],
                            "status": "complete",
                            "output_metadata": result.output_metadata,
                        }
                        context["steps"][step.id] = {
                            "status": "complete",
                            "module": step.module,
                            "output": result.output,
                            "output_dataset": result.output_dataset,
                        }
                await self.store.update_run(root_run_id, {"steps": step_state})
            if not progress:
                raise RuntimeError(f"scheduler stalled with pending steps: {sorted(pending)}")

        statuses = {item["status"] for item in step_state.values()}
        if "lineage_blocked" in statuses:
            status = "lineage_blocked"
        elif "failed" in statuses:
            status = "failed"
        else:
            status = "complete"
        output: dict[str, Any] = {}
        for name, expression in spec.outputs.items():
            value = evaluate(expression, context)
            output[name] = None if value is MISSING else value
        root_output_meta = describe(
            output, f"flow/workflows/{spec.workflow.id}@{spec.workflow.version}#output", max_bytes=self.max_payload_bytes
        )
        terminal_type = "COMPLETE" if status == "complete" else "FAIL"
        root_terminal = build_workflow_event(
            terminal_type,
            run_id=root_run_id,
            workflow_id=spec.workflow.id,
            workflow_version=spec.workflow.version,
            inputs=_workflow_output_sources(spec.outputs, context),
            outputs=[
                dataset(
                    f"{root_run_id}/workflow/output/{root_output_meta['hash'].split(':')[1]}",
                    root_output_meta,
                )
            ],
        )
        try:
            await self.publisher.publish(
                event_id(root_run_id, "__workflow__", terminal_type), root_terminal
            )
        except Exception:  # noqa: BLE001 - all publisher failures block completion
            status = "lineage_blocked"
        await self.store.update_run(root_run_id, {"status": status, "steps": step_state})
        return {"run_id": root_run_id, "status": status, "output": output, "steps": step_state}

    async def _contracts(self, spec: WorkflowSpec) -> dict[str, ModuleContract]:
        refs = list(dict.fromkeys(step.module for step in spec.steps))
        contracts = await asyncio.gather(*[self.registry.contract(ref) for ref in refs])
        return dict(zip(refs, contracts, strict=True))

    async def _run_one(
        self,
        semaphore: asyncio.Semaphore,
        spec: WorkflowSpec,
        step: Any,
        contract: ModuleContract,
        context: dict[str, Any],
        root_run_id: str,
    ) -> StepExecution:
        async with semaphore:
            if step.when and not bool(evaluate(step.when, context, missing=False)):
                raise SkippedStep()
            step_input = bind_input(step.bindings, context)
            try:
                validate(instance=step_input, schema=contract.input_schema)
                input_meta = describe(
                    step_input, f"{step.module}#input", max_bytes=self.max_payload_bytes
                )
            except (ValidationError, ValueError) as exc:
                raise WorkflowError(f"step {step.id} input rejected: {exc.message if isinstance(exc, ValidationError) else exc}") from exc
            step_run_id = str(uuid.uuid4())
            input_dataset = dataset(
                f"{root_run_id}/{step.id}/input/{input_meta['hash'].split(':')[1]}", input_meta
            )
            source_steps = _binding_step_ids(step.bindings)
            start = build_event(
                "START",
                run_id=step_run_id,
                root_run_id=root_run_id,
                workflow_id=spec.workflow.id,
                workflow_version=spec.workflow.version,
                step_id=step.id,
                module_ref=step.module,
                inputs=[input_dataset],
                outputs=[],
                upstream_steps=source_steps,
                dependency_steps=step.needs,
                upstream_modules=[
                    str(context["steps"][source].get("module", "")) for source in source_steps
                ],
                upstream_datasets=_upstream_datasets(step.bindings, context),
                sources=_transfer_sources(step.bindings, context),
                bindings=_binding_sources(step.bindings),
            )
            try:
                await self.publisher.publish(event_id(root_run_id, step.id, "START"), start)
            except Exception as exc:
                raise LineageBlocked(str(exc)) from exc
            try:
                value = await call_module(
                    self.client,
                    self.registry.deployment(step.module),
                    step.module,
                    step_run_id,
                    step_input,
                    retries=self.module_retries,
                )
                validate(instance=value, schema=contract.output_schema)
                output_meta = describe(
                    value, f"{step.module}#output", max_bytes=self.max_payload_bytes
                )
            except Exception as exc:
                safe_error = _safe_error(exc)
                fail = build_event(
                    "FAIL",
                    run_id=step_run_id,
                    root_run_id=root_run_id,
                    workflow_id=spec.workflow.id,
                    workflow_version=spec.workflow.version,
                    step_id=step.id,
                    module_ref=step.module,
                    inputs=[input_dataset],
                    outputs=[],
                    upstream_steps=source_steps,
                    dependency_steps=step.needs,
                    upstream_modules=[
                        str(context["steps"][source].get("module", ""))
                        for source in source_steps
                    ],
                    upstream_datasets=_upstream_datasets(step.bindings, context),
                    sources=_transfer_sources(step.bindings, context),
                    bindings=_binding_sources(step.bindings),
                    error=safe_error,
                )
                try:
                    await self.publisher.publish(event_id(root_run_id, step.id, "FAIL"), fail)
                except Exception as lineage_exc:
                    raise LineageBlocked(str(lineage_exc)) from lineage_exc
                raise
            output_dataset = dataset(
                f"{root_run_id}/{step.id}/output/{output_meta['hash'].split(':')[1]}", output_meta
            )
            complete = build_event(
                "COMPLETE",
                run_id=step_run_id,
                root_run_id=root_run_id,
                workflow_id=spec.workflow.id,
                workflow_version=spec.workflow.version,
                step_id=step.id,
                module_ref=step.module,
                inputs=[input_dataset],
                outputs=[output_dataset],
                upstream_steps=source_steps,
                dependency_steps=step.needs,
                upstream_modules=[
                    str(context["steps"][source].get("module", "")) for source in source_steps
                ],
                upstream_datasets=_upstream_datasets(step.bindings, context),
                sources=_transfer_sources(step.bindings, context),
                bindings=_binding_sources(step.bindings),
            )
            try:
                await self.publisher.publish(event_id(root_run_id, step.id, "COMPLETE"), complete)
            except Exception as exc:
                raise LineageBlocked(str(exc)) from exc
            sources = [context["steps"][source]["module"] for source in source_steps]
            if _uses_workflow_input(step.bindings):
                sources.insert(0, "workflow.input")
            for source in sources:
                await self.flowprint.emit(root_run_id, str(source), step.module)
            return StepExecution(value, output_meta, output_dataset)


class SkippedStep(Exception):
    pass


class LineageBlocked(Exception):
    pass


def _safe_error(exc: Exception) -> str:
    if isinstance(exc, ValidationError):
        location = ".".join(str(part) for part in exc.absolute_path) or "$"
        return f"ValidationError at {location}: rule={exc.validator}"
    return type(exc).__name__


def _upstream_datasets(bindings: dict[str, Any], context: dict[str, Any]) -> list[str]:
    values: list[dict[str, Any]] = []
    if _uses_workflow_input(bindings):
        values.append(context["workflow"]["input_dataset"])
    values.extend(
        context["steps"][step_id]["output_dataset"] for step_id in _binding_step_ids(bindings)
    )
    return [dataset_reference(value) for value in values]


def _transfer_sources(bindings: dict[str, Any], context: dict[str, Any]) -> list[dict[str, Any]]:
    sources: list[dict[str, Any]] = []
    if _uses_workflow_input(bindings):
        sources.append(
            {
                "step": "workflow.input",
                "module": None,
                "dataset": dataset_reference(context["workflow"]["input_dataset"]),
            }
        )
    for step_id in _binding_step_ids(bindings):
        step = context["steps"][step_id]
        sources.append(
            {
                "step": step_id,
                "module": step["module"],
                "dataset": dataset_reference(step["output_dataset"]),
            }
        )
    return sources


def _binding_sources(bindings: dict[str, Any]) -> dict[str, str]:
    return {
        name: value if isinstance(value, str) else value.source
        for name, value in bindings.items()
    }


def _binding_step_ids(bindings: dict[str, Any]) -> list[str]:
    pattern = re.compile(r"(?:^|[^A-Za-z0-9_])steps\.([A-Za-z_][A-Za-z0-9_]*)")
    result: list[str] = []
    for source in _binding_sources(bindings).values():
        for step_id in pattern.findall(source):
            if step_id not in result:
                result.append(step_id)
    return result


def _uses_workflow_input(bindings: dict[str, Any]) -> bool:
    return any("workflow.input" in source for source in _binding_sources(bindings).values())


def _workflow_output_sources(
    outputs: dict[str, str], context: dict[str, Any]
) -> list[dict[str, Any]]:
    pattern = re.compile(r"(?:^|[^A-Za-z0-9_])steps\.([A-Za-z_][A-Za-z0-9_]*)")
    step_ids: list[str] = []
    for expression in outputs.values():
        for step_id in pattern.findall(expression):
            if step_id not in step_ids:
                step_ids.append(step_id)
    return [
        context["steps"][step_id]["output_dataset"]
        for step_id in step_ids
        if "output_dataset" in context["steps"].get(step_id, {})
    ]
