from __future__ import annotations

import json
import re
from collections import deque
from pathlib import Path
from typing import Any

import jmespath
import yaml

from .models import Binding, WorkflowSpec

STEP_REFERENCE = re.compile(r"(?:^|[^A-Za-z0-9_])steps\.([A-Za-z_][A-Za-z0-9_]*)")
MISSING = object()


class WorkflowError(ValueError):
    pass


def load_document(path: str | Path) -> dict[str, Any]:
    source = Path(path)
    text = source.read_text(encoding="utf-8")
    value = json.loads(text) if source.suffix.lower() == ".json" else yaml.safe_load(text)
    if not isinstance(value, dict):
        raise WorkflowError("workflow document must be an object")
    return value


def _references(expression: str) -> set[str]:
    try:
        jmespath.compile(expression)
    except Exception as exc:
        raise WorkflowError(f"invalid JMESPath expression {expression!r}: {exc}") from exc
    return set(STEP_REFERENCE.findall(expression))


def validate_workflow(document: dict[str, Any]) -> WorkflowSpec:
    spec = WorkflowSpec.model_validate(document)
    ids = [step.id for step in spec.steps]
    if len(ids) != len(set(ids)):
        raise WorkflowError("step ids must be unique")
    known = set(ids)
    dependencies = {step.id: set(step.needs) for step in spec.steps}
    for step in spec.steps:
        unknown = set(step.needs) - known
        if unknown:
            raise WorkflowError(f"step {step.id} has unknown needs: {sorted(unknown)}")
        if step.id in step.needs:
            raise WorkflowError(f"step {step.id} cannot need itself")

    order = topological_order(dependencies)
    ancestors: dict[str, set[str]] = {}
    for step_id in order:
        ancestors[step_id] = set(dependencies[step_id])
        for need in dependencies[step_id]:
            ancestors[step_id].update(ancestors[need])

    for step in spec.steps:
        expressions = [value if isinstance(value, str) else value.source for value in step.bindings.values()]
        if step.when:
            expressions.append(step.when)
        for expression in expressions:
            illegal = _references(expression) - ancestors[step.id]
            if illegal:
                raise WorkflowError(
                    f"step {step.id} references non-upstream steps: {sorted(illegal)}"
                )
    all_ancestors = set(ids)
    for name, expression in spec.outputs.items():
        illegal = _references(expression) - all_ancestors
        if illegal:
            raise WorkflowError(f"output {name} references unknown steps: {sorted(illegal)}")
    return spec


def topological_order(dependencies: dict[str, set[str]]) -> list[str]:
    remaining = {name: set(needs) for name, needs in dependencies.items()}
    ready = deque(name for name, needs in remaining.items() if not needs)
    order: list[str] = []
    while ready:
        current = ready.popleft()
        order.append(current)
        for name, needs in remaining.items():
            if current in needs:
                needs.remove(current)
                if not needs and name not in order and name not in ready:
                    ready.append(name)
    if len(order) != len(remaining):
        blocked = sorted(set(remaining) - set(order))
        raise WorkflowError(f"workflow contains a cycle involving: {blocked}")
    return order


def evaluate(expression: str, context: dict[str, Any], *, missing: Any = MISSING) -> Any:
    value = jmespath.search(expression, context)
    if value is None:
        return missing
    return value


def bind_input(bindings: dict[str, str | Binding], context: dict[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for name, raw in bindings.items():
        binding = Binding(source=raw) if isinstance(raw, str) else Binding.model_validate(raw)
        value = evaluate(binding.source, context)
        if value is MISSING:
            if not binding.optional:
                raise WorkflowError(f"required binding {name!r} did not resolve: {binding.source}")
            value = binding.default
        result[name] = value
    return result
