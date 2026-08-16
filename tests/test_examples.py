from pathlib import Path

import yaml

from flow_runner.registry import _normalize_contract
from flow_runner.workflow import bind_input, validate_workflow

EXAMPLES = Path(__file__).parents[1] / "examples"


def load(name: str):
    return yaml.safe_load((EXAMPLES / name).read_text(encoding="utf-8"))


def test_public_module_catalog_contract_is_normalized():
    value = _normalize_contract(
        {
            "id": "score-rank",
            "module_ref": "flow-decisioning/score-rank@0.2.0",
            "version": "0.2.0",
            "schema_version": "1.1.0",
            "input_ports": {"type": "object"},
            "output_ports": {"type": "object"},
            "schema_hash": "sha256:example",
        }
    )
    assert value == {
        "ref": "flow-decisioning/score-rank@0.2.0",
        "input_schema": {"type": "object"},
        "output_schema": {"type": "object"},
        "schema_version": "1.1.0",
    }


def test_forward_and_reverse_examples_bind_real_contract_fields():
    forward = validate_workflow(load("forward.yaml"))
    rank = forward.steps[1]
    rank_input = bind_input(
        rank.bindings,
        {
            "workflow": {"input": {}},
            "steps": {"aggregate": {"output": {"count": 3, "min": 1, "max": 3, "mean": 2}}},
        },
    )
    assert rank_input == {
        "items": [
            {"name": "mean", "score": 2},
            {"name": "max", "score": 3},
            {"name": "min", "score": 1},
        ]
    }

    reverse = validate_workflow(load("reverse.yaml"))
    aggregate = reverse.steps[1]
    aggregate_input = bind_input(
        aggregate.bindings,
        {
            "workflow": {"input": {}},
            "steps": {
                "rank": {
                    "output": {
                        "items": [
                            {"score": 3, "rank": 1},
                            {"score": 1, "rank": 2},
                        ]
                    }
                }
            },
        },
    )
    assert aggregate_input == {"points": [{"value": 3}, {"value": 1}]}


def test_branch_merge_example_uses_two_actual_upstream_outputs():
    workflow = validate_workflow(load("branch-merge.yaml"))
    merge = workflow.steps[-1]
    value = bind_input(
        merge.bindings,
        {
            "workflow": {"input": {}},
            "steps": {
                "left": {"output": {"labels": ["a", "b"]}},
                "right": {"output": {"fields": ["text", "score", "source"]}},
            },
        },
    )
    assert value == {
        "items": [
            {"branch": "labels", "score": 2},
            {"branch": "fields", "score": 3},
        ]
    }
