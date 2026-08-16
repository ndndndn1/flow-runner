from __future__ import annotations

import pytest

from flow_runner.workflow import WorkflowError, bind_input, validate_workflow


def workflow(*steps):
    return {
        "schema_version": "1.0",
        "workflow": {"id": "test_flow", "version": "1.0.0"},
        "steps": list(steps),
    }


def test_rejects_cycles():
    with pytest.raises(WorkflowError, match="cycle"):
        validate_workflow(
            workflow(
                {"id": "a", "module": "repo/a@1.0.0", "needs": ["b"]},
                {"id": "b", "module": "repo/b@1.0.0", "needs": ["a"]},
            )
        )


def test_rejects_non_upstream_reference():
    with pytest.raises(WorkflowError, match="non-upstream"):
        validate_workflow(
            workflow(
                {"id": "a", "module": "repo/a@1.0.0"},
                {
                    "id": "b",
                    "module": "repo/b@1.0.0",
                    "bindings": {"value": "steps.a.output"},
                },
            )
        )


def test_allows_transitive_upstream_reference():
    validate_workflow(
        workflow(
            {"id": "a", "module": "repo/a@1.0.0"},
            {"id": "b", "module": "repo/b@1.0.0", "needs": ["a"]},
            {
                "id": "c",
                "module": "repo/c@1.0.0",
                "needs": ["b"],
                "bindings": {"value": "steps.a.output"},
            },
        )
    )


def test_optional_binding_default_and_required_missing():
    context = {"workflow": {"input": {}}, "steps": {}}
    value = bind_input(
        {"limit": {"source": "workflow.input.limit", "optional": True, "default": 10}},
        context,
    )
    assert value == {"limit": 10}
    with pytest.raises(WorkflowError, match="required binding"):
        bind_input({"limit": "workflow.input.limit"}, context)


def test_rejects_unsafe_ids_and_unknown_needs():
    with pytest.raises(ValueError, match="safe identifier"):
        validate_workflow(workflow({"id": "not-safe", "module": "repo/a@1.0.0"}))
    with pytest.raises(WorkflowError, match="unknown needs"):
        validate_workflow(
            workflow({"id": "a", "module": "repo/a@1.0.0", "needs": ["missing"]})
        )


def test_parallelism_is_capped_at_four():
    document = workflow({"id": "a", "module": "repo/a@1.0.0"})
    document["workflow"]["max_parallel"] = 5
    with pytest.raises(ValueError, match="less than or equal to 4"):
        validate_workflow(document)
