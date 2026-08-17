# flow-runner

`flow-runner` executes arbitrary module DAGs while recording the actual data movement. It is
domain-neutral infrastructure: module order is defined by each workflow, so the same two modules
can run in either direction in different workflows.

## Guarantees

- Branch and merge DAG scheduling with a configurable parallelism limit (default 4).
- JMESPath-only bindings and conditions. A step may only reference declared upstream steps.
- Module contract discovery through `GET /v1/modules`, JSON Schema validation, and execution
  through `POST /v1/run`.
- 10 MiB payload limit, 30-second request timeout, and two retries for transport/5xx failures.
- Durable MongoDB outbox in `flow_runtime`. A module is not called before its OpenLineage START
  event is acknowledged, and its output is not exposed downstream before COMPLETE is acknowledged.
- Pending outbox events from an interrupted process are replayed and acknowledged during startup;
  startup fails closed while the lineage collector is unavailable.
- Raw inputs and outputs are never persisted. Run state and lineage retain canonical SHA-256,
  byte count, record count, and schema reference only.
- Standard OpenLineage parent-run facets plus public `flow_*` facets. Optional Flowprint emission
  uses the same root run ID.

## Workflow

```yaml
schema_version: "1.0"
workflow: {id: example, version: "1.0.0", max_parallel: 4}
steps:
  - id: first
    module: module-a/normalize@1.0.0
    bindings: {records: workflow.input.records}
  - id: second
    module: module-b/score@1.0.0
    needs: [first]
    when: steps.first.output.records != null
    bindings: {records: steps.first.output.records}
outputs: {result: steps.second.output}
```

Step IDs are restricted to JMESPath-safe identifiers. Binding defaults must be declared as
`{source: path, optional: true, default: value}`. Cycles, unknown dependencies, and references to
non-upstream steps are rejected before execution.

## Commands

```bash
uv sync --extra dev
uv run flow-runner validate examples/branch-merge.yaml
FLOW_REGISTRY_PATH=/secure/registry.yaml uv run flow-runner plan examples/forward.yaml
FLOW_REGISTRY_PATH=/secure/registry.yaml uv run flow-runner run examples/forward.yaml \
  --input '{"points":[{"value":1},{"value":2},{"value":3}]}'
FLOW_REGISTRY_PATH=/secure/registry.yaml uv run flow-runner status RUN_ID
uv run pytest -q
uv run ruff check .
```

The private deployment registry maps versioned module refs to base URLs and optional environment
variable names holding bearer tokens. Do not put endpoint credentials in workflow documents or the
public repository.

The API exposes `POST /v1/runs`, `GET /v1/runs/{run_id}`, and `GET /healthz`. The default Compose
binding is `127.0.0.1:18090`; the stack is on-demand and should be stopped after use.

```bash
FLOW_REGISTRY_FILE=/secure/registry.yaml \
FLOW_MONGODB_URI='mongodb://...@mongodb:27017/' \
FLOW_LINEAGE_TOKEN='...' docker compose up --build -d
docker compose stop
```

The standalone Compose file joins existing `flow-net` and MongoDB networks. The private
`flow-stack` deployment owns those networks and supplies the lineage and module services.
