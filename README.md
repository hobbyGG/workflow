# Workflow

A dynamic DAG engine built with Go, Kratos, Temporal, and PostgreSQL. The same
registered activity implementation can run in a workflow graph or independently
through the tool API.

## Capabilities

- Persistent workflow definitions and asynchronous workflow executions
- Concurrent DAG scheduling with field mappings and control-only edges
- Per-node-instance preset inputs
- Exclusive/inclusive routers and skip propagation through joins
- JSON Schema metadata for node inputs and outputs
- Per-activity Temporal retry policies
- Durable standalone node execution, result polling, and recent execution profiles
- Separate API and Temporal worker processes
- Prometheus HTTP/Temporal metrics and structured logging
- A Go HTTP SDK in `sdk/wfx`, backed by generated transport bindings
- Protobuf, OpenAPI, Wire, and sqlc code generation

The backend exposes graph definitions and node schemas; it does not include a
canvas UI. The included activities are provider-neutral examples:
`SAY_SOMETHING` returns a supplied string, and `EXAMPLE` sends a GET request to
`https://example.com`. Application-specific activities and integrations are not
included.

## Layout

```text
api/                  Protobuf sources and generated bindings
cmd/workflow/         API process
cmd/worker/           Temporal worker process
internal/service/     DTO/domain conversion and worker registration
internal/biz/         Node contracts, graph runtime, usecases, repository ports
internal/data/        PostgreSQL, Temporal dispatch, and example activities
pkg/metrics/          Prometheus adapters
sdk/wfx/              Go HTTP client
configs/              Local configuration
example/              Existing example DAG definition
```

`Node` provides type and schema metadata. `ActivityNode` adds an activity and its
retry policy; `RouterNode` provides deterministic routing. The worker registers
all activities returned by the node repository. Adding a node means implementing
the existing interface and registering it in `internal/data/node.go`; the DAG
scheduler does not need a new type-specific execution branch.

## Run locally

Requirements: Go 1.26.1+, PostgreSQL, and Temporal Server.

Use a fresh PostgreSQL database with this schema:

```bash
psql "$PG_DSN" -v ON_ERROR_STOP=1 -f internal/data/db/migrations/000001_init.sql
```

The schema uses `workflow_definition`, `workflow_execution`, and
`node_execution`. An automatic upgrade from earlier table layouts is not
provided; keep an existing database separate from this setup.

Update `configs/config.yaml`, then start the API and worker in separate
terminals:

```bash
go run ./cmd/workflow -conf ./configs
go run ./cmd/worker -conf ./configs
```

The configured HTTP port defaults to `8000`; give the worker a separate config
path and HTTP port when both processes run on the same host. Both processes
must share the Temporal namespace and task queue. The checked-in PostgreSQL
credentials are local examples.

The running application enables HTTP. Protobuf definitions, generated gRPC
bindings, and gRPC constructors are retained, but a gRPC listener is not wired
into the application.

UID is supplied by callers for workflow grouping. The service does not provide
login or authorization. Private configuration providers, request-context
integrations, and application adapters are excluded.

## HTTP API

| Operation | Method and path |
| --- | --- |
| Create/list definitions | `POST` / `GET /internal/v1/definitions` |
| Get definition | `GET /internal/v1/definitions/{definition_id}` |
| Start/list workflow executions | `POST` / `GET /internal/v1/workflow-executions` |
| Get workflow execution | `GET /internal/v1/workflow-executions/{workflow_execution_id}` |
| List node schemas | `GET /internal/v1/node-schemas` |
| List standalone tools and recent profiles | `GET /internal/v1/tool/nodes` |
| Execute a standalone tool | `POST /internal/v1/tool/node` |
| Check standalone result | `POST /internal/v1/tool/execution` |
| Prometheus metrics | `GET /metrics` |

See `api/workflow/v1/*.proto`, the generated `openapi.yaml`, and the existing
`example/definition.json` for request shapes and graph structure. The SDK
provides `Create`, `Get`, `List`, `CreateWorkflowExecution`,
`GetWorkflowExecution`, `ListWorkflowExecutions`, `NodeTypes`, `ListTools`,
`ExecuteTool`, and `CheckToolExecution` using regular Go types.

## Existing execution semantics

A regular node begins with its `preset`. An active data edge maps a top-level
source field to a destination field, overriding that field's preset. Leaving
`to_input` empty creates a control-only dependency. Router `preset.branches`
stores expressions; expressions read runtime data under `inputs`.

An exclusive router selects the first matching branch; an inclusive router
selects all matching branches. A join waits for all incoming edges to resolve,
runs when at least one is active, and is skipped when all incoming edges were
skipped. Definitions must be acyclic, have unique node IDs and exactly one
`OUTPUT`, and every node must reach that output.

The workflow-execution API persists its `input` field, but this field is not
passed to the graph runtime. Configure graph inputs with presets and edges.
Standalone tool input is passed directly to its activity. JSON Schema is
metadata exposed for clients; node implementations enforce their own input
contracts. No automatic schema-validation wrapper is added.

Workflow executions reuse the first record for `(uid, idempotency_key)`.
Standalone tools use a globally unique idempotency key, and reusing it with a
different node type or input returns a conflict. Temporal dispatch uses stable
execution IDs and rejects creating a second run for an existing ID. Callers
should retry a create request if dispatch fails after record insertion.

The example activities and status-writing activities use at most three
attempts; activities have a 30-minute start-to-close timeout. Individual activity
nodes declare their own retry policy. Standalone records expose
pending/running/completed/failed; cancellation is recorded as failure. Graph
executions and standalone tools retain their existing status handling.

Provider-specific task submission, polling, and side-effect deduplication are
not included. Integrations that perform external writes must provide their own
retry-safe behaviour. Keep Temporal workflow and router code deterministic;
perform external I/O in activities.

## Development

```bash
make build
go test ./...
go test -race ./...
go vet ./...
```

The retained tests exercise usecases, DAG routing and joins, standalone node
execution, retry/status behaviour, repository conversions, HTTP routes, and the
SDK. They use Temporal's test suite and fakes rather than live infrastructure.

Regenerate checked-in files from their sources:

```bash
make api
make config
make sqlc
make generate
```

Generated protobuf, OpenAPI, sqlc, and Wire files must not be edited manually.

## Docker

```bash
docker build -t workflow .
docker run --rm -p 8000:8000 \
  -v "$PWD/configs:/data/conf:ro" \
  workflow
```
