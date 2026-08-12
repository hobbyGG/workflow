# Workflow

An independent workflow engine built with Go, Kratos, Temporal, and PostgreSQL.

## Features

- Protobuf-first HTTP and gRPC APIs
- Workflow definition persistence in PostgreSQL
- Idempotent asynchronous execution
- Temporal-based workflow and activity execution
- Concurrent DAG scheduling and data propagation
- Exclusive and inclusive router nodes
- Separate API and worker processes
- Structured logging and OpenTelemetry instrumentation
- Buf, Wire, and sqlc code generation

The built-in activity nodes are intentionally provider-neutral:

- `SAY_SOMETHING` returns a supplied string.
- `EXAMPLE` performs a standard HTTP request and demonstrates external I/O.

Add application-specific activities by implementing `biz.ActivityNode` and
registering them in `internal/data/node.go`.

## Architecture

```text
client -> Kratos transport -> service -> biz -> repository -> PostgreSQL
                                      |
                                      +-> Temporal -> worker -> activity nodes
```

- `api/`: protobuf API definitions and generated bindings
- `cmd/workflow/`: API process
- `cmd/worker/`: Temporal worker process
- `internal/service/`: transport-to-domain conversion
- `internal/biz/`: workflow, execution, DAG, and node contracts
- `internal/data/`: PostgreSQL, Temporal, and activity adapters
- `internal/server/`: Kratos HTTP/gRPC and worker wiring
- `configs/`: local configuration
- `example/`: runnable workflow definition

## Requirements

- Go 1.26.1 or later
- PostgreSQL
- Temporal Server

Create the schema using:

```bash
psql "$PG_DSN" -f internal/data/db/migrations/000001_init.sql
```

## Run

Edit `configs/config.yaml`, then start the API and worker in separate terminals:

```bash
go run ./cmd/workflow -conf ./configs
go run ./cmd/worker -conf ./configs
```

The default HTTP address is `0.0.0.0:8000`. Run the worker on a different HTTP
address when both processes run on the same host.

## Development

```bash
make build
go test ./...
go vet ./...
```

Regenerate checked-in code:

```bash
make api
make config
make sqlc
make generate
```

Generated protobuf, OpenAPI, sqlc, and Wire files should not be edited manually.

## Docker

```bash
docker build -t workflow .
docker run --rm -p 8000:8000 \
  -v "$PWD/configs:/data/conf:ro" \
  workflow
```
