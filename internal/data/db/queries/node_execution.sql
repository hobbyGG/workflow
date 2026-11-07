-- name: ListNodeExecutionsSince :many
SELECT node_type, status, duration_ms
FROM node_execution
WHERE created_at >= sqlc.arg(since);

-- name: GetNodeExecution :one
SELECT *
FROM node_execution
WHERE node_execution_id = sqlc.arg(node_execution_id);

-- name: GetNodeExecutionByIdempotencyKey :one
SELECT *
FROM node_execution
WHERE idempotency_key = sqlc.arg(idempotency_key);

-- name: CreateNodeExecution :one
INSERT INTO node_execution (
    node_execution_id,
    workflow_execution_id,
    idempotency_key,
    node_id,
    node_type,
    input,
    status
) VALUES (
    sqlc.arg(node_execution_id),
    sqlc.narg(workflow_execution_id),
    sqlc.arg(idempotency_key),
    sqlc.narg(node_id),
    sqlc.arg(node_type),
    sqlc.arg(input),
    sqlc.arg(status)
)
RETURNING *;

-- name: UpdateNodeExecutionStatusToRunning :one
UPDATE node_execution
SET status = 'running'
WHERE node_execution_id = sqlc.arg(node_execution_id)
  AND status = 'pending'
RETURNING node_execution_id;

-- name: UpdateNodeExecutionStatusToCompleted :one
UPDATE node_execution
SET status = 'completed',
    output = sqlc.narg(output),
    internal_error_message = NULL,
    duration_ms = sqlc.arg(duration_ms)
WHERE node_execution_id = sqlc.arg(node_execution_id)
  AND status = 'running'
RETURNING node_execution_id;

-- name: UpdateNodeExecutionStatusToFailed :one
UPDATE node_execution
SET status = 'failed',
    output = sqlc.narg(output),
    internal_error_message = sqlc.narg(internal_error_message),
    duration_ms = sqlc.arg(duration_ms)
WHERE node_execution_id = sqlc.arg(node_execution_id)
  AND status IN ('pending', 'running')
RETURNING node_execution_id;

-- name: GetNodeExecutionCreatedAt :one
SELECT created_at
FROM node_execution
WHERE node_execution_id = sqlc.arg(node_execution_id);

-- name: GetNodeExecutionStatus :one
SELECT status
FROM node_execution
WHERE node_execution_id = sqlc.arg(node_execution_id);
