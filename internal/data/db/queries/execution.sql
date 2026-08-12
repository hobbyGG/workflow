-- name: GetExecution :one
SELECT *
FROM execution
WHERE execution_id = sqlc.arg(execution_id);

-- name: GetExecutionByIdempotencyKey :one
SELECT *
FROM execution
WHERE uid = sqlc.arg(uid)
  AND idempotency_key = sqlc.arg(idempotency_key);

-- name: ListExecutions :many
SELECT sqlc.embed(execution), workflow.type AS workflow_type
FROM execution
JOIN workflow ON workflow.workflow_id = execution.workflow_id
             AND workflow.uid = execution.uid
WHERE execution.uid = sqlc.arg(uid)
  AND (sqlc.arg(workflow_type)::text = '' OR workflow.type = sqlc.arg(workflow_type)::text)
ORDER BY execution.created_at DESC, execution.execution_id DESC
LIMIT sqlc.arg(page_limit)
OFFSET sqlc.arg(page_offset);

-- name: CreateExecution :one
INSERT INTO execution (
    execution_id,
    uid,
    idempotency_key,
    workflow_id,
    definition,
    status,
    input
) VALUES (
    sqlc.arg(execution_id),
    sqlc.arg(uid),
    sqlc.arg(idempotency_key),
    sqlc.arg(workflow_id),
    sqlc.arg(definition),
    sqlc.arg(status),
    sqlc.arg(input)
)
RETURNING *;

-- name: UpdateExecutionStatusToRunning :one
UPDATE execution
SET status = 'running',
    started_at = now(),
    updated_at = now()
WHERE execution_id = sqlc.arg(execution_id)
  AND status = 'pending'
RETURNING execution_id;

-- name: UpdateExecutionStatusToTerminal :one
UPDATE execution
SET status = sqlc.arg(status),
    output = sqlc.narg(output),
    internal_error_code = sqlc.narg(internal_error_code),
    internal_error_message = sqlc.narg(internal_error_message),
    failed_node_id = sqlc.narg(failed_node_id),
    completed_at = now(),
    updated_at = now()
WHERE execution_id = sqlc.arg(execution_id)
  AND status IN ('pending', 'running')
RETURNING execution_id;

-- name: GetExecutionStatus :one
SELECT status
FROM execution
WHERE execution_id = sqlc.arg(execution_id);
