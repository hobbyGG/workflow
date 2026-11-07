-- name: GetWorkflowExecution :one
SELECT *
FROM workflow_execution
WHERE workflow_execution_id = sqlc.arg(workflow_execution_id);

-- name: GetWorkflowExecutionByIdempotencyKey :one
SELECT *
FROM workflow_execution
WHERE uid = sqlc.arg(uid)
  AND idempotency_key = sqlc.arg(idempotency_key);

-- name: ListWorkflowExecutions :many
SELECT sqlc.embed(workflow_execution), workflow_definition.type AS definition_type
FROM workflow_execution
JOIN workflow_definition ON workflow_definition.definition_id = workflow_execution.definition_id
                        AND workflow_definition.uid = workflow_execution.uid
WHERE workflow_execution.uid = sqlc.arg(uid)
  AND (sqlc.arg(definition_type)::text = '' OR workflow_definition.type = sqlc.arg(definition_type)::text)
ORDER BY workflow_execution.created_at DESC, workflow_execution.workflow_execution_id DESC
LIMIT sqlc.arg(page_limit)
OFFSET sqlc.arg(page_offset);

-- name: CreateWorkflowExecution :one
INSERT INTO workflow_execution (
    workflow_execution_id,
    uid,
    idempotency_key,
    definition_id,
    definition,
    status,
    input
) VALUES (
    sqlc.arg(workflow_execution_id),
    sqlc.arg(uid),
    sqlc.arg(idempotency_key),
    sqlc.arg(definition_id),
    sqlc.arg(definition),
    sqlc.arg(status),
    sqlc.arg(input)
)
RETURNING *;

-- name: UpdateWorkflowExecutionStatusToRunning :one
UPDATE workflow_execution
SET status = 'running',
    started_at = now(),
    updated_at = now()
WHERE workflow_execution_id = sqlc.arg(workflow_execution_id)
  AND status = 'pending'
RETURNING workflow_execution_id;

-- name: UpdateWorkflowExecutionStatusToTerminal :one
UPDATE workflow_execution
SET status = sqlc.arg(status),
    output = sqlc.narg(output),
    internal_error_code = sqlc.narg(internal_error_code),
    internal_error_message = sqlc.narg(internal_error_message),
    failed_node_id = sqlc.narg(failed_node_id),
    completed_at = now(),
    updated_at = now()
WHERE workflow_execution_id = sqlc.arg(workflow_execution_id)
  AND status IN ('pending', 'running')
RETURNING workflow_execution_id;

-- name: GetWorkflowExecutionStatus :one
SELECT status
FROM workflow_execution
WHERE workflow_execution_id = sqlc.arg(workflow_execution_id);
