-- name: GetWorkflowByID :one
SELECT *
FROM workflow
WHERE workflow_id = sqlc.arg(workflow_id);

-- name: ListWorkflows :many
SELECT *
FROM workflow
WHERE workflow.uid = sqlc.arg(uid)::bigint
  AND (sqlc.arg(type)::text = '' OR workflow.type = sqlc.arg(type)::text)
ORDER BY workflow.created_at DESC, workflow.workflow_id DESC
LIMIT sqlc.arg(page_limit)
OFFSET sqlc.arg(page_offset);

-- name: ListWorkflowTypesByIDs :many
SELECT workflow_id, type
FROM workflow
WHERE workflow_id = ANY(sqlc.arg(workflow_ids)::varchar[]);

-- name: CreateWorkflow :one
INSERT INTO workflow (
    workflow_id,
    uid,
    name,
    type,
    definition
)
VALUES (
    sqlc.arg(workflow_id),
    sqlc.arg(uid),
    sqlc.arg(name),
    sqlc.arg(type),
    sqlc.arg(definition)
)
RETURNING *;
