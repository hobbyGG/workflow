-- name: GetDefinition :one
SELECT *
FROM workflow_definition
WHERE definition_id = sqlc.arg(definition_id);

-- name: ListDefinitions :many
SELECT *
FROM workflow_definition
WHERE workflow_definition.uid = sqlc.arg(uid)::bigint
  AND (sqlc.arg(type)::text = '' OR workflow_definition.type = sqlc.arg(type)::text)
ORDER BY workflow_definition.created_at DESC, workflow_definition.definition_id DESC
LIMIT sqlc.arg(page_limit)
OFFSET sqlc.arg(page_offset);

-- name: ListDefinitionTypesByIDs :many
SELECT definition_id, type
FROM workflow_definition
WHERE definition_id = ANY(sqlc.arg(definition_ids)::varchar[]);

-- name: CreateDefinition :one
INSERT INTO workflow_definition (
    definition_id,
    uid,
    name,
    type,
    definition
)
VALUES (
    sqlc.arg(definition_id),
    sqlc.arg(uid),
    sqlc.arg(name),
    sqlc.arg(type),
    sqlc.arg(definition)
)
RETURNING *;
