CREATE TABLE workflow_definition (
    definition_id  VARCHAR(128) PRIMARY KEY,
    uid          BIGINT,
    name         VARCHAR(128) NOT NULL,
    -- 标识工作流类型，例如 template 或 AI 工作流。
    type         VARCHAR(32) NOT NULL,
    definition   JSONB NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE workflow_execution (
    workflow_execution_id  VARCHAR(64) PRIMARY KEY,
    uid                    BIGINT NOT NULL,
    idempotency_key        VARCHAR(128),
    definition_id          VARCHAR(128) NOT NULL,
    definition             JSONB NOT NULL,
    status                 VARCHAR(16) NOT NULL,
    input                  JSONB NOT NULL,
    output                 JSONB,
    internal_error_code    INTEGER,
    internal_error_message TEXT,
    failed_node_id         VARCHAR(128),
    created_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at             TIMESTAMPTZ,
    completed_at           TIMESTAMPTZ,
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (uid, idempotency_key),
    FOREIGN KEY (definition_id) REFERENCES workflow_definition (definition_id)
);

-- 记录每次 ActivityNode 调用，包括 MCP 直接调用和 workflow DAG 节点执行。
CREATE TABLE node_execution (
    node_execution_id      VARCHAR(64) PRIMARY KEY,
    workflow_execution_id  VARCHAR(64),
    idempotency_key        VARCHAR(128) NOT NULL UNIQUE,
    node_id                VARCHAR(128),
    node_type              VARCHAR(128) NOT NULL,
    input                  JSONB NOT NULL,
    status                 VARCHAR(16) NOT NULL DEFAULT 'pending',
    output                 JSONB,
    internal_error_message TEXT,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    duration_ms            BIGINT,

    CHECK (status IN ('pending', 'running', 'completed', 'failed')),
    FOREIGN KEY (workflow_execution_id) REFERENCES workflow_execution (workflow_execution_id)
);

CREATE INDEX workflow_definition_uid_type_idx
    ON workflow_definition (uid, type);

CREATE INDEX workflow_execution_definition_id_idx
    ON workflow_execution (definition_id);

CREATE INDEX workflow_execution_uid_created_at_id_idx
    ON workflow_execution (uid, created_at DESC, workflow_execution_id DESC);

CREATE INDEX node_execution_workflow_execution_id_idx
    ON node_execution (workflow_execution_id);

CREATE INDEX node_execution_created_at_node_type_idx
    ON node_execution (created_at, node_type);
