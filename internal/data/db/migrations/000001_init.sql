CREATE TABLE workflow (
    workflow_id  VARCHAR(128) PRIMARY KEY,
    uid          BIGINT,
    name         VARCHAR(128) NOT NULL,
    -- 标识工作流类型，例如 template 或 AI 工作流。
    type         VARCHAR(32) NOT NULL,
    definition   JSONB NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE execution (
    execution_id           VARCHAR(64) PRIMARY KEY,
    uid                    BIGINT NOT NULL,
    idempotency_key        VARCHAR(128),
    workflow_id            VARCHAR(128) NOT NULL,
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
    FOREIGN KEY (workflow_id) REFERENCES workflow (workflow_id)
);

CREATE INDEX workflow_uid_type_idx
    ON workflow (uid, type);

CREATE INDEX execution_workflow_id_idx
    ON execution (workflow_id);

CREATE INDEX execution_uid_created_at_execution_id_idx
    ON execution (uid, created_at DESC, execution_id DESC);
