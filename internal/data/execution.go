package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/hobbyGG/workflow/internal/biz"
	"github.com/hobbyGG/workflow/internal/data/db/sqlcgen"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
)

const (
	executionIdempotencyConstraint = "execution_uid_idempotency_key_key"
	executionWorkflowConstraint    = "execution_workflow_id_fkey"
)

type executionRepo struct {
	data *Data
}

var (
	_ biz.ExecutionRepo       = (*executionRepo)(nil)
	_ biz.ExecutionStatusRepo = (*executionRepo)(nil)
)

// NewExecutionRepo 创建 ExecutionRepo。
func NewExecutionRepo(data *Data) biz.ExecutionRepo {
	return &executionRepo{data: data}
}

// NewExecutionStatusRepo 创建 Worker 使用的 ExecutionStatusRepo。
func NewExecutionStatusRepo(data *Data) biz.ExecutionStatusRepo {
	return &executionRepo{data: data}
}

func (r *executionRepo) FindByID(ctx context.Context, executionID string) (*biz.Execution, error) {
	execution, err := r.data.queries.GetExecution(ctx, executionID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, biz.ErrExecutionNotFound
	}
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, fmt.Errorf("find execution by ID: %v: %w", err, biz.ErrExecutionInternal)
	}

	workflowTypes, err := r.data.queries.ListWorkflowTypesByIDs(ctx, []string{execution.WorkflowID})
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, fmt.Errorf("find execution workflow type: %v: %w", err, biz.ErrExecutionInternal)
	}
	if len(workflowTypes) != 1 {
		return nil, fmt.Errorf("find execution workflow type: workflow %q not found: %w", execution.WorkflowID, biz.ErrExecutionInternal)
	}
	return toBizExecution(execution, workflowTypes[0].Type)
}

func (r *executionRepo) FindByIdempotencyKey(ctx context.Context, uid int64, idempotencyKey string) (*biz.Execution, error) {
	execution, err := r.data.queries.GetExecutionByIdempotencyKey(ctx, sqlcgen.GetExecutionByIdempotencyKeyParams{
		Uid:            uid,
		IdempotencyKey: pgtype.Text{String: idempotencyKey, Valid: true},
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, biz.ErrExecutionNotFound
	}
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, fmt.Errorf("find execution by idempotency key: %v: %w", err, biz.ErrExecutionInternal)
	}
	return toBizExecution(execution, "")
}

func (r *executionRepo) ListExecutions(ctx context.Context, query biz.ListExecutionsQuery) ([]*biz.Execution, error) {
	rows, err := r.data.queries.ListExecutions(ctx, sqlcgen.ListExecutionsParams{
		Uid:          query.UID,
		WorkflowType: query.WorkflowType,
		PageLimit:    query.Limit,
		PageOffset:   query.Offset,
	})
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, fmt.Errorf("list executions: %v: %w", err, biz.ErrExecutionInternal)
	}

	executions := make([]*biz.Execution, 0, len(rows))
	for _, row := range rows {
		execution, err := toBizExecution(row.Execution, row.WorkflowType)
		if err != nil {
			return nil, err
		}
		executions = append(executions, execution)
	}
	return executions, nil
}

func (r *executionRepo) CreateExecution(ctx context.Context, execution *biz.Execution) (*biz.Execution, error) {
	params, err := newExecution(execution)
	if err != nil {
		return nil, err
	}
	created, err := r.data.queries.CreateExecution(ctx, params)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, mapCreateExecutionError(err)
	}
	return toBizExecution(created, "")
}

func (r *executionRepo) Start(ctx context.Context, executionID string, param biz.DynamicWorkflowParam) error {
	_, err := r.data.temporalClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                       executionID,
		TaskQueue:                r.data.taskQueue,
		WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		WorkflowIDReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
	}, biz.DynamicWorkflowType, param)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return fmt.Errorf("start dynamic workflow: %v: %w", err, biz.ErrExecutionInternal)
	}
	return nil
}

func (r *executionRepo) UpdateExecutionStatus(ctx context.Context, input biz.UpdateExecutionStatusInput) error {
	var err error
	switch input.Status {
	case biz.ExecutionStatusRunning:
		_, err = r.data.queries.UpdateExecutionStatusToRunning(ctx, input.ExecutionID)
	case biz.ExecutionStatusCompleted, biz.ExecutionStatusFailed, biz.ExecutionStatusCanceled:
		params, buildErr := newExecutionStatusUpdate(input)
		if buildErr != nil {
			return buildErr
		}
		_, err = r.data.queries.UpdateExecutionStatusToTerminal(ctx, params)
	default:
		return biz.ErrExecutionInvalidArgument
	}
	if err == nil {
		return nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return fmt.Errorf("update execution status: %v: %w", err, biz.ErrExecutionInternal)
	}

	currentStatus, err := r.data.queries.GetExecutionStatus(ctx, input.ExecutionID)
	if errors.Is(err, pgx.ErrNoRows) {
		return biz.ErrExecutionNotFound
	}
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return fmt.Errorf("get execution status: %v: %w", err, biz.ErrExecutionInternal)
	}
	if biz.ExecutionStatus(currentStatus) == input.Status {
		return nil
	}
	return fmt.Errorf(
		"update execution status from %q to %q: %w",
		currentStatus,
		input.Status,
		biz.ErrExecutionInvalidStatusTransition,
	)
}

func newExecutionStatusUpdate(input biz.UpdateExecutionStatusInput) (sqlcgen.UpdateExecutionStatusToTerminalParams, error) {
	params := sqlcgen.UpdateExecutionStatusToTerminalParams{
		ExecutionID: input.ExecutionID,
		Status:      string(input.Status),
	}
	if input.Output != nil {
		output, err := json.Marshal(input.Output)
		if err != nil {
			return sqlcgen.UpdateExecutionStatusToTerminalParams{}, fmt.Errorf(
				"marshal execution output: %v: %w",
				err,
				biz.ErrExecutionInvalidArgument,
			)
		}
		params.Output = output
	}
	if input.ErrorInfo == nil {
		return params, nil
	}
	if input.ErrorInfo.InternalErrorCode != nil {
		params.InternalErrorCode = pgtype.Int4{Int32: *input.ErrorInfo.InternalErrorCode, Valid: true}
	}
	if input.ErrorInfo.InternalErrorMessage != nil {
		params.InternalErrorMessage = pgtype.Text{String: *input.ErrorInfo.InternalErrorMessage, Valid: true}
	}
	if input.ErrorInfo.FailedNodeID != nil {
		params.FailedNodeID = pgtype.Text{String: *input.ErrorInfo.FailedNodeID, Valid: true}
	}
	return params, nil
}

func newExecution(execution *biz.Execution) (sqlcgen.CreateExecutionParams, error) {
	input, err := json.Marshal(execution.Input)
	if err != nil {
		return sqlcgen.CreateExecutionParams{}, fmt.Errorf("marshal execution input: %v: %w", err, biz.ErrExecutionInvalidArgument)
	}
	definition, err := json.Marshal(newDefinition(execution.Definition))
	if err != nil {
		return sqlcgen.CreateExecutionParams{}, fmt.Errorf("marshal execution definition: %v: %w", err, biz.ErrExecutionInternal)
	}
	return sqlcgen.CreateExecutionParams{
		ExecutionID:    execution.ID,
		Uid:            execution.UID,
		IdempotencyKey: pgtype.Text{String: execution.IdempotencyKey, Valid: true},
		WorkflowID:     execution.WorkflowID,
		Definition:     definition,
		Status:         string(execution.Status),
		Input:          input,
	}, nil
}

func mapCreateExecutionError(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch {
		case pgErr.Code == pgerrcode.UniqueViolation && pgErr.ConstraintName == executionIdempotencyConstraint:
			return biz.ErrExecutionAlreadyExists
		case pgErr.Code == pgerrcode.ForeignKeyViolation && pgErr.ConstraintName == executionWorkflowConstraint:
			return biz.ErrWorkflowNotFound
		}
	}
	return fmt.Errorf("create execution: %v: %w", err, biz.ErrExecutionInternal)
}

func toBizExecution(execution sqlcgen.Execution, workflowType string) (*biz.Execution, error) {
	var input map[string]any
	if err := json.Unmarshal(execution.Input, &input); err != nil {
		return nil, fmt.Errorf("unmarshal execution input: %v: %w", err, biz.ErrExecutionInternal)
	}
	var output any
	if execution.Output != nil {
		if err := json.Unmarshal(execution.Output, &output); err != nil {
			return nil, fmt.Errorf("unmarshal execution output: %v: %w", err, biz.ErrExecutionInternal)
		}
	}
	var def definition
	if err := json.Unmarshal(execution.Definition, &def); err != nil {
		return nil, fmt.Errorf("unmarshal execution definition: %v: %w", err, biz.ErrExecutionInternal)
	}

	var internalErrorCode *int32
	if execution.InternalErrorCode.Valid {
		internalErrorCode = &execution.InternalErrorCode.Int32
	}
	var internalErrorMessage *string
	if execution.InternalErrorMessage.Valid {
		internalErrorMessage = &execution.InternalErrorMessage.String
	}
	var failedNodeID *string
	if execution.FailedNodeID.Valid {
		failedNodeID = &execution.FailedNodeID.String
	}
	var errorInfo *biz.ErrorInfo
	if internalErrorCode != nil || internalErrorMessage != nil || failedNodeID != nil {
		errorInfo = &biz.ErrorInfo{
			InternalErrorCode:    internalErrorCode,
			InternalErrorMessage: internalErrorMessage,
			FailedNodeID:         failedNodeID,
		}
	}
	var startedAt *time.Time
	if execution.StartedAt.Valid {
		startedAt = &execution.StartedAt.Time
	}
	var completedAt *time.Time
	if execution.CompletedAt.Valid {
		completedAt = &execution.CompletedAt.Time
	}
	return &biz.Execution{
		ID:             execution.ExecutionID,
		UID:            execution.Uid,
		IdempotencyKey: execution.IdempotencyKey.String,
		WorkflowID:     execution.WorkflowID,
		WorkflowType:   workflowType,
		Definition:     toBizDefinition(def),
		Input:          input,
		Output:         output,
		Status:         biz.ExecutionStatus(execution.Status),
		ErrorInfo:      errorInfo,
		CreatedAt:      execution.CreatedAt.Time,
		StartedAt:      startedAt,
		CompletedAt:    completedAt,
		UpdatedAt:      execution.UpdatedAt.Time,
	}, nil
}
