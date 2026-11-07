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
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
)

const (
	workflowExecutionIdempotencyConstraint = "workflow_execution_uid_idempotency_key_key"
	workflowExecutionDefinitionConstraint  = "workflow_execution_definition_id_fkey"
)

type wfExecutionRepo struct {
	data *Data
}

var (
	_ biz.WfExecutionRepo       = (*wfExecutionRepo)(nil)
	_ biz.WfExecutionStatusRepo = (*wfExecutionRepo)(nil)
)

// NewWfExecutionRepo 创建 WfExecutionRepo。
func NewWfExecutionRepo(data *Data) biz.WfExecutionRepo {
	return &wfExecutionRepo{data: data}
}

// NewWfExecutionStatusRepo 创建 Worker 使用的 WfExecutionStatusRepo。
func NewWfExecutionStatusRepo(data *Data) biz.WfExecutionStatusRepo {
	return &wfExecutionRepo{data: data}
}

func (r *wfExecutionRepo) GetWfExecution(ctx context.Context, workflowExecutionID string) (*biz.WfExecution, error) {
	workflowExecution, err := r.data.queries.GetWorkflowExecution(ctx, workflowExecutionID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, biz.ErrWfExecutionNotFound
	}
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, fmt.Errorf("get workflow execution by ID: %v: %w", err, biz.ErrWfExecutionInternal)
	}

	definitionTypes, err := r.data.queries.ListDefinitionTypesByIDs(ctx, []string{workflowExecution.DefinitionID})
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, fmt.Errorf("get workflow execution definition type: %v: %w", err, biz.ErrWfExecutionInternal)
	}
	if len(definitionTypes) != 1 {
		return nil, fmt.Errorf("get workflow execution definition type: definition %q not found: %w", workflowExecution.DefinitionID, biz.ErrWfExecutionInternal)
	}
	return toBizWfExecution(workflowExecution, definitionTypes[0].Type)
}

func (r *wfExecutionRepo) GetWfExecutionByIdempotencyKey(ctx context.Context, uid int64, idempotencyKey string) (*biz.WfExecution, error) {
	workflowExecution, err := r.data.queries.GetWorkflowExecutionByIdempotencyKey(ctx, sqlcgen.GetWorkflowExecutionByIdempotencyKeyParams{
		Uid:            uid,
		IdempotencyKey: pgtype.Text{String: idempotencyKey, Valid: true},
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, biz.ErrWfExecutionNotFound
	}
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, fmt.Errorf("get workflow execution by idempotency key: %v: %w", err, biz.ErrWfExecutionInternal)
	}
	return toBizWfExecution(workflowExecution, "")
}

func (r *wfExecutionRepo) ListWfExecutions(ctx context.Context, query biz.ListWfExecutionsQuery) ([]*biz.WfExecution, error) {
	rows, err := r.data.queries.ListWorkflowExecutions(ctx, sqlcgen.ListWorkflowExecutionsParams{
		Uid:            query.UID,
		DefinitionType: query.DefinitionType,
		PageLimit:      query.Limit,
		PageOffset:     query.Offset,
	})
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, fmt.Errorf("list workflow executions: %v: %w", err, biz.ErrWfExecutionInternal)
	}

	wfExecutions := make([]*biz.WfExecution, 0, len(rows))
	for _, row := range rows {
		wfExecution, err := toBizWfExecution(row.WorkflowExecution, row.DefinitionType)
		if err != nil {
			return nil, err
		}
		wfExecutions = append(wfExecutions, wfExecution)
	}
	return wfExecutions, nil
}

func (r *wfExecutionRepo) CreateWfExecution(ctx context.Context, wfExecution *biz.WfExecution) (*biz.WfExecution, error) {
	params, err := newWorkflowExecution(wfExecution)
	if err != nil {
		return nil, err
	}
	created, err := r.data.queries.CreateWorkflowExecution(ctx, params)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, mapCreateWorkflowExecutionError(err)
	}
	return toBizWfExecution(created, "")
}

func (r *wfExecutionRepo) DispatchWfExecution(ctx context.Context, workflowExecutionID string, param biz.DynamicWorkflowParam) error {
	_, err := r.data.temporalClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                       workflowExecutionID,
		TaskQueue:                r.data.taskQueue,
		WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		WorkflowIDReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
	}, biz.DynamicWorkflowType, param)
	if err == nil {
		return nil
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}

	var alreadyStarted *serviceerror.WorkflowExecutionAlreadyStarted
	if errors.As(err, &alreadyStarted) {
		return nil
	}
	return fmt.Errorf("dispatch dynamic workflow: %v: %w", err, biz.ErrWfExecutionInternal)
}

func (r *wfExecutionRepo) MarkWfExecutionRunning(ctx context.Context, workflowExecutionID string) error {
	return r.updateWfExecutionStatus(ctx, biz.UpdateWfExecutionStatusParam{
		WorkflowExecutionID: workflowExecutionID,
		Status:              biz.WfExecutionStatusRunning,
	})
}

func (r *wfExecutionRepo) FinishWfExecution(ctx context.Context, param biz.FinishWfExecutionParam) error {
	return r.updateWfExecutionStatus(ctx, biz.UpdateWfExecutionStatusParam{
		WorkflowExecutionID: param.WorkflowExecutionID,
		Status:              param.Status,
		Output:              param.Output,
		ErrorInfo:           param.ErrorInfo,
	})
}

func (r *wfExecutionRepo) updateWfExecutionStatus(ctx context.Context, param biz.UpdateWfExecutionStatusParam) error {
	var err error
	switch param.Status {
	case biz.WfExecutionStatusRunning:
		_, err = r.data.queries.UpdateWorkflowExecutionStatusToRunning(ctx, param.WorkflowExecutionID)
	case biz.WfExecutionStatusCompleted, biz.WfExecutionStatusFailed, biz.WfExecutionStatusCanceled:
		params, buildErr := newFinishWfExecutionParams(param)
		if buildErr != nil {
			return buildErr
		}
		_, err = r.data.queries.UpdateWorkflowExecutionStatusToTerminal(ctx, params)
	default:
		return biz.ErrWfExecutionInvalidArgument
	}
	if err == nil {
		return nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return fmt.Errorf("update workflow execution status: %v: %w", err, biz.ErrWfExecutionInternal)
	}

	currentStatus, err := r.data.queries.GetWorkflowExecutionStatus(ctx, param.WorkflowExecutionID)
	if errors.Is(err, pgx.ErrNoRows) {
		return biz.ErrWfExecutionNotFound
	}
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return fmt.Errorf("get workflow execution status: %v: %w", err, biz.ErrWfExecutionInternal)
	}
	if biz.WfExecutionStatus(currentStatus) == param.Status {
		return nil
	}
	return fmt.Errorf(
		"update workflow execution status from %q to %q: %w",
		currentStatus,
		param.Status,
		biz.ErrWfExecutionInvalidStatusTransition,
	)
}

func newFinishWfExecutionParams(param biz.UpdateWfExecutionStatusParam) (sqlcgen.UpdateWorkflowExecutionStatusToTerminalParams, error) {
	params := sqlcgen.UpdateWorkflowExecutionStatusToTerminalParams{
		WorkflowExecutionID: param.WorkflowExecutionID,
		Status:              string(param.Status),
	}
	if param.Output != nil {
		output, err := json.Marshal(param.Output)
		if err != nil {
			return sqlcgen.UpdateWorkflowExecutionStatusToTerminalParams{}, fmt.Errorf(
				"marshal workflow execution output: %v: %w",
				err,
				biz.ErrWfExecutionInvalidArgument,
			)
		}
		params.Output = output
	}
	if param.ErrorInfo == nil {
		return params, nil
	}
	if param.ErrorInfo.InternalErrorCode != nil {
		params.InternalErrorCode = pgtype.Int4{Int32: *param.ErrorInfo.InternalErrorCode, Valid: true}
	}
	if param.ErrorInfo.InternalErrorMessage != nil {
		params.InternalErrorMessage = pgtype.Text{String: *param.ErrorInfo.InternalErrorMessage, Valid: true}
	}
	if param.ErrorInfo.FailedNodeID != nil {
		params.FailedNodeID = pgtype.Text{String: *param.ErrorInfo.FailedNodeID, Valid: true}
	}
	return params, nil
}

func newWorkflowExecution(wfExecution *biz.WfExecution) (sqlcgen.CreateWorkflowExecutionParams, error) {
	input, err := json.Marshal(wfExecution.Input)
	if err != nil {
		return sqlcgen.CreateWorkflowExecutionParams{}, fmt.Errorf("marshal workflow execution input: %v: %w", err, biz.ErrWfExecutionInvalidArgument)
	}
	definition, err := json.Marshal(newDefinition(wfExecution.Definition))
	if err != nil {
		return sqlcgen.CreateWorkflowExecutionParams{}, fmt.Errorf("marshal workflow execution definition: %v: %w", err, biz.ErrWfExecutionInternal)
	}
	return sqlcgen.CreateWorkflowExecutionParams{
		WorkflowExecutionID: wfExecution.ID,
		Uid:                 wfExecution.UID,
		IdempotencyKey:      pgtype.Text{String: wfExecution.IdempotencyKey, Valid: true},
		DefinitionID:        wfExecution.DefinitionID,
		Definition:          definition,
		Status:              string(wfExecution.Status),
		Input:               input,
	}, nil
}

func mapCreateWorkflowExecutionError(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch {
		case pgErr.Code == pgerrcode.UniqueViolation && pgErr.ConstraintName == workflowExecutionIdempotencyConstraint:
			return biz.ErrWfExecutionAlreadyExists
		case pgErr.Code == pgerrcode.ForeignKeyViolation && pgErr.ConstraintName == workflowExecutionDefinitionConstraint:
			return biz.ErrDefinitionNotFound
		}
	}
	return fmt.Errorf("create workflow execution: %v: %w", err, biz.ErrWfExecutionInternal)
}

func toBizWfExecution(workflowExecution sqlcgen.WorkflowExecution, definitionType string) (*biz.WfExecution, error) {
	var input map[string]any
	if err := json.Unmarshal(workflowExecution.Input, &input); err != nil {
		return nil, fmt.Errorf("unmarshal workflow execution input: %v: %w", err, biz.ErrWfExecutionInternal)
	}
	var output any
	if workflowExecution.Output != nil {
		if err := json.Unmarshal(workflowExecution.Output, &output); err != nil {
			return nil, fmt.Errorf("unmarshal workflow execution output: %v: %w", err, biz.ErrWfExecutionInternal)
		}
	}
	var def definition
	if err := json.Unmarshal(workflowExecution.Definition, &def); err != nil {
		return nil, fmt.Errorf("unmarshal workflow execution definition: %v: %w", err, biz.ErrWfExecutionInternal)
	}

	var internalErrorCode *int32
	if workflowExecution.InternalErrorCode.Valid {
		internalErrorCode = &workflowExecution.InternalErrorCode.Int32
	}
	var internalErrorMessage *string
	if workflowExecution.InternalErrorMessage.Valid {
		internalErrorMessage = &workflowExecution.InternalErrorMessage.String
	}
	var failedNodeID *string
	if workflowExecution.FailedNodeID.Valid {
		failedNodeID = &workflowExecution.FailedNodeID.String
	}
	var errorInfo *biz.WfExecutionErrorInfo
	if internalErrorCode != nil || internalErrorMessage != nil || failedNodeID != nil {
		errorInfo = &biz.WfExecutionErrorInfo{
			InternalErrorCode:    internalErrorCode,
			InternalErrorMessage: internalErrorMessage,
			FailedNodeID:         failedNodeID,
		}
	}
	var startedAt *time.Time
	if workflowExecution.StartedAt.Valid {
		startedAt = &workflowExecution.StartedAt.Time
	}
	var completedAt *time.Time
	if workflowExecution.CompletedAt.Valid {
		completedAt = &workflowExecution.CompletedAt.Time
	}
	return &biz.WfExecution{
		ID:             workflowExecution.WorkflowExecutionID,
		UID:            workflowExecution.Uid,
		IdempotencyKey: workflowExecution.IdempotencyKey.String,
		DefinitionID:   workflowExecution.DefinitionID,
		DefinitionType: definitionType,
		Definition:     toBizDefinition(def),
		Input:          input,
		Output:         output,
		Status:         biz.WfExecutionStatus(workflowExecution.Status),
		ErrorInfo:      errorInfo,
		CreatedAt:      workflowExecution.CreatedAt.Time,
		StartedAt:      startedAt,
		CompletedAt:    completedAt,
		UpdatedAt:      workflowExecution.UpdatedAt.Time,
	}, nil
}
