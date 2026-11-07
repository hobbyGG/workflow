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
	recentNodeExecutionWindow          = 3 * 24 * time.Hour
	nodeExecutionIdempotencyConstraint = "node_execution_idempotency_key_key"
)

type nodeExecutionRepo struct {
	data *Data
}

type updateNodeExecutionStatusParam struct {
	nodeExecutionID      string
	status               biz.NodeExecutionStatus
	output               map[string]any
	internalErrorMessage *string
}

var (
	_ biz.NodeExecutionRepo       = (*nodeExecutionRepo)(nil)
	_ biz.NodeExecutionStatusRepo = (*nodeExecutionRepo)(nil)
)

// NewNodeExecutionRepo 创建 NodeExecutionRepo。
func NewNodeExecutionRepo(data *Data) biz.NodeExecutionRepo {
	return &nodeExecutionRepo{data: data}
}

// NewNodeExecutionStatusRepo 创建 Worker 使用的 NodeExecutionStatusRepo。
func NewNodeExecutionStatusRepo(data *Data) biz.NodeExecutionStatusRepo {
	return &nodeExecutionRepo{data: data}
}

func (r *nodeExecutionRepo) GetNodeExecutionInfos(ctx context.Context) (map[biz.NodeType]biz.NodeExecutionInfo, error) {
	rows, err := r.data.queries.ListNodeExecutionsSince(ctx, pgtype.Timestamptz{
		Time:  time.Now().Add(-recentNodeExecutionWindow),
		Valid: true,
	})
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, fmt.Errorf("list recent node executions: %v: %w", err, biz.ErrToolInternal)
	}
	return toBiznodeExecutionInfos(rows), nil
}

func (r *nodeExecutionRepo) GetNodeExecution(ctx context.Context, nodeExecutionID string) (*biz.NodeExecution, error) {
	execution, err := r.data.queries.GetNodeExecution(ctx, nodeExecutionID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, biz.ErrToolExecutionNotFound
	}
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, fmt.Errorf("get node execution by ID: %v: %w", err, biz.ErrToolInternal)
	}
	return toBizNodeExecution(execution)
}

func (r *nodeExecutionRepo) CreateNodeExecution(ctx context.Context, execution *biz.NodeExecution) (*biz.NodeExecution, error) {
	params, err := newNodeExecution(execution)
	if err != nil {
		return nil, err
	}

	created, err := r.data.queries.CreateNodeExecution(ctx, params)
	if err == nil {
		return toBizNodeExecution(created)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}

	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) ||
		pgErr.Code != pgerrcode.UniqueViolation ||
		pgErr.ConstraintName != nodeExecutionIdempotencyConstraint {
		return nil, fmt.Errorf("create node execution: %v: %w", err, biz.ErrToolInternal)
	}

	existing, err := r.data.queries.GetNodeExecutionByIdempotencyKey(ctx, execution.IdempotencyKey)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, fmt.Errorf("get node execution after idempotency conflict: %v: %w", err, biz.ErrToolInternal)
	}
	return toBizNodeExecution(existing)
}

func (r *nodeExecutionRepo) DispatchNodeExecution(ctx context.Context, nodeExecutionID string, param biz.ActivityNodeWorkflowParam) error {
	_, err := r.data.temporalClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                       nodeExecutionID,
		TaskQueue:                r.data.taskQueue,
		WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		WorkflowIDReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
	}, biz.ActivityNodeWorkflowType, param)
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
	return fmt.Errorf("dispatch activity node workflow: %v: %w", err, biz.ErrToolInternal)
}

func (r *nodeExecutionRepo) MarkNodeExecutionRunning(ctx context.Context, nodeExecutionID string) error {
	return r.updateNodeExecutionStatus(ctx, updateNodeExecutionStatusParam{
		nodeExecutionID: nodeExecutionID,
		status:          biz.NodeExecutionStatusRunning,
	})
}

func (r *nodeExecutionRepo) FinishNodeExecution(ctx context.Context, param biz.FinishNodeExecutionParam) error {
	return r.updateNodeExecutionStatus(ctx, updateNodeExecutionStatusParam{
		nodeExecutionID:      param.NodeExecutionID,
		status:               param.Status,
		output:               param.Output,
		internalErrorMessage: param.InternalErrorMessage,
	})
}

func (r *nodeExecutionRepo) updateNodeExecutionStatus(ctx context.Context, param updateNodeExecutionStatusParam) error {
	var err error
	switch param.status {
	case biz.NodeExecutionStatusRunning:
		_, err = r.data.queries.UpdateNodeExecutionStatusToRunning(ctx, param.nodeExecutionID)
	case biz.NodeExecutionStatusCompleted, biz.NodeExecutionStatusFailed:
		var output []byte
		if param.output != nil {
			output, err = json.Marshal(param.output)
			if err != nil {
				return fmt.Errorf("marshal node execution output: %v: %w", err, biz.ErrToolInvalidArgument)
			}
		}
		createdAt, getErr := r.data.queries.GetNodeExecutionCreatedAt(ctx, param.nodeExecutionID)
		if errors.Is(getErr, pgx.ErrNoRows) {
			return biz.ErrToolExecutionNotFound
		}
		if getErr != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			return fmt.Errorf("get node execution created time: %v: %w", getErr, biz.ErrToolInternal)
		}
		durationMS := time.Since(createdAt.Time).Milliseconds()
		if durationMS < 0 {
			durationMS = 0
		}
		duration := pgtype.Int8{Int64: durationMS, Valid: true}

		if param.status == biz.NodeExecutionStatusCompleted {
			_, err = r.data.queries.UpdateNodeExecutionStatusToCompleted(ctx, sqlcgen.UpdateNodeExecutionStatusToCompletedParams{
				Output:          output,
				DurationMs:      duration,
				NodeExecutionID: param.nodeExecutionID,
			})
		} else {
			internalErrorMessage := pgtype.Text{}
			if param.internalErrorMessage != nil {
				internalErrorMessage = pgtype.Text{String: *param.internalErrorMessage, Valid: true}
			}
			_, err = r.data.queries.UpdateNodeExecutionStatusToFailed(ctx, sqlcgen.UpdateNodeExecutionStatusToFailedParams{
				Output:               output,
				InternalErrorMessage: internalErrorMessage,
				DurationMs:           duration,
				NodeExecutionID:      param.nodeExecutionID,
			})
		}
	default:
		return biz.ErrToolInvalidArgument
	}
	if err == nil {
		return nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return fmt.Errorf("update node execution status: %v: %w", err, biz.ErrToolInternal)
	}

	currentStatus, err := r.data.queries.GetNodeExecutionStatus(ctx, param.nodeExecutionID)
	if errors.Is(err, pgx.ErrNoRows) {
		return biz.ErrToolExecutionNotFound
	}
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return fmt.Errorf("get node execution status: %v: %w", err, biz.ErrToolInternal)
	}
	if biz.NodeExecutionStatus(currentStatus) == param.status {
		return nil
	}
	return fmt.Errorf(
		"update node execution status from %q to %q: %w",
		currentStatus,
		param.status,
		biz.ErrToolExecutionInvalidStatusTransition,
	)
}

type nodeExecutionStats struct {
	completedCount int64
	terminalCount  int64
	durationTotal  int64
	durationCount  int64
}

func toBiznodeExecutionInfos(rows []sqlcgen.ListNodeExecutionsSinceRow) map[biz.NodeType]biz.NodeExecutionInfo {
	statsByNodeType := make(map[biz.NodeType]nodeExecutionStats)
	for _, row := range rows {
		status := biz.NodeExecutionStatus(row.Status)
		if status != biz.NodeExecutionStatusCompleted && status != biz.NodeExecutionStatusFailed {
			continue
		}

		nodeType := biz.NodeType(row.NodeType)
		stats := statsByNodeType[nodeType]
		stats.terminalCount++
		if status == biz.NodeExecutionStatusCompleted {
			stats.completedCount++
		}
		if row.DurationMs.Valid {
			stats.durationTotal += row.DurationMs.Int64
			stats.durationCount++
		}
		statsByNodeType[nodeType] = stats
	}

	infos := make(map[biz.NodeType]biz.NodeExecutionInfo, len(statsByNodeType))
	for nodeType, stats := range statsByNodeType {
		info := biz.NodeExecutionInfo{
			SuccessRatio: float64(stats.completedCount) / float64(stats.terminalCount),
		}
		if stats.durationCount > 0 {
			info.AvgDurationsMS = float64(stats.durationTotal) / float64(stats.durationCount)
		}
		infos[nodeType] = info
	}
	return infos
}

func newNodeExecution(execution *biz.NodeExecution) (sqlcgen.CreateNodeExecutionParams, error) {
	input, err := json.Marshal(execution.Input)
	if err != nil {
		return sqlcgen.CreateNodeExecutionParams{}, fmt.Errorf("marshal node execution input: %v: %w", err, biz.ErrToolInvalidArgument)
	}

	params := sqlcgen.CreateNodeExecutionParams{
		NodeExecutionID: execution.ID,
		IdempotencyKey:  execution.IdempotencyKey,
		NodeType:        execution.NodeType.String(),
		Input:           input,
		Status:          string(execution.Status),
	}
	if execution.WorkflowExecutionID != nil {
		params.WorkflowExecutionID = pgtype.Text{String: *execution.WorkflowExecutionID, Valid: true}
	}
	if execution.NodeID != nil {
		params.NodeID = pgtype.Text{String: *execution.NodeID, Valid: true}
	}
	return params, nil
}

func toBizNodeExecution(execution sqlcgen.NodeExecution) (*biz.NodeExecution, error) {
	var input map[string]any
	if err := json.Unmarshal(execution.Input, &input); err != nil {
		return nil, fmt.Errorf("unmarshal node execution input: %v: %w", err, biz.ErrToolInternal)
	}

	var output map[string]any
	if execution.Output != nil {
		if err := json.Unmarshal(execution.Output, &output); err != nil {
			return nil, fmt.Errorf("unmarshal node execution output: %v: %w", err, biz.ErrToolInternal)
		}
	}

	result := &biz.NodeExecution{
		ID:             execution.NodeExecutionID,
		IdempotencyKey: execution.IdempotencyKey,
		NodeType:       biz.NodeType(execution.NodeType),
		Input:          input,
		Status:         biz.NodeExecutionStatus(execution.Status),
		Output:         output,
		CreatedAt:      execution.CreatedAt.Time,
	}
	if execution.WorkflowExecutionID.Valid {
		workflowExecutionID := execution.WorkflowExecutionID.String
		result.WorkflowExecutionID = &workflowExecutionID
	}
	if execution.NodeID.Valid {
		nodeID := execution.NodeID.String
		result.NodeID = &nodeID
	}
	if execution.InternalErrorMessage.Valid {
		internalErrorMessage := execution.InternalErrorMessage.String
		result.InternalErrorMessage = &internalErrorMessage
	}
	if execution.DurationMs.Valid {
		durationMS := execution.DurationMs.Int64
		result.DurationMS = &durationMS
	}
	return result, nil
}
