package biz

import (
	"context"
	"strings"
	"time"

	v1 "github.com/hobbyGG/workflow/api/workflow/v1"

	"github.com/go-kratos/kratos/v3/errors"
	"github.com/google/uuid"
)

var (
	// ErrExecutionNotFound 表示执行记录不存在。
	ErrExecutionNotFound = errors.NotFound(v1.ErrorReason_EXECUTION_NOT_FOUND.String(), "execution not found")
	// ErrExecutionAlreadyExists 表示执行记录已存在。
	ErrExecutionAlreadyExists = errors.Conflict(v1.ErrorReason_EXECUTION_ALREADY_EXISTS.String(), "execution already exists")
	// ErrExecutionInvalidArgument 表示发起执行的参数无效。
	ErrExecutionInvalidArgument = errors.BadRequest(v1.ErrorReason_EXECUTION_INVALID_ARGUMENT.String(), "invalid execution argument")
	// ErrExecutionInvalidStatusTransition 表示执行状态不能按请求完成迁移。
	ErrExecutionInvalidStatusTransition = errors.Conflict(v1.ErrorReason_EXECUTION_INVALID_ARGUMENT.String(), "invalid execution status transition")
	// ErrExecutionInternal 表示执行相关的基础设施操作失败。
	ErrExecutionInternal = errors.InternalServer(v1.ErrorReason_EXECUTION_INTERNAL.String(), "execution internal error")
)

// ExecutionStatus 表示持久化的执行状态。
type ExecutionStatus string

const (
	// ExecutionStatusPending 表示执行记录已创建但尚未完成。
	ExecutionStatusPending ExecutionStatus = "pending"
	// ExecutionStatusRunning 对应 Temporal 的 Running 状态。
	ExecutionStatusRunning ExecutionStatus = "running"
	// ExecutionStatusCompleted 对应 Temporal 的 Completed 状态。
	ExecutionStatusCompleted ExecutionStatus = "completed"
	// ExecutionStatusFailed 对应 Temporal 的 Failed 状态。
	ExecutionStatusFailed ExecutionStatus = "failed"
	// ExecutionStatusCanceled 对应 Temporal 的 Canceled 状态。
	ExecutionStatusCanceled ExecutionStatus = "canceled"
	// ExecutionStatusTerminated 对应 Temporal 的 Terminated 状态。
	ExecutionStatusTerminated ExecutionStatus = "terminated"
	// ExecutionStatusContinuedAsNew 对应 Temporal 的 ContinuedAsNew 状态。
	ExecutionStatusContinuedAsNew ExecutionStatus = "continued_as_new"
	// ExecutionStatusTimedOut 对应 Temporal 的 TimedOut 状态。
	ExecutionStatusTimedOut ExecutionStatus = "timed_out"
	// ExecutionStatusPaused 对应 Temporal 的 Paused 状态。
	ExecutionStatusPaused ExecutionStatus = "paused"
)

// Execution 表示一次工作流执行。
type Execution struct {
	ID             string
	UID            int64
	IdempotencyKey string
	WorkflowID     string
	WorkflowType   string
	Definition     DefinitionGraph
	Input          map[string]any
	Output         any
	Status         ExecutionStatus
	ErrorInfo      *ErrorInfo
	CreatedAt      time.Time
	StartedAt      *time.Time
	CompletedAt    *time.Time
	UpdatedAt      time.Time
}

// ErrorInfo 表示执行失败信息。
type ErrorInfo struct {
	InternalErrorCode    *int32
	InternalErrorMessage *string
	FailedNodeID         *string
}

// CreateExecutionInput 包含发起执行所需的参数。
type CreateExecutionInput struct {
	UID            int64
	IdempotencyKey string
	WorkflowID     string
	Input          map[string]any
}

// ListExecutionsQuery 包含查询执行记录列表所需的参数。
type ListExecutionsQuery struct {
	UID          int64
	WorkflowType string
	Limit        int32
	Offset       int32
}

// ExecutionRepo 定义 execution 业务所需的数据访问和外部服务交互能力。
type ExecutionRepo interface {
	FindByID(ctx context.Context, executionID string) (*Execution, error)
	FindByIdempotencyKey(ctx context.Context, uid int64, idempotencyKey string) (*Execution, error)
	ListExecutions(ctx context.Context, query ListExecutionsQuery) ([]*Execution, error)
	CreateExecution(ctx context.Context, execution *Execution) (*Execution, error)
	// Start 启动执行记录对应的工作流，对同一 executionID 必须具备幂等性。
	Start(ctx context.Context, executionID string, param DynamicWorkflowParam) error
}

// ExecutionUsecase 负责创建执行记录并确保其工作流已启动。
type ExecutionUsecase struct {
	executionRepo ExecutionRepo
	workflowRepo  WorkflowRepo
}

// NewExecutionUsecase 创建 ExecutionUsecase。
func NewExecutionUsecase(executionRepo ExecutionRepo, workflowRepo WorkflowRepo) *ExecutionUsecase {
	return &ExecutionUsecase{
		executionRepo: executionRepo,
		workflowRepo:  workflowRepo,
	}
}

// GetExecution 返回指定的执行记录。
func (uc *ExecutionUsecase) GetExecution(ctx context.Context, executionID string) (*Execution, error) {
	return uc.executionRepo.FindByID(ctx, executionID)
}

// ListExecutions 返回满足查询条件的执行记录。
func (uc *ExecutionUsecase) ListExecutions(ctx context.Context, query ListExecutionsQuery) ([]*Execution, error) {
	if query.UID <= 0 || query.Limit <= 0 || query.Limit > 200 || query.Offset < 0 {
		return nil, ErrExecutionInvalidArgument
	}
	query.WorkflowType = strings.TrimSpace(query.WorkflowType)
	return uc.executionRepo.ListExecutions(ctx, query)
}

// CreateExecution 创建或复用执行记录，并确保其工作流已启动。
func (uc *ExecutionUsecase) CreateExecution(ctx context.Context, input *CreateExecutionInput) (*Execution, error) {
	if input == nil ||
		input.UID <= 0 ||
		strings.TrimSpace(input.IdempotencyKey) == "" ||
		strings.TrimSpace(input.WorkflowID) == "" {
		return nil, ErrExecutionInvalidArgument
	}

	execution, err := uc.executionRepo.FindByIdempotencyKey(ctx, input.UID, input.IdempotencyKey)
	if errors.Is(err, ErrExecutionNotFound) {
		workflowDefinition, findErr := uc.workflowRepo.FindByID(ctx, input.WorkflowID)
		if findErr != nil {
			return nil, findErr
		}
		if workflowDefinition.UID != input.UID {
			return nil, ErrWorkflowNotFound
		}
		execution, err = uc.executionRepo.CreateExecution(ctx, &Execution{
			ID:             uuid.NewString(),
			UID:            input.UID,
			IdempotencyKey: input.IdempotencyKey,
			WorkflowID:     input.WorkflowID,
			Definition:     workflowDefinition.Definition,
			Input:          input.Input,
			Status:         ExecutionStatusPending,
		})
		if errors.Is(err, ErrExecutionAlreadyExists) {
			execution, err = uc.executionRepo.FindByIdempotencyKey(ctx, input.UID, input.IdempotencyKey)
		}
	}
	if err != nil {
		return nil, err
	}

	if err := uc.executionRepo.Start(ctx, execution.ID, DynamicWorkflowParam{
		Definition: execution.Definition,
	}); err != nil {
		return nil, err
	}
	return execution, nil
}
