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
	// ErrWfExecutionNotFound 表示工作流执行记录不存在。
	ErrWfExecutionNotFound = errors.NotFound(v1.ErrorReason_WORKFLOW_EXECUTION_NOT_FOUND.String(), "workflow execution not found")
	// ErrWfExecutionAlreadyExists 表示工作流执行记录已存在。
	ErrWfExecutionAlreadyExists = errors.Conflict(v1.ErrorReason_WORKFLOW_EXECUTION_ALREADY_EXISTS.String(), "workflow execution already exists")
	// ErrWfExecutionInvalidArgument 表示发起工作流执行的参数无效。
	ErrWfExecutionInvalidArgument = errors.BadRequest(v1.ErrorReason_WORKFLOW_EXECUTION_INVALID_ARGUMENT.String(), "invalid workflow execution argument")
	// ErrWfExecutionInvalidStatusTransition 表示工作流执行状态不能按请求完成迁移。
	ErrWfExecutionInvalidStatusTransition = errors.Conflict(v1.ErrorReason_WORKFLOW_EXECUTION_INVALID_ARGUMENT.String(), "invalid workflow execution status transition")
	// ErrWfExecutionInternal 表示工作流执行相关的基础设施操作失败。
	ErrWfExecutionInternal = errors.InternalServer(v1.ErrorReason_WORKFLOW_EXECUTION_INTERNAL.String(), "workflow execution internal error")
)

// WfExecutionStatus 表示持久化的工作流执行状态。
type WfExecutionStatus string

const (
	// WfExecutionStatusPending 表示工作流执行记录已创建但尚未完成。
	WfExecutionStatusPending WfExecutionStatus = "pending"
	// WfExecutionStatusRunning 对应 Temporal 的 Running 状态。
	WfExecutionStatusRunning WfExecutionStatus = "running"
	// WfExecutionStatusCompleted 对应 Temporal 的 Completed 状态。
	WfExecutionStatusCompleted WfExecutionStatus = "completed"
	// WfExecutionStatusFailed 对应 Temporal 的 Failed 状态。
	WfExecutionStatusFailed WfExecutionStatus = "failed"
	// WfExecutionStatusCanceled 对应 Temporal 的 Canceled 状态。
	WfExecutionStatusCanceled WfExecutionStatus = "canceled"
	// WfExecutionStatusTerminated 对应 Temporal 的 Terminated 状态。
	WfExecutionStatusTerminated WfExecutionStatus = "terminated"
	// WfExecutionStatusContinuedAsNew 对应 Temporal 的 ContinuedAsNew 状态。
	WfExecutionStatusContinuedAsNew WfExecutionStatus = "continued_as_new"
	// WfExecutionStatusTimedOut 对应 Temporal 的 TimedOut 状态。
	WfExecutionStatusTimedOut WfExecutionStatus = "timed_out"
	// WfExecutionStatusPaused 对应 Temporal 的 Paused 状态。
	WfExecutionStatusPaused WfExecutionStatus = "paused"
)

// WfExecution 表示一次工作流执行。
type WfExecution struct {
	ID             string
	UID            int64
	IdempotencyKey string
	DefinitionID   string
	DefinitionType string
	Definition     DefinitionGraph
	Input          map[string]any
	Output         any
	Status         WfExecutionStatus
	ErrorInfo      *WfExecutionErrorInfo
	CreatedAt      time.Time
	StartedAt      *time.Time
	CompletedAt    *time.Time
	UpdatedAt      time.Time
}

// WfExecutionErrorInfo 表示工作流执行失败信息。
type WfExecutionErrorInfo struct {
	InternalErrorCode    *int32
	InternalErrorMessage *string
	FailedNodeID         *string
}

// CreateWfExecutionParam 包含发起工作流执行所需的参数。
type CreateWfExecutionParam struct {
	UID            int64
	IdempotencyKey string
	DefinitionID   string
	Input          map[string]any
}

// ListWfExecutionsQuery 包含查询工作流执行记录列表所需的参数。
type ListWfExecutionsQuery struct {
	UID            int64
	DefinitionType string
	Limit          int32
	Offset         int32
}

// WfExecutionRepo 定义工作流执行业务所需的数据访问和外部服务交互能力。
type WfExecutionRepo interface {
	GetWfExecution(ctx context.Context, workflowExecutionID string) (*WfExecution, error)
	GetWfExecutionByIdempotencyKey(ctx context.Context, uid int64, idempotencyKey string) (*WfExecution, error)
	ListWfExecutions(ctx context.Context, query ListWfExecutionsQuery) ([]*WfExecution, error)
	CreateWfExecution(ctx context.Context, wfExecution *WfExecution) (*WfExecution, error)
	// DispatchWfExecution 调度记录对应的工作流，对同一 workflowExecutionID 必须具备幂等性。
	DispatchWfExecution(ctx context.Context, workflowExecutionID string, param DynamicWorkflowParam) error
}

// WfExecutionUsecase 负责创建工作流执行记录并确保其工作流已调度。
type WfExecutionUsecase struct {
	wfExecutionRepo WfExecutionRepo
	definitionRepo  DefinitionRepo
}

// NewWfExecutionUsecase 创建 WfExecutionUsecase。
func NewWfExecutionUsecase(wfExecutionRepo WfExecutionRepo, definitionRepo DefinitionRepo) *WfExecutionUsecase {
	return &WfExecutionUsecase{
		wfExecutionRepo: wfExecutionRepo,
		definitionRepo:  definitionRepo,
	}
}

// GetWfExecution 返回指定的工作流执行记录。
func (uc *WfExecutionUsecase) GetWfExecution(ctx context.Context, workflowExecutionID string) (*WfExecution, error) {
	return uc.wfExecutionRepo.GetWfExecution(ctx, workflowExecutionID)
}

// ListWfExecutions 返回满足查询条件的工作流执行记录。
func (uc *WfExecutionUsecase) ListWfExecutions(ctx context.Context, query ListWfExecutionsQuery) ([]*WfExecution, error) {
	if query.UID <= 0 || query.Limit <= 0 || query.Limit > 200 || query.Offset < 0 {
		return nil, ErrWfExecutionInvalidArgument
	}
	query.DefinitionType = strings.TrimSpace(query.DefinitionType)
	return uc.wfExecutionRepo.ListWfExecutions(ctx, query)
}

// CreateWfExecution 创建或复用工作流执行记录，并确保其工作流已调度。
func (uc *WfExecutionUsecase) CreateWfExecution(ctx context.Context, param *CreateWfExecutionParam) (*WfExecution, error) {
	if param == nil ||
		param.UID <= 0 ||
		strings.TrimSpace(param.IdempotencyKey) == "" ||
		strings.TrimSpace(param.DefinitionID) == "" {
		return nil, ErrWfExecutionInvalidArgument
	}

	wfExecution, err := uc.wfExecutionRepo.GetWfExecutionByIdempotencyKey(ctx, param.UID, param.IdempotencyKey)
	if errors.Is(err, ErrWfExecutionNotFound) {
		definition, findErr := uc.definitionRepo.GetDefinition(ctx, param.DefinitionID)
		if findErr != nil {
			return nil, findErr
		}
		if definition.UID != param.UID {
			return nil, ErrDefinitionNotFound
		}
		wfExecution, err = uc.wfExecutionRepo.CreateWfExecution(ctx, &WfExecution{
			ID:             uuid.NewString(),
			UID:            param.UID,
			IdempotencyKey: param.IdempotencyKey,
			DefinitionID:   param.DefinitionID,
			Definition:     definition.DefGraph,
			Input:          param.Input,
			Status:         WfExecutionStatusPending,
		})
		if errors.Is(err, ErrWfExecutionAlreadyExists) {
			wfExecution, err = uc.wfExecutionRepo.GetWfExecutionByIdempotencyKey(ctx, param.UID, param.IdempotencyKey)
		}
	}
	if err != nil {
		return nil, err
	}

	if err := uc.wfExecutionRepo.DispatchWfExecution(ctx, wfExecution.ID, DynamicWorkflowParam{
		Definition: wfExecution.Definition,
	}); err != nil {
		return nil, err
	}
	return wfExecution, nil
}

// ExecuteActivityNode 发起只执行一个 ActivityNode 的固定 Workflow。
// NodeExecution 由该 Workflow 在执行过程中创建和维护。
func (uc *WfExecutionUsecase) ExecuteActivityNode(
	ctx context.Context,
	nodeType NodeType,
	nodeInput map[string]any,
	idempotencyKey string,
) (string, error) {
	// TODO: 实现固定单节点 Workflow 的发起逻辑。
	return "", nil
}
