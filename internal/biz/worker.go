package biz

import "context"

const (
	// UpdateWfExecutionStatusActivityType 是工作流执行状态回写 Activity 注册到 Temporal 的类型。
	UpdateWfExecutionStatusActivityType = "UpdateWorkflowExecutionStatus"
	// MarkNodeExecutionRunningActivityType 是节点执行开始状态回写 Activity 注册到 Temporal 的类型。
	MarkNodeExecutionRunningActivityType = "MarkNodeExecutionRunning"
	// FinishNodeExecutionActivityType 是节点执行终态回写 Activity 注册到 Temporal 的类型。
	FinishNodeExecutionActivityType = "FinishNodeExecution"
)

// WorkerNodeRepo 提供 Worker 运行时需要的 Activity 节点实现。
type WorkerNodeRepo interface {
	GetActivityNode(nodeType NodeType) ActivityNode
	GetActivityNodes() []ActivityNode
}

type ActivityNodeWorkflowParam struct {
	NodeExecutionID string
	NodeType        NodeType
	Input           map[string]any
}

// WfExecutionStatusRepo 提供 Worker 回写工作流执行状态的能力。
type WfExecutionStatusRepo interface {
	// MarkWfExecutionRunning 将 pending 状态的工作流执行标记为 running。
	MarkWfExecutionRunning(ctx context.Context, workflowExecutionID string) error
	// FinishWfExecution 写入工作流执行终态及其附属结果。
	FinishWfExecution(ctx context.Context, param FinishWfExecutionParam) error
}

// NodeExecutionStatusRepo 提供 Worker 回写节点执行状态的能力。
type NodeExecutionStatusRepo interface {
	// MarkNodeExecutionRunning 将 pending 状态的节点执行标记为 running。
	MarkNodeExecutionRunning(ctx context.Context, nodeExecutionID string) error
	// FinishNodeExecution 写入节点执行终态及其附属结果。
	FinishNodeExecution(ctx context.Context, param FinishNodeExecutionParam) error
}

// UpdateWfExecutionStatusParam 包含一次工作流执行状态迁移所需的数据。
type UpdateWfExecutionStatusParam struct {
	WorkflowExecutionID string
	Status              WfExecutionStatus
	Output              any
	ErrorInfo           *WfExecutionErrorInfo
}

// FinishWfExecutionParam 包含工作流执行终态需要持久化的数据。
type FinishWfExecutionParam struct {
	WorkflowExecutionID string
	Status              WfExecutionStatus
	Output              any
	ErrorInfo           *WfExecutionErrorInfo
}

// FinishNodeExecutionParam 包含节点执行终态需要持久化的数据。
type FinishNodeExecutionParam struct {
	NodeExecutionID      string
	Status               NodeExecutionStatus
	Output               map[string]any
	InternalErrorMessage *string
}

// WorkerUsecase 负责 Temporal Workflow 和 Activity 的 Worker 侧能力。
type WorkerUsecase struct {
	wfExecutionRepo   WfExecutionStatusRepo
	nodeExecutionRepo NodeExecutionStatusRepo
	nodeRepo          WorkerNodeRepo
}

// NewWorkerUsecase 创建 WorkerUsecase。
func NewWorkerUsecase(
	wfExecutionRepo WfExecutionStatusRepo,
	nodeExecutionRepo NodeExecutionStatusRepo,
	nodeRepo WorkerNodeRepo,
) *WorkerUsecase {
	return &WorkerUsecase{
		wfExecutionRepo:   wfExecutionRepo,
		nodeExecutionRepo: nodeExecutionRepo,
		nodeRepo:          nodeRepo,
	}
}

// GetActivityNodes 返回需要注册到 Temporal Worker 的 Activity 节点。
func (uc *WorkerUsecase) GetActivityNodes() []ActivityNode {
	return uc.nodeRepo.GetActivityNodes()
}

// UpdateWfExecutionStatus 回写由 DynamicWorkflow 管理的工作流执行状态。
func (uc *WorkerUsecase) UpdateWfExecutionStatus(ctx context.Context, param UpdateWfExecutionStatusParam) error {
	switch param.Status {
	case WfExecutionStatusRunning:
		return uc.wfExecutionRepo.MarkWfExecutionRunning(ctx, param.WorkflowExecutionID)
	case WfExecutionStatusCompleted, WfExecutionStatusFailed, WfExecutionStatusCanceled:
		return uc.wfExecutionRepo.FinishWfExecution(ctx, FinishWfExecutionParam{
			WorkflowExecutionID: param.WorkflowExecutionID,
			Status:              param.Status,
			Output:              param.Output,
			ErrorInfo:           param.ErrorInfo,
		})
	default:
		return ErrWfExecutionInvalidArgument
	}
}

// MarkNodeExecutionRunning 将节点执行状态从 pending 更新为 running。
func (uc *WorkerUsecase) MarkNodeExecutionRunning(ctx context.Context, nodeExecutionID string) error {
	return uc.nodeExecutionRepo.MarkNodeExecutionRunning(ctx, nodeExecutionID)
}

// FinishNodeExecution 写入节点执行终态及其附属结果。
func (uc *WorkerUsecase) FinishNodeExecution(ctx context.Context, param FinishNodeExecutionParam) error {
	switch param.Status {
	case NodeExecutionStatusCompleted, NodeExecutionStatusFailed:
		return uc.nodeExecutionRepo.FinishNodeExecution(ctx, param)
	default:
		return ErrToolInvalidArgument
	}
}
