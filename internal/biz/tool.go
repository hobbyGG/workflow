package biz

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"time"

	workflowv1 "github.com/hobbyGG/workflow/api/workflow/v1"

	"github.com/go-kratos/kratos/v3/errors"
	"github.com/google/uuid"
)

var (
	// ErrToolInvalidArgument 表示工具调用参数无效。
	ErrToolInvalidArgument = errors.BadRequest(workflowv1.ErrorReason_TOOL_INVALID_ARGUMENT.String(), "invalid tool argument")
	// ErrToolExecutionNotFound 表示工具执行记录不存在。
	ErrToolExecutionNotFound = errors.NotFound(workflowv1.ErrorReason_TOOL_EXECUTION_NOT_FOUND.String(), "tool execution not found")
	// ErrToolInternal 表示工具相关的内部处理失败。
	ErrToolInternal = errors.InternalServer(workflowv1.ErrorReason_TOOL_INTERNAL.String(), "tool internal error")
	// ErrToolExecutionConflict 表示幂等键已被另一组工具调用参数占用。
	ErrToolExecutionConflict = errors.Conflict(workflowv1.ErrorReason_TOOL_INVALID_ARGUMENT.String(), "idempotency key conflicts with another tool execution")
	// ErrToolExecutionInvalidStatusTransition 表示工具执行状态不能按请求完成迁移。
	ErrToolExecutionInvalidStatusTransition = errors.Conflict(workflowv1.ErrorReason_TOOL_INVALID_ARGUMENT.String(), "invalid tool execution status transition")
	// ErrToolExecutionNotImplemented 表示工具执行流程尚未实现。
	ErrToolExecutionNotImplemented = errors.New(501, workflowv1.ErrorReason_TOOL_EXECUTION_NOT_IMPLEMENTED.String(), "tool execution is not implemented")
)

// NodeExecutionStatus 表示节点执行状态。
type NodeExecutionStatus string

const (
	NodeExecutionStatusPending   NodeExecutionStatus = "pending"
	NodeExecutionStatusRunning   NodeExecutionStatus = "running"
	NodeExecutionStatusCompleted NodeExecutionStatus = "completed"
	NodeExecutionStatusFailed    NodeExecutionStatus = "failed"
)

// NodeExecution 表示一次 ActivityNode 逻辑调用。
type NodeExecution struct {
	ID                   string
	WorkflowExecutionID  *string
	IdempotencyKey       string
	NodeID               *string
	NodeType             NodeType
	Input                map[string]any
	Status               NodeExecutionStatus
	Output               map[string]any
	InternalErrorMessage *string
	CreatedAt            time.Time
	DurationMS           *int64
}

type NodeExecutionInfo struct {
	AvgDurationsMS float64
	SuccessRatio   float64
}

// ToolInfo 描述可独立执行的 ActivityNode 及其历史执行画像。
type ToolInfo struct {
	Schema  NodeSchema
	Profile *NodeExecutionInfo
}

// ExecuteToolParam 包含一次工具执行所需的参数。
type ExecuteToolParam struct {
	NodeType       NodeType
	Input          map[string]any
	IdempotencyKey string
}

type NodeExecutionRepo interface {
	GetNodeExecutionInfos(ctx context.Context) (map[NodeType]NodeExecutionInfo, error)
	GetNodeExecution(ctx context.Context, nodeExecutionID string) (*NodeExecution, error)
	// CreateNodeExecution 创建执行记录，或按幂等键返回已存在的非空规范记录。
	CreateNodeExecution(ctx context.Context, nodeExecution *NodeExecution) (*NodeExecution, error)
	// DispatchNodeExecution 调度记录对应的单节点工作流，对同一 nodeExecutionID 必须具备幂等性。
	DispatchNodeExecution(ctx context.Context, nodeExecutionID string, param ActivityNodeWorkflowParam) error
}

// ToolUsecase 提供工具相关业务能力。
type ToolUsecase struct {
	nodeRepo          NodeRepo
	nodeExecutionRepo NodeExecutionRepo
}

// NewToolUsecase 创建 ToolUsecase。
func NewToolUsecase(nodeRepo NodeRepo, nodeExecutionRepo NodeExecutionRepo) *ToolUsecase {
	return &ToolUsecase{
		nodeRepo:          nodeRepo,
		nodeExecutionRepo: nodeExecutionRepo,
	}
}

// ListTools 返回可独立执行的 ActivityNode，并按节点类型关联历史执行画像。
func (uc *ToolUsecase) ListTools(ctx context.Context) ([]ToolInfo, error) {
	nodes := uc.nodeRepo.GetActivityNodes()
	executionInfos, err := uc.nodeExecutionRepo.GetNodeExecutionInfos(ctx)
	if err != nil {
		return nil, err
	}

	tools := make([]ToolInfo, 0, len(nodes))
	for _, node := range nodes {
		tool := ToolInfo{Schema: node.Schema()}
		if executionInfo, ok := executionInfos[node.Type()]; ok {
			tool.Profile = &executionInfo
		}
		tools = append(tools, tool)
	}
	return tools, nil
}

// ExecuteTool 创建或复用工具执行，并确保其单节点工作流已调度。
func (uc *ToolUsecase) ExecuteTool(ctx context.Context, param *ExecuteToolParam) (*NodeExecution, error) {
	if param == nil {
		return nil, ErrToolInvalidArgument
	}
	validNodeType := false
	for _, node := range uc.nodeRepo.GetActivityNodes() {
		if node.Type() == param.NodeType {
			validNodeType = true
			break
		}
	}
	if !validNodeType {
		return nil, ErrToolInvalidArgument
	}
	nodeInput := param.Input
	if nodeInput == nil {
		nodeInput = map[string]any{}
	}
	requestedInput, err := json.Marshal(nodeInput)
	if err != nil {
		return nil, ErrToolInvalidArgument
	}

	execution, err := uc.nodeExecutionRepo.CreateNodeExecution(ctx, &NodeExecution{
		ID:             uuid.NewString(),
		IdempotencyKey: param.IdempotencyKey,
		NodeType:       param.NodeType,
		Input:          nodeInput,
		Status:         NodeExecutionStatusPending,
	})
	if err != nil {
		return nil, err
	}
	existingInput := execution.Input
	if existingInput == nil {
		existingInput = map[string]any{}
	}
	canonicalInput, err := json.Marshal(existingInput)
	if err != nil {
		return nil, ErrToolInternal
	}
	if execution.NodeType != param.NodeType || !bytes.Equal(canonicalInput, requestedInput) {
		return nil, ErrToolExecutionConflict
	}
	if err := uc.nodeExecutionRepo.DispatchNodeExecution(ctx, execution.ID, ActivityNodeWorkflowParam{
		NodeExecutionID: execution.ID,
		NodeType:        execution.NodeType,
		Input:           execution.Input,
	}); err != nil {
		return nil, err
	}
	return execution, nil
}

// GetToolExecution 返回指定的工具执行记录。
func (uc *ToolUsecase) GetToolExecution(ctx context.Context, nodeExecutionID string) (*NodeExecution, error) {
	nodeExecutionID = strings.TrimSpace(nodeExecutionID)
	if nodeExecutionID == "" {
		return nil, ErrToolInvalidArgument
	}

	execution, err := uc.nodeExecutionRepo.GetNodeExecution(ctx, nodeExecutionID)
	if err != nil {
		return nil, err
	}
	if execution == nil {
		return nil, ErrToolExecutionNotFound
	}
	return execution, nil
}
