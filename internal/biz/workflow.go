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
	// ErrWorkflowNotFound 表示工作流不存在。
	ErrWorkflowNotFound = errors.NotFound(v1.ErrorReason_WORKFLOW_NOT_FOUND.String(), "workflow not found")
	// ErrWorkflowInvalidArgument 表示工作流参数或图定义无效。
	ErrWorkflowInvalidArgument = errors.BadRequest(v1.ErrorReason_WORKFLOW_INVALID_ARGUMENT.String(), "invalid workflow argument")
	// ErrWorkflowInternal 表示工作流持久化数据不可用。
	ErrWorkflowInternal = errors.InternalServer(v1.ErrorReason_WORKFLOW_INTERNAL.String(), "workflow internal error")
)

// Workflow 表示工作流定义。
type Workflow struct {
	ID         string
	UID        int64
	Name       string
	Type       string
	Definition DefinitionGraph
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

// ListWorkflowsQuery 包含查询工作流列表所需的参数。
type ListWorkflowsQuery struct {
	UID    int64
	Type   string
	Limit  int32
	Offset int32
}

// WorkflowRepo 定义工作流的持久化接口。
type WorkflowRepo interface {
	FindByID(ctx context.Context, workflowID string) (*Workflow, error)
	ListWorkflows(ctx context.Context, query ListWorkflowsQuery) ([]*Workflow, error)
	CreateWorkflow(ctx context.Context, workflow *Workflow) (*Workflow, error)
}

// WorkflowUsecase 负责工作流定义的基础校验和持久化。
type WorkflowUsecase struct {
	repo WorkflowRepo
}

// NewWorkflowUsecase 创建 WorkflowUsecase。
func NewWorkflowUsecase(repo WorkflowRepo) *WorkflowUsecase {
	return &WorkflowUsecase{repo: repo}
}

// GetWorkflow 根据 ID 查询工作流。
func (uc *WorkflowUsecase) GetWorkflow(ctx context.Context, workflowID string) (*Workflow, error) {
	workflowID = strings.TrimSpace(workflowID)
	if workflowID == "" {
		return nil, ErrWorkflowInvalidArgument
	}
	return uc.repo.FindByID(ctx, workflowID)
}

// ListWorkflows 返回满足查询条件的工作流。
func (uc *WorkflowUsecase) ListWorkflows(ctx context.Context, query ListWorkflowsQuery) ([]*Workflow, error) {
	if query.UID <= 0 || query.Limit <= 0 || query.Limit > 200 || query.Offset < 0 {
		return nil, ErrWorkflowInvalidArgument
	}
	query.Type = strings.TrimSpace(query.Type)
	return uc.repo.ListWorkflows(ctx, query)
}

// CreateWorkflow 创建工作流。
func (uc *WorkflowUsecase) CreateWorkflow(ctx context.Context, workflow *Workflow) (*Workflow, error) {
	if workflow == nil ||
		workflow.UID <= 0 ||
		strings.TrimSpace(workflow.Name) == "" ||
		strings.TrimSpace(workflow.Type) == "" {
		return nil, ErrWorkflowInvalidArgument
	}
	if err := validateDefinitionGraph(workflow.Definition); err != nil {
		return nil, err
	}

	workflow.ID = uuid.NewString()
	return uc.repo.CreateWorkflow(ctx, workflow)
}

func validateDefinitionGraph(definition DefinitionGraph) error {
	if len(definition.Nodes) == 0 {
		return ErrWorkflowInvalidArgument
	}

	// 建立节点入度表，校验节点 ID 非空且在图内唯一。
	indegree := make(map[string]int, len(definition.Nodes))
	outputNodeID := ""
	for _, node := range definition.Nodes {
		if strings.TrimSpace(node.ID) == "" {
			return ErrWorkflowInvalidArgument
		}
		if _, exists := indegree[node.ID]; exists {
			return ErrWorkflowInvalidArgument
		}
		indegree[node.ID] = 0
		if node.Type == NodeTypeOutput {
			if outputNodeID != "" {
				return ErrWorkflowInvalidArgument
			}
			outputNodeID = node.ID
		}
	}
	if outputNodeID == "" {
		return ErrWorkflowInvalidArgument
	}

	// 校验边的起止节点均存在，并构建邻接表、统计目标节点入度。
	adjacency := make(map[string][]string, len(definition.Nodes))
	reverseAdjacency := make(map[string][]string, len(definition.Nodes))
	for _, edge := range definition.Edges {
		if _, exists := indegree[edge.FromNode]; !exists {
			return ErrWorkflowInvalidArgument
		}
		if _, exists := indegree[edge.ToNode]; !exists {
			return ErrWorkflowInvalidArgument
		}
		adjacency[edge.FromNode] = append(adjacency[edge.FromNode], edge.ToNode)
		reverseAdjacency[edge.ToNode] = append(reverseAdjacency[edge.ToNode], edge.FromNode)
		indegree[edge.ToNode]++
	}

	// 收集所有零入度节点，作为拓扑遍历的初始节点。
	ready := make([]string, 0, len(definition.Nodes))
	for nodeID, degree := range indegree {
		if degree == 0 {
			ready = append(ready, nodeID)
		}
	}

	// 按 Kahn 算法逐步移除零入度节点，并释放其下游节点。
	visited := 0
	for len(ready) > 0 {
		nodeID := ready[len(ready)-1]
		ready = ready[:len(ready)-1]
		visited++

		for _, targetID := range adjacency[nodeID] {
			indegree[targetID]--
			if indegree[targetID] == 0 {
				ready = append(ready, targetID)
			}
		}
	}

	// 无法遍历全部节点说明图中存在环。
	if visited != len(definition.Nodes) {
		return ErrWorkflowInvalidArgument
	}

	// 所有节点必须最终汇聚到唯一 OUTPUT，避免执行无结果的孤立子图。
	reachesOutput := map[string]struct{}{outputNodeID: {}}
	pending := []string{outputNodeID}
	for len(pending) > 0 {
		nodeID := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		for _, sourceID := range reverseAdjacency[nodeID] {
			if _, exists := reachesOutput[sourceID]; exists {
				continue
			}
			reachesOutput[sourceID] = struct{}{}
			pending = append(pending, sourceID)
		}
	}
	if len(reachesOutput) != len(definition.Nodes) {
		return ErrWorkflowInvalidArgument
	}
	return nil
}
