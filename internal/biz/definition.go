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
	// ErrDefinitionNotFound 表示工作流定义不存在。
	ErrDefinitionNotFound = errors.NotFound(v1.ErrorReason_DEFINITION_NOT_FOUND.String(), "definition not found")
	// ErrDefinitionInvalidArgument 表示工作流定义参数或图定义无效。
	ErrDefinitionInvalidArgument = errors.BadRequest(v1.ErrorReason_DEFINITION_INVALID_ARGUMENT.String(), "invalid definition argument")
	// ErrDefinitionInternal 表示工作流定义持久化数据不可用。
	ErrDefinitionInternal = errors.InternalServer(v1.ErrorReason_DEFINITION_INTERNAL.String(), "definition internal error")
)

// Definition 表示工作流定义资源。
type Definition struct {
	ID        string
	UID       int64
	Name      string
	Type      string
	DefGraph  DefinitionGraph
	CreatedAt time.Time
	UpdatedAt time.Time
}

// ListDefinitionsQuery 包含查询工作流定义列表所需的参数。
type ListDefinitionsQuery struct {
	UID    int64
	Type   string
	Limit  int32
	Offset int32
}

// DefinitionRepo 定义工作流定义的持久化接口。
type DefinitionRepo interface {
	GetDefinition(ctx context.Context, definitionID string) (*Definition, error)
	ListDefinitions(ctx context.Context, query ListDefinitionsQuery) ([]*Definition, error)
	CreateDefinition(ctx context.Context, definition *Definition) (*Definition, error)
}

// DefinitionUsecase 负责工作流定义的基础校验和持久化。
type DefinitionUsecase struct {
	repo DefinitionRepo
}

// NewDefinitionUsecase 创建 DefinitionUsecase。
func NewDefinitionUsecase(repo DefinitionRepo) *DefinitionUsecase {
	return &DefinitionUsecase{repo: repo}
}

// GetDefinition 根据 ID 查询工作流定义。
func (uc *DefinitionUsecase) GetDefinition(ctx context.Context, definitionID string) (*Definition, error) {
	definitionID = strings.TrimSpace(definitionID)
	if definitionID == "" {
		return nil, ErrDefinitionInvalidArgument
	}
	return uc.repo.GetDefinition(ctx, definitionID)
}

// ListDefinitions 返回满足查询条件的工作流定义。
func (uc *DefinitionUsecase) ListDefinitions(ctx context.Context, query ListDefinitionsQuery) ([]*Definition, error) {
	if query.UID <= 0 || query.Limit <= 0 || query.Limit > 200 || query.Offset < 0 {
		return nil, ErrDefinitionInvalidArgument
	}
	query.Type = strings.TrimSpace(query.Type)
	return uc.repo.ListDefinitions(ctx, query)
}

// CreateDefinition 创建工作流定义。
func (uc *DefinitionUsecase) CreateDefinition(ctx context.Context, definition *Definition) (*Definition, error) {
	if definition == nil ||
		definition.UID <= 0 ||
		strings.TrimSpace(definition.Name) == "" ||
		strings.TrimSpace(definition.Type) == "" {
		return nil, ErrDefinitionInvalidArgument
	}
	if err := validateDefinitionGraph(definition.DefGraph); err != nil {
		return nil, err
	}

	definition.ID = uuid.NewString()
	return uc.repo.CreateDefinition(ctx, definition)
}

func validateDefinitionGraph(definition DefinitionGraph) error {
	if len(definition.Nodes) == 0 {
		return ErrDefinitionInvalidArgument
	}

	// 建立节点入度表，校验节点 ID 非空且在图内唯一。
	indegree := make(map[string]int, len(definition.Nodes))
	outputNodeID := ""
	for _, node := range definition.Nodes {
		if strings.TrimSpace(node.ID) == "" {
			return ErrDefinitionInvalidArgument
		}
		if _, exists := indegree[node.ID]; exists {
			return ErrDefinitionInvalidArgument
		}
		indegree[node.ID] = 0
		if node.Type == NodeTypeOutput {
			if outputNodeID != "" {
				return ErrDefinitionInvalidArgument
			}
			outputNodeID = node.ID
		}
	}
	if outputNodeID == "" {
		return ErrDefinitionInvalidArgument
	}

	// 校验边的起止节点均存在，并构建邻接表、统计目标节点入度。
	adjacency := make(map[string][]string, len(definition.Nodes))
	reverseAdjacency := make(map[string][]string, len(definition.Nodes))
	for _, edge := range definition.Edges {
		if _, exists := indegree[edge.FromNode]; !exists {
			return ErrDefinitionInvalidArgument
		}
		if _, exists := indegree[edge.ToNode]; !exists {
			return ErrDefinitionInvalidArgument
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
		return ErrDefinitionInvalidArgument
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
		return ErrDefinitionInvalidArgument
	}
	return nil
}
