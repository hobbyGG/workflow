package biz

import (
	"context"
	"encoding/json"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
	"github.com/google/jsonschema-go/jsonschema"
	"go.temporal.io/sdk/temporal"
)

type NodeType string

func (nt *NodeType) String() string {
	return string(*nt)
}

// 这里定义需要的节点类型
const (
	NodeTypeOutput       NodeType = "OUTPUT"
	NodeTypeExclusive    NodeType = "EXCLUSIVE"
	NodeTypeInclusive    NodeType = "INCLUSIVE"
	NodeTypeSaySomething NodeType = "SAY_SOMETHING"
	NodeTypeExample      NodeType = "EXAMPLE"

	presetBranchesKey = "branches"
)

type Node interface {
	Type() NodeType
	Schema() NodeSchema
}

type ActivityNode interface {
	Node
	RetryPolicy() temporal.RetryPolicy
	Activity(ctx context.Context, inputValues map[string]any) (map[string]any, error)
}

type RouterNode interface {
	Node
	Route(inputValues map[string]any) (selectedBranchIDs []string, err error)
}

// NodeSchema 描述节点类型的数据端口。
type NodeSchema struct {
	Type         NodeType
	InputSchema  *jsonschema.Schema
	OutputSchema *jsonschema.Schema
}

// NodeRepo 提供节点信息查询能力。
type NodeRepo interface {
	GetActivityNodes() []ActivityNode
}

// NodeUsecase 负责查询节点信息。
type NodeUsecase struct {
	repo NodeRepo
}

// NewNodeUsecase 创建 NodeUsecase。
func NewNodeUsecase(repo NodeRepo) *NodeUsecase {
	return &NodeUsecase{repo: repo}
}

// GetNodeSchemas 返回画布可选节点的 schema。
func (uc *NodeUsecase) GetNodeSchemas() []NodeSchema {
	nodes := []Node{
		&outputNode{},
		&routerNode{nodeType: NodeTypeExclusive},
		&routerNode{nodeType: NodeTypeInclusive},
	}
	for _, node := range uc.repo.GetActivityNodes() {
		nodes = append(nodes, node)
	}

	schemas := make([]NodeSchema, 0, len(nodes))
	for _, node := range nodes {
		schemas = append(schemas, node.Schema())
	}
	return schemas
}

type outputNode struct{}

func (*outputNode) Type() NodeType {
	return NodeTypeOutput
}

func (*outputNode) Schema() NodeSchema {
	return objectNodeSchema(NodeTypeOutput)
}

type Branch struct {
	ID         string `json:"id"`
	Expression string `json:"expression"`

	program *vm.Program
}

type routerNode struct {
	nodeType NodeType
	branches []Branch
}

func (n *routerNode) Type() NodeType {
	return n.nodeType
}

func (n *routerNode) Schema() NodeSchema {
	return objectNodeSchema(n.nodeType)
}

func objectNodeSchema(nodeType NodeType) NodeSchema {
	return NodeSchema{
		Type:         nodeType,
		InputSchema:  &jsonschema.Schema{Type: "object"},
		OutputSchema: &jsonschema.Schema{Type: "object"},
	}
}

func (n *routerNode) Route(inputValues map[string]any) (selectedBranchIDs []string, err error) {
	env := map[string]any{"inputs": inputValues}
	for _, branch := range n.branches {
		output, err := expr.Run(branch.program, env)
		if err != nil {
			return nil, err
		}
		if !output.(bool) {
			continue
		}
		selectedBranchIDs = append(selectedBranchIDs, branch.ID)
		if n.nodeType == NodeTypeExclusive {
			break
		}
	}
	return selectedBranchIDs, nil
}

func newRouterNode(nodeType NodeType, preset map[string]any) (*routerNode, error) {
	value, exists := preset[presetBranchesKey]
	if !exists {
		return nil, ErrDynamicWorkflowInvalidDefinition
	}
	data, err := json.Marshal(value)
	if err != nil {
		return nil, ErrDynamicWorkflowInvalidDefinition
	}
	var branches []Branch
	if err := json.Unmarshal(data, &branches); err != nil {
		return nil, ErrDynamicWorkflowInvalidDefinition
	}
	for i := range branches {
		program, err := expr.Compile(
			branches[i].Expression,
			expr.Env(map[string]any{"inputs": map[string]any{}}),
			expr.AsBool(),
			expr.DisableBuiltin("now"),
			expr.DisableBuiltin("date"),
			expr.DisableBuiltin("keys"),
			expr.DisableBuiltin("values"),
			expr.DisableBuiltin("toPairs"),
			expr.DisableBuiltin("timezone"),
		)
		if err != nil {
			return nil, ErrDynamicWorkflowInvalidDefinition
		}
		branches[i].program = program
	}
	return &routerNode{
		nodeType: nodeType,
		branches: branches,
	}, nil
}
