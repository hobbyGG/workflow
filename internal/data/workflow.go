package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/hobbyGG/workflow/internal/biz"
	"github.com/hobbyGG/workflow/internal/data/db/sqlcgen"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

type workflowRepo struct {
	data *Data
}

type definition struct {
	Nodes []node `json:"nodes"`
	Edges []edge `json:"edges"`
}

type node struct {
	ID     string         `json:"id"`
	Type   string         `json:"type"`
	Preset map[string]any `json:"preset,omitempty"`
}

type edge struct {
	FromNode   string `json:"from_node"`
	FromBranch string `json:"from_branch,omitempty"`
	FromPort   string `json:"from_port"`
	ToNode     string `json:"to_node"`
	ToInput    string `json:"to_input,omitempty"`
}

// NewWorkflowRepo 创建 WorkflowRepo。
func NewWorkflowRepo(data *Data) biz.WorkflowRepo {
	return &workflowRepo{data: data}
}

func (r *workflowRepo) FindByID(ctx context.Context, workflowID string) (*biz.Workflow, error) {
	row, err := r.data.queries.GetWorkflowByID(ctx, workflowID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, biz.ErrWorkflowNotFound
	}
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, fmt.Errorf("find workflow by ID: %v: %w", err, biz.ErrWorkflowInternal)
	}
	return toBizWorkflow(row)
}

func (r *workflowRepo) ListWorkflows(ctx context.Context, query biz.ListWorkflowsQuery) ([]*biz.Workflow, error) {
	rows, err := r.data.queries.ListWorkflows(ctx, sqlcgen.ListWorkflowsParams{
		Uid:        query.UID,
		Type:       query.Type,
		PageLimit:  query.Limit,
		PageOffset: query.Offset,
	})
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, fmt.Errorf("list workflows: %v: %w", err, biz.ErrWorkflowInternal)
	}

	workflows := make([]*biz.Workflow, 0, len(rows))
	for _, row := range rows {
		workflow, err := toBizWorkflow(row)
		if err != nil {
			return nil, err
		}
		workflows = append(workflows, workflow)
	}
	return workflows, nil
}

func (r *workflowRepo) CreateWorkflow(ctx context.Context, workflow *biz.Workflow) (*biz.Workflow, error) {
	params, err := newWorkflow(workflow)
	if err != nil {
		return nil, fmt.Errorf("marshal workflow definition: %v: %w", err, biz.ErrWorkflowInternal)
	}

	row, err := r.data.queries.CreateWorkflow(ctx, params)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, fmt.Errorf("create workflow: %v: %w", err, biz.ErrWorkflowInternal)
	}
	return toBizWorkflow(row)
}

func newWorkflow(workflow *biz.Workflow) (sqlcgen.CreateWorkflowParams, error) {
	definition, err := json.Marshal(newDefinition(workflow.Definition))
	if err != nil {
		return sqlcgen.CreateWorkflowParams{}, err
	}
	return sqlcgen.CreateWorkflowParams{
		WorkflowID: workflow.ID,
		Uid:        pgtype.Int8{Int64: workflow.UID, Valid: true},
		Name:       workflow.Name,
		Type:       workflow.Type,
		Definition: definition,
	}, nil
}

func toBizWorkflow(row sqlcgen.Workflow) (*biz.Workflow, error) {
	var def definition
	if err := json.Unmarshal(row.Definition, &def); err != nil {
		return nil, fmt.Errorf("unmarshal workflow definition: %v: %w", err, biz.ErrWorkflowInternal)
	}
	if !row.Uid.Valid || !row.CreatedAt.Valid || !row.UpdatedAt.Valid {
		return nil, biz.ErrWorkflowInternal
	}

	return &biz.Workflow{
		ID:         row.WorkflowID,
		UID:        row.Uid.Int64,
		Name:       row.Name,
		Type:       row.Type,
		Definition: toBizDefinition(def),
		CreatedAt:  row.CreatedAt.Time,
		UpdatedAt:  row.UpdatedAt.Time,
	}, nil
}

func newDefinition(def biz.DefinitionGraph) definition {
	var nodes []node
	if def.Nodes != nil {
		nodes = make([]node, len(def.Nodes))
		for i, defNode := range def.Nodes {
			nodes[i] = node{
				ID:     defNode.ID,
				Type:   string(defNode.Type),
				Preset: defNode.Preset,
			}
		}
	}

	var edges []edge
	if def.Edges != nil {
		edges = make([]edge, len(def.Edges))
		for i, defEdge := range def.Edges {
			edges[i] = edge{
				FromNode:   defEdge.FromNode,
				FromBranch: defEdge.FromBranch,
				FromPort:   defEdge.FromKey,
				ToNode:     defEdge.ToNode,
				ToInput:    defEdge.ToKey,
			}
		}
	}

	return definition{
		Nodes: nodes,
		Edges: edges,
	}
}

func toBizDefinition(def definition) biz.DefinitionGraph {
	var nodes []biz.DefNode
	if def.Nodes != nil {
		nodes = make([]biz.DefNode, len(def.Nodes))
		for i, node := range def.Nodes {
			nodes[i] = biz.DefNode{
				ID:     node.ID,
				Type:   biz.NodeType(node.Type),
				Preset: node.Preset,
			}
		}
	}

	var edges []biz.DefEdge
	if def.Edges != nil {
		edges = make([]biz.DefEdge, len(def.Edges))
		for i, edge := range def.Edges {
			edges[i] = biz.DefEdge{
				FromNode:   edge.FromNode,
				FromBranch: edge.FromBranch,
				FromKey:    edge.FromPort,
				ToNode:     edge.ToNode,
				ToKey:      edge.ToInput,
			}
		}
	}

	return biz.DefinitionGraph{
		Nodes: nodes,
		Edges: edges,
	}
}
