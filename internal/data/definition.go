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

type definitionRepo struct {
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

// NewDefinitionRepo 创建 DefinitionRepo。
func NewDefinitionRepo(data *Data) biz.DefinitionRepo {
	return &definitionRepo{data: data}
}

func (r *definitionRepo) GetDefinition(ctx context.Context, definitionID string) (*biz.Definition, error) {
	row, err := r.data.queries.GetDefinition(ctx, definitionID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, biz.ErrDefinitionNotFound
	}
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, fmt.Errorf("get definition by ID: %v: %w", err, biz.ErrDefinitionInternal)
	}
	return toBizDefinitionResource(row)
}

func (r *definitionRepo) ListDefinitions(ctx context.Context, query biz.ListDefinitionsQuery) ([]*biz.Definition, error) {
	rows, err := r.data.queries.ListDefinitions(ctx, sqlcgen.ListDefinitionsParams{
		Uid:        query.UID,
		Type:       query.Type,
		PageLimit:  query.Limit,
		PageOffset: query.Offset,
	})
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, fmt.Errorf("list definitions: %v: %w", err, biz.ErrDefinitionInternal)
	}

	definitions := make([]*biz.Definition, 0, len(rows))
	for _, row := range rows {
		definition, err := toBizDefinitionResource(row)
		if err != nil {
			return nil, err
		}
		definitions = append(definitions, definition)
	}
	return definitions, nil
}

func (r *definitionRepo) CreateDefinition(ctx context.Context, definition *biz.Definition) (*biz.Definition, error) {
	params, err := newDefinitionResource(definition)
	if err != nil {
		return nil, fmt.Errorf("marshal definition graph: %v: %w", err, biz.ErrDefinitionInternal)
	}

	row, err := r.data.queries.CreateDefinition(ctx, params)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, fmt.Errorf("create definition: %v: %w", err, biz.ErrDefinitionInternal)
	}
	return toBizDefinitionResource(row)
}

func newDefinitionResource(definitionResource *biz.Definition) (sqlcgen.CreateDefinitionParams, error) {
	definition, err := json.Marshal(newDefinition(definitionResource.DefGraph))
	if err != nil {
		return sqlcgen.CreateDefinitionParams{}, err
	}
	return sqlcgen.CreateDefinitionParams{
		DefinitionID: definitionResource.ID,
		Uid:          pgtype.Int8{Int64: definitionResource.UID, Valid: true},
		Name:         definitionResource.Name,
		Type:         definitionResource.Type,
		Definition:   definition,
	}, nil
}

func toBizDefinitionResource(row sqlcgen.WorkflowDefinition) (*biz.Definition, error) {
	var def definition
	if err := json.Unmarshal(row.Definition, &def); err != nil {
		return nil, fmt.Errorf("unmarshal definition graph: %v: %w", err, biz.ErrDefinitionInternal)
	}
	if !row.Uid.Valid || !row.CreatedAt.Valid || !row.UpdatedAt.Valid {
		return nil, biz.ErrDefinitionInternal
	}

	return &biz.Definition{
		ID:        row.DefinitionID,
		UID:       row.Uid.Int64,
		Name:      row.Name,
		Type:      row.Type,
		DefGraph:  toBizDefinition(def),
		CreatedAt: row.CreatedAt.Time,
		UpdatedAt: row.UpdatedAt.Time,
	}, nil
}

func newDefinition(def biz.DefinitionGraph) definition {
	var nodes []node
	if def.Nodes != nil {
		nodes = make([]node, len(def.Nodes))
		for i, defNode := range def.Nodes {
			nodes[i] = node{
				ID:     defNode.ID,
				Type:   defNode.Type.String(),
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
