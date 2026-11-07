package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	pb "github.com/hobbyGG/workflow/api/workflow/v1"
	"github.com/hobbyGG/workflow/internal/biz"

	"github.com/google/jsonschema-go/jsonschema"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (*WorkflowService) Healthz(context.Context, *pb.WorkflowServiceHealthzRequest) (*pb.WorkflowServiceHealthzResponse, error) {
	return &pb.WorkflowServiceHealthzResponse{}, nil
}

func (*WorkflowService) Readyz(context.Context, *pb.WorkflowServiceReadyzRequest) (*pb.WorkflowServiceReadyzResponse, error) {
	return &pb.WorkflowServiceReadyzResponse{}, nil
}

func (s *WorkflowService) ListNodeSchemas(context.Context, *pb.ListNodeSchemasRequest) (*pb.ListNodeSchemasResponse, error) {
	schemas := s.node.GetNodeSchemas()
	nodeSchemas := make([]*pb.NodeSchema, 0, len(schemas))
	for _, schema := range schemas {
		converted, err := convertNodeSchema(schema)
		if err != nil {
			return nil, biz.ErrDefinitionInternal
		}
		nodeSchemas = append(nodeSchemas, converted)
	}
	return &pb.ListNodeSchemasResponse{NodeSchemas: nodeSchemas}, nil
}

func convertNodeSchema(schema biz.NodeSchema) (*pb.NodeSchema, error) {
	inputSchema, err := schemaToStruct(schema.InputSchema)
	if err != nil {
		return nil, fmt.Errorf("convert %s input schema: %w", schema.Type, err)
	}

	outputSchema, err := schemaToStruct(schema.OutputSchema)
	if err != nil {
		return nil, fmt.Errorf("convert %s output schema: %w", schema.Type, err)
	}

	return &pb.NodeSchema{
		Type:         schema.Type.String(),
		InputSchema:  inputSchema,
		OutputSchema: outputSchema,
	}, nil
}

func schemaToStruct(schema *jsonschema.Schema) (*structpb.Struct, error) {
	if schema == nil {
		return nil, errors.New("schema is missing")
	}
	raw, err := json.Marshal(schema)
	if err != nil {
		return nil, err
	}
	result := new(structpb.Struct)
	if err := protojson.Unmarshal(raw, result); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *WorkflowService) CreateDefinition(ctx context.Context, req *pb.CreateDefinitionRequest) (*pb.CreateDefinitionResponse, error) {
	if req == nil || req.GetUid() <= 0 || req.GetDefinition() == nil {
		return nil, biz.ErrDefinitionInvalidArgument
	}
	name := strings.TrimSpace(req.GetName())
	definitionType := strings.TrimSpace(req.GetType())
	if name == "" || definitionType == "" {
		return nil, biz.ErrDefinitionInvalidArgument
	}

	definition, err := s.definition.CreateDefinition(ctx, &biz.Definition{
		UID:      req.GetUid(),
		Name:     name,
		Type:     definitionType,
		DefGraph: convertDefinition(req.GetDefinition()),
	})
	if err != nil {
		return nil, err
	}
	reply, err := convertDefinitionResourceReply(definition)
	if err != nil {
		return nil, err
	}
	return &pb.CreateDefinitionResponse{Data: reply}, nil
}

func (s *WorkflowService) GetDefinition(ctx context.Context, req *pb.GetDefinitionRequest) (*pb.GetDefinitionResponse, error) {
	if req == nil {
		return nil, biz.ErrDefinitionInvalidArgument
	}
	definitionID := strings.TrimSpace(req.GetDefinitionId())
	if definitionID == "" {
		return nil, biz.ErrDefinitionInvalidArgument
	}

	definition, err := s.definition.GetDefinition(ctx, definitionID)
	if err != nil {
		return nil, err
	}
	reply, err := convertDefinitionResourceReply(definition)
	if err != nil {
		return nil, err
	}
	return &pb.GetDefinitionResponse{Data: reply}, nil
}

const (
	defaultListDefinitionsLimit int32 = 20
	maxListDefinitionsLimit     int32 = 200
)

// ListDefinitions 返回满足查询条件的工作流定义。
func (s *WorkflowService) ListDefinitions(ctx context.Context, req *pb.ListDefinitionsRequest) (*pb.ListDefinitionsResponse, error) {
	if req == nil ||
		req.GetUid() <= 0 ||
		req.GetLimit() < 0 ||
		req.GetLimit() > maxListDefinitionsLimit ||
		req.GetOffset() < 0 {
		return nil, biz.ErrDefinitionInvalidArgument
	}

	limit := req.GetLimit()
	if limit == 0 {
		limit = defaultListDefinitionsLimit
	}
	definitions, err := s.definition.ListDefinitions(ctx, biz.ListDefinitionsQuery{
		UID:    req.GetUid(),
		Type:   strings.TrimSpace(req.GetType()),
		Limit:  limit,
		Offset: req.GetOffset(),
	})
	if err != nil {
		return nil, err
	}

	page := &pb.DefinitionPage{
		Limit:       limit,
		Offset:      req.GetOffset(),
		Definitions: make([]*pb.Definition, 0, len(definitions)),
	}
	for _, definition := range definitions {
		reply, err := convertDefinitionResourceReply(definition)
		if err != nil {
			return nil, err
		}
		page.Definitions = append(page.Definitions, reply)
	}
	return &pb.ListDefinitionsResponse{Data: page}, nil
}

func convertDefinitionResourceReply(definitionResource *biz.Definition) (*pb.Definition, error) {
	if definitionResource == nil {
		return nil, biz.ErrDefinitionInternal
	}
	definition, err := convertDefinitionReply(definitionResource.DefGraph)
	if err != nil {
		return nil, err
	}
	reply := &pb.Definition{
		DefinitionId: definitionResource.ID,
		Uid:          definitionResource.UID,
		Name:         definitionResource.Name,
		Type:         definitionResource.Type,
		Definition:   definition,
	}
	if !definitionResource.CreatedAt.IsZero() {
		reply.CreatedAt = timestamppb.New(definitionResource.CreatedAt)
	}
	if !definitionResource.UpdatedAt.IsZero() {
		reply.UpdatedAt = timestamppb.New(definitionResource.UpdatedAt)
	}
	return reply, nil
}

func convertDefinition(in *pb.DefinitionGraph) biz.DefinitionGraph {
	nodes := make([]biz.DefNode, len(in.GetNodes()))
	for i, node := range in.GetNodes() {
		preset := make(map[string]any, len(node.GetPreset()))
		for key, value := range node.GetPreset() {
			if value == nil {
				preset[key] = nil
				continue
			}
			preset[key] = value.AsInterface()
		}
		nodes[i] = biz.DefNode{
			ID:     node.GetId(),
			Type:   biz.NodeType(node.GetType()),
			Preset: preset,
		}
	}

	edges := make([]biz.DefEdge, len(in.GetEdges()))
	for i, edge := range in.GetEdges() {
		edges[i] = biz.DefEdge{
			FromNode:   edge.GetFromNode(),
			FromBranch: edge.GetFromBranch(),
			FromKey:    edge.GetFromPort(),
			ToNode:     edge.GetToNode(),
			ToKey:      edge.GetToInput(),
		}
	}
	return biz.DefinitionGraph{Nodes: nodes, Edges: edges}
}

func convertDefinitionReply(in biz.DefinitionGraph) (*pb.DefinitionGraph, error) {
	nodes := make([]*pb.Node, len(in.Nodes))
	for i, node := range in.Nodes {
		preset := make(map[string]*structpb.Value, len(node.Preset))
		for key, value := range node.Preset {
			converted, err := structpb.NewValue(value)
			if err != nil {
				return nil, biz.ErrDefinitionInternal
			}
			preset[key] = converted
		}
		nodes[i] = &pb.Node{
			Id:     node.ID,
			Type:   node.Type.String(),
			Preset: preset,
		}
	}

	edges := make([]*pb.Edge, len(in.Edges))
	for i, edge := range in.Edges {
		edges[i] = &pb.Edge{
			FromNode:   edge.FromNode,
			FromBranch: edge.FromBranch,
			FromPort:   edge.FromKey,
			ToNode:     edge.ToNode,
			ToInput:    edge.ToKey,
		}
	}
	return &pb.DefinitionGraph{Nodes: nodes, Edges: edges}, nil
}
