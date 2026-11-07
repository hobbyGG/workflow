package wfx

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	v1 "github.com/hobbyGG/workflow/api/workflow/v1"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func inputToProto(input map[string]any) (*structpb.Struct, error) {
	if input == nil {
		return nil, nil
	}
	return structpb.NewStruct(input)
}

func definitionToProto(definition Definition) (*v1.DefinitionGraph, error) {
	nodes := make([]*v1.Node, 0, len(definition.Nodes))
	for _, node := range definition.Nodes {
		preset := make(map[string]*structpb.Value, len(node.Preset))
		for name, value := range node.Preset {
			encoded, err := structpb.NewValue(value)
			if err != nil {
				return nil, fmt.Errorf("node %q preset %q: %w", node.ID, name, err)
			}
			preset[name] = encoded
		}
		nodes = append(nodes, &v1.Node{
			Id:     node.ID,
			Type:   node.Type,
			Preset: preset,
		})
	}

	edges := make([]*v1.Edge, 0, len(definition.Edges))
	for _, edge := range definition.Edges {
		edges = append(edges, &v1.Edge{
			FromNode:   edge.FromNode,
			FromBranch: edge.FromBranch,
			FromPort:   edge.FromPort,
			ToNode:     edge.ToNode,
			ToInput:    edge.ToInput,
		})
	}
	return &v1.DefinitionGraph{Nodes: nodes, Edges: edges}, nil
}

func workflowFromProto(definition *v1.Definition) *Workflow {
	if definition == nil {
		return nil
	}
	return &Workflow{
		ID:         definition.GetDefinitionId(),
		UserID:     definition.GetUid(),
		Name:       definition.GetName(),
		Type:       definition.GetType(),
		Definition: definitionFromProto(definition.GetDefinition()),
		CreatedAt:  timeFromProto(definition.GetCreatedAt()),
		UpdatedAt:  timeFromProto(definition.GetUpdatedAt()),
	}
}

func definitionFromProto(definition *v1.DefinitionGraph) Definition {
	if definition == nil {
		return Definition{}
	}
	nodes := make([]Node, 0, len(definition.GetNodes()))
	for _, node := range definition.GetNodes() {
		if node == nil {
			continue
		}
		preset := make(map[string]any, len(node.GetPreset()))
		for name, value := range node.GetPreset() {
			if value == nil {
				preset[name] = nil
				continue
			}
			preset[name] = value.AsInterface()
		}
		nodes = append(nodes, Node{
			ID:     node.GetId(),
			Type:   node.GetType(),
			Preset: preset,
		})
	}

	edges := make([]Edge, 0, len(definition.GetEdges()))
	for _, edge := range definition.GetEdges() {
		if edge == nil {
			continue
		}
		edges = append(edges, Edge{
			FromNode:   edge.GetFromNode(),
			FromBranch: edge.GetFromBranch(),
			FromPort:   edge.GetFromPort(),
			ToNode:     edge.GetToNode(),
			ToInput:    edge.GetToInput(),
		})
	}
	return Definition{Nodes: nodes, Edges: edges}
}

func wfExecutionFromProto(wfExecution *v1.WorkflowExecution) *WorkflowExecution {
	if wfExecution == nil {
		return nil
	}
	result := &WorkflowExecution{
		ID:             wfExecution.GetWorkflowExecutionId(),
		UserID:         wfExecution.GetUid(),
		IdempotencyKey: wfExecution.GetIdempotencyKey(),
		DefinitionID:   wfExecution.GetDefinitionId(),
		DefinitionType: wfExecution.GetDefinitionType(),
		Status:         WorkflowExecutionStatus(wfExecution.GetStatus()),
		CreatedAt:      timeFromProto(wfExecution.GetCreatedAt()),
		StartedAt:      optionalTimeFromProto(wfExecution.GetStartedAt()),
		CompletedAt:    optionalTimeFromProto(wfExecution.GetCompletedAt()),
		UpdatedAt:      timeFromProto(wfExecution.GetUpdatedAt()),
	}
	if wfExecution.GetInput() != nil {
		result.Input = wfExecution.GetInput().AsMap()
	}
	if wfExecution.GetOutput() != nil {
		result.Output = wfExecution.GetOutput().AsInterface()
	}
	if wfExecution.GetInternalErrorCode() != 0 || wfExecution.GetInternalErrorMessage() != "" || wfExecution.GetFailedNodeId() != "" {
		result.Error = &WorkflowExecutionError{
			Code:         wfExecution.GetInternalErrorCode(),
			Message:      wfExecution.GetInternalErrorMessage(),
			FailedNodeID: wfExecution.GetFailedNodeId(),
		}
	}
	return result
}

func nodeTypesFromProto(nodeSchemas []*v1.NodeSchema) ([]NodeType, error) {
	result := make([]NodeType, 0, len(nodeSchemas))
	for i, nodeSchema := range nodeSchemas {
		converted, err := nodeTypeFromProto(nodeSchema)
		if err != nil {
			return nil, fmt.Errorf("node schema %d: %w", i, err)
		}
		result = append(result, converted)
	}
	return result, nil
}

func nodeTypeFromProto(nodeSchema *v1.NodeSchema) (NodeType, error) {
	if nodeSchema == nil {
		return NodeType{}, errors.New("node schema is missing")
	}
	if nodeSchema.GetType() == "" {
		return NodeType{}, errors.New("node type is missing")
	}
	inputSchema, err := schemaFromProto(nodeSchema.GetInputSchema())
	if err != nil {
		return NodeType{}, fmt.Errorf("input schema for node %q: %w", nodeSchema.GetType(), err)
	}
	outputSchema, err := schemaFromProto(nodeSchema.GetOutputSchema())
	if err != nil {
		return NodeType{}, fmt.Errorf("output schema for node %q: %w", nodeSchema.GetType(), err)
	}
	return NodeType{
		Type:         nodeSchema.GetType(),
		InputSchema:  inputSchema,
		OutputSchema: outputSchema,
	}, nil
}

func schemaFromProto(schema *structpb.Struct) (json.RawMessage, error) {
	if schema == nil {
		return nil, errors.New("schema is missing")
	}
	encoded, err := protojson.Marshal(schema)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(encoded), nil
}

func toolsFromProto(tools []*v1.ToolNode) ([]Tool, error) {
	result := make([]Tool, 0, len(tools))
	for i, tool := range tools {
		if tool == nil {
			return nil, fmt.Errorf("tool %d is missing", i)
		}
		schema, err := nodeTypeFromProto(tool.GetSchema())
		if err != nil {
			return nil, fmt.Errorf("tool %d: %w", i, err)
		}
		converted := Tool{Schema: schema}
		if profile := tool.GetProfile(); profile != nil {
			converted.Profile = &ToolProfile{
				SuccessRate:       profile.GetSuccessRate(),
				AverageDurationMS: profile.GetAverageDurationMs(),
			}
		}
		result = append(result, converted)
	}
	return result, nil
}

func toolExecutionFromProto(execution *v1.CheckToolExecutionResponse) *ToolExecution {
	if execution == nil {
		return nil
	}
	result := &ToolExecution{
		Status:               ToolExecutionStatus(execution.GetStatus()),
		InternalErrorMessage: execution.GetInternalErrorMessage(),
		DurationMS:           execution.GetDurationMs(),
	}
	if execution.GetOutput() != nil {
		result.Output = execution.GetOutput().AsMap()
	}
	return result
}

func timeFromProto(timestamp *timestamppb.Timestamp) time.Time {
	if timestamp == nil {
		return time.Time{}
	}
	return timestamp.AsTime()
}

func optionalTimeFromProto(timestamp *timestamppb.Timestamp) *time.Time {
	if timestamp == nil {
		return nil
	}
	value := timestamp.AsTime()
	return &value
}
