package service

import (
	"context"
	"strings"

	pb "github.com/hobbyGG/workflow/api/workflow/v1"
	"github.com/hobbyGG/workflow/internal/biz"

	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type WorkflowService struct {
	pb.UnimplementedWorkflowServiceServer

	execution *biz.ExecutionUsecase
	workflow  *biz.WorkflowUsecase
	node      *biz.NodeUsecase
}

func NewWorkflowService(execution *biz.ExecutionUsecase, workflow *biz.WorkflowUsecase, node *biz.NodeUsecase) *WorkflowService {
	return &WorkflowService{
		execution: execution,
		workflow:  workflow,
		node:      node,
	}
}

func (*WorkflowService) Healthz(context.Context, *pb.WorkflowServiceHealthzRequest) (*pb.WorkflowServiceHealthzResponse, error) {
	return &pb.WorkflowServiceHealthzResponse{}, nil
}

func (*WorkflowService) Readyz(context.Context, *pb.WorkflowServiceReadyzRequest) (*pb.WorkflowServiceReadyzResponse, error) {
	return &pb.WorkflowServiceReadyzResponse{}, nil
}

func (s *WorkflowService) ListNodeTypes(context.Context, *pb.ListNodeTypesRequest) (*pb.ListNodeTypesResponse, error) {
	schemas := s.node.GetNodeTypes()
	nodeTypes := make([]*pb.NodeType, 0, len(schemas))
	for _, schema := range schemas {
		inputs := make(map[string]*pb.InputDefinition, len(schema.Inputs))
		for name, input := range schema.Inputs {
			inputs[name] = &pb.InputDefinition{
				Type:     input.Type,
				Required: input.Required,
			}
		}
		outputs := make(map[string]*pb.OutputDefinition, len(schema.Outputs))
		for name, output := range schema.Outputs {
			outputs[name] = &pb.OutputDefinition{Type: output.Type}
		}
		nodeTypes = append(nodeTypes, &pb.NodeType{
			Type:    string(schema.Type),
			Inputs:  inputs,
			Outputs: outputs,
		})
	}
	return &pb.ListNodeTypesResponse{NodeTypes: nodeTypes}, nil
}

func (s *WorkflowService) CreateWorkflow(ctx context.Context, req *pb.CreateWorkflowRequest) (*pb.CreateWorkflowResponse, error) {
	if req == nil || req.GetUid() <= 0 || req.GetDefinition() == nil {
		return nil, biz.ErrWorkflowInvalidArgument
	}
	name := strings.TrimSpace(req.GetName())
	workflowType := strings.TrimSpace(req.GetType())
	if name == "" || workflowType == "" {
		return nil, biz.ErrWorkflowInvalidArgument
	}

	workflow, err := s.workflow.CreateWorkflow(ctx, &biz.Workflow{
		UID:        req.GetUid(),
		Name:       name,
		Type:       workflowType,
		Definition: convertDefinition(req.GetDefinition()),
	})
	if err != nil {
		return nil, err
	}
	reply, err := convertWorkflowReply(workflow)
	if err != nil {
		return nil, err
	}
	return &pb.CreateWorkflowResponse{Data: reply}, nil
}

func (s *WorkflowService) GetWorkflow(ctx context.Context, req *pb.GetWorkflowRequest) (*pb.GetWorkflowResponse, error) {
	if req == nil {
		return nil, biz.ErrWorkflowInvalidArgument
	}
	workflowID := strings.TrimSpace(req.GetWorkflowId())
	if workflowID == "" {
		return nil, biz.ErrWorkflowInvalidArgument
	}

	workflow, err := s.workflow.GetWorkflow(ctx, workflowID)
	if err != nil {
		return nil, err
	}
	reply, err := convertWorkflowReply(workflow)
	if err != nil {
		return nil, err
	}
	return &pb.GetWorkflowResponse{Data: reply}, nil
}

const (
	defaultListWorkflowsLimit int32 = 20
	maxListWorkflowsLimit     int32 = 200
)

// ListWorkflows 返回满足查询条件的工作流。
func (s *WorkflowService) ListWorkflows(ctx context.Context, req *pb.ListWorkflowsRequest) (*pb.ListWorkflowsResponse, error) {
	if req == nil ||
		req.GetUid() <= 0 ||
		req.GetLimit() < 0 ||
		req.GetLimit() > maxListWorkflowsLimit ||
		req.GetOffset() < 0 {
		return nil, biz.ErrWorkflowInvalidArgument
	}

	limit := req.GetLimit()
	if limit == 0 {
		limit = defaultListWorkflowsLimit
	}
	workflows, err := s.workflow.ListWorkflows(ctx, biz.ListWorkflowsQuery{
		UID:    req.GetUid(),
		Type:   strings.TrimSpace(req.GetType()),
		Limit:  limit,
		Offset: req.GetOffset(),
	})
	if err != nil {
		return nil, err
	}

	page := &pb.WorkflowPage{
		Limit:     limit,
		Offset:    req.GetOffset(),
		Workflows: make([]*pb.Workflow, 0, len(workflows)),
	}
	for _, workflow := range workflows {
		reply, err := convertWorkflowReply(workflow)
		if err != nil {
			return nil, err
		}
		page.Workflows = append(page.Workflows, reply)
	}
	return &pb.ListWorkflowsResponse{Data: page}, nil
}

func convertWorkflowReply(workflow *biz.Workflow) (*pb.Workflow, error) {
	if workflow == nil {
		return nil, biz.ErrWorkflowInternal
	}
	definition, err := convertDefinitionReply(workflow.Definition)
	if err != nil {
		return nil, err
	}
	reply := &pb.Workflow{
		WorkflowId: workflow.ID,
		Uid:        workflow.UID,
		Name:       workflow.Name,
		Type:       workflow.Type,
		Definition: definition,
	}
	if !workflow.CreatedAt.IsZero() {
		reply.CreatedAt = timestamppb.New(workflow.CreatedAt)
	}
	if !workflow.UpdatedAt.IsZero() {
		reply.UpdatedAt = timestamppb.New(workflow.UpdatedAt)
	}
	return reply, nil
}

func convertDefinition(in *pb.Definition) biz.DefinitionGraph {
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

func convertDefinitionReply(in biz.DefinitionGraph) (*pb.Definition, error) {
	nodes := make([]*pb.Node, len(in.Nodes))
	for i, node := range in.Nodes {
		preset := make(map[string]*structpb.Value, len(node.Preset))
		for key, value := range node.Preset {
			converted, err := structpb.NewValue(value)
			if err != nil {
				return nil, biz.ErrWorkflowInternal
			}
			preset[key] = converted
		}
		nodes[i] = &pb.Node{
			Id:     node.ID,
			Type:   string(node.Type),
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
	return &pb.Definition{Nodes: nodes, Edges: edges}, nil
}
