package service

import (
	"context"
	"strings"

	pb "github.com/hobbyGG/workflow/api/workflow/v1"
	"github.com/hobbyGG/workflow/internal/biz"

	"google.golang.org/protobuf/types/known/structpb"
)

type ToolService struct {
	pb.UnimplementedToolServiceServer

	uc *biz.ToolUsecase
}

func NewToolService(uc *biz.ToolUsecase) *ToolService {
	return &ToolService{uc: uc}
}

func (s *ToolService) ListTools(ctx context.Context, _ *pb.ListToolsRequest) (*pb.ListToolsResponse, error) {
	toolInfos, err := s.uc.ListTools(ctx)
	if err != nil {
		return nil, err
	}

	tools := make([]*pb.ToolNode, 0, len(toolInfos))
	for _, toolInfo := range toolInfos {
		schema, err := convertNodeSchema(toolInfo.Schema)
		if err != nil {
			return nil, biz.ErrToolInternal
		}

		tool := &pb.ToolNode{
			Schema: schema,
		}
		if toolInfo.Profile != nil {
			tool.Profile = &pb.ToolProfile{
				SuccessRate:       toolInfo.Profile.SuccessRatio,
				AverageDurationMs: toolInfo.Profile.AvgDurationsMS,
			}
		}
		tools = append(tools, tool)
	}
	return &pb.ListToolsResponse{Tools: tools}, nil
}

func (s *ToolService) ExecuteTool(ctx context.Context, req *pb.ExecuteToolRequest) (*pb.ExecuteToolResponse, error) {
	if req == nil {
		return nil, biz.ErrToolInvalidArgument
	}
	nodeType := strings.TrimSpace(req.GetNodeType())
	idempotencyKey := strings.TrimSpace(req.GetIdempotencyKey())
	if nodeType == "" || idempotencyKey == "" {
		return nil, biz.ErrToolInvalidArgument
	}

	input := make(map[string]any)
	if req.GetInput() != nil {
		input = req.GetInput().AsMap()
	}
	execution, err := s.uc.ExecuteTool(ctx, &biz.ExecuteToolParam{
		NodeType:       biz.NodeType(nodeType),
		Input:          input,
		IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		return nil, err
	}
	return &pb.ExecuteToolResponse{NodeExecutionId: execution.ID}, nil
}

func (s *ToolService) CheckToolExecution(ctx context.Context, req *pb.CheckToolExecutionRequest) (*pb.CheckToolExecutionResponse, error) {
	if req == nil {
		return nil, biz.ErrToolInvalidArgument
	}
	nodeExecutionID := strings.TrimSpace(req.GetNodeExecutionId())
	if nodeExecutionID == "" {
		return nil, biz.ErrToolInvalidArgument
	}

	execution, err := s.uc.GetToolExecution(ctx, nodeExecutionID)
	if err != nil {
		return nil, err
	}

	reply := &pb.CheckToolExecutionResponse{Status: string(execution.Status)}
	if execution.Output != nil {
		reply.Output, err = structpb.NewStruct(execution.Output)
		if err != nil {
			return nil, biz.ErrToolInternal
		}
	}
	if execution.InternalErrorMessage != nil {
		reply.InternalErrorMessage = *execution.InternalErrorMessage
	}
	if execution.DurationMS != nil {
		reply.DurationMs = *execution.DurationMS
	}
	return reply, nil
}
