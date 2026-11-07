package service

import (
	"context"
	"strings"

	pb "github.com/hobbyGG/workflow/api/workflow/v1"
	"github.com/hobbyGG/workflow/internal/biz"

	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	defaultListWfExecutionsLimit int32 = 20
	maxListWfExecutionsLimit     int32 = 200
)

func (s *WorkflowService) CreateWorkflowExecution(ctx context.Context, req *pb.CreateWorkflowExecutionRequest) (*pb.CreateWorkflowExecutionResponse, error) {
	if req == nil ||
		req.GetUid() <= 0 ||
		strings.TrimSpace(req.GetIdempotencyKey()) == "" ||
		strings.TrimSpace(req.GetDefinitionId()) == "" {
		return nil, biz.ErrWfExecutionInvalidArgument
	}
	wfExecution, err := s.wfExecution.CreateWfExecution(ctx, convertWfExecution(req))
	if err != nil {
		return nil, err
	}
	return &pb.CreateWorkflowExecutionResponse{
		Data: &pb.CreateWorkflowExecutionData{
			WorkflowExecutionId: wfExecution.ID,
		},
	}, nil
}

func convertWfExecution(req *pb.CreateWorkflowExecutionRequest) *biz.CreateWfExecutionParam {
	input := make(map[string]any)
	if req.GetInput() != nil {
		input = req.GetInput().AsMap()
	}
	return &biz.CreateWfExecutionParam{
		UID:            req.GetUid(),
		IdempotencyKey: req.GetIdempotencyKey(),
		DefinitionID:   req.GetDefinitionId(),
		Input:          input,
	}
}

// GetWorkflowExecution 返回指定的工作流执行记录。
func (s *WorkflowService) GetWorkflowExecution(ctx context.Context, req *pb.GetWorkflowExecutionRequest) (*pb.GetWorkflowExecutionResponse, error) {
	if req == nil {
		return nil, biz.ErrWfExecutionInvalidArgument
	}
	workflowExecutionID := strings.TrimSpace(req.GetWorkflowExecutionId())
	if workflowExecutionID == "" {
		return nil, biz.ErrWfExecutionInvalidArgument
	}

	wfExecution, err := s.wfExecution.GetWfExecution(ctx, workflowExecutionID)
	if err != nil {
		return nil, err
	}
	reply, err := convertWfExecutionReply(wfExecution)
	if err != nil {
		return nil, err
	}
	return &pb.GetWorkflowExecutionResponse{Data: reply}, nil
}

// ListWorkflowExecutions 返回满足查询条件的工作流执行记录。
func (s *WorkflowService) ListWorkflowExecutions(ctx context.Context, req *pb.ListWorkflowExecutionsRequest) (*pb.ListWorkflowExecutionsResponse, error) {
	if req == nil ||
		req.GetUid() <= 0 ||
		req.GetLimit() < 0 ||
		req.GetLimit() > maxListWfExecutionsLimit ||
		req.GetOffset() < 0 {
		return nil, biz.ErrWfExecutionInvalidArgument
	}

	limit := req.GetLimit()
	if limit == 0 {
		limit = defaultListWfExecutionsLimit
	}
	wfExecutions, err := s.wfExecution.ListWfExecutions(ctx, biz.ListWfExecutionsQuery{
		UID:            req.GetUid(),
		DefinitionType: strings.TrimSpace(req.GetDefinitionType()),
		Limit:          limit,
		Offset:         req.GetOffset(),
	})
	if err != nil {
		return nil, err
	}

	page := &pb.WorkflowExecutionPage{
		Limit:              limit,
		Offset:             req.GetOffset(),
		WorkflowExecutions: make([]*pb.WorkflowExecution, 0, len(wfExecutions)),
	}
	for _, wfExecution := range wfExecutions {
		reply, err := convertWfExecutionReply(wfExecution)
		if err != nil {
			return nil, err
		}
		page.WorkflowExecutions = append(page.WorkflowExecutions, reply)
	}
	return &pb.ListWorkflowExecutionsResponse{Data: page}, nil
}

func convertWfExecutionReply(in *biz.WfExecution) (*pb.WorkflowExecution, error) {
	if in == nil {
		return nil, biz.ErrWfExecutionInternal
	}
	input, err := structpb.NewStruct(in.Input)
	if err != nil {
		return nil, biz.ErrWfExecutionInternal
	}
	reply := &pb.WorkflowExecution{
		WorkflowExecutionId: in.ID,
		Uid:                 in.UID,
		IdempotencyKey:      in.IdempotencyKey,
		DefinitionId:        in.DefinitionID,
		DefinitionType:      in.DefinitionType,
		Status:              string(in.Status),
		Input:               input,
	}
	if in.Output != nil {
		reply.Output, err = structpb.NewValue(in.Output)
		if err != nil {
			return nil, biz.ErrWfExecutionInternal
		}
	}
	if in.ErrorInfo != nil {
		if in.ErrorInfo.InternalErrorCode != nil {
			reply.InternalErrorCode = *in.ErrorInfo.InternalErrorCode
		}
		if in.ErrorInfo.InternalErrorMessage != nil {
			reply.InternalErrorMessage = *in.ErrorInfo.InternalErrorMessage
		}
		if in.ErrorInfo.FailedNodeID != nil {
			reply.FailedNodeId = *in.ErrorInfo.FailedNodeID
		}
	}
	if !in.CreatedAt.IsZero() {
		reply.CreatedAt = timestamppb.New(in.CreatedAt)
	}
	if in.StartedAt != nil {
		reply.StartedAt = timestamppb.New(*in.StartedAt)
	}
	if in.CompletedAt != nil {
		reply.CompletedAt = timestamppb.New(*in.CompletedAt)
	}
	if !in.UpdatedAt.IsZero() {
		reply.UpdatedAt = timestamppb.New(in.UpdatedAt)
	}
	return reply, nil
}
