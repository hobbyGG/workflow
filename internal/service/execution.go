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
	defaultListExecutionsLimit int32 = 20
	maxListExecutionsLimit     int32 = 200
)

func (s *WorkflowService) CreateExecution(ctx context.Context, req *pb.CreateExecutionRequest) (*pb.CreateExecutionResponse, error) {
	if req == nil ||
		req.GetUid() <= 0 ||
		strings.TrimSpace(req.GetIdempotencyKey()) == "" ||
		strings.TrimSpace(req.GetWorkflowId()) == "" {
		return nil, biz.ErrExecutionInvalidArgument
	}
	execution, err := s.execution.CreateExecution(ctx, convertExecution(req))
	if err != nil {
		return nil, err
	}
	return &pb.CreateExecutionResponse{
		Data: &pb.CreateExecutionData{
			ExecutionId: execution.ID,
		},
	}, nil
}

func convertExecution(req *pb.CreateExecutionRequest) *biz.CreateExecutionInput {
	input := make(map[string]any)
	if req.GetInput() != nil {
		input = req.GetInput().AsMap()
	}
	return &biz.CreateExecutionInput{
		UID:            req.GetUid(),
		IdempotencyKey: req.GetIdempotencyKey(),
		WorkflowID:     req.GetWorkflowId(),
		Input:          input,
	}
}

// GetExecution 返回指定的执行记录。
func (s *WorkflowService) GetExecution(ctx context.Context, req *pb.GetExecutionRequest) (*pb.GetExecutionResponse, error) {
	if req == nil {
		return nil, biz.ErrExecutionInvalidArgument
	}
	executionID := strings.TrimSpace(req.GetExecutionId())
	if executionID == "" {
		return nil, biz.ErrExecutionInvalidArgument
	}

	execution, err := s.execution.GetExecution(ctx, executionID)
	if err != nil {
		return nil, err
	}
	reply, err := convertExecutionReply(execution)
	if err != nil {
		return nil, err
	}
	return &pb.GetExecutionResponse{Data: reply}, nil
}

// ListExecutions 返回满足查询条件的执行记录。
func (s *WorkflowService) ListExecutions(ctx context.Context, req *pb.ListExecutionsRequest) (*pb.ListExecutionsResponse, error) {
	if req == nil ||
		req.GetUid() <= 0 ||
		req.GetLimit() < 0 ||
		req.GetLimit() > maxListExecutionsLimit ||
		req.GetOffset() < 0 {
		return nil, biz.ErrExecutionInvalidArgument
	}

	limit := req.GetLimit()
	if limit == 0 {
		limit = defaultListExecutionsLimit
	}
	executions, err := s.execution.ListExecutions(ctx, biz.ListExecutionsQuery{
		UID:          req.GetUid(),
		WorkflowType: strings.TrimSpace(req.GetWorkflowType()),
		Limit:        limit,
		Offset:       req.GetOffset(),
	})
	if err != nil {
		return nil, err
	}

	page := &pb.ExecutionPage{
		Limit:      limit,
		Offset:     req.GetOffset(),
		Executions: make([]*pb.Execution, 0, len(executions)),
	}
	for _, execution := range executions {
		reply, err := convertExecutionReply(execution)
		if err != nil {
			return nil, err
		}
		page.Executions = append(page.Executions, reply)
	}
	return &pb.ListExecutionsResponse{Data: page}, nil
}

func convertExecutionReply(in *biz.Execution) (*pb.Execution, error) {
	if in == nil {
		return nil, biz.ErrExecutionInternal
	}
	input, err := structpb.NewStruct(in.Input)
	if err != nil {
		return nil, biz.ErrExecutionInternal
	}
	reply := &pb.Execution{
		ExecutionId:    in.ID,
		Uid:            in.UID,
		IdempotencyKey: in.IdempotencyKey,
		WorkflowId:     in.WorkflowID,
		WorkflowType:   in.WorkflowType,
		Status:         string(in.Status),
		Input:          input,
	}
	if in.Output != nil {
		reply.Output, err = structpb.NewValue(in.Output)
		if err != nil {
			return nil, biz.ErrExecutionInternal
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
