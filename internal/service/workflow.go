package service

import (
	pb "github.com/hobbyGG/workflow/api/workflow/v1"
	"github.com/hobbyGG/workflow/internal/biz"
)

type WorkflowService struct {
	pb.UnimplementedWorkflowServiceServer

	wfExecution *biz.WfExecutionUsecase
	definition  *biz.DefinitionUsecase
	node        *biz.NodeUsecase
}

func NewWorkflowService(wfExecution *biz.WfExecutionUsecase, definition *biz.DefinitionUsecase, node *biz.NodeUsecase) *WorkflowService {
	return &WorkflowService{
		wfExecution: wfExecution,
		definition:  definition,
		node:        node,
	}
}
