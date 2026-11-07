package service

import (
	"context"

	pb "github.com/hobbyGG/workflow/api/worker/v1"
	"github.com/hobbyGG/workflow/internal/biz"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/worker"
	temporalworkflow "go.temporal.io/sdk/workflow"
)

// WorkerService is a worker service.
type WorkerService struct {
	pb.UnimplementedWorkerServiceServer

	worker *biz.WorkerUsecase
}

// NewWorkerService creates a worker service.
func NewWorkerService(worker *biz.WorkerUsecase) *WorkerService {
	return &WorkerService{worker: worker}
}

// RegisterTemporalWorker 注册 WorkerService 提供的 Workflow 和 Activity。
func (s *WorkerService) RegisterTemporalWorker(w worker.Worker) {
	w.RegisterWorkflowWithOptions(s.worker.DynamicWorkflow, temporalworkflow.RegisterOptions{
		Name: biz.DynamicWorkflowType,
	})
	w.RegisterWorkflowWithOptions(s.worker.StandAloneNodeWorkflow, temporalworkflow.RegisterOptions{
		Name: biz.ActivityNodeWorkflowType,
	})
	w.RegisterActivityWithOptions(s.worker.UpdateWfExecutionStatus, activity.RegisterOptions{
		Name: biz.UpdateWfExecutionStatusActivityType,
	})
	w.RegisterActivityWithOptions(s.worker.MarkNodeExecutionRunning, activity.RegisterOptions{
		Name: biz.MarkNodeExecutionRunningActivityType,
	})
	w.RegisterActivityWithOptions(s.worker.FinishNodeExecution, activity.RegisterOptions{
		Name: biz.FinishNodeExecutionActivityType,
	})
	for _, node := range s.worker.GetActivityNodes() {
		nodeType := node.Type()
		w.RegisterActivityWithOptions(node.Activity, activity.RegisterOptions{
			Name: nodeType.String(),
		})
	}
}

func (*WorkerService) Healthz(context.Context, *pb.HealthzRequest) (*pb.HealthzResponse, error) {
	return &pb.HealthzResponse{}, nil
}

func (*WorkerService) Readyz(context.Context, *pb.ReadyzRequest) (*pb.ReadyzResponse, error) {
	return &pb.ReadyzResponse{}, nil
}
