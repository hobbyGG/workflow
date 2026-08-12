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
	w.RegisterActivityWithOptions(s.worker.UpdateExecutionStatus, activity.RegisterOptions{
		Name: biz.UpdateExecutionStatusActivityType,
	})
	for _, node := range s.worker.GetActivityNodes() {
		w.RegisterActivityWithOptions(node.Activity, activity.RegisterOptions{
			Name: string(node.Type()),
		})
	}
}

func (*WorkerService) Healthz(context.Context, *pb.HealthzRequest) (*pb.HealthzResponse, error) {
	return &pb.HealthzResponse{}, nil
}

func (*WorkerService) Readyz(context.Context, *pb.ReadyzRequest) (*pb.ReadyzResponse, error) {
	return &pb.ReadyzResponse{}, nil
}
