package server

import (
	"context"
	"log/slog"
	"strings"
	"sync"

	workerv1 "github.com/hobbyGG/workflow/api/worker/v1"
	"github.com/hobbyGG/workflow/internal/conf"
	"github.com/hobbyGG/workflow/internal/service"

	"github.com/go-kratos/kratos/v3/transport/grpc"
	"github.com/go-kratos/kratos/v3/transport/http"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

// NewWorkerHTTPServer 创建并注册 Worker HTTP 服务。
func NewWorkerHTTPServer(c *conf.Server, workerService *service.WorkerService, logger *slog.Logger) *http.Server {
	srv := newHTTPServer(c, logger)
	workerv1.RegisterWorkerServiceHTTPServer(srv, workerService)
	return srv
}

// NewWorkerGRPCServer 创建并注册 Worker gRPC 服务。
func NewWorkerGRPCServer(c *conf.Server, workerService *service.WorkerService) *grpc.Server {
	srv := newGRPCServer(c)
	workerv1.RegisterWorkerServiceServer(srv, workerService)
	return srv
}

// TemporalWorker 将 Temporal Worker 接入 Kratos 的服务生命周期。
type TemporalWorker struct {
	worker     worker.Worker
	stopSignal chan any
	done       chan struct{}
	stopOnce   sync.Once
}

// NewTemporalWorker 创建 Temporal Worker 并注册 WorkerService。
func NewTemporalWorker(c *conf.Data, temporalClient client.Client, workerService *service.WorkerService) *TemporalWorker {
	taskQueue := strings.TrimSpace(c.Temporal.TaskQueue)
	s := &TemporalWorker{
		stopSignal: make(chan any),
		done:       make(chan struct{}),
	}
	s.worker = worker.New(temporalClient, taskQueue, worker.Options{
		DisableRegistrationAliasing: true,
	})
	workerService.RegisterTemporalWorker(s.worker)
	return s
}

// Start 启动 Temporal Worker，并持续运行到 Worker 停止。
func (s *TemporalWorker) Start(context.Context) error {
	defer close(s.done)
	return s.worker.Run(s.stopSignal)
}

// Stop 停止 Temporal Worker。
func (s *TemporalWorker) Stop(ctx context.Context) error {
	s.stopOnce.Do(func() {
		close(s.stopSignal)
	})

	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
