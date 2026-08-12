package server

import (
	workflowv1 "github.com/hobbyGG/workflow/api/workflow/v1"
	"github.com/hobbyGG/workflow/internal/conf"
	"github.com/hobbyGG/workflow/internal/service"

	"github.com/go-kratos/kratos/v3/middleware/recovery"
	"github.com/go-kratos/kratos/v3/transport/grpc"
)

func NewGRPCServer(
	c *conf.Server,
	workflow *service.WorkflowService,
) *grpc.Server {
	srv := newGRPCServer(c)
	workflowv1.RegisterWorkflowServiceServer(srv, workflow)
	return srv
}

func newGRPCServer(c *conf.Server) *grpc.Server {
	var opts = []grpc.ServerOption{
		grpc.Middleware(
			recovery.Recovery(),
		),
	}
	if c.Grpc.Network != "" {
		opts = append(opts, grpc.Network(c.Grpc.Network))
	}
	if c.Grpc.Addr != "" {
		opts = append(opts, grpc.Address(c.Grpc.Addr))
	}
	if c.Grpc.Timeout != nil {
		opts = append(opts, grpc.Timeout(c.Grpc.Timeout.AsDuration()))
	}
	return grpc.NewServer(opts...)
}
