package server

import (
	"context"
	"log/slog"
	"time"

	workflowv1 "github.com/hobbyGG/workflow/api/workflow/v1"
	"github.com/hobbyGG/workflow/internal/conf"
	"github.com/hobbyGG/workflow/internal/service"
	"github.com/hobbyGG/workflow/pkg/metrics"

	"github.com/go-kratos/kratos/contrib/otel/v3/tracing"
	"github.com/go-kratos/kratos/v3/errors"
	"github.com/go-kratos/kratos/v3/middleware"
	"github.com/go-kratos/kratos/v3/middleware/recovery"
	"github.com/go-kratos/kratos/v3/middleware/validate"
	"github.com/go-kratos/kratos/v3/transport"
	"github.com/go-kratos/kratos/v3/transport/http"

	"go.einride.tech/aip/fieldbehavior"
	"google.golang.org/protobuf/proto"
)

func NewHTTPServer(
	c *conf.Server,
	workflow *service.WorkflowService,
	tool *service.ToolService,
	logger *slog.Logger,
	metricsRegistry *metrics.Registry,
) *http.Server {
	srv := newHTTPServer(c, logger, metricsRegistry)
	workflowv1.RegisterWorkflowServiceHTTPServer(srv, workflow)
	workflowv1.RegisterToolServiceHTTPServer(srv, tool)
	return srv
}

func newHTTPServer(c *conf.Server, logger *slog.Logger, metricsRegistry *metrics.Registry) *http.Server {
	var opts = []http.ServerOption{
		http.Middleware(
			tracing.Server(),
			accessLog(logger),
			metricsRegistry.HTTPMiddleware(),
			recovery.Recovery(recovery.WithLogger(logger)),
			validate.Validator(func(req any) error {
				if msg, ok := req.(proto.Message); ok {
					if err := fieldbehavior.ValidateRequiredFields(msg); err != nil {
						return err
					}
				}
				return nil
			}),
		),
	}
	if c.Http.Network != "" {
		opts = append(opts, http.Network(c.Http.Network))
	}
	if c.Http.Addr != "" {
		opts = append(opts, http.Address(c.Http.Addr))
	}
	if c.Http.Timeout != nil {
		opts = append(opts, http.Timeout(c.Http.Timeout.AsDuration()))
	}
	srv := http.NewServer(opts...)
	srv.Handle("/metrics", metricsRegistry.Handler())
	return srv
}

// accessLog records HTTP metadata without serializing decoded request or
// response bodies.
func accessLog(logger *slog.Logger) middleware.Middleware {
	if logger == nil {
		logger = slog.Default()
	}
	return func(handler middleware.Handler) middleware.Handler {
		return func(ctx context.Context, req any) (reply any, err error) {
			start := time.Now()
			reply, err = handler(ctx, req)

			method := ""
			path := ""
			if request, ok := http.RequestFromServerContext(ctx); ok {
				method = request.Method
				path = request.URL.Path
			}
			operation := ""
			if tr, ok := transport.FromServerContext(ctx); ok {
				operation = tr.Operation()
			}
			status := errors.Code(err)
			if err == nil && (path == "/live" || path == "/ready") {
				return reply, nil
			}
			attrs := []slog.Attr{
				slog.String("method", method),
				slog.String("operation", operation),
				slog.Int("status", status),
				slog.Float64("duration_ms", time.Since(start).Seconds()*1000),
			}
			if err != nil {
				attrs = append(attrs,
					slog.String("reason", errors.Reason(err)),
					slog.Any("error", err),
				)
			}

			level := slog.LevelInfo
			if status >= 500 {
				level = slog.LevelError
			} else if status >= 400 {
				level = slog.LevelWarn
			}
			logger.LogAttrs(ctx, level, "http server request", attrs...)
			return reply, err
		}
	}
}
