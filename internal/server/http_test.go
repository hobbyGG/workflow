package server

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/hobbyGG/workflow/internal/biz"
	"github.com/hobbyGG/workflow/internal/conf"
	"github.com/hobbyGG/workflow/internal/service"
	"github.com/hobbyGG/workflow/pkg/metrics"
)

type routeToolNodeRepo struct{}

func (routeToolNodeRepo) GetActivityNodes() []biz.ActivityNode {
	return nil
}

type routeToolExecutionRepo struct{}

func (routeToolExecutionRepo) GetNodeExecutionInfos(context.Context) (map[biz.NodeType]biz.NodeExecutionInfo, error) {
	return nil, nil
}

func (routeToolExecutionRepo) GetNodeExecution(context.Context, string) (*biz.NodeExecution, error) {
	return nil, nil
}

func (routeToolExecutionRepo) CreateNodeExecution(_ context.Context, execution *biz.NodeExecution) (*biz.NodeExecution, error) {
	return execution, nil
}

func (routeToolExecutionRepo) DispatchNodeExecution(context.Context, string, biz.ActivityNodeWorkflowParam) error {
	return nil
}

func TestNewHTTPServerRegistersToolRoutes(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	registry, cleanup, err := metrics.New(logger)
	if err != nil {
		t.Fatalf("metrics.New() error = %v", err)
	}
	defer cleanup()

	tool := service.NewToolService(biz.NewToolUsecase(routeToolNodeRepo{}, routeToolExecutionRepo{}))
	srv := NewHTTPServer(
		&conf.Server{Http: &conf.Server_HTTP{}},
		service.NewWorkflowService(nil, nil, nil),
		tool,
		logger,
		registry,
	)

	response := httptest.NewRecorder()
	srv.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/internal/v1/tool/nodes", nil))

	if response.Code != http.StatusOK {
		t.Fatalf("GET /internal/v1/tool/nodes status = %d, want %d; body = %s", response.Code, http.StatusOK, response.Body.String())
	}
}
