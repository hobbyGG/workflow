package data

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/hobbyGG/workflow/internal/conf"
	"github.com/hobbyGG/workflow/internal/data/db/sqlcgen"

	"github.com/google/wire"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.temporal.io/sdk/client"
	temporalotel "go.temporal.io/sdk/contrib/opentelemetry"
	"go.temporal.io/sdk/interceptor"
	temporallog "go.temporal.io/sdk/log"
)

// ProviderSet 包含 data 层的依赖注入提供者。
var ProviderSet = wire.NewSet(
	NewDB,
	NewTemporalClient,
	NewHTTPClient,
	NewData,
	NewWfExecutionRepo,
	NewWfExecutionStatusRepo,
	NewDefinitionRepo,
	NewNodeRepo,
	NewNodeExecutionRepo,
	NewNodeExecutionStatusRepo,
	NewWorkerNodeRepo,
)

func NewHTTPClient() *http.Client {
	return &http.Client{Transport: otelhttp.NewTransport(http.DefaultTransport), Timeout: 30 * time.Second}
}

// Data 持有 data 层共享的客户端。
type Data struct {
	db             *pgxpool.Pool
	queries        *sqlcgen.Queries
	temporalClient client.Client
	httpClient     *http.Client
	taskQueue      string
}

// NewData 组装 data 层共享依赖。
func NewData(c *conf.Data, db *pgxpool.Pool, temporalClient client.Client, httpClient *http.Client) *Data {
	taskQueue := strings.TrimSpace(c.Temporal.TaskQueue)

	return &Data{
		db:             db,
		queries:        sqlcgen.New(db),
		temporalClient: temporalClient,
		httpClient:     httpClient,
		taskQueue:      taskQueue,
	}
}

// NewDB 创建并验证数据库客户端。
func NewDB(c *conf.Data) (*pgxpool.Pool, func(), error) {
	if c == nil || c.Database == nil || strings.TrimSpace(c.Database.Source) == "" {
		return nil, nil, errors.New("database config is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	db, err := pgxpool.New(ctx, c.Database.Source)
	if err != nil {
		return nil, nil, fmt.Errorf("create database pool: %w", err)
	}

	if err := db.Ping(ctx); err != nil {
		db.Close()
		return nil, nil, fmt.Errorf("ping database: %w", err)
	}
	return db, db.Close, nil
}

// NewTemporalClient 创建 Temporal 客户端。
func NewTemporalClient(c *conf.Data, logger *slog.Logger, metricsHandler client.MetricsHandler) (client.Client, func(), error) {
	if c == nil ||
		c.Temporal == nil ||
		strings.TrimSpace(c.Temporal.Addr) == "" ||
		strings.TrimSpace(c.Temporal.Namespace) == "" ||
		strings.TrimSpace(c.Temporal.TaskQueue) == "" {
		return nil, nil, errors.New("temporal config is required")
	}

	tracingInterceptor, err := temporalotel.NewTracingInterceptor(temporalotel.TracerOptions{
		TextMapPropagator: otel.GetTextMapPropagator(),
	})
	if err != nil {
		return nil, nil, fmt.Errorf("create temporal tracing interceptor: %w", err)
	}

	temporalClient, err := client.Dial(client.Options{
		HostPort:       c.Temporal.Addr,
		Namespace:      c.Temporal.Namespace,
		Logger:         temporallog.NewStructuredLogger(logger),
		MetricsHandler: metricsHandler,
		Interceptors:   []interceptor.ClientInterceptor{tracingInterceptor},
	})
	if err != nil {
		return nil, nil, fmt.Errorf("dial temporal: %w", err)
	}
	return temporalClient, temporalClient.Close, nil
}
