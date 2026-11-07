// Package metrics adapts Kratos and Temporal metrics to the workflow service's
// Prometheus contract.
package metrics

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	otelprometheus "go.opentelemetry.io/otel/exporters/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

var durationBuckets = append(
	append([]float64(nil), prometheus.DefBuckets...),
	30, 60, 120, 300, 600, 1800, 3600, 7200,
)

// Registry owns the process-local Prometheus registry and metric instruments.
type Registry struct {
	promRegistry        *prometheus.Registry
	httpRequestsTotal   *prometheus.CounterVec
	httpRequestDuration *prometheus.HistogramVec
	meterProvider       *sdkmetric.MeterProvider
	logger              *slog.Logger
}

// New creates an isolated registry shared by the HTTP and Temporal adapters.
func New(logger *slog.Logger) (*Registry, func(), error) {
	promRegistry := prometheus.NewRegistry()
	r := &Registry{
		promRegistry: promRegistry,
		httpRequestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_server_requests_total",
			Help: "Total number of HTTP requests handled, by route template.",
		}, []string{"method", "route", "status"}),
		httpRequestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "http_server_request_duration_seconds",
			Help:    "Duration of HTTP requests in seconds, by route template.",
			Buckets: durationBuckets,
		}, []string{"method", "route", "status"}),
		logger: logger,
	}

	promRegistry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		r.httpRequestsTotal,
		r.httpRequestDuration,
	)

	exporter, err := otelprometheus.New(otelprometheus.WithRegisterer(promRegistry))
	if err != nil {
		return nil, nil, fmt.Errorf("create prometheus exporter: %w", err)
	}
	r.meterProvider = sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(exporter),
		sdkmetric.WithView(sdkmetric.NewView(
			sdkmetric.Instrument{
				Name: "temporal_*",
				Kind: sdkmetric.InstrumentKindHistogram,
			},
			sdkmetric.Stream{
				Aggregation: sdkmetric.AggregationExplicitBucketHistogram{
					Boundaries: durationBuckets,
				},
			},
		)),
	)

	cleanup := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := r.meterProvider.Shutdown(ctx); err != nil {
			logger.Error("metrics shutdown failed", "error", err)
		}
	}
	return r, cleanup, nil
}

// Handler returns the scrape endpoint for all process-local metrics.
func (r *Registry) Handler() http.Handler {
	return promhttp.HandlerFor(r.promRegistry, promhttp.HandlerOpts{})
}
