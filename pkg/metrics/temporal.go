package metrics

import (
	"go.temporal.io/sdk/client"
	temporalotel "go.temporal.io/sdk/contrib/opentelemetry"
)

// NewTemporalHandler adapts Temporal SDK metrics to the local OTel meter.
func NewTemporalHandler(r *Registry) client.MetricsHandler {
	return temporalotel.NewMetricsHandler(temporalotel.MetricsHandlerOptions{
		Meter:                r.meterProvider.Meter("temporal-sdk-go"),
		UseMonotonicCounters: true,
		OnError: func(err error) {
			r.logger.Error("temporal metrics error", "error", err)
		},
	})
}
