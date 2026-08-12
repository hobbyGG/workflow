// Package slog configures the workflow service logger.
package slog

import (
	"context"
	stdslog "log/slog"
	"os"
	"strconv"
	"strings"

	"github.com/go-kratos/kratos/v3/log"
	"go.opentelemetry.io/otel/trace"
)

// Init creates the Kratos logger and installs it as the process default.
func Init(serviceID, serviceName, serviceVersion string) *stdslog.Logger {
	level := stdslog.LevelInfo
	switch value := strings.ToLower(strings.TrimSpace(os.Getenv("LOG_LEVEL"))); value {
	case "debug":
		level = stdslog.LevelDebug
	case "warn", "warning":
		level = stdslog.LevelWarn
	case "error":
		level = stdslog.LevelError
	}
	maxLineBytes := defaultLogMaxLineBytes
	if value := strings.TrimSpace(os.Getenv("LOG_MAX_LINE_BYTES")); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil && parsed > 0 {
			maxLineBytes = parsed
		}
	}
	if maxLineBytes < minLogMaxLineBytes {
		maxLineBytes = minLogMaxLineBytes
	}

	handler := stdslog.NewJSONHandler(
		boundedWriter{out: os.Stdout, maxLineBytes: maxLineBytes},
		&stdslog.HandlerOptions{
			AddSource: true,
			Level:     level,
			ReplaceAttr: func(_ []string, attr stdslog.Attr) stdslog.Attr {
				if attr.Key == stdslog.LevelKey {
					attr.Value = stdslog.StringValue(strings.ToLower(attr.Value.String()))
				}
				return attr
			},
		},
	)
	logger := log.NewLogger(
		handler,
		log.WithExtractor(func(ctx context.Context) []stdslog.Attr {
			attrs := make([]stdslog.Attr, 0, 3)
			if spanContext := trace.SpanContextFromContext(ctx); spanContext.IsValid() {
				attrs = append(attrs,
					stdslog.String("trace_id", spanContext.TraceID().String()),
					stdslog.String("span_id", spanContext.SpanID().String()),
					stdslog.String("trace_flags", spanContext.TraceFlags().String()),
				)
			}
			return attrs
		}),
	).With(
		stdslog.String("service.id", serviceID),
		stdslog.String("service.name", serviceName),
		stdslog.String("service.version", serviceVersion),
	)
	log.SetDefault(logger)
	return logger
}
