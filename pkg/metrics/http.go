package metrics

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-kratos/kratos/v3/errors"
	"github.com/go-kratos/kratos/v3/middleware"
	"github.com/go-kratos/kratos/v3/transport"
	kratoshttp "github.com/go-kratos/kratos/v3/transport/http"
)

// HTTPMiddleware records per-route RED metrics for Kratos HTTP handlers.
func (r *Registry) HTTPMiddleware() middleware.Middleware {
	return func(next middleware.Handler) middleware.Handler {
		return func(ctx context.Context, req any) (any, error) {
			start := time.Now()
			reply, err := next(ctx, req)

			tr, ok := transport.FromServerContext(ctx)
			if !ok {
				return reply, err
			}
			httpTransport, ok := tr.(kratoshttp.Transporter)
			if !ok {
				return reply, err
			}
			httpRequest := httpTransport.Request()
			if httpRequest == nil {
				return reply, err
			}

			route := httpTransport.PathTemplate()
			switch route {
			case "/metrics", "/live", "/ready":
				return reply, err
			}
			userAgent := httpRequest.UserAgent()
			if strings.HasPrefix(userAgent, "kube-probe/") || strings.HasPrefix(userAgent, "Envoy/") {
				return reply, err
			}
			if route == "" {
				route = "unmatched"
			}

			method := httpRequest.Method
			switch method {
			case http.MethodGet, http.MethodPost, http.MethodPut, http.MethodPatch,
				http.MethodDelete, http.MethodHead, http.MethodOptions:
			default:
				method = "OTHER"
			}
			statusCode := strconv.Itoa(errors.Code(err))
			r.httpRequestsTotal.WithLabelValues(method, route, statusCode).Inc()
			r.httpRequestDuration.WithLabelValues(method, route, statusCode).Observe(time.Since(start).Seconds())
			return reply, err
		}
	}
}
