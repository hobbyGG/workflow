package wfx

import (
	"net/http"
	"time"
)

type options struct {
	httpClient *http.Client
	timeout    *time.Duration
}

// Option configures a Client.
type Option func(*options)

// WithHTTPClient configures the HTTP client's transport and timeout.
// If it is not set, NewClient uses NewHTTPClient.
func WithHTTPClient(client *http.Client) Option {
	return func(opts *options) {
		opts.httpClient = client
	}
}

// WithTimeout overrides the timeout applied to each workflow request.
// A zero timeout disables the request timeout.
func WithTimeout(timeout time.Duration) Option {
	return func(opts *options) {
		opts.timeout = &timeout
	}
}
