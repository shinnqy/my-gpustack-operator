package httpx

import (
	"context"
	"net/http"
)

// DefaultInsecureClient is the default http.Client used by the package,
// with TLS insecure skip verify.
//
// It is used for requests pooling.
var DefaultInsecureClient = &http.Client{
	Transport: DefaultInsecureTransport,
}

// NewGetRequestWithContext returns a new http.MethodGet request,
// which is saving your life from http.NewRequestWithContext.
func NewGetRequestWithContext(ctx context.Context, uri string) (*http.Request, error) {
	return http.NewRequestWithContext(ctx, http.MethodGet, uri, nil)
}

// Close closes the http response body without error.
func Close(resp *http.Response) {
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
}
