package httpx

import (
	"context"
	"net/http"
)

// DefaultInsecureClient 是本包使用的默认 http.Client，
// 启用了跳过 TLS 证书校验。
//
// 它用于请求池化复用。
var DefaultInsecureClient = &http.Client{
	Transport: DefaultInsecureTransport,
}

// NewGetRequestWithContext 返回一个新的 http.MethodGet 请求，
// 省去直接调用 http.NewRequestWithContext 的繁琐。
func NewGetRequestWithContext(ctx context.Context, uri string) (*http.Request, error) {
	return http.NewRequestWithContext(ctx, http.MethodGet, uri, nil)
}

// Close 关闭 HTTP 响应体并忽略错误。
func Close(resp *http.Response) {
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
}
