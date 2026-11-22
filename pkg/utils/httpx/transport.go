package httpx

import (
	"net/http"
)

// DefaultInsecureTransport 是本包使用的默认 http.DefaultTransport，
// 启用了跳过 TLS 证书校验。
var DefaultInsecureTransport http.RoundTripper = Transport(TransportOptions().WithoutInsecureVerify())

// Transport 使用给定选项返回一个新的 http.Transport，
// 返回的 http.Transport 用于构造 http.Client。
func Transport(opts ...*TransportOption) *http.Transport {
	var o *TransportOption
	if len(opts) > 0 {
		o = opts[0]
	} else {
		o = TransportOptions()
	}

	return o.transport
}
