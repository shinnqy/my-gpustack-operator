package httpx

import (
	"crypto/tls"
	"net"
	"net/http"
	"time"
)

type TransportOption struct {
	transport *http.Transport
}

func TransportOptions() *TransportOption {
	transport := &http.Transport{
		Proxy: ProxyFromEnvironment,
		TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
		},
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	return &TransportOption{
		transport: transport,
	}
}

// WithoutProxy 禁用代理。
func (o *TransportOption) WithoutProxy() *TransportOption {
	if o == nil || o.transport == nil {
		return o
	}
	o.transport.Proxy = nil
	return o
}

// WithoutInsecureVerify 跳过 TLS 证书校验。
func (o *TransportOption) WithoutInsecureVerify() *TransportOption {
	if o == nil || o.transport == nil {
		return o
	}
	o.transport.TLSClientConfig.InsecureSkipVerify = true
	return o
}

// WithTLSClientConfig 设置 tls.Config。
func (o *TransportOption) WithTLSClientConfig(config *tls.Config) *TransportOption {
	if o == nil || o.transport == nil {
		return o
	}
	o.transport.TLSClientConfig = config
	return o
}
