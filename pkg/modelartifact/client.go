package modelartifact

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"golang.org/x/net/http/httpproxy"

	"gpustack.ai/gpustack/pkg/utils/httpx"
)

// 数据源拒绝或解析失败的原因。它们与 Resolved condition 的 reason 完全一致，
// 调用方无需进行映射。
const (
	// ReasonRevisionNotFound 表示仓库中不存在的分支、标签或提交。
	ReasonRevisionNotFound = "RevisionNotFound"
	// ReasonAccessDenied 表示凭据无法读取的仓库，包括不存在的仓库：
	// Hub 对无权限的调用方会返回相同的响应。
	ReasonAccessDenied = "AccessDenied"
	// ReasonSourceUnavailable 表示完全未能获得响应的失败情况，这与制品本身无关，会进行重试。
	ReasonSourceUnavailable = "SourceUnavailable"

	// ReasonDigestMismatch 表示数据源内容的摘要与制品的 expectedDigest 不符：
	// 用户声明的断言与数据源提供的内容不一致。
	ReasonDigestMismatch = "DigestMismatch"
	// ReasonInvalidManifest 表示响应内容违反了清单格式。
	ReasonInvalidManifest = "InvalidManifest"
	// ReasonEmptyManifest 表示不包含任何文件的提交。
	ReasonEmptyManifest = "EmptyManifest"
	// ReasonManifestTooLarge 表示目录树超出了客户端的条目上限。
	ReasonManifestTooLarge = "ManifestTooLarge"
)

// SourceError 是带有对应映射原因的解析失败错误。
//
// 其消息仅由状态码和数据源的错误码构造，绝不包含响应体或请求头，
// 因此不会泄露任何凭据。
type SourceError struct {
	Reason  string
	Message string
}

func (e *SourceError) Error() string {
	return e.Reason + ": " + e.Message
}

func sourceErrorf(reason, format string, args ...any) *SourceError {
	return &SourceError{Reason: reason, Message: fmt.Sprintf(format, args...)}
}

// ReasonOf 返回 err 错误链中任意位置的 SourceError 原因，
// 对任何其他错误则返回 ReasonSourceUnavailable：非数据源判定产生的错误属于访问数据源失败。
func ReasonOf(err error) string {
	if se, ok := errors.AsType[*SourceError](err); ok {
		return se.Reason
	}

	return ReasonSourceUnavailable
}

// HTTPClientOptions 定义解析客户端如何访问数据源。每个字段均为管理员配置；
// 租户编写的任何内容都不会传入其中。
type HTTPClientOptions struct {
	// HTTPSProxy 是 HTTPS 请求使用的代理。为空时沿用 worker 自身的环境变量。
	HTTPSProxy string
	// NoProxy 是绕过 HTTPSProxy 的逗号分隔主机列表。
	NoProxy string
	// CABundle 是在系统证书池之外额外信任的 PEM 证书。为空时仅信任系统证书池。
	CABundle []byte
	// Timeout 限制单次请求的超时时间。为零时使用 DefaultRequestTimeout。
	Timeout time.Duration
}

// DefaultRequestTimeout 在选项未指定时限制对数据源的单次请求时间。
// 包含一千个条目的目录树分页响应远低于一秒；该上限用于防范挂起的数据源。
const DefaultRequestTimeout = 30 * time.Second

// ValidateProxy 在拨号或渲染前校验代理 URL：必须是包含主机且不含凭据的 http 或 https URL。
// 该值还会作为 HTTPS_PROXY 渲染到租户 Pod 中，任何能读取 Pod 的人都能看到它。
//
// 该检查会在使用处执行，而非仅在写入处执行：Setting 的默认值来自 worker 的环境变量，
// 而该路径不经过准入校验。
func ValidateProxy(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("invalid HTTPS proxy: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("invalid HTTPS proxy: scheme %q is not http or https", u.Scheme)
	}
	if u.Host == "" {
		return errors.New("invalid HTTPS proxy: no host")
	}
	if u.User != nil {
		return errors.New("invalid HTTPS proxy: it must not carry credentials")
	}

	return nil
}

// NewHTTPClient 返回用于发送解析请求的客户端。
func NewHTTPClient(opts HTTPClientOptions) (*http.Client, error) {
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	if len(opts.CABundle) > 0 {
		pool, err := x509.SystemCertPool()
		if err != nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM(opts.CABundle) {
			return nil, errors.New("the CA bundle holds no PEM certificate")
		}
		tlsConfig.RootCAs = pool
	}

	transport := httpx.Transport(httpx.TransportOptions().WithTLSClientConfig(tlsConfig))
	if opts.HTTPSProxy != "" {
		if err := ValidateProxy(opts.HTTPSProxy); err != nil {
			return nil, err
		}
		proxy := (&httpproxy.Config{HTTPSProxy: opts.HTTPSProxy, NoProxy: opts.NoProxy}).ProxyFunc()
		transport.Proxy = func(req *http.Request) (*url.URL, error) { return proxy(req.URL) }
	}

	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = DefaultRequestTimeout
	}

	return &http.Client{Transport: transport, Timeout: timeout}, nil
}
