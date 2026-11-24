package peer

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	authentication "k8s.io/api/authentication/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	klog "k8s.io/klog/v2"

	"gpustack.ai/gpustack/pkg/utils/json"
	"gpustack.ai/gpustack/pkg/utils/stringx"
)

// AuthMode 指定两个节点插件之间相互认证的方式。
type AuthMode string

const (
	// AuthNone 不进行认证直接服务与拉取，仅用于基线测试。
	AuthNone AuthMode = "none"
	// AuthToken 携带拉取方插件的投影 ServiceAccount 令牌，由服务方通过绑定到特定 audience 的 TokenReview 校验。
	AuthToken AuthMode = "token"
)

const (
	// tokenFile 是 Pod 中挂载的专用于本功能的短期投影 ServiceAccount 令牌路径，由 chart 声明该卷。
	tokenFile = "/var/run/secrets/gpustack-model-peer/token"
	// PeerAudience 是所有 peer 令牌签发和审核共用的 audience：服务端只认这一固定受众。
	PeerAudience = "gpustack-model-peer"
)

// tokenExpiry 从 Bearer 令牌自身的 claims 中读取过期时间，不做签名校验：
// 签名验证由 API Server 的 TokenReview 负责，这里仅用于限定该令牌审核结果的缓存上限。
func tokenExpiry(token string) (time.Time, bool) {
	_, rest, ok := strings.Cut(token, ".")
	if !ok || rest == "" {
		return time.Time{}, false
	}
	payload, _, _ := strings.Cut(rest, ".")
	claims, err := stringx.DecodeBase64(payload)
	if err != nil {
		return time.Time{}, false
	}
	var parsed struct {
		Exp int64 `json:"exp"`
	}
	if json.Unmarshal([]byte(claims), &parsed) != nil || parsed.Exp <= 0 {
		return time.Time{}, false
	}

	return time.Unix(parsed.Exp, 0), true
}

// AuthOptions 保存 peer 连接双方构建认证逻辑所需的全部配置。
type AuthOptions struct {
	// Mode 指定 peer 相互认证的方式。
	Mode AuthMode
	// Audience 是 peer 令牌签发和审核绑定的 audience。token 模式必须校验该字段，
	// 避免仅按用户名放行签发给 API Server 的普通令牌。
	Audience string
	// Namespace 是插件运行所在的 Namespace。
	Namespace string
	// ServiceAccount 是插件的 ServiceAccount 名称（不含 Namespace），token 模式只放行该账号的令牌。
	ServiceAccount string
}

// Validate 检查认证模式是否受当前包支持。
func (a AuthOptions) Validate() error {
	switch a.Mode {
	case AuthNone, AuthToken:
		return nil
	default:
		return fmt.Errorf("peer auth %q is none of %q or %q", a.Mode, AuthNone, AuthToken)
	}
}

// RoundTripper 包装底层 transport，使每个请求携带拉取方凭据。
// 非 token 模式下直接原样返回 transport。
func (a AuthOptions) RoundTripper(transport http.RoundTripper) http.RoundTripper {
	if a.Mode != AuthToken {
		return transport
	}

	return &tokenTransport{base: transport}
}

// tokenTransport 在每次请求前读取 Pod 的投影 ServiceAccount 令牌并写入请求头，
// 确保令牌轮换后能够立即在下一次请求中生效。
type tokenTransport struct {
	base http.RoundTripper
}

func (t *tokenTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	b, err := readFileLimited(tokenFile)
	if err != nil {
		return nil, fmt.Errorf("read the projected peer token: %w", err)
	}
	req = req.Clone(req.Context())
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(b)))

	return t.base.RoundTrip(req)
}

// reviewToken 向 API Server 校验 Bearer 令牌归属。通过的结果会缓存至缓存 TTL 与令牌过期时间中的较早者；
// 拒绝结果绝不缓存，防止滚动更新的新 Pod 在投影卷轮换前提交一次旧令牌后被持续锁定。
type reviewToken func(ctx context.Context, token string) (string, error)

func (a AuthOptions) reviewer(cli tokenClient) reviewToken {
	const cacheTTL = 2 * time.Minute

	var (
		mu     sync.Mutex
		cached = map[string]reviewed{}
	)
	return func(ctx context.Context, token string) (string, error) {
		mu.Lock()
		if r, ok := cached[token]; ok && r.validUntil.After(time.Now()) {
			mu.Unlock()

			return r.username, nil
		}
		mu.Unlock()

		username, err := a.review(ctx, cli, token)
		if err != nil {
			return "", err
		}
		validUntil := time.Now().Add(cacheTTL)
		if exp, ok := tokenExpiry(token); ok && exp.Before(validUntil) {
			validUntil = exp
		}
		mu.Lock()
		cached[token] = reviewed{username: username, validUntil: validUntil}
		for k, r := range cached {
			if r.validUntil.Before(time.Now()) {
				delete(cached, k)
			}
		}
		mu.Unlock()

		return username, nil
	}
}

type reviewed struct {
	username   string
	validUntil time.Time
}

// tokenClient 是执行 TokenReview 所需的 API Server 最小接口。
type tokenClient interface {
	Create(ctx context.Context, review *authentication.TokenReview, opts meta.CreateOptions) (*authentication.TokenReview, error)
}

// review 将令牌按指定 audience 提交给 API Server 审核，判断其是否属于当前 chart 部署的插件账号。
func (a AuthOptions) review(ctx context.Context, cli tokenClient, token string) (string, error) {
	tr, err := cli.Create(ctx, &authentication.TokenReview{
		Spec: authentication.TokenReviewSpec{Token: token, Audiences: []string{a.Audience}},
	}, meta.CreateOptions{})
	if err != nil {
		return "", fmt.Errorf("token review: %w", err)
	}
	if !tr.Status.Authenticated {
		return "", fmt.Errorf("the peer request's token did not authenticate: %s", tr.Status.Error)
	}
	if !slicesContain(tr.Status.Audiences, a.Audience) {
		return "", fmt.Errorf("the peer request's token carries audiences %q, not %q",
			strings.Join(tr.Status.Audiences, ","), a.Audience)
	}

	want := "system:serviceaccount:" + a.Namespace + ":" + a.ServiceAccount
	if tr.Status.User.Username != want {
		return "", fmt.Errorf("the peer request's token belongs to %q, not a plugin", tr.Status.User.Username)
	}

	return tr.Status.User.Username, nil
}

func slicesContain(list []string, want string) bool {
	for _, s := range list {
		if s == want {
			return true
		}
	}

	return false
}

// AuthError 表示被拒绝的请求凭据。
type AuthError struct{ Msg string }

func (e *AuthError) Error() string { return e.Msg }

// Admit 构建服务端的单请求准入校验函数；none 模式下返回 nil 表示放行全部请求。
func (a AuthOptions) Admit(cli tokenClient) (func(*http.Request) error, error) {
	switch a.Mode {
	case AuthNone:
		return nil, nil
	case AuthToken:
		if a.Audience == "" {
			return nil, fmt.Errorf("peer auth %q needs the token audience", a.Mode)
		}
		if a.Namespace == "" || a.ServiceAccount == "" {
			return nil, fmt.Errorf("peer auth %q needs the plugins' namespace and ServiceAccount name", a.Mode)
		}
		if cli == nil {
			return nil, fmt.Errorf("peer auth %q needs the API server to review tokens with", a.Mode)
		}
		review := a.reviewer(cli)

		return func(r *http.Request) error {
			token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
			if !ok || strings.TrimSpace(token) == "" {
				return &AuthError{Msg: "no bearer token"}
			}
			if _, err := review(r.Context(), strings.TrimSpace(token)); err != nil {
				klog.V(1).InfoS("a peer request's token was refused", "error", err.Error())

				return &AuthError{Msg: "the bearer token is not a plugin's"}
			}

			return nil
		}, nil
	default:
		return nil, a.Validate()
	}
}

// ClientTLSConfig 返回拉取方的 TLS 配置：与插件安全端口一致，服务端证书为自签名证书。
func (a AuthOptions) ClientTLSConfig() (*tls.Config, error) {
	if err := a.Validate(); err != nil {
		return nil, err
	}

	return &tls.Config{
		// peer 服务端证书与插件安全端口相同，均在同一信任域内自签名签发。
		InsecureSkipVerify: true, // nolint: gosec
		MinVersion:         tls.VersionTLS12,
	}, nil
}

// ServerTLSConfig 返回服务方的 TLS 配置，通过 getCertificate 提供监听证书。
func (a AuthOptions) ServerTLSConfig(getCertificate func(*tls.ClientHelloInfo) (*tls.Certificate, error)) (*tls.Config, error) {
	if err := a.Validate(); err != nil {
		return nil, err
	}

	return &tls.Config{
		MinVersion:     tls.VersionTLS12,
		GetCertificate: getCertificate,
	}, nil
}
