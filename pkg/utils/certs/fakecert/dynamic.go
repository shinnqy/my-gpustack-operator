package fakecert

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/idna"
	klog "k8s.io/klog/v2"

	"gpustack.ai/gpustack/pkg/utils/certs"
)

// DynamicManager 能够动态管理自签名主机证书，根据 hello 信息生成主机证书。
type DynamicManager struct {
	// 缓存可以可选地存储和检索之前获得的证书及其他状态信息。如果为 `nil`，则证书仅会在 DynamicManager
	// 的生命周期内被缓存。
	//
	// 多个 DynamicManager 实例可以共享同一个缓存。
	Cache certs.Cache

	// HostPolicy 控制 DynamicManager 将尝试获取新证书的域。它不会影响已缓存的证书。
	//
	// 如果不为空，会在请求新的 getCert 之前调用 HostPolicy。如果为空，则所有主机均被允许。
	HostPolicy certs.HostPolicy

	state sync.Map
}

// GetCertificate 实现了 tls.Config.GetCertificate 钩子。它为 hello.ServerName 主机提供一份 TLS 证书。
// hello 的其他所有字段均被忽略。
//
// 如果 m.HostPolicy 不为空，则 GetCertificate 在请求新的 getCert 之前会调用该策略。从 m.HostPolicy 返回的
// 非空错误会中断 TLS 协商过程。该错误会被传递回 GetCertificate 的调用者，并且对用户可见。这不会影响已缓存
// 的证书。
func (m *DynamicManager) GetCertificate(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
	var (
		host   = hello.ServerName
		hostIP string
	)
	if host != "" {
		n, err := idna.Lookup.ToASCII(host)
		if err != nil {
			return nil, errors.New("fakecert: host contains invalid character")
		}
		host = strings.TrimSuffix(n, ".")
	} else {
		addr := hello.Conn.LocalAddr()
		if addr == nil {
			return nil, errors.New("fakecert: missing address")
		}
		tcpAddr, ok := addr.(*net.TCPAddr)
		if !ok {
			return nil, errors.New("fakecert: invalid address")
		}
		hostIP = tcpAddr.IP.String()
	}
	if host == "" {
		host = hostIP
		hostIP = ""
	}

	// Timeout 需要处理最坏情况。
	ctx, cancel := context.WithTimeout(hello.Context(), 30*time.Second)
	defer cancel()

	// 通过主机获取证书。
	ck := certKey{
		host:     host,
		alterIPs: hostIP,
		rsa:      !certs.RequestECDSA(hello),
	}

	tlsCert, err := m.getCert(ctx, ck)
	if err == nil {
		return tlsCert, nil
	}

	if !errors.Is(err, certs.ErrCacheMiss) {
		return nil, fmt.Errorf("fakecert: get cert: %w", err)
	}

	// 创建主机证书。
	err = m.allowHost(ctx, host)
	if err != nil {
		return nil, fmt.Errorf("fakecert: disallowed host: %s", host)
	}

	tlsCert, err = m.createCert(ctx, ck)
	if err != nil {
		return nil, fmt.Errorf("fakecert: create cert: %w", err)
	}

	return tlsCert, nil
}

// createCert 开始主机所有权验证，成功时将为该主机生成证书。
//
// 如果主机已处于验证状态，它将等待现有验证完成。无论哪种情况，createCert 都会在整个过程中处于阻塞状态。
func (m *DynamicManager) createCert(ctx context.Context, ck certKey) (*tls.Certificate, error) {
	s := &certState{c: sync.NewCond(&sync.Mutex{})}
	if v, ok := m.state.LoadOrStore(ck, s); ok {
		s = v.(*certState)
	} else {
		caKey, caCert, err := loadOrGenCA(ctx, m.Cache)
		if err != nil {
			return nil, err
		}

		s.Generate(ctx, caKey, caCert, ck.host, ck.alterIPs, ck.alterDNSNames, ck.rsa)
	}

	tlsCert, err := s.Get()
	if err != nil {
		return nil, err
	}

	m.cachePut(ctx, ck, tlsCert)

	return tlsCert, nil
}

// getCert 返回现有的证书，可能来自 m.state 或缓存。如果证书存在于缓存中但不在 m.state 中，则 m.state 将被
// 填充为缓存值。
func (m *DynamicManager) getCert(ctx context.Context, ck certKey) (*tls.Certificate, error) {
	logger := klog.Background().WithName("fakecert").WithName("dynamic")

	if v, ok := m.state.Load(ck); ok {
		s := v.(*certState)

		tlsCert, err := s.Get()
		if err != nil {
			return nil, err
		}

		err = certs.VerifyX509Certificate(tlsCert.Leaf, ck.host)
		if err != nil {
			logger.V(4).
				Error(err, "verify certificate")
			// 视为缺失的缓存，因此 GetCertificate 将会重新生成。
			return nil, certs.ErrCacheMiss
		}

		return tlsCert, nil
	}

	tlsCert, err := m.cacheGet(ctx, ck)
	if err != nil {
		return nil, err
	}

	logger.V(4).
		Info("loaded certificate from cache", "host", ck.host)
	m.state.Store(ck, &certState{tlsCert: tlsCert})

	return tlsCert, nil
}

// cacheGet 从缓存中加载数据，并解码私钥和证书。
func (m *DynamicManager) cacheGet(ctx context.Context, ck certKey) (*tls.Certificate, error) {
	logger := klog.Background().WithName("fakecert").WithName("dynamic")

	if m.Cache == nil {
		return nil, certs.ErrCacheMiss
	}

	bs, err := m.Cache.Get(ctx, ck.String())
	if err != nil {
		return nil, err
	}

	tlsCert, err := certs.DecodeTLSCertificate(bs, ck.host)
	if err != nil {
		logger.V(4).
			Error(err, "decode tls certificate")
		// 视为缺失的缓存，因此 GetCertificate 将会重新生成。
		return nil, certs.ErrCacheMiss
	}

	return tlsCert, nil
}

// cachePut 会将私钥和证书一起编码，并存储到缓存中。
func (m *DynamicManager) cachePut(ctx context.Context, ck certKey, tlsCert *tls.Certificate) {
	logger := klog.Background().WithName("fakecert").WithName("dynamic")

	if m.Cache == nil {
		return
	}

	bs, err := certs.EncodeTLSCertificate(tlsCert)
	if err != nil {
		logger.V(4).
			Error(err, "encode tls certificate")
		return
	}

	err = m.Cache.Put(ctx, ck.String(), bs)
	if err != nil {
		logger.V(4).
			Error(err, "cache tls certificate bytes")
	}
}

// allowHost 返回 true 表示该主机被允许。
func (m *DynamicManager) allowHost(ctx context.Context, hostname string) error {
	if m.HostPolicy != nil {
		return m.HostPolicy(ctx, hostname)
	}

	return nil
}
