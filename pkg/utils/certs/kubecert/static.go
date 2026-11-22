package kubecert

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"strings"
	"sync"
	"time"

	"k8s.io/apiserver/pkg/server/dynamiccertificates"
	klog "k8s.io/klog/v2"

	"gpustack.ai/gpustack/pkg/utils/certs"
	"gpustack.ai/gpustack/pkg/utils/stringx"
)

// StaticManager 管理 Kubernetes CertificateSigningRequest 证书，
// 根据给定的 Host、IP 和 DNS 名称生成服务端证书。
type StaticManager struct {
	// CertCli 表示用于生成证书的客户端。
	// 若为 nil，StaticManager 将不会尝试生成新证书。
	CertCli certs.CertificateSigningRequestInterface

	// Cache 可选地用于存储和检索先前获取的证书及其他状态。
	// 若为 nil，证书仅在 StaticManager 的生命周期内缓存。
	//
	// 多个 StaticManager 实例可以共享同一个 Cache。
	Cache certs.Cache

	// Host 表示用于生成证书的主机。
	// 若为空，StaticManager 将不会尝试生成新证书。
	Host string

	// AlternateIPs 表示用于生成证书的备用 IP。
	AlternateIPs []net.IP

	// AlternateDNSNames 表示用于生成证书的备用 DNS 名称。
	AlternateDNSNames []string

	state sync.Map
}

func (m *StaticManager) Name() string {
	return "kubecert.static"
}

func (m *StaticManager) AddListener(_ dynamiccertificates.Listener) {}

func (m *StaticManager) CurrentCertKeyContent() ([]byte, []byte) {
	logger := klog.Background().WithName("kubecert").WithName("static")

	// 超时时间需要满足最坏情况的处理需求。
	ctx, cancel := context.WithTimeout(context.TODO(), 30*time.Second)
	defer cancel()

	ck := certKey{
		host:          m.Host,
		alterIPs:      strings.Join(stringx.Strings(m.AlternateIPs), ","),
		alterDNSNames: strings.Join(m.AlternateDNSNames, ","),
	}

	tlsCert, err := m.getCert(ctx, ck)
	if err != nil {
		if !errors.Is(err, certs.ErrCacheMiss) {
			logger.Error(err, "get cert")
			return nil, nil
		}

		// 为服务端创建证书。
		tlsCert, err = m.createCert(ctx, ck)
		if err != nil {
			logger.Error(err, "create cert")
			return nil, nil
		}
	}

	certPEM, keyPEM, err := certs.EncodeTLSCertificateAndKey(tlsCert)
	if err != nil {
		logger.Error(err, "encode tls certificate and key")
	}

	return certPEM, keyPEM
}

// createCert 启动服务端所有权验证，并在成功后返回该服务端的证书。
//
// 如果该服务端已在验证中，则等待现有验证完成。
// 无论哪种情况，createCert 都会在整个过程期间阻塞。
func (m *StaticManager) createCert(ctx context.Context, ck certKey) (*tls.Certificate, error) {
	s := &certState{c: sync.NewCond(&sync.Mutex{})}
	if v, ok := m.state.LoadOrStore(ck, s); ok {
		s = v.(*certState)
	} else {
		s.Generate(ctx, m.CertCli, ck.host, ck.alterIPs, ck.alterDNSNames, ck.rsa)
	}

	tlsCert, err := s.Get()
	if err != nil {
		return nil, err
	}

	m.cachePut(ctx, ck, tlsCert)

	return tlsCert, nil
}

// getCert 从 m.state 或缓存中返回现有证书，
// 如果在缓存中找到证书但 m.state 中没有，则会将缓存值填充到后者。
func (m *StaticManager) getCert(ctx context.Context, ck certKey) (*tls.Certificate, error) {
	logger := klog.Background().WithName("kubecert").WithName("dynamic")

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
			// 视为缓存未命中，
			// 以便 GetCertificate 重新生成证书。
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

// cacheGet 从缓存中加载，
// 并解码私钥和证书。
func (m *StaticManager) cacheGet(ctx context.Context, ck certKey) (*tls.Certificate, error) {
	logger := klog.Background().WithName("kubecert").WithName("dynamic")

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
		// 视为缓存未命中，
		// 以便 GetCertificate 重新生成证书。
		return nil, certs.ErrCacheMiss
	}

	return tlsCert, nil
}

// cachePut 将私钥和证书一起编码，
// 并存入缓存。
func (m *StaticManager) cachePut(ctx context.Context, ck certKey, tlsCert *tls.Certificate) {
	logger := klog.Background().WithName("kubecert").WithName("dynamic")

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
