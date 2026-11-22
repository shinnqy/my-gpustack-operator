package fakecert

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"math/big"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/acme/autocert"
	klog "k8s.io/klog/v2"

	"gpustack.ai/gpustack/pkg/utils/certs"
)

// certKey 是用于在状态和缓存中追踪证书的密钥。
type certKey struct {
	// host 表示用于生成的宿主。
	host string
	// alterIPs 表示用于生成的替代 IPs，以逗号分隔。
	alterIPs string
	// alterDNSNames 表示用于生成的替代 DNS 名称，这些名称用逗号分隔。
	alterDNSNames string
	// rsa 表示使用 RSA 算法，默认使用 ECDSA。
	rsa bool
}

func (c certKey) String() string {
	if c.rsa {
		return strings.Join([]string{c.host, c.alterIPs, c.alterDNSNames, "rsa"}, ":")
	}

	return strings.Join([]string{c.host, c.alterIPs, c.alterDNSNames}, ":")
}

// certState 是指证书可以读取的状态。
type certState struct {
	c       *sync.Cond
	tlsCert *tls.Certificate
	err     error
}

func (s *certState) Done() bool {
	return s.tlsCert != nil || s.err != nil
}

func (s *certState) Generate(
	ctx context.Context,
	caKey crypto.Signer, caCert *x509.Certificate,
	host, alterIPs, alterDNSNames string, isRSA bool,
) {
	if caKey == nil || caCert == nil {
		s.err = errors.New("fakecert: CA is nil")
		return
	}

	s.c.L.Lock()
	defer func() {
		s.c.L.Unlock()
		s.c.Broadcast()
	}()

	var key crypto.Signer
	if isRSA {
		key, s.err = rsa.GenerateKey(rand.Reader, 2048)
	} else {
		key, s.err = ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	}
	if s.err != nil {
		return
	}

	if host == "" {
		s.err = errors.New("fakecert: missing host")
		return
	}

	hosts := []string{host}
	if alterIPs != "" {
		hosts = append(hosts, strings.Split(alterIPs, ",")...)
	}
	if alterDNSNames != "" {
		hosts = append(hosts, strings.Split(alterDNSNames, ",")...)
	}

	var srvCert *x509.Certificate
	srvCert, s.err = genCert(caKey, caCert, key, hosts)
	if s.err != nil {
		return
	}

	s.tlsCert = &tls.Certificate{
		PrivateKey: key,
		Certificate: [][]byte{
			srvCert.Raw,
		},
		Leaf: srvCert,
	}
}

func (s *certState) Get() (*tls.Certificate, error) {
	if !s.Done() {
		s.c.L.Lock()
		for !s.Done() {
			s.c.Wait()
		}
		s.c.L.Unlock()
	}

	return s.tlsCert, s.err
}

// loadOrGenCA 用于加载或生成 CA 证书和私钥。
func loadOrGenCA(ctx context.Context, c certs.Cache) (crypto.Signer, *x509.Certificate, error) {
	bs, err := c.Get(ctx, "ca")
	if err != nil {
		if !errors.Is(err, autocert.ErrCacheMiss) {
			return nil, nil, err
		}

		key, x509Cert, err := genCA()
		if err != nil {
			return nil, nil, err
		}

		bs, err = certs.EncodeTLSCertificate(&tls.Certificate{
			PrivateKey: key,
			Certificate: [][]byte{
				x509Cert.Raw,
			},
		})
		if err != nil {
			return nil, nil, err
		}

		err = c.Put(ctx, "ca", bs)
		if err != nil {
			return nil, nil, err
		}

		return key, x509Cert, nil
	}

	tlsCert, err := certs.DecodeTLSCertificate(bs, "")
	if err != nil {
		return nil, nil, err
	}

	return tlsCert.PrivateKey.(crypto.Signer), tlsCert.Leaf, nil
}

// genCA 用于生成 CA 证书和私钥。
func genCA() (crypto.Signer, *x509.Certificate, error) {
	// 生成私钥。
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}

	// 生成证书。
	var (
		now      = time.Now()
		x509Cert = &x509.Certificate{
			BasicConstraintsValid: true,
			IsCA:                  true,
			KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
			NotBefore:             now.Add(-5 * time.Minute).UTC(),
			NotAfter:              now.Add(time.Hour * 24 * 365 * 10).UTC(), // 10 年。
			SerialNumber:          new(big.Int).SetInt64(now.Unix()),
			Subject: pkix.Name{
				CommonName:   "fakecert-ca@" + strconv.FormatInt(now.Unix(), 10),
				Organization: []string{"gpustack.ai"},
			},
		}
	)

	certDER, err := x509.CreateCertificate(rand.Reader, x509Cert, x509Cert, key.Public(), key)
	if err != nil {
		return nil, nil, err
	}

	x509Cert, err = x509.ParseCertificate(certDER)
	if err != nil {
		return nil, nil, err
	}

	klog.Background().WithName("fakecert").V(4).
		Info("generated",
			"CA", x509Cert.Subject,
			"not before", x509Cert.NotBefore,
			"not after", x509Cert.NotAfter)

	return key, x509Cert, nil
}

// genCert 会生成证书。
func genCert(caKey crypto.Signer, caX509Cert *x509.Certificate, key crypto.Signer, servers []string) (*x509.Certificate, error) {
	var (
		dnsNames    []string
		ipAddresses []net.IP
	)
	for i := range servers {
		if ip := net.ParseIP(servers[i]); ip != nil {
			ipAddresses = append(ipAddresses, ip)
		} else {
			dnsNames = append(dnsNames, servers[i])
		}
	}

	now := time.Now()
	x509Cert := &x509.Certificate{
		DNSNames:     dnsNames,
		IPAddresses:  ipAddresses,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		NotAfter:     time.Now().Add(time.Hour * 24 * 92).UTC(), // 3 个月。
		NotBefore:    caX509Cert.NotBefore,
		SerialNumber: new(big.Int).SetInt64(now.Unix()),
		Subject: pkix.Name{
			CommonName:   "fakecert@" + strconv.FormatInt(now.Unix(), 10),
			Organization: []string{"gpustack.ai"},
		},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, x509Cert, caX509Cert, key.Public(), caKey)
	if err != nil {
		return nil, err
	}

	x509Cert, err = x509.ParseCertificate(certDER)
	if err != nil {
		return nil, err
	}

	klog.Background().WithName("fakecert").V(4).
		Info("cert generated",
			"servers", servers,
			"certificate", x509Cert.Subject,
			"not before", x509Cert.NotBefore,
			"not after", x509Cert.NotAfter)

	return x509Cert, nil
}
