package kubecert

import (
	"bytes"
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
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	cert "k8s.io/api/certificates/v1"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	klog "k8s.io/klog/v2"
	"k8s.io/utils/ptr"

	"gpustack.ai/gpustack/pkg/utils/certs"
	"gpustack.ai/gpustack/pkg/utils/funcx"
	"gpustack.ai/gpustack/pkg/utils/gox"
)

// certKey 是用于在 state 和缓存中跟踪证书的键。
type certKey struct {
	// host 表示用于生成证书的主机。
	host string
	// alterIPs 表示用于生成证书的备用 IP，
	// 以逗号分隔。
	alterIPs string
	// alterDNSNames 表示用于生成证书的备用 DNS 名称，
	// 以逗号分隔。
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

// certState 表示证书已就绪可读的状态。
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
	cli certs.CertificateSigningRequestInterface,
	host, alterIPs, alterDNSNames string, isRSA bool,
) {
	if cli == nil {
		s.err = errors.New("kubecert: certificate signing request client is nil")
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
		s.err = errors.New("kubecert: missing host")
		return
	}

	hosts := []string{host}
	if alterIPs != "" {
		hosts = append(hosts, strings.Split(alterIPs, ",")...)
	}
	if alterDNSNames != "" {
		hosts = append(hosts, strings.Split(alterDNSNames, ",")...)
	}

	var srvCertReq *x509.CertificateRequest
	srvCertReq, s.err = genCertRequest(key, hosts)
	if s.err != nil {
		return
	}

	// 创建。
	csr := &cert.CertificateSigningRequest{
		ObjectMeta: meta.ObjectMeta{
			GenerateName: "kubecert-",
		},
		Spec: cert.CertificateSigningRequestSpec{
			SignerName:        cert.KubeletServingSignerName,
			Request:           funcx.MustNoError(certs.EncodeX509CertificateRequest(srvCertReq, key)),
			ExpirationSeconds: ptr.To[int32](90 * 24 * 60 * 60), // 90 天
			Usages: []cert.KeyUsage{
				cert.UsageKeyEncipherment,
				cert.UsageDigitalSignature,
				cert.UsageServerAuth,
			},
		},
	}
	csr, s.err = cli.Create(ctx, csr, meta.CreateOptions{})
	if s.err != nil {
		return
	}

	// 等待。
	var (
		wc watch.Interface
		cc = make(chan any)
	)
	wc, s.err = cli.Watch(ctx,
		meta.ListOptions{
			ResourceVersion: "0",
			FieldSelector:   "metadata.name=" + csr.Name,
		})
	if s.err != nil {
		return
	}
	defer wc.Stop()

	gox.Go(func() {
		defer close(cc)

		for {
			var (
				e  watch.Event
				ok bool
			)
			select {
			case <-ctx.Done():
				return
			case e, ok = <-wc.ResultChan():
				if !ok {
					return
				}
			}

			switch e.Type {
			default:
				continue
			case watch.Deleted:
				cc <- fmt.Errorf("kubecert: certificate signing request deleted")
				return
			case watch.Error:
				cc <- fmt.Errorf("kubecert: watch error: %v", e.Object)
				return
			case watch.Modified:
			}

			t, ok := e.Object.(*cert.CertificateSigningRequest)
			if !ok {
				continue
			}

			for _, cond := range t.Status.Conditions {
				switch cond.Type {
				default:
					continue
				case cert.CertificateDenied:
					cc <- fmt.Errorf("kubecert: certificate signing request denied: %s", cond.Reason)
					return
				case cert.CertificateFailed:
					cc <- fmt.Errorf("kubecert: certificate signing request failed: %s", cond.Reason)
					return
				case cert.CertificateApproved:
					goto Approved
				}
			}

		Approved:
			if len(t.Status.Certificate) != 0 {
				cc <- bytes.Clone(t.Status.Certificate)
				return
			}
		}
	})

	// 批准。
	csr.Status.Conditions = append(csr.Status.Conditions, cert.CertificateSigningRequestCondition{
		Type:    cert.CertificateApproved,
		Status:  core.ConditionTrue,
		Reason:  "ApprovedByKubecert",
		Message: "This CSR was approved by kubecert",
	})
	_, s.err = cli.UpdateApproval(ctx, csr.Name, csr, meta.UpdateOptions{})
	if s.err != nil {
		return
	}

	var srvCert *x509.Certificate
	r := <-cc
	switch v := r.(type) {
	case error:
		s.err = v
	case []byte:
		srvCert, s.err = certs.DecodeX509Certificate(v, host)
	default:
		s.err = fmt.Errorf("kubecert: unknown watch result: %#v", v)
	}
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

// genCertRequest 生成证书签名请求。
func genCertRequest(key crypto.Signer, servers []string) (*x509.CertificateRequest, error) {
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
	x509CertReq := &x509.CertificateRequest{
		DNSNames:    dnsNames,
		IPAddresses: ipAddresses,
		Subject: pkix.Name{
			CommonName:   "system:node:kubecert-" + strconv.FormatInt(now.Unix(), 10),
			Organization: []string{"system:nodes"},
		},
	}

	csrASN1, err := x509.CreateCertificateRequest(rand.Reader, x509CertReq, key)
	if err != nil {
		return nil, err
	}

	x509CertReq, err = x509.ParseCertificateRequest(csrASN1)
	if err != nil {
		return nil, err
	}

	klog.Background().WithName("kubecert").V(4).
		Info("csr generated",
			"servers", servers,
			"certificate", x509CertReq.Subject)

	return x509CertReq, nil
}
