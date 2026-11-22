package certs

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"strings"
	"time"

	"gpustack.ai/gpustack/pkg/utils/bytex"
)

// EncodeTLSCertificate 将 TLS 证书编码为 PEM 格式，包含私钥和公钥。
func EncodeTLSCertificate(cert *tls.Certificate) ([]byte, error) {
	buf := bytex.GetBuffer()

	// 对私钥进行编码。
	switch k := cert.PrivateKey.(type) {
	case *ecdsa.PrivateKey:
		b, err := x509.MarshalECPrivateKey(k)
		if err != nil {
			return nil, err
		}

		pb := &pem.Block{Type: "EC PRIVATE KEY", Bytes: b}
		if err = pem.Encode(buf, pb); err != nil {
			return nil, err
		}
	case *rsa.PrivateKey:
		b := x509.MarshalPKCS1PrivateKey(k)

		pb := &pem.Block{Type: "RSA PRIVATE KEY", Bytes: b}
		if err := pem.Encode(buf, pb); err != nil {
			return nil, err
		}
	default:
		return nil, errors.New("unknown private key type")
	}

	// 对公共密钥进行编码。
	for _, b := range cert.Certificate {
		pb := &pem.Block{Type: "CERTIFICATE", Bytes: b}
		if err := pem.Encode(buf, pb); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

// DecodeTLSCertificate 用于解码来自 PEM 格式的 TLS 证书，包含私钥和公钥。
func DecodeTLSCertificate(data []byte, hostname string) (*tls.Certificate, error) {
	// Decode 私有密钥。
	keyBlock, data := pem.Decode(data)
	if keyBlock == nil || !strings.Contains(keyBlock.Type, "PRIVATE") {
		return nil, errors.New("corrupt private key: invalid format")
	}

	var (
		key crypto.PrivateKey
		// Decode 公共密钥。
		certs [][]byte
		// 获取叶证书。
		n int
	)

	key, err := x509.ParseECPrivateKey(keyBlock.Bytes)
	if err != nil {
		// 尝试 RSA。
		k, err := x509.ParsePKCS8PrivateKey(keyBlock.Bytes)
		if err != nil {
			return nil, errors.New("corrupt private key: parse failed")
		}
		key = k.(*rsa.PrivateKey)
	}

	for len(data) > 0 {
		var certBlock *pem.Block

		certBlock, data = pem.Decode(data)
		if certBlock == nil {
			break
		}

		certs = append(certs, certBlock.Bytes)
	}

	if len(data) > 0 {
		return nil, errors.New("corrupt certificate: invalid format")
	}

	for i := range certs {
		n += len(certs[i])
	}
	der := make([]byte, n)
	n = 0

	for i := range certs {
		n += copy(der[n:], certs[i])
	}

	x509Certs, err := x509.ParseCertificates(der)
	if err != nil {
		return nil, err
	}

	if len(x509Certs) == 0 {
		return nil, errors.New("corrupt certificate: not found")
	}
	leaf := x509Certs[0]

	// 验证叶证书。
	err = VerifyX509Certificate(leaf, hostname)
	if err != nil {
		return nil, err
	}

	switch prv := key.(type) {
	case *ecdsa.PrivateKey:
		pub, ok := leaf.PublicKey.(*ecdsa.PublicKey)
		if !ok {
			return nil, errors.New("corrupt certificate: not ECDSA type")
		}

		if pub.X.Cmp(prv.X) != 0 || pub.Y.Cmp(prv.Y) != 0 {
			return nil, errors.New("corrupt private key: not match ECDSA certificate")
		}
	case *rsa.PrivateKey:
		pub, ok := leaf.PublicKey.(*rsa.PublicKey)
		if !ok {
			return nil, errors.New("corrupt certificate: not RSA type")
		}

		if pub.N.Cmp(prv.N) != 0 {
			return nil, errors.New("corrupt private key: not match RSA certificate")
		}
	}

	return &tls.Certificate{
		Certificate: certs,
		PrivateKey:  key,
		Leaf:        leaf,
	}, nil
}

// VerifyX509Certificate 用于验证 x509 证书。
func VerifyX509Certificate(cert *x509.Certificate, hostname string) error {
	now := time.Now()
	if now.Before(cert.NotBefore) {
		return errors.New("corrupt certificate: not valid yet")
	}

	if now.After(cert.NotAfter) {
		return errors.New("corrupt certificate: expired")
	}

	if hostname != "" {
		err := cert.VerifyHostname(hostname)
		if err != nil {
			return fmt.Errorf("corrupt certificate: %w", err)
		}
	}

	return nil
}

// EncodeX509CertificateRequest 将 x509 证书请求编码为 PEM 格式。
func EncodeX509CertificateRequest(certReq *x509.CertificateRequest, key crypto.Signer) ([]byte, error) {
	b, err := x509.CreateCertificateRequest(rand.Reader, certReq, key)
	if err != nil {
		return nil, err
	}

	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: b}), nil
}

// DecodeX509Certificate 用于解码来自 PEM 格式的 x509 证书。
func DecodeX509Certificate(data []byte, hostname string) (*x509.Certificate, error) {
	block, _ := pem.Decode(data)
	if block == nil {
		return nil, errors.New("corrupt certificate: invalid format")
	}

	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, err
	}

	err = VerifyX509Certificate(cert, hostname)
	if err != nil {
		return nil, err
	}

	return cert, nil
}

// RequestECDSA 返回 true 表示正在请求 ECDSA 算法。
func RequestECDSA(hello *tls.ClientHelloInfo) bool {
	if hello.SignatureSchemes != nil {
		var ecdsaOK bool
	schemeLoop:
		for _, scheme := range hello.SignatureSchemes {
			switch scheme {
			case tls.ECDSAWithSHA1, tls.ECDSAWithP256AndSHA256,
				tls.ECDSAWithP384AndSHA384, tls.ECDSAWithP521AndSHA512:
				ecdsaOK = true
				break schemeLoop
			}
		}

		if !ecdsaOK {
			return false
		}
	}

	if hello.SupportedCurves != nil {
		var ecdsaOK bool

		for _, curve := range hello.SupportedCurves {
			if curve == tls.CurveP256 {
				ecdsaOK = true
				break
			}
		}

		if !ecdsaOK {
			return false
		}
	}

	for _, suite := range hello.CipherSuites {
		switch suite {
		case tls.TLS_ECDHE_ECDSA_WITH_RC4_128_SHA,
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA,
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA,
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256,
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305:
			return true
		}
	}

	return false
}

// EncodeTLSCertificateAndKey 与 EncodeTLSCertificate 类似，但会分别返回私钥和证书。
func EncodeTLSCertificateAndKey(cert *tls.Certificate) ([]byte, []byte, error) {
	// 对私钥进行编码。
	priBuf := bytex.GetBuffer()
	switch k := cert.PrivateKey.(type) {
	case *ecdsa.PrivateKey:
		b, err := x509.MarshalECPrivateKey(k)
		if err != nil {
			return nil, nil, err
		}

		pb := &pem.Block{Type: "EC PRIVATE KEY", Bytes: b}
		if err = pem.Encode(priBuf, pb); err != nil {
			return nil, nil, err
		}
	case *rsa.PrivateKey:
		b := x509.MarshalPKCS1PrivateKey(k)

		pb := &pem.Block{Type: "RSA PRIVATE KEY", Bytes: b}
		if err := pem.Encode(priBuf, pb); err != nil {
			return nil, nil, err
		}
	default:
		return nil, nil, errors.New("unknown private key type")
	}

	// 对公共密钥进行编码。
	pubBuf := bytex.GetBuffer()
	for _, b := range cert.Certificate {
		pb := &pem.Block{Type: "CERTIFICATE", Bytes: b}
		if err := pem.Encode(pubBuf, pb); err != nil {
			return nil, nil, err
		}
	}

	return pubBuf.Bytes(), priBuf.Bytes(), nil
}
