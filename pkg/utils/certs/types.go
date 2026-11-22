package certs

import (
	"context"

	"golang.org/x/crypto/acme/autocert"
	cert "k8s.io/api/certificates/v1"
	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
)

// 当证书未在缓存中找到时，会返回 ErrCacheMiss。
var ErrCacheMiss = autocert.ErrCacheMiss

// Cache 被 Manager 用于存储和检索之前获得的证书及其他账户数据，以不透明的 Blob 形式进行存储。
//
// 缓存实现不应依赖键命名模式。键可以包含任何可打印的 ASCII 字符，但以下字符除外：\/:*?"<>|.
type Cache = autocert.Cache

// HostPolicy 指定了管理器允许响应哪些主机名称。如果应拒绝该主机，它将返回非空错误。该错误可通过
// tls.Conn.Handshake 及其调用方访问。
type HostPolicy = autocert.HostPolicy

type SecretInterface interface {
	Create(context.Context, *core.Secret, metav1.CreateOptions) (*core.Secret, error)
	Update(context.Context, *core.Secret, metav1.UpdateOptions) (*core.Secret, error)
	Delete(context.Context, string, metav1.DeleteOptions) error
	Get(context.Context, string, metav1.GetOptions) (*core.Secret, error)
	List(context.Context, metav1.ListOptions) (*core.SecretList, error)
	Watch(context.Context, metav1.ListOptions) (watch.Interface, error)
}

// CertificateSigningRequestInterface 包含与证书签名请求相关的操作。
type CertificateSigningRequestInterface interface {
	Create(context.Context, *cert.CertificateSigningRequest, metav1.CreateOptions) (*cert.CertificateSigningRequest, error)
	Update(context.Context, *cert.CertificateSigningRequest, metav1.UpdateOptions) (*cert.CertificateSigningRequest, error)
	Delete(context.Context, string, metav1.DeleteOptions) error
	Get(context.Context, string, metav1.GetOptions) (*cert.CertificateSigningRequest, error)
	List(context.Context, metav1.ListOptions) (*cert.CertificateSigningRequestList, error)
	Watch(context.Context, metav1.ListOptions) (watch.Interface, error)
	UpdateApproval(context.Context, string, *cert.CertificateSigningRequest, metav1.UpdateOptions) (
		*cert.CertificateSigningRequest, error)
}
