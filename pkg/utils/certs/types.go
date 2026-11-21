package certs

import (
	"context"

	"golang.org/x/crypto/acme/autocert"
	cert "k8s.io/api/certificates/v1"
	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
)

// ErrCacheMiss is returned when a certificate is not found in cache.
var ErrCacheMiss = autocert.ErrCacheMiss

// Cache is used by Manager to store and retrieve previously obtained certificates
// and other account data as opaque blobs.
//
// Cache implementations should not rely on the key naming pattern. Keys can
// include any printable ASCII characters, except the following: \/:*?"<>|.
type Cache = autocert.Cache

type SecretInterface interface {
	Create(context.Context, *core.Secret, metav1.CreateOptions) (*core.Secret, error)
	Update(context.Context, *core.Secret, metav1.UpdateOptions) (*core.Secret, error)
	Delete(context.Context, string, metav1.DeleteOptions) error
	Get(context.Context, string, metav1.GetOptions) (*core.Secret, error)
	List(context.Context, metav1.ListOptions) (*core.SecretList, error)
	Watch(context.Context, metav1.ListOptions) (watch.Interface, error)
}

// CertificateSigningRequestInterface holds the operations on certificate signing requests.
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
