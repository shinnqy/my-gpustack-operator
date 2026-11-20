package worker

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	apiextensions "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/server/healthz"
	genericoptions "k8s.io/apiserver/pkg/server/options"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/component-base/compatibility"
	apiregistration "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"gpustack.ai/gpustack/pkg/manager"
	"gpustack.ai/gpustack/pkg/worker/extensionapis"
)

type Config struct {
	RestConfig  *rest.Config
	Kubeconfig  string
	BindAddress net.IP
	BindPort    int
	CertDir     string
	Namespace   string
	Service     string
}

func (c *Config) Apply(ctx context.Context) (_ *Worker, err error) {
	scheme := runtime.NewScheme()
	for _, register := range []func(*runtime.Scheme) error{
		clientgoscheme.AddToScheme,
		apiextensions.AddToScheme,
		apiregistration.AddToScheme,
	} {
		if err := register(scheme); err != nil {
			return nil, fmt.Errorf("register Kubernetes API types: %w", err)
		}
	}

	mgr, err := (&manager.Config{RestConfig: c.RestConfig, Scheme: scheme}).Apply(ctx)
	if err != nil {
		return nil, err
	}
	// Prepare 发生在 Manager 启动前。安装资源时直接访问 API，不经过缓存。
	installer, err := client.New(c.RestConfig, client.Options{
		Scheme:     scheme,
		Mapper:     mgr.CtrlManager.GetRESTMapper(),
		HTTPClient: mgr.CtrlManager.GetHTTPClient(),
	})
	if err != nil {
		return nil, fmt.Errorf("create API installer client: %w", err)
	}

	codecs := serializer.NewCodecFactory(scheme)
	apiConfig := genericapiserver.NewRecommendedConfig(codecs)
	apiConfig.EffectiveVersion = compatibility.NewEffectiveVersionFromString("1.36", "", "")
	apiConfig.EnableProfiling = false
	apiConfig.EnableMetrics = false

	// 一旦申请监听端口，后续构造失败也要释放它。
	defer func() {
		if err != nil && apiConfig.SecureServing != nil {
			_ = apiConfig.SecureServing.Listener.Close()
		}
	}()

	serve, caBundle, err := c.servingOptions()
	if err != nil {
		return nil, err
	}
	if err := serve.WithLoopback().ApplyToConfig(&apiConfig.Config); err != nil {
		return nil, fmt.Errorf("configure HTTPS: %w", err)
	}

	// 使用 Kubernetes 的委托认证授权，并信任集群提供的聚合层证书配置。
	authn := genericoptions.NewDelegatingAuthenticationOptions()
	authn.RemoteKubeConfigFile = c.Kubeconfig
	if err := authn.ApplyTo(&apiConfig.Authentication, apiConfig.SecureServing, nil); err != nil {
		return nil, fmt.Errorf("configure delegated authentication: %w", err)
	}
	authz := genericoptions.NewDelegatingAuthorizationOptions()
	authz.RemoteKubeConfigFile = c.Kubeconfig
	if err := authz.ApplyTo(&apiConfig.Authorization); err != nil {
		return nil, fmt.Errorf("configure delegated authorization: %w", err)
	}

	server, err := apiConfig.Complete().New("gpustack", genericapiserver.NewEmptyDelegate())
	if err != nil {
		return nil, fmt.Errorf("create extension API server: %w", err)
	}
	extensionapis.Setup(server, codecs, mgr.CtrlManager.GetAPIReader())
	if err := server.AddReadyzChecks(healthz.NamedCheck("controller-manager", func(r *http.Request) error {
		ctx, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		return mgr.WaitForReady(ctx)
	})); err != nil {
		return nil, fmt.Errorf("register manager readiness check: %w", err)
	}

	return &Worker{
		Manager:         mgr,
		APIServer:       server,
		installer:       installer,
		routingCABundle: caBundle,
		namespace:       c.Namespace,
		service:         c.Service,
	}, nil
}

func (c *Config) servingOptions() (*genericoptions.SecureServingOptions, []byte, error) {
	serve := genericoptions.NewSecureServingOptions()
	serve.BindAddress = c.BindAddress
	serve.BindPort = c.BindPort
	if c.CertDir == "" {
		// 单个学习用 Worker 可以使用内存证书，并将它注册给 Kubernetes 聚合层。
		serve.ServerCert.CertDirectory = ""
		dns := c.Service + "." + c.Namespace + ".svc"
		if err := serve.MaybeDefaultWithSelfSignedCerts(dns, nil, nil); err != nil {
			return nil, nil, fmt.Errorf("generate serving certificate: %w", err)
		}
		caBundle, _ := serve.ServerCert.GeneratedCert.CurrentCertKeyContent()
		return serve, caBundle, nil
	}

	serve.ServerCert.CertKey = genericoptions.CertKey{
		CertFile: filepath.Join(c.CertDir, "tls.crt"),
		KeyFile:  filepath.Join(c.CertDir, "tls.key"),
	}
	caBundle, err := os.ReadFile(filepath.Join(c.CertDir, "ca.crt"))
	if errors.Is(err, os.ErrNotExist) {
		caBundle, err = os.ReadFile(serve.ServerCert.CertKey.CertFile)
	}
	if err != nil {
		return nil, nil, fmt.Errorf("read serving CA bundle: %w", err)
	}
	if !x509.NewCertPool().AppendCertsFromPEM(caBundle) {
		return nil, nil, errors.New("serving CA bundle contains no PEM certificates")
	}
	return serve, caBundle, nil
}
