package worker

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/pflag"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apiserver/pkg/admission/plugin/namespace/lifecycle"
	"k8s.io/apiserver/pkg/admission/plugin/policy/mutating"
	"k8s.io/apiserver/pkg/admission/plugin/policy/validating"
	"k8s.io/apiserver/pkg/apis/apiserver"
	genericapiserver "k8s.io/apiserver/pkg/server"
	genericoptions "k8s.io/apiserver/pkg/server/options"
	"k8s.io/client-go/informers"
	kclientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	cliflag "k8s.io/component-base/cli/flag"
	"k8s.io/component-base/compatibility"
	klog "k8s.io/klog/v2"

	"gpustack.ai/gpustack/pkg/manager"
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/system"
	certcache "gpustack.ai/gpustack/pkg/utils/certs/cache"
	"gpustack.ai/gpustack/pkg/utils/certs/kubecert"
	"gpustack.ai/gpustack/pkg/utils/osx"
	"gpustack.ai/gpustack/pkg/utils/version"
	"gpustack.ai/gpustack/pkg/worker/extensionapis"
	"gpustack.ai/gpustack/pkg/worker/kuberess"
	"gpustack.ai/gpustack/pkg/worker/settings"
)

type Options struct {
	// 服务建立。
	BindAddress net.IP
	BindPort    int
	CertDir     string

	// Manager 配置。
	ManagerOptions *manager.Options

	// 控制开关。
	DisableAuths        bool
	DisableApplications []string
	CorsAllowedOrigins  []string

	// 认证。
	AuthnTokenWebhookCacheTTL time.Duration
	AuthnTokenRequestTimeout  time.Duration

	// 鉴权。
	AuthzAllowCacheTTL time.Duration
	AuthzDenyCacheTTL  time.Duration

	// 审计。
	AuditPolicyFile        string
	AuditLogFile           string
	AuditWebhookConfigFile string

	// Device Manager 配置。
	Manufacturers []string

	// Model Manager 配置。
	ModelManagerServiceAccount string
}

func NewOptions() *Options {
	opts := &Options{
		// 服务建立。
		BindAddress: net.ParseIP("0.0.0.0"),
		BindPort:    31443,

		// Manager 配置。
		ManagerOptions: manager.NewOptions(),

		// 控制开关。
		DisableAuths:        false,
		DisableApplications: []string{},
		CorsAllowedOrigins:  []string{},

		// 认证。
		AuthnTokenWebhookCacheTTL: 10 * time.Second,
		AuthnTokenRequestTimeout:  10 * time.Second,

		// 鉴权。
		AuthzAllowCacheTTL: 10 * time.Second,
		AuthzDenyCacheTTL:  10 * time.Second,

		// 审计。
		AuditPolicyFile:        "",
		AuditLogFile:           "",
		AuditWebhookConfigFile: "",

		// Device Manager 配置。
		Manufacturers: nodefeature.GetKnownAcceleratableManufacturers(),

		// Model Manager。
		ModelManagerServiceAccount: system.ModelManagerServiceAccount.Get(),
	}
	opts.ManagerOptions.KubeLeaderElectionID = "worker.gpustack.ai"

	return opts
}

func (o *Options) AddFlags(fs *pflag.FlagSet) {
	// 服务监听。
	fs.IPVar(&o.BindAddress, "bind-address", o.BindAddress,
		"the IP address(without port) on which to serve.")
	fs.IntVar(&o.BindPort, "secure-port", o.BindPort,
		"the port on which to serve HTTPS.")
	fs.StringVar(&o.CertDir, "cert-dir", o.CertDir,
		"the directory where the TLS certs are located. "+
			"if provided, must place tls.crt and tls.key under --cert-dir.")

	// Manager。
	o.ManagerOptions.AddFlags(fs)

	// 控制开关。
	fs.BoolVar(&o.DisableAuths, "disable-auths", o.DisableAuths,
		"disable checking authentication and authorization.")
	fs.StringSliceVar(&o.DisableApplications, "disable-applications", o.DisableApplications,
		"comma separated list of applications to skip installing, "+
			"or \"*\" for all of them. "+
			"valid applications: "+strings.Join(kuberess.ApplicationNames(), ","))
	fs.StringSliceVar(&o.CorsAllowedOrigins, "cors-allowed-origins", o.CorsAllowedOrigins,
		"the list of origins a cross-domain request can be executed from, comma separated. "+
			"an allowed origin can be a regular expression to support subdomain matching. "+
			"empty means all origins are allowed. "+
			"ensure each expression matches the entire hostname by anchoring to the start with '^' or including the '//' prefix, "+
			"and by anchoring to the end with '$' or including the ':' port separator suffix. "+
			"examples of valid expressions are '//example.com(:|$)' and '^https://example.com(:|$)'.")

	// 身份认证。
	fs.DurationVar(&o.AuthnTokenWebhookCacheTTL, "authentication-token-webhook-cache-ttl",
		o.AuthnTokenWebhookCacheTTL,
		"the duration to cache responses from the webhook token authenticator.")
	fs.DurationVar(&o.AuthnTokenRequestTimeout, "authentication-token-request-timeout",
		o.AuthnTokenRequestTimeout,
		"the duration to wait for a response from the webhook token authenticator.")

	// 权限鉴权。
	fs.DurationVar(&o.AuthzAllowCacheTTL, "authorization-webhook-cache-authorized-ttl",
		o.AuthzAllowCacheTTL,
		"the duration to cache 'authorized' responses from the webhook authorizer.")
	fs.DurationVar(&o.AuthzDenyCacheTTL, "authorization-webhook-cache-unauthorized-ttl",
		o.AuthzDenyCacheTTL,
		"the duration to cache 'unauthorized' responses from the webhook authorizer.")

	// 审计。
	fs.StringVar(&o.AuditPolicyFile, "audit-policy-file", o.AuditPolicyFile,
		"path to the file that defines the audit policy configuration.")
	fs.StringVar(&o.AuditLogFile, "audit-log-path", o.AuditLogFile,
		"if set, all requests coming to the server will be logged to this file. "+
			"'-' means standard out.")
	fs.StringVar(&o.AuditWebhookConfigFile, "audit-webhook-config-file", o.AuditWebhookConfigFile,
		"path to a kubeconfig formatted file that defines the audit webhook configuration.")

	// Device Manager。
	fs.StringSliceVar(&o.Manufacturers, "manufacturer", o.Manufacturers,
		"comma separated list of manufacturers to detect.")

	// Model Manager。
	fs.StringVar(&o.ModelManagerServiceAccount, "model-manager-service-account", o.ModelManagerServiceAccount,
		"the ServiceAccount, in the system namespace, the model-manager plugin runs as; "+
			"only it may write a NodeModelStore's status.")
}

func (o *Options) Validate(ctx context.Context) error {
	// 服务监听。
	if o.BindPort < 1 || o.BindPort > 65535 {
		return errors.New("--secure-port: out of range")
	}
	if o.CertDir != "" {
		if !osx.ExistsDir(o.CertDir) {
			return errors.New("--cert-dir: no found directory")
		}
		if !osx.Exists(filepath.Join(o.CertDir, "tls.crt")) {
			return errors.New("--cert-dir: no found tls.crt")
		}
		if !osx.Exists(filepath.Join(o.CertDir, "tls.key")) {
			return errors.New("--cert-dir: no found tls.key")
		}
	}

	// Manager。
	if err := o.ManagerOptions.Validate(ctx); err != nil {
		return err
	}

	// 控制开关。
	if len(o.DisableApplications) != 0 {
		applicationNames := kuberess.ApplicationNames()
		if !sets.New[string](applicationNames...).HasAll(o.DisableApplications...) {
			return errors.New("--disable-applications: should be a comma separated list of valid applications, " +
				"valid applications: " + strings.Join(applicationNames, ","))
		}
	}

	// 认证与鉴权。
	if !o.DisableAuths {
		if o.AuthnTokenWebhookCacheTTL < 10*time.Second {
			return errors.New("--authentication-token-webhook-cache-ttl: less than 10s")
		}
		if o.AuthnTokenRequestTimeout < 10*time.Second {
			return errors.New("--authentication-token-request-timeout: less than 10s")
		}
		if o.AuthzAllowCacheTTL < 10*time.Second {
			return errors.New("--authorization-webhook-cache-authorized-ttl: less than 10s")
		}
		if o.AuthzDenyCacheTTL < 10*time.Second {
			return errors.New("--authorization-webhook-cache-unauthorized-ttl: less than 10s")
		}
	}

	// 审计。
	if o.AuditPolicyFile != "" && !osx.ExistsFile(o.AuditPolicyFile) {
		return errors.New("--audit-policy-file: no found file")
	}
	if o.AuditLogFile != "" && o.AuditLogFile != "-" && !osx.ExistsDir(filepath.Dir(o.AuditLogFile)) {
		return errors.New("--audit-log-path: no found parent directory")
	}
	if o.AuditWebhookConfigFile != "" && !osx.ExistsFile(o.AuditWebhookConfigFile) {
		return errors.New("--audit-webhook-config-file: no found file")
	}

	// 配置项。默认值来自环境变量，不会经过准入校验。
	if err := settings.ValidateModelStoreDefaults(); err != nil {
		return err
	}

	// Model Manager。状态守卫仅放行该 ServiceAccount，因此任何非法 ServiceAccount 名称
	// 都会导致插件写入的所有状态更新被拒绝。
	if errs := validation.IsDNS1123Subdomain(o.ModelManagerServiceAccount); len(errs) != 0 {
		return fmt.Errorf("--model-manager-service-account: %s", strings.Join(errs, "; "))
	}

	// Device Manager。
	if len(o.Manufacturers) != 0 {
		knownManufacturers := nodefeature.GetKnownAcceleratableManufacturers()
		if !sets.New[string](knownManufacturers...).HasAll(o.Manufacturers...) {
			return errors.New("--manufacturer: should be a comma separated list of valid manufacturers, " +
				"valid manufacturers: " + strings.Join(knownManufacturers, ","))
		}
	}

	return nil
}

func newExtensionAPIServerAdmissionOptions() *genericoptions.AdmissionOptions {
	options := genericoptions.NewAdmissionOptions()
	// 宿主集群可能缺少这些插件所需的策略 API。
	options.DisablePlugins = []string{lifecycle.PluginName, mutating.PluginName, validating.PluginName}
	return options
}

func (o *Options) Complete(ctx context.Context) (*Config, error) {
	system.ConfigureControl(
		"",
		o.DisableAuths,
		o.DisableApplications,
	)
	system.ModelManagerServiceAccount.Configure(o.ModelManagerServiceAccount)

	mgrConfig, err := o.ManagerOptions.Complete(ctx)
	if err != nil {
		return nil, err
	}

	serve := &genericoptions.SecureServingOptions{
		BindAddress:                  o.BindAddress,
		BindPort:                     o.BindPort,
		CipherSuites:                 cliflag.PreferredTLSCipherNames(),
		MinTLSVersion:                "VersionTLS12",
		HTTP2MaxStreamsPerConnection: 1000,
	}
	if o.CertDir == "" {
		klog.Info("no cert dir provided, going to use Kubecert to serve")
		lpCli := mgrConfig.LoopbackKubeClient
		certCache, err := certcache.NewK8sCache(ctx,
			"worker", lpCli.CoreV1().Secrets(kuberess.SystemNamespaceName))
		if err != nil {
			return nil, fmt.Errorf("create cert cache: %w", err)
		}
		certMgr := &kubecert.StaticManager{
			CertCli: lpCli.CertificatesV1().CertificateSigningRequests(),
			Cache:   certCache,
			Host:    kuberess.SystemRoutingServiceName,
			AlternateIPs: func() []net.IP {
				if system.LoopbackKubeInside.Get() {
					return nil
				}
				return []net.IP{
					net.ParseIP("127.0.0.1"),
					net.ParseIP(system.PrimaryIP.Get()),
				}
			}(),
			AlternateDNSNames: []string{
				fmt.Sprintf("%s.%s.svc", kuberess.SystemRoutingServiceName, kuberess.SystemNamespaceName),
				fmt.Sprintf("%s.%s", kuberess.SystemRoutingServiceName, kuberess.SystemNamespaceName),
				kuberess.SystemRoutingServiceName,
				"localhost",
			},
		}
		serve.ServerCert.GeneratedCert = certMgr
	} else {
		serve.ServerCert.CertKey = genericoptions.CertKey{
			CertFile: filepath.Join(o.CertDir, "tls.crt"),
			KeyFile:  filepath.Join(o.CertDir, "tls.key"),
		}
		klog.InfoS("cert dir provided, going to serve", "path", o.CertDir)
	}

	var (
		authn *genericoptions.DelegatingAuthenticationOptions
		authz *genericoptions.DelegatingAuthorizationOptions
	)
	if !o.DisableAuths {
		authn = &genericoptions.DelegatingAuthenticationOptions{
			CacheTTL:             o.AuthnTokenWebhookCacheTTL,
			TokenRequestTimeout:  o.AuthnTokenRequestTimeout,
			WebhookRetryBackoff:  genericoptions.DefaultAuthWebhookRetryBackoff(),
			RemoteKubeConfigFile: mgrConfig.LoopbackKubeConfigPath,
			Anonymous: &apiserver.AnonymousAuthConfig{
				Enabled: false,
			},
		}
		authz = &genericoptions.DelegatingAuthorizationOptions{
			AllowCacheTTL:        o.AuthzAllowCacheTTL,
			DenyCacheTTL:         o.AuthzDenyCacheTTL,
			WebhookRetryBackoff:  genericoptions.DefaultAuthWebhookRetryBackoff(),
			RemoteKubeConfigFile: mgrConfig.LoopbackKubeConfigPath,
			ClientTimeout:        10 * time.Second,
			AlwaysAllowGroups:    []string{"system:masters"},
			AlwaysAllowPaths: []string{
				"/mutate-*", "/validate-*", // Webhook
				"/livez", "/readyz", "/metrics", "/debug/*", // 健康检查与指标
				"/openapi", "/openapi/*", // OpenAPI
				"/swagger", "/swagger/*", // Swagger
			},
		}
	}

	audit := genericoptions.NewAuditOptions()
	audit.PolicyFile = o.AuditPolicyFile
	audit.LogOptions.Path = o.AuditLogFile
	audit.WebhookOptions.ConfigFile = o.AuditWebhookConfigFile

	admit := newExtensionAPIServerAdmissionOptions()

	// 由于 manager 的运行时 scheme 受限，为服务端重建环回 Kubernetes 客户端。
	lpRestCfg, lpHttpCli := rest.CopyConfig(&mgrConfig.LoopbackKubeRestConfig), mgrConfig.LoopbackKubeHTTPClient
	lpCli, err := kclientset.NewForConfigAndClient(rest.CopyConfig(lpRestCfg), lpHttpCli)
	if err != nil {
		return nil, fmt.Errorf("create kubernete native client: %w", err)
	}
	lpInf := informers.NewSharedInformerFactory(lpCli, o.ManagerOptions.InformerCacheResyncPeriod)

	apiSrvCfg := genericapiserver.NewRecommendedConfig(extensionapis.Codecs)
	{
		// 配置共享 Informer 工厂。
		apiSrvCfg.SharedInformerFactory = lpInf
		// 配置 CORS 允许的源。
		apiSrvCfg.CorsAllowedOriginList = o.CorsAllowedOrigins
		// 回填 Kubernetes 客户端配置。
		apiSrvCfg.LoopbackClientConfig = lpRestCfg
		apiSrvCfg.ClientConfig = lpRestCfg
		// 禁用默认指标服务。
		apiSrvCfg.EnableMetrics = false
		// 禁用默认性能分析服务。
		apiSrvCfg.EnableProfiling = false
		// 禁用默认索引服务。
		apiSrvCfg.EnableIndex = false
		// 禁用以下启动后钩子，
		// 因为已注册的 apiserver 可以自行管理它们。
		apiSrvCfg.DisabledPostStartHooks.Insert(
			"priority-and-fairness-filter",
			"max-in-flight-filter",
			"storage-object-count-tracker-hook",
		)
		// 配置生效版本。
		apiSrvCfg.EffectiveVersion = compatibility.NewEffectiveVersionFromString(version.MajorMinor(), "", "")
		// 配置关闭期间 watch 终止宽限期。
		apiSrvCfg.ShutdownWatchTerminationGracePeriod = 15 * time.Second
	}

	return &Config{
		Manufacturers:      o.Manufacturers,
		ManagerConfig:      mgrConfig,
		APIServerConfig:    apiSrvCfg,
		Serve:              serve,
		Authn:              authn,
		Authz:              authz,
		Audit:              audit,
		Admit:              admit,
		KubeNativeClient:   lpCli,
		KubeNativeInformer: lpInf,
	}, nil
}
