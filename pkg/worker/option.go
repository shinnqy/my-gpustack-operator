package worker

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"

	"github.com/spf13/pflag"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type Options struct {
	BindAddress net.IP
	BindPort    int
	CertDir     string
	Kubeconfig  string
}

func NewOptions() *Options {
	return &Options{
		BindAddress: net.IPv4zero,
		BindPort:    31443,
	}
}

func (o *Options) AddFlags(flags *pflag.FlagSet) {
	flags.IPVar(&o.BindAddress, "bind-address", o.BindAddress, "HTTPS 监听地址")
	flags.IntVar(&o.BindPort, "secure-port", o.BindPort, "HTTPS 监听端口")
	flags.StringVar(&o.CertDir, "cert-dir", o.CertDir, "TLS 证书目录，包含 tls.crt 和 tls.key，可选 ca.crt")
	flags.StringVar(&o.Kubeconfig, "kubeconfig", o.Kubeconfig, "连接 Kubernetes 的 kubeconfig 文件")
}

func (o *Options) Validate(ctx context.Context) error {
	if o.BindPort < 1 || o.BindPort > 65535 {
		return errors.New("--secure-port: out of range")
	}
	return ctx.Err()
}

func (o *Options) Complete(ctx context.Context) (*Config, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	restConfig, kubeconfig, err := loadKubeConfig(o.Kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("load Kubernetes config: %w", err)
	}

	namespace := os.Getenv("KUBERNETES_POD_NAMESPACE")
	if namespace == "" {
		namespace = "gpustack-system"
	}
	service := os.Getenv("KUBERNETES_SERVICE_NAME")
	if service == "" {
		service = "gpustack-operator-worker"
	}

	return &Config{
		RestConfig:  restConfig,
		Kubeconfig:  kubeconfig,
		BindAddress: o.BindAddress,
		BindPort:    o.BindPort,
		CertDir:     o.CertDir,
		Namespace:   namespace,
		Service:     service,
	}, nil
}

func loadKubeConfig(path string) (*rest.Config, string, error) {
	if path == "" {
		// 优先使用 Pod 的 ServiceAccount。在集群外则读取 kubeconfig。
		cfg, err := rest.InClusterConfig()
		if !errors.Is(err, rest.ErrNotInCluster) {
			return cfg, "", err
		}
		path = clientcmd.NewDefaultClientConfigLoadingRules().GetDefaultFilename()
	}
	cfg, err := clientcmd.BuildConfigFromFlags("", path)
	return cfg, path, err
}
