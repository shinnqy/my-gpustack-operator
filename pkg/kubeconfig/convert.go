package kubeconfig

import (
	kmeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/tools/clientcmd"
)

// ConvertRestConfigToRestClientGetter 将 rest.Config 转换为 RESTClientGetter。
func ConvertRestConfigToRestClientGetter(cfg *rest.Config) genericclioptions.RESTClientGetter {
	rcg := restClientGetter(*cfg)
	return &rcg
}

// restClientGetter 是供 Helm Go 包使用的 RESTClientGetter 接口实现。
type restClientGetter rest.Config

func (g restClientGetter) ToRESTConfig() (*rest.Config, error) {
	r := rest.Config(g)
	return &r, nil
}

func (g restClientGetter) ToDiscoveryClient() (discovery.CachedDiscoveryInterface, error) {
	config, err := g.ToRESTConfig()
	if err != nil {
		return nil, err
	}
	return memory.NewMemCacheClient(discovery.NewDiscoveryClientForConfigOrDie(config)), nil
}

func (g restClientGetter) ToRESTMapper() (kmeta.RESTMapper, error) {
	discoveryClient, err := g.ToDiscoveryClient()
	if err != nil {
		return nil, err
	}

	mapper := restmapper.NewDeferredDiscoveryRESTMapper(discoveryClient)
	expander := restmapper.NewShortcutExpander(mapper, discoveryClient, nil)

	return expander, nil
}

func (g restClientGetter) ToRawKubeConfigLoader() clientcmd.ClientConfig {
	// 构建配置和客户端。
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		clientcmd.NewDefaultClientConfigLoadingRules(),
		&clientcmd.ConfigOverrides{},
	)
}
