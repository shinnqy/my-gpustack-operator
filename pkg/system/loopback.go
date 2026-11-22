package system

import (
	"net"
	"net/http"
	"net/url"
	"slices"
	"strings"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/rest"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"

	"gpustack.ai/gpustack/pkg/kubeclients/kubernetes"
	"gpustack.ai/gpustack/pkg/kubediscovery"
	"gpustack.ai/gpustack/pkg/utils/netx"
	"gpustack.ai/gpustack/pkg/utils/varx"
)

var (
	// LoopbackKubeInside 是指示系统是否运行在环回 Kubernetes 集群内部的标志。
	LoopbackKubeInside varx.Once[bool]

	// LoopbackKubeNearby 是指示系统是否运行在环回 Kubernetes 集群附近的标志。
	// 如果系统运行在集群附近，即使不在集群内部，也可以连接到该环回 Kubernetes 集群。
	LoopbackKubeNearby varx.Once[bool]

	// LoopbackKubeClientConfigPath 是环回 Kubernetes 客户端配置文件的路径。
	LoopbackKubeClientConfigPath varx.Once[string]

	// LoopbackKubeRestConfig 是环回 Kubernetes 的 rest 配置。
	LoopbackKubeRestConfig varx.Once[rest.Config]

	// LoopbackKubeHTTPClient 是环回 Kubernetes 的 HTTP 客户端。
	LoopbackKubeHTTPClient varx.Once[*http.Client]

	// LoopbackKubeClient 是环回 Kubernetes 客户端。
	LoopbackKubeClient varx.Once[kubernetes.Interface]

	// LoopbackKubeVersion 是环回 Kubernetes 集群版本，
	// 在连接集群后于启动时读取一次。
	LoopbackKubeVersion varx.Once[kubediscovery.Version]
)

// ConfigureLoopbackKube 配置环回 Kubernetes。
func ConfigureLoopbackKube(inside bool, configPath string, config rest.Config,
	httpClient *http.Client, client kubernetes.Interface, version kubediscovery.Version,
) {
	LoopbackKubeInside.Configure(inside)
	LoopbackKubeNearby.Configure(inside || isLoopbackClusterNearby(&config))
	LoopbackKubeClientConfigPath.Configure(configPath)
	LoopbackKubeRestConfig.Configure(config)
	LoopbackKubeHTTPClient.Configure(httpClient)
	LoopbackKubeClient.Configure(client)
	LoopbackKubeVersion.Configure(version)
}

var (
	// LoopbackCtrlClient 是环回 Kubernetes 集群的控制器客户端。
	//
	// LoopbackCtrlClient 与 LoopbackKubeClient 类似，
	// 但它拥有自管理的缓存以优化只读访问，
	// 这意味着我们无需手动处理 list/watch。
	LoopbackCtrlClient varx.Once[ctrlcli.Client]

	// LoopbackCtrlAPIReader 是环回 Kubernetes 集群的控制器 API 读取器。
	//
	// LoopbackCtrlAPIReader 与 LoopbackCtrlClient 类似，
	// 但它对 API server 仅具有只读访问权限。
	LoopbackCtrlAPIReader varx.Once[ctrlcli.Reader]
)

// ConfigureLoopbackCtrlRuntime 配置环回 Kubernetes 控制器运行时。
func ConfigureLoopbackCtrlRuntime(client ctrlcli.Client, apiReader ctrlcli.Reader) {
	LoopbackCtrlClient.Configure(client)
	LoopbackCtrlAPIReader.Configure(apiReader)
}

func isLoopbackClusterNearby(restCfg *rest.Config) bool {
	// 从 rest 配置中提取主机地址。
	var host string
	if strings.Contains(restCfg.Host, "://") {
		u, _ := url.Parse(restCfg.Host)
		host = u.Host
	} else {
		host = restCfg.Host
	}
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	} else if strings.Contains(host, ":") {
		host = strings.Split(host, ":")[0]
	}

	// 通过快速路径检测主机。
	knownLoopbackHosts := []string{
		"kubernetes.docker.internal",
		"host.docker.internal",
		"localhost",
		"127.0.0.1",
		"[::1]",
		"[::1%lo0]",
	}
	if slices.Contains(knownLoopbackHosts, host) {
		return true
	}

	// 通过慢速路径检测主机。
	subnets := make([]netx.IPv4, 0, Subnets.Get().Len())
	for _, v := range sets.List(Subnets.Get()) {
		sn := netx.MustIPv4FromCIDR(v)
		subnets = append(subnets, sn)
	}

	// IP 检测。
	if ip := net.ParseIP(host); ip != nil {
		for j := range subnets {
			if subnets[j].Contains(ip) {
				return true
			}
		}

		return false
	}

	// 或 DNS 查询。
	ips, err := net.LookupIP(host)
	if err != nil {
		return false
	}

	for i := range ips {
		if ips[i].IsLoopback() {
			return true
		}
		for j := range subnets {
			if subnets[j].Contains(ips[i]) {
				return true
			}
		}
	}

	return false
}
