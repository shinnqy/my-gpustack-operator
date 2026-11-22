package httpx

import (
	"net"
	"net/http"
	"net/url"
	"strings"

	"gpustack.ai/gpustack/pkg/utils/osx"
)

var noProxies []*net.IPNet

func init() {
	noProxyEnv := osx.Getenv("NO_PROXY", osx.Getenv("no_proxy"))
	noProxyRules := strings.Split(noProxyEnv, ",")
	for i := range noProxyRules {
		_, cidr, _ := net.ParseCIDR(noProxyRules[i])
		if cidr != nil {
			noProxies = append(noProxies, cidr)
		}
	}
}

// ProxyFromEnvironment 与 http.ProxyFromEnvironment 类似，
// 但同样遵循 NO_PROXY 环境变量。
func ProxyFromEnvironment(r *http.Request) (*url.URL, error) {
	if ip := net.ParseIP(r.URL.Hostname()); ip != nil {
		for i := range noProxies {
			if noProxies[i].Contains(ip) {
				return nil, nil
			}
		}
	}

	return http.ProxyFromEnvironment(r)
}
