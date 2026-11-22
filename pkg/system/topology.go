package system

import (
	"fmt"
	"net"

	knet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/apimachinery/pkg/util/sets"

	"gpustack.ai/gpustack/pkg/utils/osx"
	"gpustack.ai/gpustack/pkg/utils/varx"
)

var (
	// IPs 是系统非环回 IP 地址的集合。
	IPs = varx.NewOnce(sets.New[string]())

	// Subnets 是系统的子网集合。
	Subnets = varx.NewOnce(sets.New[string]())

	// PrimaryIP 是系统的主（出站）IP 地址。
	//
	// 如果存在环境变量 "KUBERNETES_POD_IP"，则优先从中获取分配的 IP 地址，
	// 该变量由 Kubernetes Pod 中的 downward API 注入。
	PrimaryIP = varx.Once[string]{}

	// PrimarySubnet 是系统的主子网。
	PrimarySubnet = varx.Once[string]{}
)

// ConfigureNetwork 配置系统网络。
func ConfigureNetwork() error {
	var (
		hostIP, _     = knet.ChooseHostInterface()
		ifaces, _     = net.Interfaces()
		ips           = sets.New[string]()
		subnets       = sets.New[string]()
		primaryIP     string
		primarySubnet string
	)

	var assignedHostIP net.IP
	if v := osx.Getenv("KUBERNETES_POD_IP", ""); v != "" {
		assignedHostIP = net.ParseIP(v)
	}

	for _, _if := range ifaces {
		if _if.Flags&net.FlagUp == 0 || _if.Flags&net.FlagLoopback != 0 {
			// 跳过未启用或回环接口。
			continue
		}

		addrs, err := _if.Addrs()
		if err != nil {
			return fmt.Errorf("query addresses from interface %s(%d): %w",
				_if.Name, _if.Index, err)
		}

		for _, addr := range addrs {
			if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
				// NB(thxCode): IPv6 不存在 NAT，
				// 因此只需考虑 IPv4。
				v4 := ipnet.IP.To4()
				if v4 == nil || ips.Has(v4.String()) {
					continue
				}
				if v4[3] == 0x01 {
					continue
				}
				if assignedHostIP != nil && !assignedHostIP.Equal(v4) {
					continue
				}

				ips.Insert(v4.String())
				subnets.Insert(ipnet.String())
				if hostIP != nil && hostIP.Equal(v4) {
					primaryIP = v4.String()
					primarySubnet = ipnet.String()
				}
			}
		}
	}

	if primaryIP == "" {
		if assignedHostIP == nil {
			return fmt.Errorf("primary IP is not found")
		}
		primaryIP = assignedHostIP.String()
	}

	IPs.Configure(ips)
	Subnets.Configure(subnets)
	PrimaryIP.Configure(primaryIP)
	PrimarySubnet.Configure(primarySubnet)

	return nil
}
