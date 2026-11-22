package worker

import (
	"fmt"
	"strings"

	core "k8s.io/api/core/v1"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/worker/kvcache/mooncake"
)

func modelDeploymentRequestsInterfaces(md *workercore.ModelDeployment) bool {
	for i := range md.Spec.Roles {
		resources := md.Spec.Roles[i].Resources
		if resources != nil && resources.Interface != nil && resources.Interface.Sign() > 0 {
			return true
		}
	}
	return false
}

// ModelDeploymentDirectInterfaceProtocol 返回托管直连传输或多成员 Server 集合通信分支实际渲染的协议。
// 使用用户自定义命令的 role、缺少配对或集合通信，或忽略所声明协议的引擎，均不能使用该声明来选择设备授权。
func ModelDeploymentDirectInterfaceProtocol(
	md *workercore.ModelDeployment, role *workercore.ModelDeploymentRole, manufacturer string,
) string {
	if len(role.Command) > 0 ||
		md.Spec.Engine.Name != workercore.ModelDeploymentEngineVLLM ||
		manufacturer == "" || manufacturer == nodefeature.ManufacturerAscend {
		return ""
	}
	// 托管的多 Pod Server 集合通信分支（size > 1 或 elasticEp）会选择 operator 的
	// RDMA 设备键（"rdma"）；spec.kvTransfer 仅适用于路由的 prefill/decode 配对。
	if ModelDeploymentEffectiveRoleKind(role) == workercore.ModelDeploymentRoleKindServer &&
		(modelDeploymentRoleSize(role) > 1 || role.ElasticEP != nil) {
		return "rdma"
	}
	if !modelDeploymentUsesKVTransfer(md, role, manufacturer) {
		return ""
	}
	if md.Spec.KVTransfer != nil && md.Spec.KVTransfer.Protocol != "" {
		return mooncake.TransportProtocol(md.Spec.KVTransfer.Protocol)
	}
	return mooncake.TransportProtocol("")
}

// ModelDeploymentInterfaceResource 为正值的 role 接口计数选择一个 DevicePlugin 键。
// 即使其中某个成员组使用 TCP，各成员组也必须保持一致，因为单个计数无法描述使用不同协议的后端的分配情况。
func ModelDeploymentInterfaceResource(
	storeProtocols []string, directProtocol string, count int64,
) (core.ResourceName, error) {
	if count < 1 {
		return "", fmt.Errorf("interface count must be positive")
	}
	storeProtocol := ""
	if len(storeProtocols) > 0 {
		storeProtocol = storeProtocols[0]
		for _, protocol := range storeProtocols[1:] {
			if protocol != storeProtocol {
				return "", fmt.Errorf("cache member groups use conflicting protocols: %s",
					strings.Join(storeProtocols, ", "))
			}
		}
	}

	fabric := ""
	for _, protocol := range []string{storeProtocol, directProtocol} {
		if protocol != "rdma" && protocol != "efa" {
			continue
		}
		if fabric != "" && fabric != protocol {
			return "", fmt.Errorf("cache and direct-transfer legs use %s and %s", fabric, protocol)
		}
		fabric = protocol
	}
	if fabric == "" {
		return "", fmt.Errorf("no effective RDMA or EFA transfer leg uses the requested interfaces")
	}

	width := int32(1)
	if count > 1 {
		width = 2
	}
	return mooncake.FabricDeviceResource(fabric, width), nil
}
