package kuberess

import (
	"context"
	"slices"

	"gpustack.ai/gpustack/pkg/kubeclients/kubernetes"
	"gpustack.ai/gpustack/pkg/systemkuberess"
	"gpustack.ai/gpustack/pkg/systemname"
	"gpustack.ai/gpustack/pkg/utils/osx"
)

const (
	// CSIProvisionerNFS 是内置 NFS CSI 驱动的驱动名称，即 csi-driver-nfs 子 chart 中 "driver.name" 声明的值。
	CSIProvisionerNFS = "nfs.csi.gpustack.ai"
	// CSIProvisionerS3 是内置 S3 CSI 驱动的驱动名称，即 csi-driver-s3 子 chart 中 "driver.name" 声明的值。
	CSIProvisionerS3 = "s3.csi.gpustack.ai"
)

var (
	// SystemNamespaceName 是系统资源的 Namespace 名称。
	SystemNamespaceName = systemname.NamespaceName

	// SystemRoutingServiceName 是系统路由 Service 的服务名称。
	SystemRoutingServiceName string
)

func init() {
	SystemRoutingServiceName = osx.Getenv("KUBERNETES_SERVICE_NAME", "gpustack-operator-worker")
}

// InstallSystemNamespace 创建系统 Namespace。
func InstallSystemNamespace(ctx context.Context, cli kubernetes.Interface) error {
	return systemkuberess.InstallSystemNamespace(ctx, cli, SystemNamespaceName)
}

// InstallFakeSystemRoutingService 为系统创建虚拟的路由 Service/Endpoint。
//
// 该 Service 指向系统的 SelfIP。
func InstallFakeSystemRoutingService(ctx context.Context, cli kubernetes.Interface, port int32) error {
	return systemkuberess.InstallFakeSystemRoutingService(ctx, cli, SystemNamespaceName, SystemRoutingServiceName, port)
}

// ReservedNamespaces 是保留给系统资源的 Namespace 列表，不应用于用户工作负载。
var _ReservedNamespaces = []string{
	SystemNamespaceName,
	"kube-system",
}

// IsReservedNamespace 在给定 Namespace 保留给系统资源时返回 true。
func IsReservedNamespace(ns string) bool {
	return slices.Contains(_ReservedNamespaces, ns)
}
