package apis

import (
	"context"

	apireg "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"

	gpustack "gpustack.ai/gpustack/api/v1"
	worker "gpustack.ai/gpustack/api/worker/v1"
	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/api"
	"gpustack.ai/gpustack/pkg/kubeclients/kubernetes"
)

var (
	apiCrdGetters = []api.CRDGetter{
		workercore.GetCustomResourceDefinitions,
	}
	apiSvcGetters = []api.ServiceGetter{
		worker.GetAPIService,
		gpustack.GetAPIService,
	}
)

// InstallCRDs 安装 CRD。
func InstallCRDs(ctx context.Context, cli kubernetes.Interface) error {
	return api.InstallCRDs(ctx, cli, apiCrdGetters)
}

// EnsureCRDs 保持 CRD 处于已安装状态，直到给定的 context 结束。
func EnsureCRDs(ctx context.Context, cli kubernetes.Interface) error {
	return api.EnsureCRDs(ctx, cli, apiCrdGetters)
}

// InstallServices 安装 API 服务。
func InstallServices(ctx context.Context, cli kubernetes.Interface, svc apireg.ServiceReference, ca []byte) error {
	return api.InstallServices(ctx, cli, svc, ca, apiSvcGetters)
}

// EnsureServices 保持 API 服务处于已安装状态，直到给定的 context 结束。
func EnsureServices(ctx context.Context, cli kubernetes.Interface, svc apireg.ServiceReference, ca []byte) error {
	return api.EnsureServices(ctx, cli, svc, ca, apiSvcGetters)
}

// DeleteServicesBackedBy 删除由指定 Namespace 中的 Service 支撑的 API 服务——\n// 包括当前 worker 注册的以及其子 release 注册的（即 Kueue 的 visibility 服务对），\n// 当删除流程跳过 chart 卸载时，这些资源同样会比 Namespace 存活更久。
func DeleteServicesBackedBy(ctx context.Context, cli kubernetes.Interface, namespace string) error {
	return api.DeleteServicesBackedBy(ctx, cli, namespace)
}

// WaitForServicesReady 等待 API 服务就绪。
func WaitForServicesReady(ctx context.Context, cli kubernetes.Interface) error {
	return api.WaitForServicesReady(ctx, cli, apiSvcGetters)
}
