package extensionapis

import (
	"context"

	genericapiserver "k8s.io/apiserver/pkg/server"
	ctrl "sigs.k8s.io/controller-runtime"

	"gpustack.ai/gpustack/pkg/extensionapi"
	"gpustack.ai/gpustack/pkg/worker/extensionapis/worker"
	"gpustack.ai/gpustack/pkg/worker/settings"
)

// setups 是所有扩展 API 处理器的列表。
//
// CRD 的 v1 视图必须提供 DELETE 能力。v1 是该 API 组的首选版本，而垃圾回收器仅在
// 资源的首选版本同时支持 delete、list 和 watch 时才会监听该资源：缺少 delete 的视图
// 会导致所有属主已删除的此类对象被遗留下来，例如在 Node 删除后残留的 NodeModelStore
// （已在 Kubernetes 1.36 上实测验证）。TestEveryCRDViewServesDelete 强制保证该约束。
var setups = []extensionapi.Setup{
	extensionapi.NewSettingHandler(settings.Indexer()),
	new(worker.DevicesHandler),
	new(worker.InstanceHandler),
	new(worker.ModelDeploymentHandler),
	new(worker.ModelDeploymentScalerHandler),
	new(worker.ModelArtifactHandler),
	new(worker.ModelPrefetchHandler),
	new(worker.NodeModelStoreHandler),
	new(worker.KVCacheBackendHandler),
	new(worker.KVCachePoolHandler),
	new(worker.KVCachePoolBindingHandler),
	new(worker.ModelStoreHandler),
	new(worker.ModelStoreBindingHandler),
	new(worker.TopologySourceHandler),
	new(worker.InstanceImagePullSecretHandler),
	new(worker.InstancePersistentVolumeHandler),
	new(worker.InstancePersistentVolumeTypeHandler),
	new(worker.InstanceSSHPublicKeyHandler),
	new(worker.InstanceTypeHandler),
	new(worker.InstanceTypeFlavorHandler),
}

// Setup 安装扩展 API 处理器。
func Setup(
	ctx context.Context,
	srv *genericapiserver.GenericAPIServer,
	mgr ctrl.Manager,
) error {
	return extensionapi.ExecuteSetup(ctx, srv, Scheme, ParameterCodec, Codecs, mgr, setups)
}
