package webhooks

import (
	"context"

	admreg "k8s.io/api/admissionregistration/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	"gpustack.ai/gpustack/pkg/kubeclients/kubernetes"
	"gpustack.ai/gpustack/pkg/webhook"
	"gpustack.ai/gpustack/pkg/worker/webhooks/worker"
)

// setups 是所有 webhook 处理器的列表。
var setups = []webhook.Setup{
	new(worker.InstanceWebhook),
	new(worker.InstanceTypeWebhook),
	new(worker.KVCacheBackendWebhook),
	new(worker.KVCachePoolWebhook),
	new(worker.KVCachePoolBindingWebhook),
	new(worker.ModelArtifactWebhook),
	new(worker.ModelDeploymentWebhook),
	new(worker.ModelDeploymentScalerWebhook),
	new(worker.ModelPrefetchWebhook),
	new(worker.ModelStoreBindingWebhook),
	new(worker.NodeModelStoreWebhook),
	new(worker.TopologySourceWebhook),
	new(worker.PodWebhook),
	new(worker.PodKVCacheWebhook),
	new(worker.WorkloadWebhook),
}

// cfgGetters 是所有 webhook 配置获取器的列表。
var cfgGetters = []webhook.ConfigurationsGetter{
	worker.GetWebhookConfigurations,
}

// configurationPrefix 用于生成 webhook 配置对象名称，即
// "<prefix>-mutation" 和 "<prefix>-validation"。API server 会按配置对象名称的
// 字典序串行执行变更型 webhook，因此该前缀必须排在
// "kueue-mutating-webhook-configuration" 之前：我们的变更型 webhook 会将
// 切分显存请求折叠进 .sliced.units，随后 Kueue 的 Pod webhook 会将容器资源
// 计算为哈希值写入 role 注解，因此我们的 webhook 必须先运行。"gpustack-worker"
// 排在 "kueue-" 之前（'g' < 'k'）；若前缀等于或排在 "kueue-" 之后，执行顺序会静默反转。
const configurationPrefix = "gpustack-worker"

// Setup 将 webhook API 注册到指定的 manager 和 HTTP mux。
func Setup(ctx context.Context, mgr ctrl.Manager, mux webhook.HTTPServeMux) error {
	return webhook.ExecuteSetup(ctx, mgr, mux, setups)
}

// Install 将 webhook 配置安装到 Kubernetes 集群。
func Install(ctx context.Context, cli kubernetes.Interface, cc admreg.WebhookClientConfig) error {
	return webhook.InstallConfigurations(ctx, configurationPrefix, cli, cc, cfgGetters)
}

// Delete 从 Kubernetes 集群中删除 webhook 配置。
func Delete(ctx context.Context, cli kubernetes.Interface) error {
	return webhook.DeleteConfigurations(ctx, configurationPrefix, cli)
}
