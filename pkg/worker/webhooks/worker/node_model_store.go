package worker

import (
	"context"
	"fmt"

	core "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/authentication/serviceaccount"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrladmission "sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/system"
	"gpustack.ai/gpustack/pkg/systemname"
	"gpustack.ai/gpustack/pkg/webhook"
)

// 用于拒绝 NodeModelStore status 写入的规则名称，在拒绝信息中显式列出，以便测试与排查人员区分触发了哪条规则。
const (
	nodeModelStoreRuleIdentity = "only the model-manager plugin writes a NodeModelStore's status"
	nodeModelStoreRuleBinding  = "the plugin's token must be bound to its Pod"
	nodeModelStoreRuleNode     = "the plugin writes only the status of the node its Pod runs on"
)

// NodeModelStoreWebhook 仅允许运行在对象同名 Node 上的 model-manager 插件 Pod 写入 NodeModelStore 的 status。
//
// RBAC 无法实现这一点。插件的 Role 必须允许更新 nodemodelstores/status，但 Role 无法将更新权限缩小到单个对象名称，因此若无此 Webhook，任意插件均可写入任意 Node 的 status。在本 operator 支持的所有 Kubernetes 版本中，API server 都会将绑定 ServiceAccount Token 所属 Pod 的名称和 UID 写入请求的用户 extra 字段中，从而只需读取一次即可获取该 Pod 所在 Node；自 Kubernetes 1.30 起，extra 中还会直接包含 Node 名称，可直接用于比对。缺少这些 extra 字段的旧式 Token 会被拒绝，以确保该规则不会静默降级。
//
// nolint: lll
// +k8s:webhook-gen:validating:group="worker.gpustack.ai",version="v1alpha1",resource="nodemodelstores",scope="Cluster"
// +k8s:webhook-gen:validating:operations=["UPDATE"],failurePolicy="Fail",sideEffects="None",matchPolicy="Equivalent",timeoutSeconds=10
// +k8s:webhook-gen:validating:subResources=["status"]
type NodeModelStoreWebhook struct {
	webhook.DefaultValidator

	// Reader 用于读取插件对应的 Pod。
	Reader ctrlcli.Reader
	// Namespace 和 ServiceAccount 指定插件运行的 Namespace 及所使用的 ServiceAccount。
	Namespace      string
	ServiceAccount string
}

func (r *NodeModelStoreWebhook) SetupWebhook(_ context.Context, opts webhook.SetupOptions) (runtime.Object, error) {
	r.Reader = opts.Manager.GetAPIReader()
	r.Namespace = systemname.NamespaceName
	r.ServiceAccount = system.ModelManagerServiceAccount.Get()

	return &workercore.NodeModelStore{}, nil
}

var (
	_ ctrladmission.Validator[runtime.Object] = (*NodeModelStoreWebhook)(nil)
	_ webhook.ReceiveDeletionUpdate           = (*NodeModelStoreWebhook)(nil)
)

// ReceiveDeletionUpdate 在对象排空期间保持规则生效。它不包含默认值设置，且
// 主资源更新无需读取即可准入，因此任何删除操作都不会导致其拒绝更新。
func (*NodeModelStoreWebhook) ReceiveDeletionUpdate() {}

func (r *NodeModelStoreWebhook) ValidateUpdate(ctx context.Context, _, newObj runtime.Object) (ctrladmission.Warnings, error) {
	if !isStatusRequest(ctx) {
		// worker 负责写入 spec；对象的生命周期和 spec 不属于此规则的职责范围。
		return nil, nil
	}
	nms := newObj.(*workercore.NodeModelStore)
	if rule, msg, err := r.statusWriter(ctx, nms.Name); err != nil || rule != "" {
		if err != nil {
			return nil, kerrors.NewInternalError(err)
		}
		return nil, kerrors.NewInvalid(workercore.SchemeGroupVersionKind("NodeModelStore").GroupKind(), nms.Name,
			field.ErrorList{field.Forbidden(field.NewPath("status"), rule+": "+msg)})
	}

	return nil, nil
}

// statusWriter 返回请求违反的规则及原因；如果发起方是 node 上的插件，则返回 ""。
func (r *NodeModelStoreWebhook) statusWriter(ctx context.Context, node string) (rule, msg string, err error) {
	req, err := ctrladmission.RequestFromContext(ctx)
	if err != nil {
		return "", "", fmt.Errorf("read the admission request: %w", err)
	}
	user := req.UserInfo

	want := serviceaccount.MakeUsername(r.Namespace, r.ServiceAccount)
	if user.Username != want {
		return nodeModelStoreRuleIdentity, fmt.Sprintf("%q is not %q", user.Username, want), nil
	}

	if names := user.Extra[serviceaccount.NodeNameKey]; len(names) == 1 {
		if names[0] != node {
			return nodeModelStoreRuleNode, fmt.Sprintf("the token's node is %q, not %q", names[0], node), nil
		}
		return "", "", nil
	}

	podNames, podUIDs := user.Extra[serviceaccount.PodNameKey], user.Extra[serviceaccount.PodUIDKey]
	if len(podNames) != 1 || len(podUIDs) != 1 {
		return nodeModelStoreRuleBinding, "the token names no Pod", nil
	}
	pod := new(core.Pod)
	if err := r.Reader.Get(ctx, ctrlcli.ObjectKey{Namespace: r.Namespace, Name: podNames[0]}, pod); err != nil {
		if kerrors.IsNotFound(err) {
			return nodeModelStoreRuleBinding, fmt.Sprintf("the token's Pod %q does not exist", podNames[0]), nil
		}
		return "", "", fmt.Errorf("read the plugin's Pod %q: %w", podNames[0], err)
	}
	switch {
	case string(pod.UID) != podUIDs[0]:
		return nodeModelStoreRuleBinding, fmt.Sprintf("the token's Pod %q is not the one it was issued to", podNames[0]), nil
	case pod.Spec.NodeName != node:
		return nodeModelStoreRuleNode, fmt.Sprintf("the token's Pod %q runs on %q, not %q", pod.Name, pod.Spec.NodeName, node), nil
	}

	return "", "", nil
}
