package worker

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	core "k8s.io/api/core/v1"
	networking "k8s.io/api/networking/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	ctrlreconcile "sigs.k8s.io/controller-runtime/pkg/reconcile"

	"gpustack.ai/gpustack/pkg/controller"
	"gpustack.ai/gpustack/pkg/kubemeta"
	"gpustack.ai/gpustack/pkg/setting"
	"gpustack.ai/gpustack/pkg/systemmeta"
	"gpustack.ai/gpustack/pkg/utils/ctrlclix"
	"gpustack.ai/gpustack/pkg/utils/ctrlhandlerx"
	"gpustack.ai/gpustack/pkg/worker/kuberess"
	"gpustack.ai/gpustack/pkg/worker/settings"
)

// instanceIsolationPolicyName 是每个非保留 Namespace 所创建的 NetworkPolicy 的固定名称。
// 每个 Namespace 仅此一个对象，由后续调度到此的所有 Instance 共享：其选择器使用稳定的
// 资源类型标签，因此启停或新建 Instance 均无需修改该策略。
const instanceIsolationPolicyName = "gpustack-instance-isolation"

// convertIsolationPolicy 为单个 Namespace 生成隔离策略契约：Instance Pod 允许被同一
// Namespace 内的流量以及 Pod 网络之外的任意来源访问（外部客户端通过 NodePort Service
// 进入时，其流量会被 SNAT 为节点地址），但禁止来自其他 Namespace 的 Pod 访问。出站
// 规则与此对称，使一个 Instance 内的 Shell 也无法扫描或访问相邻租户的 Pod；DNS 按端口
// 保持放行，因为 CNI 可能会将转换后的 DNS 服务器 Pod 地址传给策略引擎。
//
// ipBlock 的排除列表包含管理员在 Setting 中指定的 Pod CIDR：其中的每个地址都属于某个
// Namespace 的 Pod，而同 Namespace 规则已覆盖允许互通的部分。0.0.0.0/0 规则的 except
// 列表仅支持 IPv4 CIDR，且 Setting 的准入校验已拒绝其他格式。
func convertIsolationPolicy(podCIDRs []string) *networking.NetworkPolicy {
	external := networking.IPBlock{CIDR: "0.0.0.0/0", Except: podCIDRs}
	sameNamespace := networking.NetworkPolicyPeer{PodSelector: &meta.LabelSelector{}}
	return &networking.NetworkPolicy{
		ObjectMeta: meta.ObjectMeta{
			Name: instanceIsolationPolicyName,
		},
		Spec: networking.NetworkPolicySpec{
			PodSelector: meta.LabelSelector{
				MatchLabels: map[string]string{
					systemmeta.ResourceTypeLabel: "instances",
				},
			},
			PolicyTypes: []networking.PolicyType{
				networking.PolicyTypeIngress,
				networking.PolicyTypeEgress,
			},
			Ingress: []networking.NetworkPolicyIngressRule{{
				From: []networking.NetworkPolicyPeer{
					sameNamespace,
					{IPBlock: &external},
				},
			}},
			Egress: []networking.NetworkPolicyEgressRule{
				{To: []networking.NetworkPolicyPeer{sameNamespace}},
				{To: []networking.NetworkPolicyPeer{{IPBlock: &external}}},
				{Ports: []networking.NetworkPolicyPort{
					{Protocol: ptr.To(core.ProtocolUDP), Port: ptr.To(intstr.FromInt32(53))},
					{Protocol: ptr.To(core.ProtocolTCP), Port: ptr.To(intstr.FromInt32(53))},
				}},
			},
		},
	}
}

// InstanceIsolationReconciler 根据管理员配置的 Pod CIDR Setting，调谐每个非保留 Namespace
// 的命名空间隔离策略。它监听 Namespace，使 Namespace 自创建起（甚至在首个 Instance 出现
// 之前）即受策略覆盖；同时监听委托配置 Secret，以便在修改 Setting 时使所有 Namespace
// 收敛，而无需等待无关对象的事件。
//
// Setting 值通过 currentPodCIDRs 传递，绝不在调谐路径中实时读取：否则读取中途失败会
// 与空 Setting 无异，进而触发对隔离策略的清理回收。该缓存在初始化时预热一次，仅当 Secret
// 事件新解析的值与缓存不同时才刷新；若调谐时发现缓存为空，则重新探测并返回 error 以便
// 重试，避免基于未知状态执行操作。
type InstanceIsolationReconciler struct {
	Client ctrlcli.Client

	currentPodCIDRs atomic.Value // 存储 []string；首次成功读取前为空
}

var _ ctrlreconcile.Reconciler = (*InstanceIsolationReconciler)(nil)

// Reconcile 使单个 Namespace 的命名空间隔离策略与最近已知的 Pod CIDR 保持一致。保留
// Namespace 不承载 Instance Pod 且永不隔离，这与 LocalQueue 准入遵循相同规则；在此期间
// 已删除的 Namespace 无需处理，因为其策略已随之一并销毁。
func (r *InstanceIsolationReconciler) Reconcile(ctx context.Context, req ctrlreconcile.Request) (ctrl.Result, error) {
	logger := ctrllog.FromContext(ctx)

	ns := new(core.Namespace)
	err := r.Client.Get(ctx, ctrlcli.ObjectKey{Name: req.Name}, ns, ctrlclix.WithoutQuorum)
	if err != nil {
		if kerrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("fetch namespace: %w", err)
	}
	if ns.DeletionTimestamp != nil {
		logger.V(3).Info("skip deleted namespace")
		return ctrl.Result{}, nil
	}
	if kuberess.IsReservedNamespace(ns.Name) {
		logger.V(3).Info("skip reserved namespace")
		return ctrl.Result{}, nil
	}

	cidrs, err := r.lastKnownPodCIDRs(ctx)
	if err != nil {
		return ctrl.Result{}, err
	}
	desired := convertIsolationPolicy(cidrs)
	desired.Namespace = ns.Name

	np := new(networking.NetworkPolicy)
	err = r.Client.Get(ctx, ctrlcli.ObjectKey{
		Namespace: ns.Name, Name: instanceIsolationPolicyName,
	}, np, ctrlclix.WithoutQuorum)
	if err != nil {
		if !kerrors.IsNotFound(err) {
			return ctrl.Result{}, fmt.Errorf("fetch namespace isolation policy: %w", err)
		}
		if len(cidrs) == 0 {
			// Setting 已清空且不存在由本算子创建的策略，即已处于期望状态。
			return ctrl.Result{}, nil
		}
		systemmeta.NoteResource(desired, "instances", nil)
		if err = r.Client.Create(ctx, desired); err != nil {
			return ctrl.Result{}, fmt.Errorf("create namespace isolation policy: %w", err)
		}
		return ctrl.Result{}, nil
	}

	if len(cidrs) == 0 {
		// 关闭隔离时会回收本算子标记为自身创建的策略。若存在同名策略但并非本算子生成，则归属于
		// 其创建者，本控制器无权因修改 Setting 而删除他人的网络策略。
		if systemmeta.DescribeResourceType(np) != "instances" {
			logger.V(2).Info("namespace isolation policy was not rendered by this operator; leaving it alone",
				"namespace", ns.Name)
			return ctrl.Result{}, nil
		}
		if err = r.Client.Delete(ctx, np); err != nil {
			return ctrl.Result{}, fmt.Errorf("delete namespace isolation policy: %w", err)
		}
		return ctrl.Result{}, nil
	}

	// 与关闭隔离时遵循相同的边界：非本算子生成的同名策略归属于其创建者，本控制器同样无权因
	// Setting 变更而改写他人的网络策略。
	if systemmeta.DescribeResourceType(np) != "instances" {
		logger.V(2).Info("namespace isolation policy was not rendered by this operator; leaving it alone",
			"namespace", ns.Name)
		return ctrl.Result{}, nil
	}

	if kubemeta.DeepEqual(np.Spec, desired.Spec) {
		return ctrl.Result{}, nil
	}
	np.Spec = desired.Spec
	if err = r.Client.Update(ctx, np); err != nil {
		return ctrl.Result{}, fmt.Errorf("align namespace isolation policy: %w", err)
	}
	return ctrl.Result{}, nil
}

// SetupController 监听 Namespace（创建、更新和删除均会带来需要策略匹配的状态变更）以及
// 委托配置 Secret，其内容正是生成策略所依据的 Setting 值。Setting 只是该 Secret 之上的
// 扩展 API 视图，因此真正可被监听的对象是 Secret。缓存会在此处通过一次探测完成预热，
// 使首次调谐无需实时读取 Setting。
func (r *InstanceIsolationReconciler) SetupController(ctx context.Context, opts controller.SetupOptions) error {
	r.Client = opts.Manager.GetClient()

	if err := r.refreshPodCIDRsFromSecret(ctx); err != nil {
		// 启动时状态未知并不等同于值为空：首批调谐会重新探测并重试，直至缓存写入有效值，期间
		// 不会回收任何策略。
		ctrllog.FromContext(ctx).Error(err, "prime the pod CIDRs cache from the delegated settings secret")
	}

	return ctrl.NewControllerManagedBy(opts.Manager).
		Named("instanceisolation").
		For(&core.Namespace{}).
		Watches(
			&core.Secret{},
			ctrlhandlerx.DedupEnqueueRequestsFromMapFunc(time.Second, r.enqueueNamespacesForSettingsSecret),
		).
		Complete(r)
}

// enqueueNamespacesForSettingsSecret 将单次委托配置 Secret 变更扇出到所有 Namespace，
// 但仅当新内容确实与上次生成策略所用的值不同时才触发：内容未变的 Secret 更新事件常见于
// 重新 list，若对其扇出会导致整个集群做无意义的调谐。成功解析的变更会先刷新缓存，使
// 后续调谐读取到触发本次变更的新值。
func (r *InstanceIsolationReconciler) enqueueNamespacesForSettingsSecret(ctx context.Context, obj ctrlcli.Object) []ctrlreconcile.Request {
	if obj.GetNamespace() != setting.DelegatedSecretNamespace || obj.GetName() != setting.DelegatedSecretName {
		return nil
	}
	sec, ok := obj.(*core.Secret)
	if !ok {
		return nil
	}
	fresh := parseIsolationPodCIDRs(string(sec.Data[settings.InstanceIsolationPodCIDRs.Name()]))
	if current, ok := r.currentPodCIDRs.Load().([]string); ok && slices.Equal(current, fresh) {
		return nil
	}
	r.currentPodCIDRs.Store(fresh)

	namespaces := new(core.NamespaceList)
	if err := r.Client.List(ctx, namespaces, ctrlclix.WithoutQuorum, ctrlcli.UnsafeDisableDeepCopy); err != nil {
		ctrllog.FromContext(ctx).Error(err, "list namespaces for a settings change")
		return nil
	}
	reqs := make([]ctrlreconcile.Request, 0, len(namespaces.Items))
	for i := range namespaces.Items {
		if kuberess.IsReservedNamespace(namespaces.Items[i].Name) {
			continue
		}
		reqs = append(reqs, ctrlreconcile.Request{NamespacedName: ctrlcli.ObjectKey{Name: namespaces.Items[i].Name}})
	}
	return reqs
}

// lastKnownPodCIDRs 返回用于生成策略的 Pod CIDR。缓存值优先：它代表最近一次观测到的
// Secret 内容，而中途失败的实时读取绝不能被误判为清空 Setting（清空会触发策略回收）。
// 缓存缺失表示尚未观测到任何值——此时执行一次实时探测以确定结果，探测失败则返回 error
// 以便重试，绝不将其视为有效结果。
func (r *InstanceIsolationReconciler) lastKnownPodCIDRs(ctx context.Context) ([]string, error) {
	if current, ok := r.currentPodCIDRs.Load().([]string); ok {
		return current, nil
	}
	if err := r.refreshPodCIDRsFromSecret(ctx); err != nil {
		return nil, err
	}
	return r.currentPodCIDRs.Load().([]string), nil
}

// refreshPodCIDRsFromSecret 读取委托配置 Secret，并根据其中该项 Setting 的内容刷新缓存。
// 若键缺失则视为空白默认值——即该 Setting 从未写入过，空白表示关闭隔离。
func (r *InstanceIsolationReconciler) refreshPodCIDRsFromSecret(ctx context.Context) error {
	sec := new(core.Secret)
	err := r.Client.Get(ctx, ctrlcli.ObjectKey{
		Namespace: setting.DelegatedSecretNamespace, Name: setting.DelegatedSecretName,
	}, sec, ctrlclix.WithoutQuorum)
	if err != nil {
		return fmt.Errorf("read the delegated settings secret: %w", err)
	}
	r.currentPodCIDRs.Store(parseIsolationPodCIDRs(string(sec.Data[settings.InstanceIsolationPodCIDRs.Name()])))
	return nil
}

// parseIsolationPodCIDRs 将 Setting 中逗号分隔的值拆分为 ipBlock 排除列表所用的 CIDR，
// 并去重和排序，以保证跨调谐周期与跨 worker 重启时生成结果的字节级稳定。末尾逗号或
// 多余空格产生的空段会被丢弃；Setting 的准入校验已拒绝任何非 IPv4 CIDR 的输入。
func parseIsolationPodCIDRs(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	set := make(map[string]struct{})
	for _, segment := range strings.Split(raw, ",") {
		cidr := strings.TrimSpace(segment)
		if cidr == "" {
			continue
		}
		set[cidr] = struct{}{}
	}
	return slices.Sorted(maps.Keys(set))
}
