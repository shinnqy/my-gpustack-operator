package worker

import (
	"context"
	"fmt"
	"slices"

	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/kubeapistatus"
	"gpustack.ai/gpustack/pkg/utils/ctrlclix"
	"gpustack.ai/gpustack/pkg/worker/kvcache/mooncake"
)

// ModelDeploymentKind 是 ModelDeployment 在 KVCachePoolBinding 的 usedBy 中标识自身的类型名称。
//
// Binding 的 API 将该 kind 声明为该列表的唯一写入者，且其 finalizer 在列表非空时会拒绝释放
// Binding。因此该常量并非普通标签，而是触发拒绝保护的关键令牌；若拼写错误，将导致拒绝保护
// 机制永远作用于空列表而失效。
const ModelDeploymentKind = "ModelDeployment"

// ModelDeploymentConditionDomainRegistered 表示所引用的 KVCachePoolBinding 是否已解析并
// 成功读取其复用域。
const ModelDeploymentConditionDomainRegistered kubeapistatus.ConditionType = "DomainRegistered"

// DomainRegistered 所携带的 reason 枚举。采用四个而非两个，是因为这四个 reason 分别对应
// 四种排查方向：无需处理、创建 Binding、等待其就绪（或检查池状态）、排查是谁将其删除。
const (
	modelDeploymentReasonRegistered      = "Registered"
	modelDeploymentReasonNotApplicable   = "NotApplicable"
	modelDeploymentReasonBindingNotFound = "BindingNotFound"
	modelDeploymentReasonBindingNotReady = "BindingNotReady"
	modelDeploymentReasonBindingDeleting = "BindingDeleting"
)

// modelDeploymentDomain 表示单轮调谐中对所引用 Binding 的观测结果。
//
// 当完全无法读取 Binding 时，KVCache 为 nil。这不同于空投影：曾经解析成功但随后丢失了
// Binding 的部署会保留其已挂载的域信息，而由 condition 标明该读数已过期。若将其清空，
// 将抹除副本仍在向哪个缓存写入的唯一记录。
type modelDeploymentDomain struct {
	KVCache *workercore.ModelDeploymentKVCacheStatus
	Ready   bool
	Reason  string
	Message string

	// Blocks 和 Usage 是主节点对该复用域实际占用情况的统计，直接取自 Binding 而不会归零处理。
	// 两者在原结构中均为带 omitempty 的指针：缺失表示本次采集未包含该域——这与域中数据为空
	// 是截然不同的事实，且只有缓存挂载状态读取逻辑需要区分这两者。
	Blocks *int64
	Usage  *resource.Quantity
}

// resolveModelDeploymentDomain 读取当前部署所引用的 KVCachePoolBinding，并投影其不可变的
// 域配置块。
//
// 读取范围仅限部署所在的 Namespace，不存在其他查找路径：poolRef 为 LocalObjectReference，
// 其名称只能指向当前 Namespace 下的对象。池名称直接取自 Binding 而非再去读取池对象，因为
// 投影结果仅需要其名称。
//
// 未声明缓存的部署会获得显式的“不适用”结果而非 nil，因为 nil 专用于表示未执行检查的轮次，
// 而无缓存部署是一个明确的结论而非缺失：若不返回该结果，对象上将完全没有 DomainRegistered
// condition，而缺少 condition 与不适用的 condition 属于不同的状态。
func (r *ModelDeploymentReconciler) resolveModelDeploymentDomain(
	ctx context.Context, md *workercore.ModelDeployment,
) (*modelDeploymentDomain, error) {
	if md.Spec.KVCache == nil {
		return &modelDeploymentDomain{
			Ready:   true,
			Reason:  modelDeploymentReasonNotApplicable,
			Message: "the deployment declares no KV cache pool, so there is no Binding to resolve",
		}, nil
	}

	kvcpb, err := r.getModelDeploymentBinding(ctx, md)
	if err != nil {
		return nil, err
	}

	name := md.Spec.KVCache.PoolRef.Name
	if kvcpb == nil {
		return &modelDeploymentDomain{
			Reason: modelDeploymentReasonBindingNotFound,
			Message: fmt.Sprintf(
				"kv cache pool binding %q does not exist in namespace %q; an admin creates it, and "+
					"creating one is what grants this namespace access to the pool",
				name, md.Namespace,
			),
		}, nil
	}

	observed := &modelDeploymentDomain{
		KVCache: &workercore.ModelDeploymentKVCacheStatus{
			Binding: kvcpb.Name,
			Pool:    kvcpb.Spec.PoolRef.Name,
			// 直接取自 Binding 的 spec（该处要求并冻结了全部三个字段），而非取自其 status：域是管理员
			// 声明的配置，若投影等待 Binding 自身控制器写入 status，恰好会在部署查询自身归属哪个缓存的
			// 时间窗口内报告空域。
			Domain: workercore.ModelDeploymentKVCacheDomain{
				Name:      kvcpb.Spec.Domain.Name,
				BlockSize: kvcpb.Spec.Domain.BlockSize,
				Dtype:     kvcpb.Spec.Domain.Dtype,
			},
		},
	}
	observed.Ready, observed.Reason, observed.Message = modelDeploymentBindingUsable(kvcpb)
	observed.Blocks, observed.Usage = kvcpb.Status.Blocks, kvcpb.Status.Usage

	return observed, nil
}

// modelDeploymentBindingUsable 返回 Binding 当前是否真正具备写入能力。
//
// 该判断读取两项相互独立的事实，其冗余设计正是为了互相兜底各自可能失效的场景：
//
//   - Binding 自身的 phase，由其控制器综合所有维度推导得出。读取汇总状态而非复制维度列表，
//     意味着后续新增的维度无需修改本文件即可在此生效；若复制列表，则会在静默过期后仍误报
//     为可用。
//   - 直接检查 QuotaGranted 维度。设置该维度是因为就绪状态曾出现过在主节点授予有效配额为
//     零或完全没有账本条目时仍报告 True 的情况，导致 Binding 显示 Ready 却无法成功写入。
//     本规格正是为暴露此类状态而引入，因此对这一曾经触发过回归的维度直接检查，而不委托给
//     汇总状态。
//
// 正在删除中的 Binding 本身即视为不可用。此处不会且绝不能释放它：usedBy 中的占用声明正是
// 阻塞删除流程的关键，若丢弃该声明，仍在写入数据的副本将失去授权支撑。
func modelDeploymentBindingUsable(kvcpb *workercore.KVCachePoolBinding) (bool, string, string) {
	if kvcpb.DeletionTimestamp != nil {
		return false, modelDeploymentReasonBindingDeleting, fmt.Sprintf(
			"kv cache pool binding %q is being deleted; the replicas keep serving and this deployment "+
				"still holds it, so the deletion waits until the deployment goes away",
			kvcpb.Name,
		)
	}

	if kvcpb.Status.Phase != KVCachePoolPhaseReady {
		return false, modelDeploymentReasonBindingNotReady, fmt.Sprintf(
			"kv cache pool binding %q is %s: %s",
			kvcpb.Name, modelDeploymentBindingPhase(kvcpb), kvcpb.Status.PhaseMessage,
		)
	}

	if !KVCachePoolBindingConditionQuotaGranted.IsTrue(kvcpb) {
		return false, modelDeploymentReasonBindingNotReady, fmt.Sprintf(
			"kv cache pool binding %q reports itself ready but has no quota to write into: %s",
			kvcpb.Name, KVCachePoolBindingConditionQuotaGranted.GetMessage(kvcpb),
		)
	}

	return true, modelDeploymentReasonRegistered, fmt.Sprintf(
		"reuse domain %q is registered through kv cache pool binding %q on pool %q",
		kvcpb.Spec.Domain.Name, kvcpb.Name, kvcpb.Spec.PoolRef.Name,
	)
}

// modelDeploymentBindingPhase 将 phase 拼写为消息文本，包括没有 phase 的情况：尚未被其自身
// controller 处理的 Binding 的 phase 为空，而 "is : " 读起来更像是本 operator 的 bug，
// 而不是一个尚未被检查的 Binding。
func modelDeploymentBindingPhase(kvcpb *workercore.KVCachePoolBinding) string {
	if kvcpb.Status.Phase == "" {
		return "not observed yet"
	}

	return kvcpb.Status.Phase
}

// observeModelDeploymentDomain 将单轮对 Binding 的读取结果合入 status。
//
// 读取结果为 nil 表示本轮未进行检查（清理阶段的轮次不会检查），此时 condition 和
// projection 均保持原样。若将其覆盖为 "not observed"，会导致每次清理都报告 domain 已消失。
func observeModelDeploymentDomain(holder *workercore.ModelDeployment, observed *modelDeploymentDomain) {
	if observed == nil {
		return
	}

	if observed.KVCache != nil {
		holder.Status.KVCache = observed.KVCache
	}

	if observed.Ready {
		ModelDeploymentConditionDomainRegistered.True(holder, observed.Reason, observed.Message)

		return
	}

	ModelDeploymentConditionDomainRegistered.False(holder, observed.Reason, observed.Message)
}

// claimModelDeploymentBinding 将当前 deployment 记录到 Binding 的 usedBy 中。
//
// 该声明是 Binding 拒绝删除机制得以生效的关键：该列表约定只有一个写入方（即本类型），
// 在有写入之前，拒绝删除只会面对一个始终为空的列表。每次能够读取 Binding 的轮次都会写入该声明，
// 包括 Binding 尚不可用的情况——store leader 重启会使每个 Binding 短暂处于非 Ready 状态，
// 若声明随就绪状态反复增删，就会在 replica 仍在写入时敞开窗口，导致管理员的删除操作意外通过。
func (r *ModelDeploymentReconciler) claimModelDeploymentBinding(
	ctx context.Context, md *workercore.ModelDeployment,
) error {
	return r.syncModelDeploymentBindingClaim(ctx, md, true)
}

// releaseModelDeploymentBinding 移除该声明，使在 deployment 之后被删除的 Binding 能够完成删除。
// 仅当 replica 全部消失后才调用该函数：若在仍有 replica 运行时提前释放，授权可能会在进程
// 仍在通过它写入时失效。
func (r *ModelDeploymentReconciler) releaseModelDeploymentBinding(
	ctx context.Context, md *workercore.ModelDeployment,
) error {
	return r.syncModelDeploymentBindingClaim(ctx, md, false)
}

func (r *ModelDeploymentReconciler) syncModelDeploymentBindingClaim(
	ctx context.Context, md *workercore.ModelDeployment, claim bool,
) error {
	if md.Spec.KVCache == nil {
		return nil
	}

	kvcpb, err := r.getModelDeploymentBinding(ctx, md)
	if err != nil {
		return err
	}
	if kvcpb == nil {
		// 无需声明，也无需释放：已不存在的 Binding 连同其列表一并消失了。这种情况通过
		// DomainRegistered 上报，而不作为 error 返回，因为其 Binding 被删除的 deployment 仍会继续提供服务。
		return nil
	}

	ref := workercore.KVCacheObjectReference{
		Kind: ModelDeploymentKind,
		// 按该字段自身规则留空：此处可能出现的所有对象都位于 Binding 所在的 namespace 中，
		// 因此显式命名只会在每个条目上重复 Binding 的元数据。
		Namespace: "",
		Name:      md.Name,
	}

	at := slices.IndexFunc(kvcpb.Status.UsedBy,
		func(e workercore.KVCacheObjectReference) bool { return e == ref })
	if claim == (at >= 0) {
		// 已与预期一致。该保护可防止已稳定的 deployment 在每一轮都重写另一个 controller 的对象，
		// 否则由于 Binding 自身的 reconciler 也在写入同一 status，会导致每一轮都发生冲突。
		return nil
	}

	if claim {
		kvcpb.Status.UsedBy = append(kvcpb.Status.UsedBy, ref)
		// 同一状态对应同一顺序，避免因两个 deployment 声明顺序与上次不同而反复重写 Binding。
		slices.SortFunc(kvcpb.Status.UsedBy, compareKVCacheObjectReferences)
	} else {
		kvcpb.Status.UsedBy = slices.Delete(kvcpb.Status.UsedBy, at, at+1)
	}

	// 发生冲突时重新入队，而不是原地重试：Binding 自身的 reconciler 也会写入该 status，
	// 此时落败方对整个对象的视图已过期，仅重试列表会写入基于已变更状态的声明。
	return r.Client.Status().Update(ctx, kvcpb)
}

// getModelDeploymentBinding 读取引用的 Binding，若不存在则返回 nil。
//
// 将不存在作为返回值而非 error，是因为这属于一种正常且可上报的状态：准入控制会拒绝
// 指向不存在 Binding 的 poolRef，因此进入该状态的唯一途径是在运行中的 deployment 下删除
// 该 Binding，而此时 replica 仍继续提供服务。
func (r *ModelDeploymentReconciler) getModelDeploymentBinding(
	ctx context.Context, md *workercore.ModelDeployment,
) (*workercore.KVCachePoolBinding, error) {
	if md.Spec.KVCache == nil {
		return nil, nil
	}

	kvcpb := new(workercore.KVCachePoolBinding)
	err := r.Client.Get(ctx,
		ctrlcli.ObjectKey{Namespace: md.Namespace, Name: md.Spec.KVCache.PoolRef.Name},
		kvcpb, ctrlclix.WithoutQuorum)
	if err != nil {
		if kerrors.IsNotFound(err) {
			return nil, nil
		}

		return nil, fmt.Errorf("get kv cache pool binding %s/%s: %w",
			md.Namespace, md.Spec.KVCache.PoolRef.Name, err)
	}

	return kvcpb, nil
}

// resolveModelDeploymentInterfaceProtocols 独立于已发布的 endpoint 读取 backend 声明。
// 当 cache domain 仍在启动时，设备分配必须保持稳定。
func (r *ModelDeploymentReconciler) resolveModelDeploymentInterfaceProtocols(
	ctx context.Context, md *workercore.ModelDeployment,
) ([]string, error) {
	binding, err := r.getModelDeploymentBinding(ctx, md)
	if err != nil {
		return nil, err
	}
	if binding == nil {
		return nil, fmt.Errorf("cache binding %q is not available for interface allocation", md.Spec.KVCache.PoolRef.Name)
	}
	pool := &workercore.KVCachePool{}
	if err := r.Client.Get(ctx, ctrlcli.ObjectKey{Name: binding.Spec.PoolRef.Name}, pool); err != nil {
		return nil, fmt.Errorf("get kv cache pool %q for interface allocation: %w", binding.Spec.PoolRef.Name, err)
	}
	if len(pool.Spec.Backends) == 0 {
		return nil, fmt.Errorf("kv cache pool %q has no backend for interface allocation", pool.Name)
	}
	backend := &workercore.KVCacheBackend{}
	if err := r.Client.Get(ctx, ctrlcli.ObjectKey{Name: pool.Spec.Backends[0]}, backend); err != nil {
		return nil, fmt.Errorf("get kv cache backend %q for interface allocation: %w", pool.Spec.Backends[0], err)
	}
	return mooncake.MemberProtocols(backend), nil
}

// resolveModelDeploymentConnection 读取 pool 及其 backend 发布的信息：即同一 deployment
// 下所有角色共享的那一半合成输入。
//
// 当没有可连接目标时返回 nil，调用方随后会渲染完全不带 connector 的 replica，而不是部分配置
// 的 connector。这是规范的要求，并非出于保守：配置了无响应地址的客户端从 Pod 外部看像是
// 缓存 MISS，而从未配置过的 replica 会在其自身的 spec 中明确体现这一点。
//
// 任何缺失环节都特意走同一退出路径。缺失的 pool、未发布的 endpoint、缺失的 backend——
// 每种情况集群都能自行收敛恢复，且均已通过 DomainRegistered 或 pool 自身的 conditions 上报。
// 在此处另行判断只会增加冗余消息，而不会提供新的观测信息。
//
// 另一半输入（引擎和加速器）按角色区分：加速器来自角色的 InstanceType，因此合成过程
// 本身无法提取到该循环之外。
func (r *ModelDeploymentReconciler) resolveModelDeploymentConnection(
	ctx context.Context, domain *modelDeploymentDomain,
) (*ModelDeploymentConnectorInput, error) {
	// 在第 22 章实现此函数。
	return nil, nil
}
