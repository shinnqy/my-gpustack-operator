package worker

import (
	"context"
	"fmt"
	"slices"
	"strings"

	core "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/systemname"
	workerctrl "gpustack.ai/gpustack/pkg/worker/controllers/worker"
	"gpustack.ai/gpustack/pkg/worker/kvcache/inject"
	"gpustack.ai/gpustack/pkg/worker/kvcache/mooncake"
)

// isolation 记录了标记（stamp）中关于 Pod 所声明的复用域的信息。
//
// 它不对隔离是否实际生效做任何判断。注入的租户是否生效取决于引擎构建版本，本项目不做检查。
// 对租户所做的处理由了解情况的渲染器负责记录。
type isolation struct {
	// Domain 是 Binding 声明的复用标识。仅当对象从未经过 Binding 准入时才为空，经过准入时该字段为必填。
	Domain string
}

// resolution 包含 Default 在读取集群后所需的全部信息：要渲染的内容，以及关于其未提供隔离的说明。
type resolution struct {
	Input     inject.Input
	Isolation isolation
}

// resolve 读取 Pod 的注解及其命名的对象，并返回需要渲染的内容。
//
// 读取采用缓存优先、无缓存重试的策略，因为 Pod 与其 Binding 同时创建是常见场景，informer 可能尚未同步到该对象。
// 仅当第二次读取返回 NotFound 时才视为拒绝：超时、RBAC 拒绝或 5xx 错误并不能说明对象是否存在，而此 Webhook
// 以 failurePolicy Fail 运行，若将这类错误报告为“未找到”，就会使集群故障被误读为清单中的拼写错误。
func (r *PodKVCacheWebhook) resolve(ctx context.Context, pod *core.Pod) (*resolution, error) {
	if err := checkAnnotationVocabulary(pod); err != nil {
		return nil, err
	}

	// 该消息说明了拒绝此键的作用与局限，因为这种形式容易让人做出更强硬的解读。“Store 接受客户端发送的任意租户”
	// 是早期的表述，但在此处并不准确：只有当 master 报告存在租户账本（checkQuotaLedger）时才会转发租户，而这样的
	// master 会拒绝账本中不存在的名称。高估 Store 的宽松度与高估 Binding 的权限是两个方向上的同一类错误。
	if _, ok := pod.Annotations[KVCacheDomainAnnotationKey]; ok {
		return nil, fmt.Errorf("annotation %q is not accepted: the reuse domain comes from the "+
			"KVCachePoolBinding and only from there, so that every domain this operator provisions has "+
			"an object accounting for it. This is the shape of the declarative contract, not an "+
			"enforcement boundary. What a Binding decides is "+
			"which names EXIST: the multi-tenant master this webhook injects against refuses a name "+
			"absent from its ledger. Remove it; a namespace needing two reuse boundaries gets two "+
			"Bindings", KVCacheDomainAnnotationKey)
	}

	engine, err := inject.ParseEngine(pod.Annotations[KVCacheEngineAnnotationKey])
	if err != nil {
		return nil, fmt.Errorf("annotation %q: %w", KVCacheEngineAnnotationKey, err)
	}
	engine, err = resolveManufacturer(engine, pod.Annotations)
	if err != nil {
		return nil, err
	}
	role, err := inject.ParseRole(pod.Annotations[KVCacheRoleAnnotationKey])
	if err != nil {
		return nil, fmt.Errorf("annotation %q: %w", KVCacheRoleAnnotationKey, err)
	}

	binding, err := r.resolveBinding(ctx, pod)
	if err != nil {
		return nil, err
	}
	pool, err := r.resolvePool(ctx, binding)
	if err != nil {
		return nil, err
	}
	if err = r.checkQuotaLedger(pool); err != nil {
		return nil, err
	}
	backend, err := r.resolveBackend(ctx, pool)
	if err != nil {
		return nil, err
	}

	if pool.Status.ClientEndpoint == "" {
		return nil, fmt.Errorf("pool %q has not published a client endpoint yet, so there is no "+
			"address to point %q at; retry once the pool reports one", pool.Name, engine)
	}

	// 该匹配会拒绝不支持的传输协议，以及无约束引擎只能安装其中一种传输选项的混合池。两者均属于绑定失败，因此
	// 在准入阶段即拒绝该 Pod，而不是启动一个无法访问部分分段的客户端。
	offers := mooncake.MemberProtocols(backend)
	if err := inject.ValidateBindingTransport(engine, offers); err != nil {
		return nil, err
	}
	protocol, err := inject.MatchTransport(engine, offers)
	if err != nil {
		return nil, err
	}

	var dtype string
	if workerctrl.ModelDeploymentOwnsKVCacheDtype(ctx) {
		dtype = binding.Spec.Domain.Dtype
	}

	return &resolution{
		// 仅当 master 能够区分各个租户时，才会将复用域传递给渲染器：没有租户账本的 master 会将所有租户名称归并到
		// 其默认租户中，此时转发复用域相当于宣称了 Store 实际并不提供的隔离能力。下方的标记无论如何都会保留声明的
		// 复用域，而 TenantInjected 则记录了实际执行的是哪种情况。
		Input: inject.Input{
			Engine: engine,
			Role:   role,
			Domain: injectableDomain(binding, backend, pool),
			Dtype:  dtype,
			Connection: inject.Connection{
				MasterAddress: pool.Status.ClientEndpoint,
				Protocol:      protocol,
			},
		},
		Isolation: isolation{
			Domain: binding.Spec.Domain.Name,
		},
	}, nil
}

// injectableDomain 是 Pod 配置中使用的复用域：当 master 隔离租户时为 Binding 自身的名称，不隔离时为空 ——
// 空复用域完全不渲染租户标识，这也是无账本 master 唯一能够如实服务的配置。
func injectableDomain(
	binding *workercore.KVCachePoolBinding,
	backend *workercore.KVCacheBackend,
	pool *workercore.KVCachePool,
) string {
	if !workerctrl.KVCacheMasterSeparatesTenants(backend, pool) {
		return ""
	}
	return binding.Spec.Domain.Name
}

// resolveManufacturer 选择唯一一个连接器存在差异的 vLLM 运行时变体。这仅影响渲染：ModelDeployment 角色准入
// 在其可能的变体之间保持保守策略，因为该路径没有对应的作者声明。
func resolveManufacturer(engine inject.Engine, annotations map[string]string) (inject.Engine, error) {
	manufacturer, declared := annotations[KVCacheManufacturerAnnotationKey]
	if !declared {
		return engine, nil
	}
	if engine != inject.EngineVLLM {
		return "", fmt.Errorf("annotation %q is only accepted with engine %q", KVCacheManufacturerAnnotationKey,
			inject.EngineVLLM)
	}
	if manufacturer != nodefeature.ManufacturerAscend {
		return "", fmt.Errorf("annotation %q must be %q when declared with engine %q, got %q",
			KVCacheManufacturerAnnotationKey, nodefeature.ManufacturerAscend, inject.EngineVLLM, manufacturer)
	}

	return inject.EngineVLLMAscend, nil
}

// resolveBinding 在 Pod 自身的 Namespace 中读取 Pod 所指定的 Binding。不存在跨 Namespace 的形式：Binding 是
// 分配和计量某个 Namespace 授权的位置，因此若接受带 Namespace 的值，就会允许 Pod 通过指定名称来支取其他
// Namespace 的授权。
//
// 这里是“分配与计量”，而非“强制鉴权”。Store 通过任意 Pod 均可拨号的 Service 访问，且不会基于该对象派生任何凭证。
// 注入路径会使用 Binding 覆盖其租户环境变量，但绕过该路径访问 Store 的工作负载仍可指定其知晓的任意域。将
// Binding 称为鉴权点意味着宣称了当前技术栈并不具备的边界。它真正能够限定的是哪些名称存在：多租户 master 会
// 拒绝其账本中不存在的名称，因此未注册的名称无法绕过配额上限。问题跟踪见 #168。
func (r *PodKVCacheWebhook) resolveBinding(
	ctx context.Context, pod *core.Pod,
) (*workercore.KVCachePoolBinding, error) {
	name := pod.Annotations[KVCacheBindingAnnotationKey]
	switch {
	case name == "":
		return nil, fmt.Errorf("annotation %q is required: it names the KVCachePoolBinding in this "+
			"namespace that provisions this Pod's grant on a pool", KVCacheBindingAnnotationKey)
	case strings.Contains(name, "/"):
		return nil, fmt.Errorf("annotation %q must be a plain name, not %q: a Binding is resolved in "+
			"the Pod's own namespace and there is no cross-namespace form",
			KVCacheBindingAnnotationKey, name)
	}

	// 此处仅检查对象是否存在，刻意不检查 Binding 的就绪状态。尚未完成 reconcile 的 Binding 还未向 master 注册
	// 其租户，因此转发该租户的引擎会收到 TENANT_NOT_REGISTERED —— 随后就会停止收到该错误，因为 reconciler
	// 最终会收敛，同一 Pod 后续的写入也会成功。代价只是存在一段无缓存的窗口期，但系统会自行恢复。
	//
	// 准入拒绝则不会自愈。准入不会自动重试，因此与 Binding 同时创建的 Deployment（常见场景）将直接失败。用永久性
	// 失败来换取避免短暂降级是错误的方向。
	//
	// 由此确立的分工原则是：永久性丢失予以拒绝，临时性未收敛予以放行并等待自愈。F4b 属于永久性的一侧 —— 账本
	// 无法得到响应的池在有人干预前会一直处于该状态，再怎么等待也无法修复。唯一既非永久也非临时的答复 —— 观察到
	// master 完全不持有账本 —— 属于一种已声明的拓扑而非故障丢失，因此不带租户标识予以准入。
	//
	// 此处直接实时读取而不经过 r.get。该辅助方法仅在遇到 NotFound 时才回退到 APIReader，因此它能修复缓存缺失
	// 对象的情况，却无法修复缓存持有陈旧副本的情况 —— 而下方的删除检查恰恰读取的是陈旧副本最可能滞后的字段。
	//
	// Informer 延迟并非毫秒级临界：在 watch 重连后延迟可达数秒，足以让片刻前刚被删除的 Binding 仍被读为存活
	// 状态，从而放行一个永远无法写入的 Pod。池与后端则继续使用 r.get：它们没有任何判断依赖如此时效敏感的字段。
	binding := &workercore.KVCachePoolBinding{}
	key := ctrlcli.ObjectKey{Namespace: pod.Namespace, Name: name}
	if err := r.APIReader.Get(ctx, key, binding); err != nil {
		if kerrors.IsNotFound(err) {
			return nil, fmt.Errorf("annotation %q names KVCachePoolBinding %q, which does not exist in "+
				"namespace %q: a Pod draws on a pool through a Binding its own namespace holds, so "+
				"either the name is wrong or the Binding has not been created",
				KVCacheBindingAnnotationKey, name, pod.Namespace)
		}
		return nil, fmt.Errorf("get kv cache pool binding %q in namespace %q: %w",
			name, pod.Namespace, err)
	}
	// 处于删除中的 Binding 仍会被 Get 返回，而基于它进行准入属于上述分工中永久性的一侧而非临时性的一侧：池的
	// reconciler 正在从账本中移除该域，一旦移除，此 Pod 的每次写入都会因 TENANT_NOT_REGISTERED 而失败。等待
	// 不会使其恢复，也没有机制会回滚该删除 —— 普通 Pod 不会出现在 Binding 的 status.usedBy 中，因此保护已声明
	// 消费者的 finalizer 根本看不到这个 Pod。
	if !binding.GetDeletionTimestamp().IsZero() {
		return nil, fmt.Errorf("annotation %q names KVCachePoolBinding %q, which is being deleted: "+
			"its reuse domain is being withdrawn from the pool's ledger, so a Pod admitted against "+
			"it would be injected and then fail every cache write. Wait for the deletion to finish "+
			"and create a new Binding, or point this Pod at one that is not terminating",
			KVCacheBindingAnnotationKey, name)
	}
	return binding, nil
}

// resolvePool 顺着 Binding 查找其所属的池。池是集群作用域的资源，因此引用中不携带 Namespace，也无需检查 Namespace。
func (r *PodKVCacheWebhook) resolvePool(
	ctx context.Context, binding *workercore.KVCachePoolBinding,
) (*workercore.KVCachePool, error) {
	pool := &workercore.KVCachePool{}
	key := ctrlcli.ObjectKey{Name: binding.Spec.PoolRef.Name}
	if err := r.get(ctx, key, pool); err != nil {
		if kerrors.IsNotFound(err) {
			return nil, fmt.Errorf("KVCachePoolBinding %q in namespace %q references pool %q, which "+
				"does not exist", binding.Name, binding.Namespace, key.Name)
		}
		return nil, fmt.Errorf("get kv cache pool %q: %w", key.Name, err)
	}
	return pool, nil
}

// resolveBackend 顺着池查找提供传输能力的后端。
//
// 池的 Webhook 只允许恰好一个后端名称，因此第一项即目标后端。此处仍会处理空列表的情况：否则未经该 Webhook
// 准入的对象会在此处 panic，而变更型（mutating）Webhook 并不是暴露此类问题的合适位置。
func (r *PodKVCacheWebhook) resolveBackend(
	ctx context.Context, pool *workercore.KVCachePool,
) (*workercore.KVCacheBackend, error) {
	if len(pool.Spec.Backends) == 0 {
		return nil, fmt.Errorf("pool %q names no backend, so its transport cannot be resolved",
			pool.Name)
	}

	backend := &workercore.KVCacheBackend{}
	key := ctrlcli.ObjectKey{Name: pool.Spec.Backends[0]}
	if err := r.get(ctx, key, backend); err != nil {
		if kerrors.IsNotFound(err) {
			return nil, fmt.Errorf("pool %q references backend %q, which does not exist",
				pool.Name, key.Name)
		}
		return nil, fmt.Errorf("get kv cache backend %q: %w", key.Name, err)
	}
	return backend, nil
}

// checkQuotaLedger 实现 F4b 检查：如果池的 master 无法对其租户账本做出响应，则阻止新 Pod 加入该池；而如果池
// 的 master 被声明或观察到不持有任何账本，则在不携带租户标识的情况下加入。
//
// 两种 False 响应被分开处理，因为它们需要采取相反的动作。MultiTenancyDisabled 是一项配置事实 —— 即单租户
// 拓扑，只要不转发租户，master 就能如实提供服务 —— 因此 Pod 继续放行，渲染器会丢弃该域。其他所有 False
// 都意味着本应持有账本的 master 未能响应：此时若继续注入，就会把一次 master 故障变成静默且无隔离的写入，因此
// 必须保持拒绝。Absent 和 Unknown 代表等待状态，将其中任一状态视为“已开启”都等同于把表示未知的声明当作肯定答复。
//
// MultiTenancyDisabled 分支选择放行标记而非拒绝，是因为该关卡过去防范的退化场景如今已在更早阶段被拒绝：在已
// 被认领的后端下关闭多租户在后端准入阶段即被禁止，因此在这里遇到的无账本池是运维人员明确声明的拓扑，而非悄悄
// 渗入的故障。
func (r *PodKVCacheWebhook) checkQuotaLedger(pool *workercore.KVCachePool) error {
	condition := workerctrl.KVCachePoolConditionQuotaLedgerAvailable

	switch {
	case !condition.Exists(pool):
		return fmt.Errorf("pool %q does not report %s yet, so whether its master holds a tenant "+
			"ledger is unknown; retry once the pool reports it",
			pool.Name, condition)
	case condition.IsUnknown(pool):
		return fmt.Errorf("pool %q reports %s as Unknown, which is a wait rather than an answer; "+
			"retry once it settles", pool.Name, condition)
	case condition.IsFalse(pool) &&
		condition.GetReason(pool) != workerctrl.KVCachePoolReasonMultiTenancyDisabled:
		// 直接透传控制器自身的错误消息，而非重新措辞。False 状态有多种原因，若断言“其 master 未持有租户账本”，可能会
		// 误导运维人员在实际上是集群故障期间去重新配置多租户 —— 指出错误原因的提示比未指明原因的提示更糟糕。
		return fmt.Errorf("pool %q reports %s as False (%s): %s",
			pool.Name, condition, condition.GetReason(pool), condition.GetMessage(pool))
	}
	return nil
}

// get 读取单个对象，优先读取缓存，其次读取 API server。
//
// 第二次读取是有意采用强一致读取的。其全部目的就是区分尚未追平的 informer 与真正不存在的对象，且只有第二次读取
// 返回 NotFound 时才会导致拒绝 —— 因此陈旧的响应恰恰会引发该回退机制本要防止的结果。早期版本曾传入
// WithoutQuorum，它会设置 resourceVersion=0 并显式允许返回滞后数据：在 Pod 之前片刻创建的 Binding 仍可能
// 被读为不存在，导致该 Pod 被永久拒绝，因为准入永远不会被重试。
//
// 代价仅是在缓存未命中的路径上执行一次 quorum 读取，而这本就是极少数异常路径。
func (r *PodKVCacheWebhook) get(ctx context.Context, key ctrlcli.ObjectKey, obj ctrlcli.Object) error {
	err := r.Client.Get(ctx, key, obj)
	if err == nil || !kerrors.IsNotFound(err) {
		return err
	}
	return r.APIReader.Get(ctx, key, obj)
}

// acceptedAnnotations 包含 Pod 作者可以设置的所有 kvcache.gpustack.ai/ 键。刻意排除了 domain 键：
// 它有独立的拒绝逻辑和独立的拒绝原因。
var acceptedAnnotations = []string{
	KVCacheBindingAnnotationKey,
	KVCacheEngineAnnotationKey,
	KVCacheManufacturerAnnotationKey,
	KVCacheRoleAnnotationKey,
	KVCacheContainerAnnotationKey,
	KVCacheLaunchArgsForwardedAnnotationKey,
}

// checkAnnotationVocabulary 拒绝此 Webhook 命名空间前缀下所有不被接受的键。
//
// 该检查主要针对拼写错误。静默忽略 "kvcache.gpustack.ai/bindng" 会导致 Pod 完全没有关联 Binding，而其表现
// 症状 —— 容器正常启动却没有任何缓存效果 —— 从外部很难察觉。此 Webhook 写入的两个键也按同一规则被拒绝，但原因
// 不同：它们是决策结果的记录，外部提交的值属于伪造值。
func checkAnnotationVocabulary(pod *core.Pod) error {
	prefix := "kvcache." + systemname.LabelPrefix
	for key := range pod.Annotations {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		switch {
		case slices.Contains(acceptedAnnotations, key), key == KVCacheDomainAnnotationKey:
			continue
		case key == inject.ClientConfigAnnotationKey, key == KVCacheInjectedAnnotationKey:
			return fmt.Errorf("annotation %q is written by this webhook and may not be supplied: it "+
				"records what was decided, and a submitted value would be a record of a decision "+
				"nobody made", key)
		default:
			return fmt.Errorf("annotation %q is not one this webhook accepts; the accepted keys are "+
				"%s. A key it ignored would leave the Pod configured differently from the manifest, "+
				"with nothing reporting it", key, strings.Join(acceptedAnnotations, ", "))
		}
	}
	return nil
}
