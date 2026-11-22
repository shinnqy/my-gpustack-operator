package worker

import (
	"context"
	"fmt"
	"slices"

	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrladmission "sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/utils/ctrlclix"
	"gpustack.ai/gpustack/pkg/webhook"
	"gpustack.ai/gpustack/pkg/worker/kvcache/mooncake"
)

// KVCachePoolWebhook 校验 v1alpha1.KVCachePool。
//
// 它仅做校验（validating），原因与 KVCacheBackendWebhook 相同：本 API 的所有默认值与枚举约束
// 均由 CRD schema 规则承担，而结构化 schema 校验在 validating 链路之前执行，因此不满足枚举约束的
// 取值根本不会进入本处理器。
//
// 留给本处理器校验的，是 schema 无法表达的约束：需要给出原因的条目数量限制、读取本 Pool 所引用
// 后端的校验，以及不可变性约束。
//
// nolint: lll
// +k8s:webhook-gen:validating:group="worker.gpustack.ai",version="v1alpha1",resource="kvcachepools",scope="Cluster"
// +k8s:webhook-gen:validating:operations=["CREATE","UPDATE"],failurePolicy="Fail",sideEffects="None",matchPolicy="Equivalent",timeoutSeconds=10
type KVCachePoolWebhook struct {
	Client    ctrlcli.Client
	APIReader ctrlcli.Reader
}

func (r *KVCachePoolWebhook) SetupWebhook(_ context.Context, opts webhook.SetupOptions) (runtime.Object, error) {
	r.Client = opts.Manager.GetClient()
	r.APIReader = opts.Manager.GetAPIReader()

	return &workercore.KVCachePool{}, nil
}

var (
	_ ctrladmission.Validator[runtime.Object] = (*KVCachePoolWebhook)(nil)
	_ webhook.ReceiveDeletionUpdate           = (*KVCachePoolWebhook)(nil)
)

// ReceiveDeletionUpdate 使本 Webhook 在 Pool 处于删除中状态时仍继续校验其更新。只要还有任何
// Binding 存在，其 finalizer 就会保持不移除，因此在 Pool 仍指向承载其租户配额的 master 的整个
// 期间，spec.backends 必须保持冻结。
//
// ValidateUpdate 触及的每一条规则都仅凭传入的新旧两个对象即可判定，这也正是跳过默认删除保护是安全
// 的原因：它读取的所有状态都不会凭空消失。仅 ValidateCreate 会读取后端对象，具体原因见
// ValidateUpdate 自身的注释。
func (r *KVCachePoolWebhook) ReceiveDeletionUpdate() {}

func (r *KVCachePoolWebhook) ValidateCreate(
	ctx context.Context, obj runtime.Object,
) (ctrladmission.Warnings, error) {
	kvcp := obj.(*workercore.KVCachePool)

	errs := validateKVCachePoolSpec(kvcp)
	// 仅当对象自身结构已经明确指定了唯一一个后端时才去读取它。否则读取空名称会返回“not found”，
	// 从而掩盖真正出错的数量校验规则。
	var warnings ctrladmission.Warnings
	if len(errs) == 0 {
		warnings, errs = r.validateKVCachePoolBackend(ctx, kvcp)
	}
	if len(errs) > 0 {
		return nil, kerrors.NewInvalid(workercore.Kind("KVCachePool"), kvcp.Name, errs)
	}

	return warnings, nil
}

func (r *KVCachePoolWebhook) ValidateUpdate(
	_ context.Context, oldObj, newObj runtime.Object,
) (ctrladmission.Warnings, error) {
	oldKvcp, newKvcp := oldObj.(*workercore.KVCachePool), newObj.(*workercore.KVCachePool)

	// 此处刻意不重新读取后端对象。spec.backends 是不可变的，因此更新操作不可能改变所引用的后端或
	// 本 Pool 对其的访问资格——但后端对象自身在此期间可能已被删除或修改，若在此处重新校验，会导致已
	// 准入的 Pool 因另一个对象的变化而无法更新。协调器移除本 Pool 的 finalizer 正属于此类更新，拒绝
	// 它会导致只要后端缺失，Pool 就会一直处于无法删除的卡死状态。
	//
	// 这并不会削弱 F5 的前置条件约束：协调器以 master 返回的 409 为准，并将其体现在 Condition 中，这种
	// 水平触发机制才是真正持续生效的保障。准入校验只是前置拦截，而非最终保证。
	errs := validateKVCachePoolSpec(newKvcp)
	errs = append(errs, validateKVCachePoolImmutable(oldKvcp, newKvcp)...)
	if len(errs) > 0 {
		return nil, kerrors.NewInvalid(workercore.Kind("KVCachePool"), newKvcp.Name, errs)
	}

	return nil, nil
}

func (r *KVCachePoolWebhook) ValidateDelete(
	_ context.Context, _ runtime.Object,
) (ctrladmission.Warnings, error) {
	// 当 status.usedBy 非空时，删除操作由协调器的 finalizer 拦截，而非在此处：本处理器只能看到当前
	// 对象本身，而该决策需要依据持有它的 Binding 状态。
	return nil, nil
}

// validateKVCachePoolSpec 包含所有仅凭对象本身即可判定的校验规则。
func validateKVCachePoolSpec(kvcp *workercore.KVCachePool) field.ErrorList {
	specPath := field.NewPath("spec")

	var errs field.ErrorList

	// 这是一个 schema 本可拦截的数量约束，但放在此处拒绝以便在错误消息中说明原因。该字段设计为列表
	// 形态是因为跨多个后端的 Pool 是自然的后续演进方向，而解决该问题需要依靠配额模型，而非单纯放宽
	// 数量上限。
	if len(kvcp.Spec.Backends) != 1 {
		errs = append(errs, field.Invalid(specPath.Child("backends"), kvcp.Spec.Backends,
			fmt.Sprintf("exactly 1 backend is supported, and %d were given: quota is enforced by one "+
				"master's per-tenant ledger, and no master can account for bytes held in another",
				len(kvcp.Spec.Backends))))
	}
	for i, name := range kvcp.Spec.Backends {
		if name == "" {
			errs = append(errs, field.Required(specPath.Child("backends").Index(i),
				"a backend is named by the name of a KVCacheBackend"))
		}
	}

	// resource.Quantity 在 schema 中表现为字符串，因此注解标记中的任何数值上下界都无法作用于它。
	// 此处直接复用策略文件自身的校验规则，即来自唯一负责渲染它的校验器：本 Pool 声明的上限约束着
	// 其下所有子配额上限，因此两者对哪些数值可用绝不能存在分歧。
	errs = append(errs, mooncake.ValidateQuotaPolicyQuota(
		kvcp.Spec.Quota.Total, specPath.Child("quota", "total"),
	)...)

	return errs
}

// validateKVCachePoolBackend 拒绝指向无法提供服务的后端的 Pool，并对未启用租户账本的后端
// 发出警告。
//
// 引用尚未存在的后端属于需要修正的引用错误，因此保持拒绝。未声明多租户的托管后端属于单租户拓扑
// 而非故障：所有请求都会落入默认租户，且无法写入按租户划分的配额，这也正是警告所提示的内容——同时
// Binding Webhook 会保证此类 master 仅对应一个复用域，因此本 operator 曾经为此拒绝 Pool 的
// 冲突场景已不存在第二个可与之冲突的复用域。
func (r *KVCachePoolWebhook) validateKVCachePoolBackend(
	ctx context.Context, kvcp *workercore.KVCachePool,
) (ctrladmission.Warnings, field.ErrorList) {
	backendPath := field.NewPath("spec", "backends").Index(0)

	kvcb := &workercore.KVCacheBackend{}
	key := ctrlcli.ObjectKey{Name: kvcp.Spec.Backends[0]}
	if err := r.Client.Get(ctx, key, kvcb); err != nil {
		if !kerrors.IsNotFound(err) {
			return nil, field.ErrorList{field.InternalError(backendPath,
				fmt.Errorf("get kv cache backend: %w", err))}
		}
		// 缓存中可能只是尚未同步到该对象——在创建后端的同时创建 Pool 是常见场景——因此在未命中时会向
		// API server 再次查询确认，而后才决定是否拒绝。
		//
		// 仅当该次读取返回 NotFound 时才予以拒绝。超时、RBAC 权限拒绝或 5xx 错误均不能说明后端是否
		// 存在，且本 Webhook 以 failurePolicy Fail 运行：若将这类错误报告为“backend not found”，会把
		// 集群故障误报成提交对象中的拼写错误，而那恰恰是提交者最不会去排查的地方。
		if err = r.APIReader.Get(ctx, key, kvcb, ctrlclix.WithoutQuorum); err != nil {
			if !kerrors.IsNotFound(err) {
				return nil, field.ErrorList{field.InternalError(backendPath,
					fmt.Errorf("get kv cache backend: %w", err))}
			}
			return nil, field.ErrorList{field.NotFound(backendPath, key.Name)}
		}
	}

	// 准入阶段仅能知晓托管后端的账本状态：本 operator 会将该标志渲染到 leader 的命令行参数中。外部
	// 后端运行的是第三方管理的 master，其启动参数无法在此处得知，只能在具备条件的地方判定——由协调器
	// 读取 master 返回的 409，以及由 Pod Webhook 在 Pool 上报就绪前拒绝注入。
	managed := kvcb.Spec.Connection.Managed
	if managed == nil || managed.Leader.MultiTenancyEnabled() {
		return nil, nil
	}

	return ctrladmission.Warnings{fmt.Sprintf(
		"backend %q runs without multi-tenancy: it holds no tenant ledger, so no per-tenant quota "+
			"is in force — every write falls into the default tenant, bounded only by the store's "+
			"global capacity — and the master serves exactly one reuse domain",
		key.Name,
	)}, nil
}

// validateKVCachePoolImmutable 冻结 Pool 整个身份所依赖的后端引用。
//
// 修改 Pool 指向的后端会使之前写入旧 master 账本的所有租户配额遗留在原地，而此处再也没有对象
// 指向该 master 来清理它们——各复用域的预热缓存也会随之滞留，与此同时 Pool 却会针对一个空无一物
// 的新后端上报自身为 Ready。
func validateKVCachePoolImmutable(oldKvcp, newKvcp *workercore.KVCachePool) field.ErrorList {
	if slices.Equal(oldKvcp.Spec.Backends, newKvcp.Spec.Backends) {
		return nil
	}

	return field.ErrorList{field.Forbidden(field.NewPath("spec", "backends"),
		"backends is immutable: moving a pool to another backend strands every tenant quota it "+
			"wrote on the old master's ledger, with nothing left to delete them with")}
}
