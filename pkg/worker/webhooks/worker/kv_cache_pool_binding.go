package worker

import (
	"context"
	"fmt"
	"slices"
	"strings"

	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrladmission "sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/utils/ctrlclix"
	"gpustack.ai/gpustack/pkg/webhook"
	workerctrl "gpustack.ai/gpustack/pkg/worker/controllers/worker"
	"gpustack.ai/gpustack/pkg/worker/kvcache/mooncake"
)

// KVCachePoolBindingWebhook 校验 v1alpha1.KVCachePoolBinding。
//
// 它仅做校验（validating），承担 schema 无法表达的约束：名称格式规则、读取关联对象以确认本
// Binding 可申请的上限、按 master 维度的复用域唯一性检查，以及防止预热缓存在底层被重新解释的
// 不可变性约束。
//
// nolint: lll
// +k8s:webhook-gen:validating:group="worker.gpustack.ai",version="v1alpha1",resource="kvcachepoolbindings",scope="Namespaced"
// +k8s:webhook-gen:validating:operations=["CREATE","UPDATE"],failurePolicy="Fail",sideEffects="None",matchPolicy="Equivalent",timeoutSeconds=10
type KVCachePoolBindingWebhook struct {
	Client    ctrlcli.Client
	APIReader ctrlcli.Reader
}

func (r *KVCachePoolBindingWebhook) SetupWebhook(
	_ context.Context, opts webhook.SetupOptions,
) (runtime.Object, error) {
	r.Client = opts.Manager.GetClient()
	r.APIReader = opts.Manager.GetAPIReader()

	return &workercore.KVCachePoolBinding{}, nil
}

var (
	_ ctrladmission.Validator[runtime.Object] = (*KVCachePoolBindingWebhook)(nil)
	_ webhook.ReceiveDeletionUpdate           = (*KVCachePoolBindingWebhook)(nil)
)

// ReceiveDeletionUpdate 使本 Webhook 在 Binding 处于删除中状态时仍继续校验其更新。
//
// 不可变冻结恰恰在那个时间窗口内最有价值。只要 status.usedBy 非空，finalizer 就会一直保留，因此
// 只要还有工作负载在使用它，Binding 就会持续存在，而 Pool 在此期间也始终将本对象的 spec.domain
// 作为权威注册条目对外发布。若在该窗口跳过校验，修改 blockSize 或 dtype 的操作就会被放行，进而
// 被当作按旧格式写入的缓存块的真实属性上报。
//
// 禁止：向 ValidateUpdate 添加无条件读取其他对象的规则。此处选择退出的默认保护机制，其存在的
// 意义在于：依赖可能已消失状态的更新校验绝不能拒绝清理 finalizer 的更新，而在 Binding 之前删除
// Pool 正是常见的清理顺序。本处唯一的一处跨对象读取仅在配额上限发生变动时才触发，而释放本对象的
// 更新不会修改配额上限。
func (r *KVCachePoolBindingWebhook) ReceiveDeletionUpdate() {}

func (r *KVCachePoolBindingWebhook) ValidateCreate(
	ctx context.Context, obj runtime.Object,
) (ctrladmission.Warnings, error) {
	kvcpb := obj.(*workercore.KVCachePoolBinding)

	var warnings ctrladmission.Warnings

	errs := validateKVCachePoolBindingSpec(kvcpb)
	errs = append(errs, validateKVCachePoolBindingDtypeBinds(
		kvcpb, workerctrl.ModelDeploymentOwnsKVCacheDtype(ctx),
	)...)
	// 仅当对象自身结构合法后才执行跨对象校验。否则格式非法的 domain 名称会同时伴随“没有其他 Binding
	// 占用它”的结论一起上报，这虽然符合事实却毫无意义。
	if len(errs) == 0 {
		errs = append(errs, r.validateKVCachePoolBindingDomainIsUnclaimed(ctx, kvcpb)...)
		errs = append(errs, r.validateKVCachePoolBindingCeilingFitsPool(ctx, kvcpb)...)

		separableWarnings, separableErrs := r.validateKVCachePoolBindingDomainIsSeparable(ctx, kvcpb)
		warnings, errs = separableWarnings, append(errs, separableErrs...)
	}
	if len(errs) > 0 {
		// 在拒绝请求时刻意丢弃警告信息。警告描述的是已准入对象会遗留的状态，而对象被拒的调用方根本不会
		// 创建出这样的对象。
		return nil, kerrors.NewInvalid(workercore.Kind("KVCachePoolBinding"), kvcpb.Name, errs)
	}

	return warnings, nil
}

func (r *KVCachePoolBindingWebhook) ValidateUpdate(
	ctx context.Context, oldObj, newObj runtime.Object,
) (ctrladmission.Warnings, error) {
	oldKvcpb, newKvcpb := oldObj.(*workercore.KVCachePoolBinding), newObj.(*workercore.KVCachePoolBinding)

	errs := validateKVCachePoolBindingSpec(newKvcpb)
	errs = append(errs, validateKVCachePoolBindingImmutable(oldKvcpb, newKvcpb)...)

	// 仅当本次更新改动了配额上限时才重新读取 Pool。spec.poolRef 是不可变的，因此 Pool 的身份不可能
	// 改变——但其总配额可能已经变化，若在每次更新时都去查询 Pool，就会让另一个对象挡在移除本 Binding
	// finalizer 的路径上。在 Binding 之前删除 Pool 是正常的清理顺序，而此时若无法更新 Binding，就会
	// 导致 Binding 无法被删除。
	//
	// 因此，保持未变的上限不会针对已经缩容的 Pool 总配额重新校验。这是刻意的设计而非漏洞：协调器会
	// 观测 master 实际授予的有效配额，超出 Pool 可分配范围的申请已在协调器中以配额不足的形式上报。
	// 准入校验负责拒绝绝对无法工作的配置；而原本合法但后来不再匹配的上限则属于需要上报的状态。
	if len(errs) == 0 && ceilingMoved(oldKvcpb, newKvcpb) {
		errs = append(errs, r.validateKVCachePoolBindingCeilingFitsPool(ctx, newKvcpb)...)
	}
	if len(errs) > 0 {
		return nil, kerrors.NewInvalid(workercore.Kind("KVCachePoolBinding"), newKvcpb.Name, errs)
	}

	return nil, nil
}

func (r *KVCachePoolBindingWebhook) ValidateDelete(
	_ context.Context, _ runtime.Object,
) (ctrladmission.Warnings, error) {
	// 当 status.usedBy 非空时，删除操作由协调器的 finalizer 拦截，且在账本条目移除前会先排空该租户
	// 自身的对象：本处理器既看不到也不处理这两者。
	return nil, nil
}

// validateKVCachePoolBindingSpec 包含所有仅凭对象本身即可判定的校验规则。
func validateKVCachePoolBindingSpec(kvcpb *workercore.KVCachePoolBinding) field.ErrorList {
	specPath := field.NewPath("spec")

	var errs field.ErrorList

	if kvcpb.Spec.PoolRef.Name == "" {
		errs = append(errs, field.Required(specPath.Child("poolRef", "name"),
			"a Binding grants exactly one pool, named here"))
	}

	errs = append(errs, validateKVCachePoolBindingDomain(
		&kvcpb.Spec.Domain, specPath.Child("domain"),
	)...)

	// 此处仅校验格式。它是否超出 Pool 容量属于跨对象问题，会在可读取 Pool 的位置进行校验。
	// 此处无条件执行校验：schema 已确保该键存在，因此剩下的只需判断其值是否合法，而判断依据正是
	// master 自身的规则。
	errs = append(errs, mooncake.ValidateQuotaPolicyQuota(
		kvcpb.Spec.Quota.Ceiling, specPath.Child("quota", "ceiling"),
	)...)

	return errs
}

// validateKVCachePoolBindingDomain 校验该 Binding 注册的复用标识。
//
// 名称需同时满足两条规则，这种冗余是有意为之。DNS-1123 标签是该 API 接受的格式，它恰好也
// 满足 master 的所有规则——但最终决定渲染出的策略文件能否加载的是 master 的规则，因此
// 直接向声明这些规则的唯一出处进行校验，而非假定其已被覆盖。
func validateKVCachePoolBindingDomain(
	domain *workercore.KVCachePoolBindingDomain, fldPath *field.Path,
) field.ErrorList {
	namePath := fldPath.Child("name")

	errs := mooncake.ValidateQuotaPolicyTenantName(domain.Name, namePath)
	if domain.Name != "" {
		if msgs := validation.IsDNS1123Label(domain.Name); len(msgs) > 0 {
			errs = append(errs, field.Invalid(namePath, domain.Name, fmt.Sprintf(
				"a reuse domain is named like a Kubernetes object, so nobody learns a second naming "+
					"rule, and it travels to the storage layer as the tenant id verbatim: %s",
				strings.Join(msgs, "; "),
			)))
		}
	}

	// 不容纳任何 token 的块并不是更小的块，而是一种无法写入任何内容的缓存形态。
	if domain.BlockSize <= 0 {
		errs = append(errs, field.Invalid(fldPath.Child("blockSize"), domain.BlockSize,
			"must be greater than 0: it is the number of tokens one cache block holds"))
	}

	// 仅校验语法形式。完整枚举集合属于承载工作负载的规范，若在此处枚举，每次新增引擎
	// dtype 都会导致该 API 组发生变更。
	switch dtype := domain.Dtype; {
	case dtype == "":
		errs = append(errs, field.Required(fldPath.Child("dtype"),
			"the element type the cached tensors carry, in the engine's own spelling"))
	case dtype != strings.ToLower(dtype):
		errs = append(errs, field.Invalid(fldPath.Child("dtype"), dtype,
			"must be lowercase: engines spell it that way, and this value is compared rather than "+
				"interpreted, so two spellings of one type would read as two types"))
	}

	return errs
}

// validateKVCachePoolBindingDtypeBinds 拒绝在 operator 将 dtype 透传给每个关联引擎时声明
// 为 "auto" 的新建 Binding。
//
// "auto" 是两个引擎都接受却不绑定具体类型的值：各自会根据自身模型与 checkpoint 解析，因此
// 同一 domain 上的两个引擎仍可能写入两种元素类型。该检查仅在创建时执行，因为 dtype 不可变，
// 而已存储该值的 Binding 必须保持可更新（直至清除其 finalizer 的那次更新）。
func validateKVCachePoolBindingDtypeBinds(
	kvcpb *workercore.KVCachePoolBinding, owned bool,
) field.ErrorList {
	if !owned || kvcpb.Spec.Domain.Dtype != kvCachePoolBindingDtypeAuto {
		return nil
	}

	return field.ErrorList{field.Invalid(field.NewPath("spec", "domain", "dtype"), kvcpb.Spec.Domain.Dtype,
		"is handed to every engine attached through this Binding, and \"auto\" lets each engine "+
			"resolve its own from the model it loads, so two engines on one domain can still write "+
			"two element types; name the type, for example bfloat16")}
}

// kvCachePoolBindingDtypeAuto 是两个引擎均会自行解析的 dtype 写法。
const kvCachePoolBindingDtypeAuto = "auto"

// validateKVCachePoolBindingDomainIsUnclaimed 拒绝已被另一 Binding 在同时服务当前 Binding
// 的 master 上注册过的 domain。
//
// 同一 domain 上的两个 Binding 若由同一个 master 服务，会共享缓存（这或许正是某些场景所期望的），
// 但也会在同一个配额账本上发生冲突（这绝非预期）：master 对每个 tenant 仅保留一条记录，
// 因此两个上限会变成后者覆盖前者，每个 namespace 看到的都会是对方设置的配额。
//
// 因此唯一性是按 master 划分的——范围比按 pool 更大（因为一个 master 可服务多个 pool，
// 且 tenant 空间在 master 内全局共享），又比集群范围更窄（因为两个 master 各自持有独立的账本与
// 键空间）。此前按集群范围检查时曾暴露出问题（#166）：在两个独立后端上，只有一个能注册
// 无 tenant 引擎默认写入的 "default" domain，另一个后端中被注入的 Pod 在准入后每次写入都会因
// TENANT_NOT_REGISTERED 而失败。“一个 master 可服务多个 pool”仅说明这些 pool 之间需要唯一性，
// 并不意味着不同 master 上的 pool 之间也需要唯一性；因此该检查现在会读取两个 pool 的后端，
// 仅当存在共享后端时才拒绝。
//
// 拒绝消息中会列出共享的后端，这是关键信息而非冗余修饰：在发现名称冲突之前，检查逻辑
// 不会读取两个 Binding 的 pool，因此该消息是运维人员得知究竟哪个后端导致两者冲突的唯一途径。
//
// 消息中表述的是 pool“声明（name）了该后端”而非“由该后端服务（served by）”，这一措辞差异
// 反映的是检查实际读取到的信息。声明了多个后端的 pool 实际上不受其中任何一个服务（对应调谐器的
// BackendNotSingular 拒绝条件），因此在此情形下若写“由其服务”，就断言了交集运算并未证实的事实。
// 拒绝消息所陈述的正是实际计算出的结论，其后果也保留了前提条件：当由同一个 master 服务时，两者冲突。
//
// 措辞也与共享后端的数量无关，不随单复数变化。共享多个后端的情况是可达的，因为
// BackendNotSingular 仅报告在 status 中而非禁止写入 spec，准入阶段确实会遇到此类 pool。
// 若用单数主语则不合语法，而增加复数分支又会引入一条没有测试能覆盖验证的第二代码路径。
//
// 该检查存在竞态：基于同一份缓存状态准入的两个创建请求都会通过。这正是调谐期拒绝逻辑（F9）
// 存在的意义，也说明本检查的职责是提供清晰的提示信息，而非保证该不变量。
func (r *KVCachePoolBindingWebhook) validateKVCachePoolBindingDomainIsUnclaimed(
	ctx context.Context, kvcpb *workercore.KVCachePoolBinding,
) field.ErrorList {
	namePath := field.NewPath("spec", "domain", "name")

	var holder *workercore.KVCachePoolBinding
	var shared []string

	// 命中第一个匹配项即终止：只要在共享 master 上已有任意 Binding 占用该名称，就已推翻
	// 唯一性，继续查找第二个也不会改变判定结果或后续动作。
	err := r.forEachSharedMasterClaimant(ctx, kvcpb,
		func(domain string) bool { return domain == kvcpb.Spec.Domain.Name },
		func(h *workercore.KVCachePoolBinding, s []string) (bool, error) {
			holder, shared = h, s
			return true, nil
		})
	if err != nil {
		return field.ErrorList{field.InternalError(namePath, err)}
	}
	if holder == nil {
		return nil
	}

	return field.ErrorList{field.Duplicate(namePath, fmt.Sprintf(
		"reuse domain %q is already registered by %s/%s, and both Bindings' pools name %s: "+
			"served by one master, the two would share cache and overwrite each other's "+
			"ceiling in the single ledger entry it keeps per tenant. Two masters hold two "+
			"ledgers, so a "+
			"Binding claiming this domain against a pool on another backend is fine — register "+
			"a domain no other Binding on a shared master holds. That does not permit a second "+
			"\"default\" domain, which is also what an omitted spec.domain.name is stored as: an "+
			"older engine build may ignore the injected tenant and write under that literal name, "+
			"but the master still keeps only one ledger entry for it",
		kvcpb.Spec.Domain.Name, holder.Namespace, holder.Name,
		strings.Join(shared, " and "),
	))}
}

// validateKVCachePoolBindingDomainIsSeparable 判断一个与本 Binding 所属 pool 共享同一
// master 上已注册 domain 不同的复用 domain 是否可被隔离。
//
// 上面的兄弟检查拒绝两个使用同一名称的 Binding。本检查面对相反的情形，需要不同的判定逻辑：
// 两个名称各不相同，且各自都合法占用，问题在于下游组件能否将它们相互隔离。注册第二个 domain
// 意味着作出隔离承诺，而兑现该承诺依赖两个相互独立的守门者。
//
// 存储层是本检查能够判定的一半，因此由这一半触发拒绝。未启用 tenant 账本运行的 master 会将
// 所有请求归入同一个默认 tenant，并将分片索引退化为普通键哈希，导致使用不同 tenant 名称的
// 两个调用方互相读取和覆盖对方的块，而两个对象却都显示已成功注册。任何引擎都无法挽救这种
// 情况：即使客户端转发了自己的身份标识，master 也无处存放。这类 master 上的第一个 domain
// 可以正常准入并工作，真正引入风险的是第二个 domain——这正是拒绝发生在此处而非后续使用方的原因。
//
// 引擎层是本检查无法判定的一半，因此这一半仅发出明确说明的告警。工作负载的写入是否携带
// domain 取决于其容器内的推理引擎，而本 Webhook 能读取的任何对象中都不包含“哪个引擎将使用
// 该 pool”的信息：该信息以 annotation 的形式出现在尚未创建的 Pod 上。依据无人知晓的事实
// 进行拒绝，会误拒所有第二个 domain（包括引擎实际能够隔离的情况）；而改为拒绝 Pod，又会将
// 故障转嫁给并未创建 domain 的工作负载作者。因此，系统在 Binding 作者创建第二个 domain 的
// 那一刻告知其实际保障范围与局限，同时每个被注入的 Pod 也会携带标记说明其自身是否做到了隔离。
//
// 仅在创建时校验，这是由结构决定的而非偏好选择。spec.poolRef 与 spec.domain.name 均已由
// validateKVCachePoolBindingImmutable 冻结，因此更新操作不可能产生创建时不存在的 domain 与
// master 组合。若在更新路径复制该规则，只会使原本无关的 quota.ceiling 修改因本检查上线前就
// 已准入的冲突而失败，这反而成了隐患而非修复。已存在于无账本 master 上的 domain 维持现状；
// 准入只拦截尚未发生的情况。
func (r *KVCachePoolBindingWebhook) validateKVCachePoolBindingDomainIsSeparable(
	ctx context.Context, kvcpb *workercore.KVCachePoolBinding,
) (ctrladmission.Warnings, field.ErrorList) {
	namePath := field.NewPath("spec", "domain", "name")

	var first *workercore.KVCachePoolBinding
	var firstShared []string
	var refusal field.ErrorList

	// 必须检查每一个相邻 Binding，而不仅是找到的第一个。本规则要回答的问题是：两个 Binding
	// 共享的所有 master 中是否存在任意一个无法隔离；第一个邻居的共享 master 正常，并不能说明
	// 共享另一个 master 的第二个邻居也正常。上面的兄弟检查可以在首个匹配处停止，因为任一重复项
	// 就足以推翻唯一性；这两个问题的结构不同，若共用提前返回逻辑，恰恰会放过本检查旨在拦截的情形。
	err := r.forEachSharedMasterClaimant(ctx, kvcpb,
		func(domain string) bool { return domain != kvcpb.Spec.Domain.Name },
		func(holder *workercore.KVCachePoolBinding, shared []string) (bool, error) {
			if first == nil {
				first, firstShared = holder, shared
			}

			for _, backend := range shared {
				separates, err := r.masterSeparatesDomains(
					ctx, backend, kvcpb.Spec.PoolRef.Name, holder.Spec.PoolRef.Name,
				)
				if err != nil {
					return true, err
				}
				if separates {
					continue
				}

				refusal = field.ErrorList{field.Invalid(namePath, kvcpb.Spec.Domain.Name, fmt.Sprintf(
					"%s/%s already registers reuse domain %q on %s, and that backend holds no tenant "+
						"ledger: every request falls into one default tenant and the shard index "+
						"degrades to a plain key hash, so the two domains would read and overwrite "+
						"each other's blocks while both Bindings reported themselves registered. "+
						"Such a master serves one reuse domain and no more, whatever the engines do "+
						"— an engine that forwards its identity forwards it to a master with nowhere "+
						"to put it. Give that backend a ledger, by setting "+
						"spec.connection.managed.leader.multiTenancy on a managed one or by starting "+
						"an external one's master with multi-tenancy on, or point this Binding at a "+
						"pool on another backend",
					holder.Namespace, holder.Name, holder.Spec.Domain.Name, backend,
				))}
				return true, nil
			}

			return false, nil
		})
	if err != nil {
		return nil, field.ErrorList{field.InternalError(namePath, err)}
	}
	if len(refusal) > 0 {
		return nil, refusal
	}
	if first == nil {
		return nil, nil
	}

	// 允许准入，而告警指出了准入校验未能覆盖的部分。告警会点名具体的另一个 domain 而非泛泛而谈，
	// 因为“存在其他 domain”无法指导运维人员采取行动，而“team-b/batch 持有 beta”可以。
	//
	// 该告警不对存储层作任何断言，这是为了准确性而非图省事。走到此处放行仅意味着没有发现任何
	// 被观测到或声明为无账本的 master，其中也包含尚未被采集过的外部 master——因此若在运维人员
	// 此时最关注的告警信息中声称 master 会将两者隔开，就等于断言了降级放行路径并未证实的事情。
	return ctrladmission.Warnings{fmt.Sprintf(
		"reuse domain %q stands beside %q, which %s/%s registers on %s. Keeping two domains apart "+
			"needs both a master holding a tenant ledger and an engine that forwards the identity, "+
			"and neither is established here: an engine that forwards none writes under the store's "+
			"own \"default\" name, so its blocks and its usage land on whichever Binding registered "+
			"that name rather than on this one. Each injected Pod carries a stamp recording which "+
			"happened for it",
		kvcpb.Spec.Domain.Name, first.Spec.Domain.Name, first.Namespace, first.Name,
		strings.Join(firstShared, " and "),
	)}, nil
}

// masterSeparatesDomains 报告一个后端背后的 master 是否能够隔离两个复用 domain，读取优先级为
// 先读观测状态、再读声明配置。
//
// pool 的 QuotaLedgerAvailable 状态条件是控制器针对运行中的 master 实测得到的，因此可用于
// 判定非本 operator 启动的后端。此处仅采信代表配置事实的那个 reason：False 状态也可能是
// LedgerUnreachable（表示 master 当前不可达），因瞬时故障而拒绝对象会误导运维人员去修改本就
// 正确的配置。除该特定 reason 外的所有情况均继续向后回落。
//
// 会查询声明了该 master 的每一个 pool，而不只是当前 Binding 指向的那个。一个后端可能被
// 多个 pool 声明，而观测状态位于控制器已经调谐过的任意一个 pool 上——若仅读取当前 Binding
// 的 pool，就会纯粹因为新对象恰好指向了另一个 pool 而漏掉已有的判定结论。Binding 与其 pool
// 同步创建是常见情形，而那个新 pool 此时尚未写入任何 condition。
//
// 对于尚未有任何 pool 收敛的托管后端，则由声明配置给出答案，且在此场景下它是精确的：本
// operator 会将该标志直接渲染到 leader 自身的命令行参数中。
//
// 对象不存在时按“可隔离”作答，即选择断言更弱的一侧。尚未被采集的外部后端、pool 声明但无人
// 创建的后端、已不存在的 pool——这些都不能作为两个 domain 会混用的证据；若基于缺失读数而拒绝，
// 就会仅仅因为控制器尚未执行一轮调谐而拒掉正确的对象。该路径上的准入告警措辞也经过斟酌，不会
// 断言回落逻辑未能证实的结论。
//
// 对象无法读取则不属于上述情况，尽管两者最终都没有拿到数据而容易被混为一谈。只有 NotFound
// 代表对象不存在；超时、RBAC 拒绝或 5xx 错误无法说明对象是否存在，因此会将错误向上传播并使
// 创建请求以 InternalError 失败——这与本 Webhook 中所有其他跨对象读取的响应一致，也是其
// failurePolicy 为 Fail 所固有的语义。
func (r *KVCachePoolBindingWebhook) masterSeparatesDomains(
	ctx context.Context, backend string, pools ...string,
) (bool, error) {
	condition := workerctrl.KVCachePoolConditionQuotaLedgerAvailable
	seen := make(map[string]bool, len(pools))
	for _, pool := range pools {
		if pool == "" || seen[pool] {
			continue
		}
		seen[pool] = true

		kvcp, err := r.poolOrNil(ctx, pool)
		if err != nil {
			return false, err
		}
		// 此处不对 pool 的后端列表做成员检查，因为调用方已使其成为多余：backend 来自这两个 pool
		// 列表的交集，按构造逻辑两者必然都声明了它。在此检查只会形成任何输入都无法命中的死分支。
		if kvcp == nil {
			continue
		}
		if condition.IsFalse(kvcp) &&
			condition.GetReason(kvcp) == workerctrl.KVCachePoolReasonMultiTenancyDisabled {
			return false, nil
		}
	}

	kvcb := &workercore.KVCacheBackend{}
	key := ctrlcli.ObjectKey{Name: backend}
	if err := r.Client.Get(ctx, key, kvcb); err != nil {
		if !kerrors.IsNotFound(err) {
			return false, fmt.Errorf("get kv cache backend %q: %w", backend, err)
		}
		// 后端与引用它的对象同步创建十分常见，因此在缓存未命中时会先向 API server 重新查询，再做
		// 后续判定。
		if err = r.APIReader.Get(ctx, key, kvcb); err != nil {
			if !kerrors.IsNotFound(err) {
				return false, fmt.Errorf("get kv cache backend %q: %w", backend, err)
			}
			return true, nil
		}
	}
	if managed := kvcb.Spec.Connection.Managed; managed != nil {
		return managed.Leader.MultiTenancyEnabled(), nil
	}

	return true, nil
}

// forEachSharedMasterClaimant 遍历复用 domain 满足 matchDomain、且所属 pool 由当前 Binding
// 的 pool 也声明了的同一 master 所服务的其他 Binding，将每个对象连同两个 pool 共享的 master
// 列表一并交给 visit 处理。visit 返回 true 时停止遍历。
//
// 这一次扫描同时服务于对同一组邻居提出相反问题的两条规则——domain 是否已被占用，以及是否有
// 另一个不同的 domain 并存于一个无法区分二者的 master 上。若复制第二份遍历代码，就必须持续
// 保持双方对“哪些 Binding 属于邻居”的判定一致，否则两条规则很容易在放行方向上发生漂移，而
// 错误放行是不会产生任何报错的。
//
// 二者的差异在于何时停止，因此停止条件交由调用方而非本函数决定：发现一个重复项即可推翻
// 唯一性，而“每个共享 master 是否都能隔离这些 domain”必须询问全部对象才能回答。若共享同一个
// 提前返回条件，对一条规则是正确的，对另一条则会静默出错。
//
// 该遍历与唯一性规则一样存在竞态：列表读取自 informer 缓存，因此基于同一缓存状态准入的
// 两个创建请求都会通过，且仅当缓存返回错误而非仅仅存在延迟时才会回退查询 API server。对于
// 唯一性规则，调谐器的冲突 domain 检查阶段可作为兜底；而隔离规则没有此类兜底——同时准入的一对
// 对象会保持已准入状态——因此这只是一次附带清晰提示信息的早期拦截，而非强不变量；在此说明
// 是为了避免后续读者将其误认为不变量。
func (r *KVCachePoolBindingWebhook) forEachSharedMasterClaimant(
	ctx context.Context, kvcpb *workercore.KVCachePoolBinding, matchDomain func(domain string) bool,
	visit func(holder *workercore.KVCachePoolBinding, shared []string) (stop bool, err error),
) error {
	list := &workercore.KVCachePoolBindingList{}
	if err := r.Client.List(ctx, list); err != nil {
		if err = r.APIReader.List(ctx, list, ctrlclix.WithoutQuorum); err != nil {
			return fmt.Errorf("list kv cache pool bindings: %w", err)
		}
	}

	var ownBackends []string
	ownBackendsRead := false

	for i := range list.Items {
		holder := &list.Items[i]
		if !matchDomain(holder.Spec.Domain.Name) {
			continue
		}
		// 这是当前正在接受准入检查的对象本身，在更新重入准入时从缓存中读取到。
		if holder.Namespace == kvcpb.Namespace && holder.Name == kvcpb.Name {
			continue
		}

		shared, err := r.poolsSharedMasters(ctx, kvcpb.Spec.PoolRef.Name, holder.Spec.PoolRef.Name,
			&ownBackends, &ownBackendsRead)
		if err != nil {
			return err
		}
		if len(shared) == 0 {
			continue
		}

		stop, err := visit(holder, shared)
		if err != nil {
			return err
		}
		if stop {
			return nil
		}
	}

	return nil
}

// poolsSharedMasters 返回同时服务两个 Binding 所指向的 pool 的后端列表，这也是它们对同一
// domain 的声明发生冲突的唯一配置：一个 master 为每个 tenant 维护一条账本记录，因此同一
// master 上同一 domain 的两个 Binding 会互相覆盖配额上限，而两个 master 各自持有独立账本，
// 互不冲突。pool 的 master 即其 spec.backends，因此结果取交集；声明了多个后端的 pool 实际
// 不受其中任何一个服务（对应调谐器的 BackendNotSingular 拒绝条件），但取交集依然是安全的选择——
// 准入阶段宁可拒绝，也不对兄弟 Webhook 已经在报错的配置形态做额外推断。
//
// 没有 master 的 pool 视作不共享任何后端，涵盖无法读取的 pool 以及未声明后端（包括显式为空列表）
// 的 pool——poolBackends 会将它们统一归一化为 nil。对于当前 Binding 自身的 pool，找不到 pool
// 的拒绝将在紧随其后的上限检查中给出，在此之前基于缺失 pool 算出的 domain 结论只是干扰噪声；
// 对于既有持有者而言，pool 消失意味着没有 master 服务该持有者，因此其声明不会与任何对象冲突——
// 这与调谐器按 master 计算冲突集合的语义一致。ownBackends 在遍历持有者期间缓存当前 Binding
// 自身的 pool，因此集群中同一 domain 上存在 N 个 Binding 时仅需 N+1 次 pool 读取，而非 2N 次。
func (r *KVCachePoolBindingWebhook) poolsSharedMasters(
	ctx context.Context, ownPool, holderPool string,
	ownBackends *[]string, ownBackendsRead *bool,
) ([]string, error) {
	if !*ownBackendsRead {
		backends, err := r.poolBackends(ctx, ownPool)
		if err != nil {
			return nil, err
		}
		*ownBackends = backends
		*ownBackendsRead = true
	}
	if *ownBackends == nil {
		return nil, nil
	}

	holderBackends, err := r.poolBackends(ctx, holderPool)
	if err != nil {
		return nil, err
	}
	if holderBackends == nil {
		return nil, nil
	}

	var shared []string
	for _, b := range *ownBackends {
		if slices.Contains(holderBackends, b) {
			shared = append(shared, b)
		}
	}
	return shared, nil
}

// poolBackends 返回服务指定 pool 的 master 列表。无 error 时返回 nil 切片表示没有任何
// master 服务它，所有此类情形均归一化为 nil：无论是 pool 已不存在，还是 pool 存在但未携带后端。
//
// 这一归一化处理并非单纯为了整洁，而是关键逻辑。spec.backends 为必填字段但未设置 minItems，
// 因此显式空列表也是 API server 会接受的形态；若原样返回空切片，调用方的 `== nil` 提前返回
// 就只能算近似等价而非真正等价——虽然之后通过与空集合求交集仍能得出正确结果，但会白白多出
// 一次毫无作用的第二次 pool 读取。
//
// 读取会回退到 API server，原因与上限检查相同：Binding 与其 pool 同步创建十分常见，不能因
// 缓存未命中而得出错误的作用域判定。
func (r *KVCachePoolBindingWebhook) poolBackends(
	ctx context.Context, name string,
) ([]string, error) {
	kvcp, err := r.poolOrNil(ctx, name)
	if err != nil || kvcp == nil || len(kvcp.Spec.Backends) == 0 {
		return nil, err
	}
	return kvcp.Spec.Backends, nil
}

// poolOrNil 读取单个 pool（先查缓存，再查 API server），当其确实不存在时返回 nil 且不返回 error。
//
// 将其收拢为单一函数而非在各使用处分别编写读取逻辑，是因为这两步查询极易出现细微错误：只有
// 第二次读取返回 NotFound 才代表不存在，若将超时或 RBAC 拒绝误报为不存在，就会把集群故障
// 转化为针对待准入对象的错误判定。在两处分别实现或许在编写当天表现一致，但后续极易产生分歧。
func (r *KVCachePoolBindingWebhook) poolOrNil(
	ctx context.Context, name string,
) (*workercore.KVCachePool, error) {
	kvcp := &workercore.KVCachePool{}
	key := ctrlcli.ObjectKey{Name: name}
	if err := r.Client.Get(ctx, key, kvcp); err != nil {
		if !kerrors.IsNotFound(err) {
			return nil, fmt.Errorf("get kv cache pool %q: %w", name, err)
		}
		if err = r.APIReader.Get(ctx, key, kvcp); err != nil {
			if !kerrors.IsNotFound(err) {
				return nil, fmt.Errorf("get kv cache pool %q: %w", name, err)
			}
			return nil, nil
		}
	}
	return kvcp, nil
}

// validateKVCachePoolBindingCeilingFitsPool 拒绝 pool 无论如何都无法满足的配额请求。
//
// 上限是一个请求值（request）而非授予值（grant）——当所有请求总量超出容量时，master 会按
// 比例缩减每个 tenant 的额度——因此这里不是预留容量检查，也不会跨 pool 中的其他 Binding
// 做加总计算。它仅拒绝一种显而易见毫无意义的请求：申请量超过了整个 pool 声明的总容量。
func (r *KVCachePoolBindingWebhook) validateKVCachePoolBindingCeilingFitsPool(
	ctx context.Context, kvcpb *workercore.KVCachePoolBinding,
) field.ErrorList {
	poolPath := field.NewPath("spec", "poolRef", "name")

	kvcp := &workercore.KVCachePool{}
	key := ctrlcli.ObjectKey{Name: kvcpb.Spec.PoolRef.Name}
	if err := r.Client.Get(ctx, key, kvcp); err != nil {
		if !kerrors.IsNotFound(err) {
			return field.ErrorList{field.InternalError(poolPath,
				fmt.Errorf("get kv cache pool: %w", err))}
		}
		// Binding 与其 pool 同步创建十分常见，因此在缓存未命中时会先向 API server 重新查询，而不会
		// 直接拒绝。
		//
		// 仅当该次查询返回 NotFound 时才予以拒绝，原因与 pool Webhook 一致：在 failurePolicy 为 Fail
		// 的配置下，若将超时或 RBAC 拒绝报告为“未找到 pool”，会误导提交者在原本正确的名称中排查拼写错误。
		if err = r.APIReader.Get(ctx, key, kvcp); err != nil {
			if !kerrors.IsNotFound(err) {
				return field.ErrorList{field.InternalError(poolPath,
					fmt.Errorf("get kv cache pool: %w", err))}
			}
			return field.ErrorList{field.NotFound(poolPath, key.Name)}
		}
	}

	ceiling := kvcpb.Spec.Quota.Ceiling
	if ceiling.Cmp(kvcp.Spec.Quota.Total) <= 0 {
		return nil
	}

	return field.ErrorList{field.Invalid(field.NewPath("spec", "quota", "ceiling"), ceiling.String(),
		fmt.Sprintf("must not exceed the pool's own ceiling of %s: a request larger than everything "+
			"pool %q declares can never be granted, whatever the other Bindings ask for",
			kvcp.Spec.Quota.Total.String(), kvcp.Name))}
}

// ceilingMoved 报告一次更新是否改变了该 Binding 所申请的配额大小。
//
// 使用 Cmp 而非字符串相等比较：1Ti 与 1099511627776 是同一请求量的两种写法，若因写法改写
// 就重新读取 pool，会导致所有对容量单位做归一化处理的模板工具在每次 apply 时都触发该路径。
func ceilingMoved(oldKvcpb, newKvcpb *workercore.KVCachePoolBinding) bool {
	return oldKvcpb.Spec.Quota.Ceiling.Cmp(newKvcpb.Spec.Quota.Ceiling) != 0
}

// validateKVCachePoolBindingImmutable 冻结那些一旦变更便会导致热缓存无法继续正确工作的字段。
//
// pool 被冻结是因为重新指向会静默迁移 namespace 的配额并使其数据滞留在旧 master 上。domain
// 则逐字段冻结，其中最后两个字段尤为危险：在已有块写入的情况下修改 blockSize 或 dtype 并不会
// 在任何地方触发报错——写入会成功，读取也会成功，但取回来的 tensor 数据却是错的。
func validateKVCachePoolBindingImmutable(
	oldKvcpb, newKvcpb *workercore.KVCachePoolBinding,
) field.ErrorList {
	specPath := field.NewPath("spec")

	var errs field.ErrorList

	if oldKvcpb.Spec.PoolRef.Name != newKvcpb.Spec.PoolRef.Name {
		errs = append(errs, field.Forbidden(specPath.Child("poolRef", "name"),
			"poolRef is immutable: re-pointing a Binding moves this namespace's grant "+
				"without anything recording that it moved, and strands its bytes on the old master"))
	}

	domainPath := specPath.Child("domain")
	oldDomain, newDomain := oldKvcpb.Spec.Domain, newKvcpb.Spec.Domain
	if oldDomain.Name != newDomain.Name {
		errs = append(errs, field.Forbidden(domainPath.Child("name"),
			"name is immutable: it is the tenant this namespace's cache and quota both live under, "+
				"so renaming it strands the ledger entry and abandons every block already written"))
	}
	if oldDomain.BlockSize != newDomain.BlockSize {
		errs = append(errs, field.Forbidden(domainPath.Child("blockSize"),
			"blockSize is immutable: blocks already in the cache were written at the old size, and "+
				"reading them back at a new one is silent corruption rather than a failure"))
	}
	if oldDomain.Dtype != newDomain.Dtype {
		errs = append(errs, field.Forbidden(domainPath.Child("dtype"),
			"dtype is immutable: tensors already in the cache carry the old element type, and "+
				"reading them back as another one is silent corruption rather than a failure"))
	}

	return errs
}
