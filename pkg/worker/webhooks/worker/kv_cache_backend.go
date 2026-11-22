package worker

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net"
	"net/url"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	conregname "github.com/google/go-containerregistry/pkg/name"
	core "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/utils/ptr"
	ctrladmission "sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/kubemeta"
	"gpustack.ai/gpustack/pkg/utils/quantityx"
	"gpustack.ai/gpustack/pkg/webhook"
	"gpustack.ai/gpustack/pkg/worker/kvcache/mooncake"
	"gpustack.ai/gpustack/pkg/worker/settings"
)

// MaxLeaderReplicas 是单个后端运行的最大 leader 进程数；导出该常量是因为 SCHEMA
// 带有相同的上限，且有测试保证两者相等。
//
// REQUIRED: 必须同时调高两者。两者拦截的是不同场景的缺失——此处用于给出说明，而当未安装
// webhook 时 schema 仍会生效——仅调高其中之一会产生最坏的组合：准入阶段放行，而 schema
// 随后拒绝，且报错指向一个并未填错的字段。测试断言的是两者相等而非具体数值，因此在调整上限时
// 测试依然有效。
//
// 该数值是为了防止误解而设的保护限制，而非选举机制本身的属性：Lease 允许任意数量的候选者。
// 实际只有一个 leader 提供服务，其余均为不持有数据的备节点，因此调高此值只会增加闲置进程而不会
// 提升容量——设置此限制正是为了避免有人误以为调高它能换取更高吞吐量。
const MaxLeaderReplicas = 5

// KVCacheBackendWebhook 校验 v1alpha1.KVCacheBackend。
//
// 该 webhook 仅作校验用途。该 API 的所有默认值——后端类型、leader 副本数与分配策略、
// 传输协议——均为 CRD schema 默认值，且所有枚举均由 CRD schema 枚举定义。两者均无需
// webhook 处理，且对于枚举而言，即使编写了 webhook 也无能为力：结构化 schema 校验在
// rest.BeforeCreate 中执行，validating 准入链在其后运行，因此枚举之外的值在到达此
// 处理函数之前就已经被拒绝。
//
// 留在此处处理的是 schema 无法表达的约束：两个分支之间的互斥选择、跨对象读取、
// 派生标志与逃生通道之间的冲突，以及不可变性。
//
// nolint: lll
// +k8s:webhook-gen:validating:group="worker.gpustack.ai",version="v1alpha1",resource="kvcachebackends",scope="Cluster"
// +k8s:webhook-gen:validating:operations=["CREATE","UPDATE"],failurePolicy="Fail",sideEffects="None",matchPolicy="Equivalent",timeoutSeconds=10
type KVCacheBackendWebhook struct{}

func (r *KVCacheBackendWebhook) SetupWebhook(_ context.Context, _ webhook.SetupOptions) (runtime.Object, error) {
	// 此处理函数不持有客户端。下文每条规则的判断均仅依赖对象自身或设置缓存，
	// 没有任何规则读取的字段，后续阅读者都必须证明其确实未被使用。
	return &workercore.KVCacheBackend{}, nil
}

var (
	_ ctrladmission.Validator[runtime.Object] = (*KVCacheBackendWebhook)(nil)
	_ webhook.ReceiveDeletionUpdate           = (*KVCacheBackendWebhook)(nil)
)

// ReceiveDeletionUpdate 使该 webhook 在后端处于删除中时仍继续校验其更新。
// 只要 status.usedBy 非空，其 finalizer 就会保留，因此分支选择与磁盘层在仍有任何
// 消费者使用该后端期间都会保持冻结，而不仅仅是在其存活期间。
//
// FORBIDDEN: 在未改动 spec.image 的更新中读取兜底镜像设置。该设置是此处理函数在传入的
// 两个对象之外唯一读取的外部信息，validateKVCacheBackendSpec 基于镜像已发生变更这一
// 前提对其进行门控，原因在于：reconciler 移除该对象的 finalizer 也是一次更新，若在此
// 拒绝该更新，会导致在清理逻辑已移除所有关联工作负载之后，该对象仍因无法删除而被永久滞留。
func (r *KVCacheBackendWebhook) ReceiveDeletionUpdate() {}

func (r *KVCacheBackendWebhook) ValidateCreate(ctx context.Context, obj runtime.Object) (ctrladmission.Warnings, error) {
	kvcb := obj.(*workercore.KVCacheBackend)

	warnings := warnKVCacheBackendIdleFabricInterfaceCount(kvcb, nil)
	if errs := validateKVCacheBackendSpec(ctx, kvcb, nil, true); len(errs) > 0 {
		return warnings, kerrors.NewInvalid(kvcb.GroupVersionKind().GroupKind(), kvcb.Name, errs)
	}

	return warnings, nil
}

func (r *KVCacheBackendWebhook) ValidateUpdate(
	ctx context.Context, oldObj, newObj runtime.Object,
) (ctrladmission.Warnings, error) {
	oldKvcb, newKvcb := oldObj.(*workercore.KVCacheBackend), newObj.(*workercore.KVCacheBackend)

	warnings := warnKVCacheBackendIdleFabricInterfaceCount(newKvcb, oldKvcb)
	errs := validateKVCacheBackendSpec(ctx, newKvcb, oldKvcb, oldKvcb.Spec.Image != newKvcb.Spec.Image)
	errs = append(errs, validateKVCacheBackendImmutable(oldKvcb, newKvcb)...)
	errs = append(errs, validateKVCacheBackendMultiTenancyWithdrawal(oldKvcb, newKvcb)...)
	if len(errs) > 0 {
		return warnings, kerrors.NewInvalid(newKvcb.GroupVersionKind().GroupKind(), newKvcb.Name, errs)
	}

	return warnings, nil
}

func (r *KVCacheBackendWebhook) ValidateDelete(
	_ context.Context, _ runtime.Object,
) (ctrladmission.Warnings, error) {
	// 当 status.usedBy 非空时，删除操作由 reconciler 的 finalizer 拒绝，而非在此处拦截：
	// 此处理函数仅能看到当前对象，而该判断需要依赖其持有的消费者列表。
	return nil, nil
}

// warnKVCacheBackendIdleFabricInterfaceCount 对配置了 fabric 接口数但有效协议既非
// RDMA 也非 EFA 的成员组发出告警，因为此时该数量配置不会产生任何渲染结果。
//
// 此处仅告警而不拒绝，因为将组从 RDMA 切回 TCP 的更新通常会保留原有的数量字段，
// 若直接拒绝会阻塞此类变更。创建时 old 为 nil。在更新操作中，仅当本次更新修改了该组的数量
// 或有效协议时才发出告警：其他所有更新（包括 reconciler 移除 finalizer）若也触发，
// 只会反复输出无人读取的告警。各组按位置进行匹配，位置即其身份标识。
func warnKVCacheBackendIdleFabricInterfaceCount(kvcb, old *workercore.KVCacheBackend) ctrladmission.Warnings {
	managed := kvcb.Spec.Connection.Managed
	if managed == nil {
		return nil
	}
	var oldMembers []workercore.KVCacheBackendMember
	if old != nil && old.Spec.Connection.Managed != nil {
		oldMembers = old.Spec.Connection.Managed.Members
	}

	var warnings ctrladmission.Warnings
	membersPath := field.NewPath("spec", "connection", "managed", "members")
	for i, member := range managed.Members {
		if member.FabricInterfaceCount == nil || mooncake.MemberGroupIsHostFabric(kvcb, member) {
			continue
		}
		if old != nil && i < len(oldMembers) &&
			ptr.Equal(oldMembers[i].FabricInterfaceCount, member.FabricInterfaceCount) &&
			mooncake.MemberProtocolForGroup(old, oldMembers[i]) == mooncake.MemberProtocolForGroup(kvcb, member) {
			continue
		}
		warnings = append(warnings, fmt.Sprintf(
			"%s has no effect: the group's effective protocol is neither RDMA nor EFA, so no "+
				"fabric device is requested; remove it, or it applies again if the group moves "+
				"to RDMA or EFA",
			membersPath.Index(i).Child("fabricInterfaceCount"),
		))
	}

	return warnings
}

// validateKVCacheBackendSpec 包含无论通过创建还是更新传入 spec 时都适用的全部规则。
//
// old 为变更前的对象，创建时为 nil。其存在原因与 checkFallback 相同：
// 若规则对更新中未触及的字段重新裁决，可能会使已准入的对象陷入困境，且并非所有更新都来自用户。
//
// checkFallback 表示本次调用是否还必须证明集群级兜底镜像依然存在。对于未改动
// spec.image 的更新，该值为 false。
//
// 兜底镜像来自一个可编辑的设置，因此若每次更新都重新读取该设置，一旦管理员清空该设置，
// 已准入的对象就会立刻无法更新——而并非每次更新都由用户发起。reconciler 移除对象
// finalizer 就是其中之一，若将其拒绝，在清理流程已移除所有关联工作负载后，该对象将
// 永久无法删除。每次调用仍会检查对象是否指定了可用镜像；仅将依赖外部状态的判断限定在
// 可能改变其结果的更新中。
func validateKVCacheBackendSpec(
	ctx context.Context, kvcb, old *workercore.KVCacheBackend, checkFallback bool,
) field.ErrorList {
	specPath := field.NewPath("spec")

	errs := validateKVCacheBackendName(kvcb)
	errs = append(errs, validateKVCacheBackendImage(ctx, kvcb, checkFallback, specPath.Child("image"))...)
	errs = append(errs, validateKVCacheBackendPullSecrets(
		kvcb.Spec.ImagePullSecrets, specPath.Child("imagePullSecrets"),
	)...)
	var oldSpec *workercore.KVCacheBackendSpec
	if old != nil {
		oldSpec = &old.Spec
	}
	errs = append(errs, validateKVCacheBackendConnection(
		&kvcb.Spec, oldSpec, specPath.Child("connection"),
	)...)

	return errs
}

// validateKVCacheBackendPullSecrets 拒绝任何 Secret 都无法满足的 secret 引用。
//
// 出于向后兼容性考虑，LocalObjectReference 的 name 为可选字段，因此生成的 schema
// 会接受 `{}` 条目——Pod 校验同样会放行它，仅检查名称前后不含空白字符。该引用会被原样
// 复制到渲染出的两个 Pod spec 中，并在拉取镜像时失败，最终在节点上表现为
// ImagePullBackOff，而不会体现在当前对象的状态上。
//
// 该校验形式与 validateInstanceVolume 在其自身引用上使用的形式一致，原因也相同：
// 任何 Secret 都不可能具备的名称永远无法解析，这与仅仅尚不存在的名称不同。
func validateKVCacheBackendPullSecrets(
	refs []core.LocalObjectReference, fldPath *field.Path,
) field.ErrorList {
	var errs field.ErrorList

	for i := range refs {
		namePath := fldPath.Index(i).Child("name")
		if refs[i].Name == "" {
			errs = append(errs, field.Required(namePath,
				"name of the referenced secret must be specified"))
			continue
		}
		if msgs := validation.IsDNS1123Subdomain(refs[i].Name); len(msgs) > 0 {
			errs = append(errs, field.Invalid(namePath, refs[i].Name, strings.Join(msgs, "; ")))
		}
	}

	return errs
}

// validateKVCacheBackendName 拒绝其名称无法被渲染出的对象承载的请求。
//
// KVCacheBackend 自身的名称是 DNS SUBDOMAIN——最长 253 个字符且允许包含点号。由它渲染出的
// 对象则更为严格：leader 的 Service 名称必须是 DNS-1035 LABEL，不允许包含点号且包含
// "-leader" 后缀在内最多 63 个字符；同时该名称还会写入 app.kubernetes.io/instance，
// 这是一个上限为 63 个字符的标签值。若名称通过了自身规则却无法满足子对象的规则，
// 对象会被准入，随后在每次 reconcile 中失败，除了在日志中留下一条创建错误外一无所有。
//
// 该函数直接检查渲染器实际生成的名称，而非在此处重述它们的长度限制，从而避免后缀变更时
// 校验规则与实际脱节。仅对托管后端执行该检查：外部后端不渲染任何对象，因此不会从其名称
// 派生出任何结果。
func validateKVCacheBackendName(kvcb *workercore.KVCacheBackend) field.ErrorList {
	managed := kvcb.Spec.Connection.Managed
	if managed == nil {
		return nil
	}

	rendered := []string{mooncake.LeaderObjectName(kvcb)}
	for i := range managed.Members {
		rendered = append(rendered, mooncake.MemberObjectName(kvcb, i))
	}

	for _, name := range rendered {
		msgs := validation.IsDNS1035Label(name)
		if len(msgs) == 0 {
			continue
		}
		// 仅返回一个错误，而非为每个渲染对象各报一个错误：它们失败的原因相同，修复方式也都是
		// 重命名该对象。
		return field.ErrorList{field.Invalid(field.NewPath("metadata", "name"), kvcb.Name,
			fmt.Sprintf("this backend renders an object named %q, which is not a valid name for "+
				"one: %s", name, strings.Join(msgs, "; ")))}
	}

	return nil
}

// validateKVCacheBackendImage 拒绝未在任何位置指定镜像的后端。该字段之所以可选，
// 是因为集群级设置更适合用来固定经过验证的版本；但两者不能同时为空，否则将无法决定
// 运行什么镜像。
//
// 仅对托管后端检查此项，原因与名称规则相同：外部后端运行的是他方部署，reconciler 不会
// 为其解析镜像，因此此处没有任何需要代为拒绝的对象。
//
// 当前该设置已自带默认值，因此在默认安装下不会触发下方的拒绝逻辑。但这段逻辑并非死代码：
// 该设置允许置空，当集群将其清空时，需要的正是这种拒绝行为——此时每个后端都必须自行指定
// 镜像。切勿以默认值使其不可达为由删除此处逻辑；默认值本就是允许管理员移除的配置。
func validateKVCacheBackendImage(
	ctx context.Context, kvcb *workercore.KVCacheBackend, checkFallback bool, fldPath *field.Path,
) field.ErrorList {
	if kvcb.Spec.Connection.Managed == nil {
		return nil
	}

	switch image := strings.TrimSpace(kvcb.Spec.Image); {
	case image != "":
		return checkImageReference(image, fldPath)

	case kvcb.Spec.Image != "":
		// 被设为了纯空白字符。此处选择拒绝而非忽略：若忽略则会回退到集群设置，运行一个该对象
		// 并未指定的镜像，而指定镜像正是该字段的唯一意义——静默放弃覆盖值并非任何人的本意。
		return field.ErrorList{field.Invalid(fldPath, kvcb.Spec.Image,
			"must not be blank: an image is either named here or left out entirely so the "+
				`"kv-cache-backend-image" setting decides`)}
	}

	if !checkFallback {
		// 本次更新未改动 spec.image，因此不可能改变是否需要兜底镜像的结论。
		// 重新检查会让一个可编辑设置挡在每一次更新路径上，波及早在设置被清空前就已准入的对象。
		return nil
	}

	// 仅当对象自身未携带镜像时才读取设置，使自带镜像的后端无论设置处于何种状态都能被准入。
	//
	// 使用 ShouldValue 而非 Value：Value 在返回默认值的同时会将“设置从未写入”报告为
	// ERROR，若将其错误视为失败，会导致在常见场景下返回“设置不可读”的报错，而非运维人员
	// 可据此排查的提示。无论如何，它的两种失败模式最终都会汇聚于此——没有非空镜像就无法
	// 准入对象，且修复方式都是同一句话。后续传输故障恢复后，同一对象在下次尝试时自然会通过，
	// 这正是我们期望的基于状态（level-based）的行为。
	if strings.TrimSpace(settings.KVCacheBackendImage.ShouldValue(ctx)) == "" {
		return field.ErrorList{field.Required(fldPath, fmt.Sprintf(
			"an image must be named either here or in the %q setting, and neither carries one",
			"kv-cache-backend-image",
		))}
	}

	return nil
}

// validateKVCacheBackendConnection 强制校验分支选择以及所选分支内部的全部规则。
func validateKVCacheBackendConnection(
	spec, oldSpec *workercore.KVCacheBackendSpec, fldPath *field.Path,
) field.ErrorList {
	managed, external := spec.Connection.Managed, spec.Connection.External

	switch {
	case managed == nil && external == nil:
		return field.ErrorList{field.Required(fldPath,
			`one of "managed" or "external" must be set: a backend with neither describes nothing`)}
	case managed != nil && external != nil:
		return field.ErrorList{field.Forbidden(fldPath,
			`only one of "managed" or "external" may be set: a backend with both describes two`)}
	case external != nil:
		return validateKVCacheBackendExternal(external, fldPath.Child("external"))
	default:
		var oldManaged *workercore.KVCacheBackendManaged
		if oldSpec != nil {
			oldManaged = oldSpec.Connection.Managed
		}
		return validateKVCacheBackendManaged(managed, oldManaged, fldPath.Child("managed"))
	}
}

// validateKVCacheBackendExternal 要求同时提供两种端点角色。schema 以 name 作为列表键，
// 因此已能拒绝重复角色；但它无法表达的是，缺少任一角色都会导致读取方无所指向。
func validateKVCacheBackendExternal(
	external *workercore.KVCacheBackendExternal, fldPath *field.Path,
) field.ErrorList {
	var errs field.ErrorList

	endpointsPath := fldPath.Child("endpoints")

	// 对每个地址既计数又做格式校验。schema 将其类型定为有界字符串，无法表达 host:port 约束——
	// 因此空白字符串在结构上是合法的，会被原样发布到 status 中，并作为无法拨号的地址
	// 传给推理引擎。如果该后端的 Admin 入口能够响应，它甚至会在这种状态下显示为 Ready。
	for i := range external.Endpoints {
		endpoint := &external.Endpoints[i]
		if err := checkHostPort(endpoint.Address); err != nil {
			errs = append(errs, field.Invalid(endpointsPath.Index(i).Child("address"),
				endpoint.Address, fmt.Sprintf(
					"must be host:port, and this operator dials it as given: %v", err,
				)))
		}
	}

	for _, role := range []string{
		workercore.KVCacheBackendEndpointNameClient,
		workercore.KVCacheBackendEndpointNameAdmin,
	} {
		if slices.ContainsFunc(external.Endpoints, func(e workercore.KVCacheBackendEndpoint) bool {
			return e.Name == role
		}) {
			continue
		}
		errs = append(errs, field.Required(endpointsPath, fmt.Sprintf(
			"an entry named %q is required: this operator reads the %q address and publishes the %q one, "+
				"so an external backend naming only one leaves either the scrape or every engine "+
				"with no address",
			role, workercore.KVCacheBackendEndpointNameAdmin,
			workercore.KVCacheBackendEndpointNameClient,
		)))
	}

	return errs
}

// checkImageReference 拒绝容器运行时无法解析的字符串。
//
// Kubernetes 对 Pod 上的非空镜像字符串来者不拒，因此“不是合法镜像”的值会在此处准入、
// 原样渲染，然后在 kubelet 中以 ImagePullBackOff（原因为 InvalidImageName）失败——
// 这会在节点上按 Pod 报错，远晚于导致问题的对象被接受的时刻。此处改为在准入阶段使用与
// 容器镜像 Settings 相同的解析器来做判断。
func checkImageReference(image string, fldPath *field.Path) field.ErrorList {
	if _, err := conregname.ParseReference(image); err != nil {
		return field.ErrorList{field.Invalid(fldPath, image,
			fmt.Sprintf("is not a container image reference: %v", err))}
	}
	return nil
}

// checkHostPort 报告为何某个地址不是本 operator 可以拨号的地址。
//
// 该检查有意保持浅层：它仅确认 host 与 port 均存在、port 为合法端口，以及整体能够顺利
// 拼接为 admin 客户端构造的 URL。至于 host 能否解析以及对端是否响应，则由 reconciler
// 负责判断并通过 conditions 上报——准入阶段仅拒绝绝无可能生效的值。
func checkHostPort(address string) error {
	// 拒绝而非修剪空白。此处没有任何逻辑可以写回对象——validating webhook 仅返回裁决结果——
	// 因此在校验前做归一化会导致未修剪的值被准入并留在 spec 中，进而原样复制到 status
	// 并交给 admin 客户端。
	// 实测结果：对 "http:// leader.example:9003 /health" 调用 http.NewRequest 会在每次
	// 观测时都因端口无效而失败，贯穿对象的整个生命周期。
	if address != strings.TrimSpace(address) {
		return errors.New("must not begin or end with a space: the address is stored and dialed " +
			"exactly as written here")
	}

	host, port, err := net.SplitHostPort(address)
	switch {
	case err != nil:
		return err
	case host == "":
		return errors.New("no host")
	}

	number, err := strconv.Atoi(port)
	if err != nil {
		return fmt.Errorf("port %q is not a number", port)
	}
	if number < 1 || number > 65535 {
		return fmt.Errorf("port %d is outside 1-65535", number)
	}

	// 仅靠 SplitHostPort 是不够的。它只在最后一个冒号处拆分且不检查拆分后的内容，因此
	// "bad host:9003" 能顺利通过解析，随后在实际使用处失败——url.Parse 会拒绝 host 中的
	// 空格。准入检查必须提出与调用方相同的问题，否则就会放行一个每次探测都必败的地址。
	parsed, err := url.Parse("http://" + address + "/")
	if err != nil {
		return fmt.Errorf("cannot be used as an address: %w", err)
	}

	// 仅靠解析也还不够，因为 url.Parse 会“重新解释”而非拒绝异常输入。以下每一种情况经实测
	// 都会以不同于原文的含义到达运行中的客户端："bad/path:9003" 会拨号到 80 端口上的
	// host "bad" 并将剩余部分当作路径，"a?b:9003" 与 "a#b:9003" 同理分别变为查询参数和
	// 片段，而 "user@host:9003" 则会把前缀段当作凭据并拨号给后半段。
	//
	// 一次相等比较即可覆盖这四种情况，且 userinfo 无需单独检查：解析后的 authority 绝不会
	// 包含凭据信息，因此凡是携带 userinfo 的地址必然与 authority 不等。
	if parsed.Host != address {
		return fmt.Errorf("is read as host %q rather than as the address itself", parsed.Host)
	}

	return nil
}

// validateKVCacheBackendManaged 强制执行两项作用域限制以及逃生通道规则。
//
// oldManaged 在创建时为 nil。它仅用于一件事：在更新未触及某个列表时跳过对该列表的
// 逃生通道规则检查。参见 unchangedPassthrough。
func validateKVCacheBackendManaged(
	managed, oldManaged *workercore.KVCacheBackendManaged, fldPath *field.Path,
) field.ErrorList {
	var errs field.ErrorList

	if replicas := managed.Leader.Replicas; replicas != nil {
		if *replicas > MaxLeaderReplicas {
			errs = append(errs, field.Invalid(fldPath.Child("leader", "replicas"), *replicas,
				fmt.Sprintf("at most %d is supported: only one leader serves at a time and the rest "+
					"are standbys, so more of them adds spare processes rather than capacity",
					MaxLeaderReplicas)))
		}

		// 当请求多个 leader 副本时，配对规则会拒绝 None 策略。
		// 存在多个 leader 却没有任何选举机制在它们之间裁决，这并非一种降级配置：每个 leader
		// 都会提供服务，而成员节点则会向各自被告知的那个 leader 注册。
		if *replicas > 1 && !managed.Leader.KubernetesElectionEnabled() {
			errs = append(errs, field.Invalid(fldPath.Child("leader", "replicas"), *replicas,
				"more than one leader requires leader.electionBackend to be Kubernetes; "+
					"with None every replica would serve"))
		}
	}

	// REQUIRED: 开启或关闭 Kubernetes 选举的更新必须重新执行这些规则，即使该更新并未触及
	// 参数列表，而下方的豁免逻辑正是使这成为必要的原因。开启该字段才会将 `enable_ha`、
	// `ha_backend_type`、`ha_backend_connstring` 与 `cluster_id` 变为派生标志；在这些标志
	// 成为派生项之前就已准入的对象可能仍携带其中一个，且渲染器会将逃生通道参数追加到派生标志
	// 之后，因此列表中遗留的 `-enable_ha=false` 会覆盖选举所需的 `-enable_ha=true`——
	// 如果只检查列表是否变动，就会放行多个未经选举的 master。
	var oldLeaderExtraArgs []string
	haUnchanged := true
	if oldManaged != nil {
		oldLeaderExtraArgs = oldManaged.Leader.ExtraArgs
		haUnchanged = oldManaged.Leader.KubernetesElectionEnabled() == managed.Leader.KubernetesElectionEnabled()
	}
	if !haUnchanged ||
		!unchangedPassthrough(oldManaged != nil, oldLeaderExtraArgs, managed.Leader.ExtraArgs) {
		errs = append(errs, validateExtraArgs(managed.Leader.ExtraArgs,
			mooncake.LeaderExtraArgsRules, fldPath.Child("leader", "extraArgs"))...)
	}

	// leader 侧的环境变量逃生通道与本校验器在成员侧强制执行的规则相同，只是针对 leader 自身
	// 的派生变量名，并且它与上述参数逃生通道耦合于相同的声明条件，而非仅依赖透传列表自身。
	//
	// 保留列表本身是无条件的，但这正是需要这种耦合的原因，而非不需要的原因。声明变更所改变的是
	// 渲染器实际输出哪些保留名称：Pod 的身份标识与 IP 仅在开启 Kubernetes 选举时才会输出。
	// 若列表中在某个名称被设为保留之前就已带有该名称，透传比较会将其豁免，而渲染器会将逃生通道
	// 追加在派生变量之后——因此启用该声明的那次更新，正是陈旧值开始覆盖 argv 所依赖引用的
	// 时刻，而这也是本规则绝不能跳过该次更新的原因。
	var oldLeaderExtraEnv []workercore.InstanceEnvVar
	if oldManaged != nil {
		oldLeaderExtraEnv = oldManaged.Leader.ExtraEnv
	}
	if !haUnchanged ||
		!unchangedPassthrough(oldManaged != nil, oldLeaderExtraEnv, managed.Leader.ExtraEnv) {
		errs = append(errs, validateExtraEnvs(managed.Leader.ExtraEnv,
			mooncake.LeaderDerivedEnvs, fldPath.Child("leader", "extraEnv"))...)
	}

	for i := range managed.Members {
		var oldMember *workercore.KVCacheBackendMember
		if oldManaged != nil && i < len(oldManaged.Members) {
			oldMember = &oldManaged.Members[i]
		}
		errs = append(errs, validateKVCacheBackendMember(&managed.Members[i], oldMember,
			fldPath.Child("members").Index(i))...)
	}

	errs = append(errs, validateKVCacheBackendLocalDiskUniqueness(managed, fldPath)...)
	errs = append(errs, validateKVCacheBackendScaleIn(managed, fldPath.Child("scaleIn"))...)

	return errs
}

// validateKVCacheBackendLocalDiskUniqueness 强制要求仅允许一个成员组声明磁盘层级。
//
// 原因在于容量契约而非某一个渲染器的覆盖范围，这与将列表本身限制为单个条目的理由相同：
// status.capacity 是整个后端的单一一组数值，无法将某个层级的字节数归因到单个磁盘，因此两个
// 层级都无法准确描述。这一限制由两个门控共同承担，每层一个——列表自身的条目上限以及跨组
// 的本规则——因此它们会随该状态结构一起变动，否则保持不变。
//
// 该规则放在 Webhook 而非 schema 中，因为它约束的是一对位置之间的关系：schema 只能判断
// 单个值有误，只有 Webhook 才能禁止两个组各自携带一个条目。
func validateKVCacheBackendLocalDiskUniqueness(
	managed *workercore.KVCacheBackendManaged, fldPath *field.Path,
) field.ErrorList {
	var withDisk []int
	for i := range managed.Members {
		if len(managed.Members[i].LocalDisks) > 0 {
			withDisk = append(withDisk, i)
		}
	}

	// 每个后端仅限一个磁盘层级，原因在于容量契约而非渲染逻辑：status.capacity 是整个后端的
	// 单一一组数值，它通过将 leader 的文件族累加到其内存族来上报磁盘层级。两个层级会落入同一个
	// 族中且无法区分，导致对象对两者都无法描述。
	//
	// 它也恰好是唯一能拒绝两个组指定同一宿主机目录的约束——否则一旦两者的 selector 重叠，就会
	// 在同一个层级上运行两个存储实例。这并非本规则的设计初衷，因此若出于容量原因放宽该规则，
	// 会使后一种情况悄然通过；固定该行为的测试用例正是以目录冲突而非容量归因命名的。
	if len(withDisk) > 1 {
		return field.ErrorList{field.Forbidden(
			fldPath.Child("members").Index(withDisk[1]).Child("localDisks"),
			fmt.Sprintf("only one member group may declare localDisks, and groups %v do: "+
				"the leader reports every disk tier through one pair of gauges, so status.capacity "+
				"could not say which figure belonged to which group", withDisk),
		)}
	}

	return nil
}

// validateKVCacheBackendScaleIn 限制待退出成员等待的宽限期。
//
// 两个边界均拒绝会导致关闭钩子直接失败而非等待的值；失败的 preStop 只会记录为一个事件
// 并被忽略，从而使钩子看似已配置却未能排空任何数据。
func validateKVCacheBackendScaleIn(
	managed *workercore.KVCacheBackendManaged, fldPath *field.Path,
) field.ErrorList {
	scaleIn := managed.ScaleIn
	if scaleIn == nil {
		return nil
	}

	var errs field.ErrorList
	gracePath := fldPath.Child("gracePeriodSeconds")

	// schema 也对此做了边界限制，这种重复是有意为之：schema 的报错只给出数字，而此处说明
	// 原因。即使先触发 schema 边界限制，也同样能阻止发布一个每次关闭都会失败的钩子。
	//
	// 限制：没有哪个宽限期会同时超过上限又为负数，因此一次只会触发一个边界，任意一个提前返回
	// 都不会暴露差异。这里仍做错误累积，是为了后续调整边界时——例如下限高于零或上限跟随
	// endpoint 变化——能同时报告两处违规而不只是第一处。目前没有测试覆盖这一点：本包测试中的
	// 聚合用例无论哪种写法都会通过，因此需要靠这条注释提醒后续修改者。
	if scaleIn.GracePeriodSeconds > mooncake.MemberMaxGracePeriodSeconds {
		errs = append(errs, field.Invalid(gracePath, scaleIn.GracePeriodSeconds, fmt.Sprintf(
			"must not exceed %d: the member's own endpoint refuses a larger grace with a 400, so "+
				"this would render a shutdown hook that fails every time it runs",
			mooncake.MemberMaxGracePeriodSeconds,
		)))
	}
	if scaleIn.GracePeriodSeconds < 0 {
		errs = append(errs, field.Invalid(gracePath, scaleIn.GracePeriodSeconds,
			"must not be negative"))
	}

	// 对没有磁盘层级的后端设置宽限期不会被拒绝。该配置此时不起作用——既无需注销也不会渲染
	// 钩子——但如果拒绝它，就会使声明缩容策略依赖于两个独立字段的编辑顺序，而接下来要添加
	// 磁盘层级的运维人员将不得不先移除宽限期再重新加上。

	return errs
}

const quantityTooLarge = "must not exceed 9223372036854775807 (2^63-1) bytes: the renderer reads " +
	"this as a signed 64-bit count, and a larger one does not survive the conversion"

// memberBucketSize 是转换为 Quantity 类型的渲染器桶大小，从而使下面的三个边界检查直接
// 与实际下发的数值（而非其副本）进行比较，并按所校验字段的格式输出。
var memberBucketSize = *resource.NewQuantity(mooncake.MemberBucketSizeLimit, resource.BinarySI)

// validateKVCacheBackendMember 包含 schema 无法表达的单组校验规则：在没有可计费设备的
// 组上指定了设备资源，以及两个在 schema 中类型为字符串的 Quantity 字段。
func validateKVCacheBackendMember(
	member, oldMember *workercore.KVCacheBackendMember, fldPath *field.Path,
) field.ErrorList {
	var errs field.ErrorList

	// 此处不再包含按介质划分的规则，这是有意省去的：schema 现已枚举了唯一受支持的值，因此
	// 本 API 不渲染的介质会在进入本处理函数前被拒绝，为其编写规则只会成为无法触达的死代码。

	// 目前整个列表仅允许每组一个层级，下面的规则与不可变性规则出于相同原因按位置读取条目：
	// 位置在其余所有地方都用于标识成员组，因此在这里也同样标识对应条目。schema 按 path
	// 键控该列表并拒绝重复项，因此指定同一目录的两个条目不会进入此循环。
	for i := range member.LocalDisks {
		var oldDisk *workercore.KVCacheBackendMemberLocalDisk
		if oldMember != nil && i < len(oldMember.LocalDisks) {
			oldDisk = &oldMember.LocalDisks[i]
		}
		errs = append(errs, validateKVCacheBackendLocalDisk(&member.LocalDisks[i], oldDisk,
			fldPath.Child("localDisks").Index(i))...)
	}

	errs = append(errs, validateKVCacheBackendMemberHostPaths(member, fldPath.Child("hostPaths"))...)

	// resource.Quantity 在 schema 中是字符串类型，任何标记中的数值边界都无法作用于它——
	// 这两处检查是唯一能拒绝非法值的地方。零值会被拒绝而非填充默认值，因为当该值非正数时，
	// 渲染器会省略其推导出的段大小，而未挂载段的成员与 leader 丢失该段的成员在表现上无法区分。
	switch {
	case member.CapacityPerMember.CmpInt64(0) <= 0:
		errs = append(errs, field.Invalid(fldPath.Child("capacityPerMember"),
			member.CapacityPerMember.String(),
			"must be greater than 0: a member contributing nothing is a Pod with no reason to run"))
	case quantityx.OverflowsInt64(member.CapacityPerMember):
		errs = append(errs, field.Invalid(fldPath.Child("capacityPerMember"),
			member.CapacityPerMember.String(), quantityTooLarge))
	case len(member.LocalDisks) > 0 &&
		member.CapacityPerMember.CmpInt64(mooncake.MemberBucketSizeLimit) < 0 &&
		(oldMember == nil || member.CapacityPerMember.Cmp(oldMember.CapacityPerMember) != 0):
		// 该下限仅适用于声明了磁盘层级的组，且仅在本次更新修改了该值时生效。若对本次更新未触及
		// 的数值重新校验，会导致在该边界引入前已准入的对象陷入卡死——而且并非每次更新都来自用户。
		errs = append(errs, field.Invalid(fldPath.Child("capacityPerMember"),
			member.CapacityPerMember.String(), fmt.Sprintf(
				"must be at least %s for a group declaring localDisks: that is one bucket, the unit the "+
					"tier is written in, and its bytes are held in this segment until the bucket is "+
					"complete — so a smaller segment leaves the tier empty under every workload, with "+
					"nothing reporting it", memberBucketSize.String(),
			)))
	}
	if member.LocalBufferSize.CmpInt64(0) < 0 {
		errs = append(errs, field.Invalid(fldPath.Child("localBufferSize"),
			member.LocalBufferSize.String(),
			"must not be negative: it is added to the member Pod's memory request"))
	} else if quantityx.OverflowsInt64(member.LocalBufferSize) {
		errs = append(errs, field.Invalid(fldPath.Child("localBufferSize"),
			member.LocalBufferSize.String(), quantityTooLarge))
	}

	// 该 selector 会原封不动复制到 DaemonSet 的 Pod 模板中，而 API server 会拒绝包含非标签
	// selector 的模板。该拒绝表现为 reconcile 过程中的创建错误，其唯一痕迹只是本 Operator
	// 日志中的一行记录——结果该组根本无法启动，且对象上没有任何原因说明。
	for _, key := range slices.Sorted(maps.Keys(member.NodeSelector)) {
		selectorPath := fldPath.Child("nodeSelector").Key(key)
		if msgs := validation.IsQualifiedName(key); len(msgs) > 0 {
			errs = append(errs, field.Invalid(selectorPath, key,
				"is not a label key: "+strings.Join(msgs, "; ")))
		}
		if msgs := validation.IsValidLabelValue(member.NodeSelector[key]); len(msgs) > 0 {
			errs = append(errs, field.Invalid(selectorPath, member.NodeSelector[key],
				"is not a label value: "+strings.Join(msgs, "; ")))
		}
	}

	// 成员渲染器会原封不动使用该覆盖值，而不会像后端级镜像那样先做空白裁剪，因此空白字符
	// 会作为镜像引用直接传给容器运行时。
	switch image := strings.TrimSpace(member.Image); {
	case member.Image == "":
	case image == "":
		errs = append(errs, field.Invalid(fldPath.Child("image"), member.Image,
			"must not be blank: an image is either named here or left out so the backend's decides"))
	default:
		errs = append(errs, checkImageReference(member.Image, fldPath.Child("image"))...)
	}

	var oldExtraArgs []string
	var oldExtraEnv []workercore.InstanceEnvVar
	if oldMember != nil {
		oldExtraArgs, oldExtraEnv = oldMember.ExtraArgs, oldMember.ExtraEnv
	}
	if !unchangedPassthrough(oldMember != nil, oldExtraArgs, member.ExtraArgs) {
		errs = append(errs, validateExtraArgs(member.ExtraArgs,
			mooncake.MemberExtraArgsRules, fldPath.Child("extraArgs"))...)
	}
	if !unchangedPassthrough(oldMember != nil, oldExtraEnv, member.ExtraEnv) {
		errs = append(errs, validateExtraEnvs(member.ExtraEnv,
			mooncake.MemberDerivedEnvs, fldPath.Child("extraEnv"))...)
	}

	return errs
}

// pathsOverlap 判断两个挂载路径是否相同或存在包含关系。
//
// 判断包含而不仅仅是相等，是因为用户声明的 /dev 会覆盖宿主机网络结构插件注入在
// /dev/infiniband 下的设备节点，而容器最终看到哪一个取决于容器运行时，而非本仓库代码。
// 依赖本仓库中无人验证过的顺序不能算作规则，因此直接拒绝此类重叠，让该问题无从发生。
//
// 比较按路径元素进行，这样 /dev/infiniband2 就不会被像普通字符串前缀那样误判为位于
// /dev/infiniband 之下。两边都会先做清理：schema 已要求绝对路径且不含空元素，只剩下末尾
// 斜杠这一处差异仍需 Clean 去除。
func pathsOverlap(a, b string) bool {
	a, b = filepath.Clean(a), filepath.Clean(b)
	if a == b {
		return true
	}
	return strings.HasPrefix(a, b+"/") || strings.HasPrefix(b, a+"/")
}

// validateKVCacheBackendMemberHostPaths 防止成员组声明的挂载路径彼此冲突，或与渲染器
// 自身管理的挂载路径冲突。
//
// Kubernetes 允许一个容器在相同路径或互为包含的两个路径上携带两个挂载，并将结果交由
// 运行时决定，因此此类冲突不会在任何地方报错：成员正常启动，只是其中一个挂载读取到的内容
// 并非预期。这正是要在 Webhook 拦截而不是留给 Pod 处理的根本原因，也是下面所有比较均
// 按重叠而非相等进行的原因。
//
// 设备树路径会被无条件拒绝，即使该成员组当前协议未分配任何设备也是如此：协议字段可在更新
// 时修改，而挂载路径仅在写入时校验，若在 tcp 下放行，就会在日后有人修改无关字段时引入
// 冲突。磁盘层级的路径则针对该组当前的 localDisks 校验，因为该路径本身就在同一位置声明
// ——移动层级的组会在同一次更新中按新路径重新校验。
func validateKVCacheBackendMemberHostPaths(
	member *workercore.KVCacheBackendMember, fldPath *field.Path,
) field.ErrorList {
	var errs field.ErrorList

	for i := range member.HostPaths {
		mountPath := member.HostPaths[i].MountPath
		mountPathPath := fldPath.Index(i).Child("mountPath")

		// 仅与排在当前条目之前的条目比较，且按重叠而非相等判断：存在包含关系的两个已声明挂载
		// 取决于它们在此列表中的顺序，而容器最终看到哪一个由运行时决定——这与下面渲染器自有路径
		// 按重叠校验的原因相同。向后比较可在较晚的条目上报告一次冲突，而不是两边各报一次。
		if prior := slices.IndexFunc(member.HostPaths[:i], func(e workercore.KVCacheBackendMemberHostPath) bool {
			return pathsOverlap(mountPath, e.MountPath)
		}); prior >= 0 {
			errs = append(errs, field.Invalid(mountPathPath, mountPath, fmt.Sprintf(
				"overlaps the mount path of hostPaths[%d] (%s): a container carrying overlapping "+
					"paths leaves which mount wins to the runtime, and reports nothing either way",
				prior, member.HostPaths[prior].MountPath,
			)))
			continue
		}

		// 先无条件检查设备树路径：该校验与是否存在磁盘层级无关，没有磁盘层级的组也必须同样满足。
		if pathsOverlap(mountPath, mooncake.RDMADevicePath) {
			errs = append(errs, field.Invalid(mountPathPath, mountPath,
				"overlaps where a host fabric's device plugin injects the granted device nodes ("+
					mooncake.RDMADevicePath+"), and is refused under every protocol because the "+
					"protocol is editable: declaring it here would collide with those nodes the "+
					"day the group becomes RDMA or EFA"))
			continue
		}

		// 再逐一检查所有已声明的磁盘层级，而不只是第一项：这个循环只比读取第零项多一行代码，
		// 却能在将来随状态结构调整放开条目上限时依然保持正确。
		for _, disk := range member.LocalDisks {
			if pathsOverlap(mountPath, disk.Path) {
				errs = append(errs, field.Invalid(mountPathPath, mountPath,
					fmt.Sprintf("overlaps where this group's localDisks tier is mounted (%s): the "+
						"tier is a declared capacity with its own deregistration hook, so it is "+
						"reached through localDisks and not through a plain mount", disk.Path)))
			}
		}
	}

	return errs
}

// validateExtraEnvs 针对传入的任意一侧派生名称列表执行环境变量透传校验规则。
//
// 它与 validateExtraArgs 分开实现而不是共用逻辑，因为两者的差异之处正是关键所在：
// 配置键与环境变量名格式不同，遵循不同的上游规则，报错时若写错术语会让人按错误方向排查。
// 两者共有的只是一次列表遍历，其代码量比为此引入的抽象还要小。
//
// 其作用域判定与 validateExtraArgs 一致，原因相同：派生列表会随着本 API 渲染更多配置
// 而增长，若对更新未触及的列表重新校验，会在每次更新（包括 reconciler 自身移除该对象
// finalizer 的更新）时追溯拒绝在此条目存在前已准入的对象。参见 unchangedPassthrough。
func validateExtraEnvs(
	extraEnv []workercore.InstanceEnvVar, derived []string, fldPath *field.Path,
) field.ErrorList {
	var errs field.ErrorList

	for _, e := range extraEnv {
		// 在检查派生列表之前先校验变量名，因为派生列表中只包含合法名称，而非法的变量名虽然不会
		// 与之冲突，却依然会被传入容器。
		//
		// 此处有意采用严格规则而非宽松规则。Kubernetes 有两种规则：此处调用的经典形式，以及
		// 接受除 "=" 外所有可打印 ASCII 字符的宽松形式（较新的 API server 将其用于 Pod env）。
		// 采用严格规则会拒绝少数较新集群本可接受的名称，但凡是它接受的名称任何版本的 API server
		// 都能接受，这能确保不会静默失败。反之，若本 Webhook 放行了名称，集群自身的 Pod 校验
		// 会在 reconcile 过程中拒绝它，而唯一痕迹只是本 Operator 日志中的一行记录，成员组却永远
		// 无法启动。该逃生口所要触达的任何配置项都不存在此类名称。
		if msgs := validation.IsEnvVarName(e.Name); len(msgs) > 0 {
			errs = append(errs, field.Invalid(fldPath.Key(e.Name), e.Name,
				"is not an environment variable name: "+strings.Join(msgs, "; ")))
			continue
		}
		if slices.Contains(derived, e.Name) {
			errs = append(errs, field.Forbidden(fldPath.Key(e.Name),
				"this variable is rendered from this spec, and two definitions of one name make the "+
					"rendered container ambiguous: Kubernetes accepts both and leaves the winner to "+
					"the container runtime, so nothing would report the collision"))
		}
	}

	return errs
}

// validateKVCacheBackendLocalDisk 拒绝节点无法承载的磁盘层级配置。
//
// 该路径会成为 hostPath 挂载，这正是要在此校验的原因：API server 接受任意字符串，而
// kubelet 对非法路径的拒绝发生在 reconcile 内部，唯一痕迹只是本 Operator 日志中的一行
// 记录，成员组则始终无法启动。
//
// 它拒绝 "/" 但不拒绝其他敏感宿主机路径，这种不对称是有意设计，而非未完成的黑名单。
// 填写 /etc 或 /var 的管理员是在显式请求该目录，本项目既不代运维人员推断权限，也不代其
// 拒绝——这与禁止 transport.protocol: Auto 自动升级为 RDMA 遵循同一原则。拒绝 "/"
// 是因为这唯一一个值不可能是合理请求：它会将节点整个文件系统挂载到第三方容器中，没有
// 任何层级需要这样做。此外，一份列出所有“危险”路径的黑名单永远无法穷尽——每多列一项，
// 反而会诱导读者误以为未列出的路径就是安全的。
func validateKVCacheBackendLocalDisk(
	disk, oldDisk *workercore.KVCacheBackendMemberLocalDisk, fldPath *field.Path,
) field.ErrorList {
	if disk == nil {
		return nil
	}

	var errs field.ErrorList

	pathPath := fldPath.Child("path")
	switch path := strings.TrimSpace(disk.Path); {
	case path == "":
		errs = append(errs, field.Required(pathPath,
			"a directory on the node is required: it is mounted from the host, and this operator "+
				"picks no default because the wrong host directory fills a filesystem nothing in "+
				"Kubernetes accounts for"))
	case path != disk.Path:
		// 直接拒绝而非自动裁剪：Validating Webhook 只返回裁决结果而无法修改请求，若在此做规范化，
		// 就会放行未经裁剪的原始值并按原样挂载。
		//
		// 错误信息使用“空白字符”而非“空格”，因为 TrimSpace 还会移除制表符和换行符，过窄的提示
		// 会让排查者去寻找根本不存在的空格。
		errs = append(errs, field.Invalid(pathPath, disk.Path,
			"must not begin or end with whitespace: the path is mounted exactly as written here"))
	case !filepath.IsAbs(path):
		errs = append(errs, field.Invalid(pathPath, disk.Path,
			"must be an absolute path: it names a directory on the node, and a relative one is "+
				"refused by the kubelet inside a reconcile rather than here"))
	case hasParentDirComponent(path):
		// 存储组件自身会在查看文件系统之前静态拒绝此类路径——因此若在此放行，会导致成员永远无法
		// 就绪，而原因只留在容器日志中。这里针对原始路径组件而非 Clean 之后的结果进行检查，
		// 因为 Clean 会消解 ".."，恰好掩盖存储组件要拦截的问题。
		errs = append(errs, field.Invalid(pathPath, disk.Path,
			`must not contain a ".." component: the store refuses a path with one as traversal, `+
				"before it checks whether the directory exists, so the member would start and "+
				"never mount its tier"))
	case filepath.Clean(path) == "/":
		errs = append(errs, field.Invalid(pathPath, disk.Path,
			"must not be the root directory: it would mount the node's whole filesystem into a "+
				"third-party container"))
	case pathOverlaps(filepath.Clean(path), mooncake.RDMADevicePath):
		// 磁盘层级与宿主机网络结构插件注入的设备节点会落入同一个容器中，因此冲突将由容器运行时
		// 裁决而不会在此报错——其中一个会遮蔽另一个，且谁胜出并不会记录在对象上。无论当前传输
		// 协议是什么都会拒绝该路径，因为传输配置是可编辑的：当前尚未冲突的层级，一旦有人将后端
		// 切换为宿主机网络结构就会立即发生冲突。两种宿主机网络结构都不会让渲染器自行挂载任何
		// 路径；EFA 成员直接使用镜像内置的 libfabric。
		errs = append(errs, field.Invalid(pathPath, disk.Path, fmt.Sprintf(
			"must not overlap %s, where a host fabric's device plugin injects the granted device "+
				"nodes into the same container: the tier would shadow them or be shadowed, and "+
				"the transport can be switched to one after this path is set",
			mooncake.RDMADevicePath,
		)))
	}

	// capacity 类型为 resource.Quantity，在 schema 中以字符串表示，因此标记无法对其做数值
	// 边界限制。零值是合法的，表示“本层不设上限”——转而采用存储组件自身的上限——这与省略该
	// 字段效果相同。正值则必须至少容纳一个完整的桶：一旦再多一个桶就会超出该上限，存储组件
	// 便会停止接收卸载任务，因此低于该值的层级永远不会写入任何键。
	switch {
	case disk.Capacity.CmpInt64(0) < 0:
		errs = append(errs, field.Invalid(fldPath.Child("capacity"), disk.Capacity.String(),
			"must not be negative: it caps what this tier stores"))
	case !disk.Capacity.IsZero() && disk.Capacity.Cmp(memberBucketSize) < 0 &&
		(oldDisk == nil || disk.Capacity.Cmp(oldDisk.Capacity) != 0):
		errs = append(errs, field.Invalid(fldPath.Child("capacity"), disk.Capacity.String(),
			fmt.Sprintf("must be at least %s, which is one bucket: the store stops taking offload "+
				"work as soon as one more bucket would not fit under this ceiling",
				memberBucketSize.String())))
	case quantityx.OverflowsInt64(disk.Capacity):
		errs = append(errs, field.Invalid(fldPath.Child("capacity"), disk.Capacity.String(),
			quantityTooLarge))
	}

	// 键数上限的另一半约束。schema 仅将其下限设为零且无法设得更高，这是标记能表达的极限；
	// 下方的一个桶的键数下限与上面的容量下限基于同一事实，只是作用于存储组件在校验字节数时
	// 一并检查的键数量。
	if disk.KeyLimit > 0 && disk.KeyLimit < mooncake.MemberBucketKeysLimit &&
		(oldDisk == nil || disk.KeyLimit != oldDisk.KeyLimit) {
		errs = append(errs, field.Invalid(fldPath.Child("keyLimit"), disk.KeyLimit,
			fmt.Sprintf("must be at least %d, which is one bucket's worth of keys: the store stops "+
				"taking offload work as soon as one more bucket would not fit under this ceiling",
				mooncake.MemberBucketKeysLimit)))
	}

	errs = append(errs, validateKVCacheBackendLocalDiskEviction(disk, fldPath)...)

	return errs
}

// validateKVCacheBackendLocalDiskEviction 拒绝会渲染出存储组件虽能接收但不会实际生效
// 的配置的驱逐块。
//
// 此处每条规则都是成对字段之间的约束，因此没有一条是 schema 标记：枚举值、百分比范围和
// enabled 的默认值都已在 schema 中定义，剩下的就是某字段在另一字段存在时的语义约束。
func validateKVCacheBackendLocalDiskEviction(
	disk *workercore.KVCacheBackendMemberLocalDisk, diskPath *field.Path,
) field.ErrorList {
	eviction := disk.Eviction
	if eviction == nil {
		return nil
	}

	var errs field.ErrorList

	fldPath := diskPath.Child("eviction")

	// schema 会将该字段默认设为 true，因此 nil 对应的是尚未经过 API server 的对象，这里按
	// schema 的解读方式对其求值。
	disabled := eviction.Enabled != nil && !*eviction.Enabled

	if disabled && eviction.Policy != "" {
		errs = append(errs, field.Forbidden(fldPath.Child("policy"),
			"eviction is disabled here, and there is no order in which nothing leaves. Remove one of "+
				"the two rather than leaving a policy that reads as taken"))
	}

	if eviction.Watermark == nil {
		return errs
	}

	watermarkPath := fldPath.Child("watermark")

	switch {
	case disabled:
		errs = append(errs, field.Forbidden(watermarkPath,
			"eviction is disabled here, so there is nothing for these marks to start and stop. "+
				"Remove one of the two rather than leaving a band that reads as configured"))
	case disk.Capacity.IsZero():
		// 水位线是该层级容量上限的百分比，而存储组件自身对该配额的默认值为零——其驱逐路径会将
		// 零视为“无配额”并直接返回而不执行驱逐。这里选择直接拒绝而非渲染该配置，因为永不触发的
		// 区间在表现上与尚未达到水位的区间完全相同。
		errs = append(errs, field.Required(diskPath.Child("capacity"), fmt.Sprintf(
			"a capacity is required when %s is set: the marks are a percentage of it, and without one "+
				"the store evicts nothing while the band reads as configured", watermarkPath,
		)))
	}

	if eviction.Watermark.Low >= eviction.Watermark.High {
		errs = append(errs, field.Invalid(watermarkPath.Child("low"), eviction.Watermark.Low,
			fmt.Sprintf("must be below high (%d): equal or inverted marks make every write past the "+
				"mark evict, and the member's own startup check refuses the pair — inside a container "+
				"log rather than here", eviction.Watermark.High)))
	}

	return errs
}

// pathOverlaps 判断两个已清理的绝对路径是否会彼此覆盖挂载——即相同或任一方包含另一方。
//
// 两个方向的包含关系都必须计入，简单的字符串前缀判断会犯两次错：既会漏掉“层级是设备树
// 父目录”的情况，又会误匹配名称恰好以相同字母开头的兄弟目录（"/dev/infiniband-x"
// 并不在 "/dev/infiniband" 之内）。
func pathOverlaps(a, b string) bool {
	if a == b {
		return true
	}
	return strings.HasPrefix(a, b+"/") || strings.HasPrefix(b, a+"/")
}

// hasParentDirComponent 判断路径中是否包含 ".." 组件。
//
// 它逐个遍历路径组件而不是直接搜索子串，因此诸如 "..data"（projected volume 挂载的
// 常见目录）等合法目录名不会被误判为路径遍历。
func hasParentDirComponent(path string) bool {
	for component := range strings.SplitSeq(filepath.ToSlash(path), "/") {
		if component == ".." {
			return true
		}
	}
	return false
}

// unchangedPassthrough 判断本次调用是否为未对透传列表做任何修改的更新操作；若是，则
// 跳过该列表的校验规则。
//
// 必需约束：这并非放宽校验，而是与回退镜像校验相同的范围限定，原因也针对同一类故障。
// 这些规则会不断增加——高可用开发就向 leader 的禁用列表添加了 `enable_oplog`——每次
// 新增都会追溯拒绝在此规则之前已准入的对象。拒绝该修改本身不是问题，但在每次更新时都
// 拒绝才是问题，因为并非每次更新都来自用户：reconciler 会通过一次更新移除该对象的
// finalizer，若在那里被拒绝，会导致对象在清理工作已删除所有工作负载后卡在无法删除的状态。
//
// 修改该列表的用户会触发此规则；仅修改其他字段的用户以及未做任何修改的控制器则不会。
// leader 的 extraArgs 调用方在此基础上增加了一个条件：若更新将 `electionBackend` 在
// None 和 Kubernetes 之间切换，会导致派生键集合发生变化，因此无论如何都会重新执行规则。
func unchangedPassthrough[T comparable](isUpdate bool, old, current []T) bool {
	return isUpdate && slices.Equal(old, current)
}

// validateExtraArgs 针对其中一侧执行逃生口校验规则。每次拒绝都会指明属于哪一类问题，
// 因为对应的修复方式各不相同：改用专用字段、将参数拼写为单个 token、删去重复项之一，或
// 移除该条目。
func validateExtraArgs(
	extraArgs []string, rules mooncake.ExtraArgsRules, fldPath *field.Path,
) field.ErrorList {
	var errs field.ErrorList

	seen := make(map[string]int, len(extraArgs))
	for i, entry := range extraArgs {
		entryPath := fldPath.Index(i)

		// 每个条目对应一个参数 token，本检查最先执行，因为拆分写法（如 ["-rpc_timeout", "5000"]）
		// 会把裸值放在键的位置上，导致冲突检查找不到可冲突的键，而两部分却仍会一起进入产出配置。
		if !strings.HasPrefix(entry, "-") {
			errs = append(errs, field.Invalid(entryPath, entry,
				`must begin with "-": one entry is one "-flag" or "-flag=value" token, and a value `+
					`written beside its flag carries no key of its own -- a value that itself `+
					`begins with "-" is spelled "-flag=--value"`))
			continue
		}

		// 前导短横线只是产物自身解析器一视同仁处理的修饰符（无论一个还是两个），键名在第一个
		// "=" 处截断，从而使写成单个 token 的布尔参数保留其名称。参数表与重复检查均使用这一
		// 键名。
		//
		// 空键和包含空白字符的键会被一并拒绝：两者都不能作为配置项名称，且都会渲染出产物无法读取
		// 的参数。
		key, _, _ := strings.Cut(strings.TrimLeft(entry, "-"), "=")
		if key == "" || strings.ContainsAny(key, " \t\n") {
			errs = append(errs, field.Invalid(entryPath, entry,
				"the key here -- what precedes the first \"=\" once the dashes are off -- is not "+
					"a setting's name, so the artifact cannot read the argument it renders"))
			continue
		}
		if _, dup := seen[key]; dup {
			errs = append(errs, field.Invalid(entryPath, entry, fmt.Sprintf(
				"carries the key %q a second time: a list does not say which of two entries the "+
					"artifact reads, and neither does anything on this object", key,
			)))
			continue
		}
		seen[key] = i

		if reason, ok := rules.Forbidden[key]; ok {
			errs = append(errs, field.Forbidden(entryPath, reason))
			continue
		}
		if slices.Contains(rules.Derived, key) {
			errs = append(errs, field.Forbidden(entryPath,
				"this key is derived from a field of this spec, and two sources for one setting "+
					"make the rendered result ambiguous"))
		}
	}

	for _, group := range rules.Exclusive {
		var present []string
		for _, key := range group {
			if _, ok := seen[key]; ok {
				present = append(present, key)
			}
		}
		if len(present) > 1 {
			errs = append(errs, field.Forbidden(fldPath,
				fmt.Sprintf("%s are mutually exclusive; set at most one",
					strings.Join(present, " and "))))
		}
	}

	return errs
}

// validateKVCacheBackendImmutable 冻结运行中后端不可变更的配置。branch 被冻结是因为切换它会
// 丢弃或接管整个工作负载；medium 被冻结是因为已挂载的内存段无法在保留其中数据的情况下更改介质类型。
//
// 该函数还会拒绝在位置之间移动分组，这属于另一类规则：下方的逐位置规则检查某个字段是否发生变化，而
// 该规则检查某个位置上的整个分组是否来自原先的其他位置。关于为何逐字段规则无法覆盖此情况，参见
// validateKVCacheBackendMembersNotMoved。
//
// 其余配置均刻意保持可编辑：镜像、节点选择器、容量、extraArgs 或 extraEnv 条目、各存储层的上限
// 及其驱逐设置，以及 transport 配置块都会在下一轮协调中收敛。
func validateKVCacheBackendImmutable(oldKvcb, newKvcb *workercore.KVCacheBackend) field.ErrorList {
	var errs field.ErrorList

	specPath := field.NewPath("spec")

	if oldKvcb.Spec.Type != newKvcb.Spec.Type {
		errs = append(errs, field.Forbidden(specPath.Child("type"), "type is immutable"))
	}

	oldManaged := oldKvcb.Spec.Connection.Managed != nil
	newManaged := newKvcb.Spec.Connection.Managed != nil
	if oldManaged != newManaged {
		errs = append(errs, field.Forbidden(specPath.Child("connection"),
			"the connection branch is immutable"))
	}

	if !oldManaged || !newManaged {
		return errs
	}

	oldMembers, newMembers := oldKvcb.Spec.Connection.Managed.Members, newKvcb.Spec.Connection.Managed.Members
	membersPath := specPath.Child("connection", "managed", "members")
	errs = append(errs, validateKVCacheBackendMembersNotMoved(oldMembers, newMembers, membersPath)...)
	for i := range newMembers {
		if i >= len(oldMembers) {
			break
		}
		// 如今该枚举已包含两个取值，此校验正式生效：在运行中的分组下更改 medium 会导致其内存段从另一种
		// 类型的内存中重新挂载，而原有数据仍在其中，在真正触发修改之前不会暴露任何错误。
		if oldMembers[i].Medium != newMembers[i].Medium {
			errs = append(errs, field.Forbidden(membersPath.Index(i).Child("medium"),
				"medium is immutable"))
		}
		errs = append(errs, validateKVCacheBackendLocalDiskImmutable(
			oldMembers[i].LocalDisks, newMembers[i].LocalDisks,
			membersPath.Index(i).Child("localDisks"),
		)...)
	}

	return errs
}

// validateKVCacheBackendMembersNotMoved 拒绝将分组从一个位置移动到另一个位置的更新，这类编辑会
// 把另一个分组的 spec 交给正在运行的 DaemonSet。
//
// 为什么仅冻结字段还不够。分组的位置就是它的身份标识：DaemonSet 的名称、不可变的选择器标签以及
// 其成员提供服务的端口全部由索引派生。重新排序会保留上述所有标识，仅替换底层的 spec，从而使该位置
// 的成员按另一个分组的配置重建，原有缓存也会随之丢失，而对象本身对此没有任何体现。下方的逐位置
// 规则仅在两个分组的某个冻结字段不同时才能捕获该问题——互换两个都没有磁盘层的分组会绕过所有逐位置
// 校验，因为它们之间所有存在差异的字段都属于 API 刻意保留的可编辑字段。
//
// 该规则能识别什么，以及为什么这已经是能力上限。由于没有独立的名称，分组只能通过其内容来识别，因此
// 唯一能与普通编辑区分开的特征，是某个分组“未作任何修改地到达”了另一个分组“离开”的位置。这涵盖了
// 发生位置移动的两种形态：互换位置，以及删除靠前分组后导致的后续分组前移。如果在重排的同时还对同一
// 分组进行了编辑，则无法与两次普通编辑区分，因而不会被捕获——在此明确说明这一边界，以免后续读者
// 自行踩坑才发现此缺口。
//
// “离开”是关键前提，并不等同于“原先持有”。仍留在原位的分组并未发生任何移动，因此仅使其他位置变得
// 与之相似的编辑属于普通编辑而非移动。若缺少该条件，文档中记载的让分组退出服务的方式——不断收窄其
// nodeSelector 直至不再匹配任何节点——在两个仅选择器不同的分组中的第二个上将被拒绝，因为收窄第二个
// 分组的选择器会使其与第一个完全相同。实测表明：去掉该条件后，此类更新确实会被拒绝。
//
// 该条件的代价是会放行一种前移场景，且别无他法避免。当后面的某个分组与顶替被删位置的分组完全相同时
// ——例如 [A,B,C] 变为 [A,C,C]——请求会进入此处并被放行，而普通的 [A,B,C] 变为 [A,C] 则会被拒绝。
// 没有任何谓词能区分这两种情况：从相同的初始列表出发，“位置 1 被编辑为与未修改的位置 2 相同”和
// “位置 1 被删除且位置 2 被复制”产生的是完全相同的两个列表，请求中没有任何信息能将两者区分。至少
// 被放行的情况会留下明显痕迹——结果 spec 中会包含两个相同的分组，而被拒绝的形态则不会。
//
// 以下操作会被刻意放行，因为它们都是本 API 支持的操作：
//
//   - 在列表末尾追加分组，这也是为何从不检查超出旧列表长度的位置；
//   - 从列表末尾删除分组，这不会改变任何留存分组的位置；
//   - 原地编辑分组，包括放宽 nodeSelector 以纳管更多节点——放宽选择器唯一可能触发本规则的情况，
//     是使该分组变得与另一个位置已有的分组完全相同，这本质上是以编辑形式写出的重排操作。
//
// 这里的比较采用语义比较而非结构比较，因此仅在容量书写方式上不同的两个 spec——例如 1Gi 与 1024Mi——
// 会被视为同一个分组，而不会作为一次编辑逃过本规则的检查。
func validateKVCacheBackendMembersNotMoved(
	oldMembers, newMembers []workercore.KVCacheBackendMember, fldPath *field.Path,
) field.ErrorList {
	for i := range newMembers {
		if i >= len(oldMembers) {
			// 此前不存在的位置属于追加操作，追加不会移动任何分组。
			break
		}
		if kubemeta.DeepEqual(newMembers[i], oldMembers[i]) {
			continue
		}

		for j := range oldMembers {
			if j == i || !kubemeta.DeepEqual(newMembers[i], oldMembers[j]) {
				continue
			}
			if j < len(newMembers) && kubemeta.DeepEqual(newMembers[j], oldMembers[j]) {
				// 当前位置如今相似的分组并未离开位置 j，因此没有发生移动：这只是一次使两个位置变得相同的
				// 编辑。若拒绝它，就相当于因为另一个未发生变化的分组而禁止了对当前分组的编辑。
				continue
			}
			// 仅返回一条错误而非每个位置一条：一次互换会在两端同时触发，一次前移会在空缺后的每个位置
			// 触发，而它们对应的修复方式是同一处修改。
			return field.ErrorList{field.Forbidden(fldPath.Index(i), fmt.Sprintf(
				"this position now carries the group that was at position %d, which moves a group "+
					"rather than editing one. The position is a group's identity here: the DaemonSet "+
					"at this position keeps its name, its selector and its port, so its members would "+
					"be rebuilt against another group's spec and the cache they hold would go with "+
					"them. Append groups at the end and remove them from the end; to take a group out "+
					"of service in place, narrow its nodeSelector until it matches no node", j,
			))}
		}
	}

	return nil
}

// kvCacheBackendMaxConsumerNames 限制下方拒绝消息中列出的占用方名称数量上限。
const kvCacheBackendMaxConsumerNames = 20

// validateKVCacheBackendMultiTenancyWithdrawal 拒绝从已被其他资源占用的后端中撤销租户账本。
//
// 构建在启用了多租户的后端之上的 KVCachePool 会将其复用域注册到该 master 的账本中。Pool 的
// 准入校验对未启用多租户的后端仅发出警告，因此准入阶段不会阻止在已有已注册 Pool 的情况下撤回该
// 标志，而本规则正是负责拦截此操作。
//
// 撤回该标志损失的不仅是配额正确性（否则所有请求都会落入同一个默认租户，导致两个复用域互相读取
// 对方的块），还会影响退出清理：Pool 的 finalizer 需要在退出时从 master 释放其已注册的条目，而
// 没有账本的 master 无从释放。
//
// 占用记录读取自旧对象，因为这才是 API server 当前持有的状态：status 属于子资源，对 spec 的
// 更新请求会沿用既有的 status，无法在关闭该标志的同一个请求中篡改本规则的判断依据。
//
// 本规则直接依据原始列表而非仍可解析的占用记录执行拒绝，因为本处理器不持有客户端。因此，即使
// 列表中包含已不存在的 Pool 名称同样会被拒绝，运维人员需要从 status.usedBy 中清理该条目——这也
// 正是后端自身的 finalizer 用来阻止删除的同一份列表。
//
// 拒绝信息中会列出具体的占用方名称，因为更新被拒的运维人员需要知道应当去清理哪些对象。列表经过
// 采样截断，因为 usedBy 没有条目数量上限，而错误消息需要承载拒绝原因。
func validateKVCacheBackendMultiTenancyWithdrawal(
	oldKvcb, newKvcb *workercore.KVCacheBackend,
) field.ErrorList {
	oldManaged, newManaged := oldKvcb.Spec.Connection.Managed, newKvcb.Spec.Connection.Managed
	if oldManaged == nil || newManaged == nil {
		return nil
	}
	if !oldManaged.Leader.MultiTenancyEnabled() || newManaged.Leader.MultiTenancyEnabled() {
		return nil
	}
	if len(oldKvcb.Status.UsedBy) == 0 {
		return nil
	}

	// 在构造任何字符串之前先做截断，从而不仅限制最终提示语长度，也限制实际处理开销：如果准入路径先把
	// 列表完整渲染后再丢弃多余部分，其开销仍会随没有条目上限的 usedBy 线性增长。基于同样的原因，剩余
	// 条目数直接从原始切片计算。此处实现形态与协调器自身的 listBoundedNames 保持一致。
	sample := oldKvcb.Status.UsedBy
	if len(sample) > kvCacheBackendMaxConsumerNames {
		sample = sample[:kvCacheBackendMaxConsumerNames]
	}
	names := make([]string, 0, len(sample))
	for _, ref := range sample {
		names = append(names, ref.Kind+"/"+ref.Name)
	}
	consumers := strings.Join(names, ", ")
	if extra := len(oldKvcb.Status.UsedBy) - len(sample); extra > 0 {
		consumers = fmt.Sprintf("%s and %d more", consumers, extra)
	}

	return field.ErrorList{field.Forbidden(
		field.NewPath("spec", "connection", "managed", "leader", "multiTenancy"),
		fmt.Sprintf("multi-tenancy cannot be turned off while %s consume(s) this backend: the master "+
			"would hold no tenant ledger, every request would fall into one default tenant where two "+
			"reuse domains read each other's blocks, and the quota each consumer registered could no "+
			"longer be released — which leaves them undeletable. Remove them first",
			consumers),
	)}
}

// validateKVCacheBackendLocalDiskImmutable 冻结分组是否配置磁盘层及其所在路径，同时保留其可容纳
// 容量的可编辑性。
//
// 这样划分依据的是变更的代价。开启或关闭磁盘层，或者将其移动到其他目录，会使成员按旧配置写入的数据
// 失去索引而滞留——数据仍留在节点上却没有任何对象指向它。提高或降低容量上限仅会重新渲染一个环境变量，
// 已有机制会通过 Pod 指纹重启该分组，而磁盘层中的数据在重启后依然保留。
//
// 该校验按位置进行比较，这并非某种更优配对方式的近似替代：其他所有地方同样以位置来标识分组。
// MemberObjectName 依据位置派生 DaemonSet 的名称，Pod 归属关系检查也据此回读。因此，重排列表
// 或删除靠前的分组，会真实地重新定义其后的每一个位置——这些位置上的成员会按另一个分组的 spec 重建，
// 原有缓存也会随之丢失。在此处拒绝不可变字段的变更正是如实报告这一影响，而非误判。下方的错误消息
// 写明“在该位置”，以便执行了重排的运维人员能判断究竟触发了哪种情况。
//
// 条目按列表的第一个元素进行比较而非按路径配对，因为该列表最多只包含一个条目，且分组位置已经唯一
// 确定了它的归属。
func validateKVCacheBackendLocalDiskImmutable(
	oldDisks, newDisks []workercore.KVCacheBackendMemberLocalDisk, fldPath *field.Path,
) field.ErrorList {
	switch {
	case len(oldDisks) == 0 && len(newDisks) == 0:
		return nil
	case len(oldDisks) == 0:
		return field.ErrorList{field.Forbidden(fldPath,
			"a local disk tier cannot be added to the group at this position: its members would "+
				"have to restart to mount the host directory, and the leader would begin "+
				"offloading to a tier that no existing key is on. If you reordered members or "+
				"removed an earlier group, note that the position identifies a group here — the "+
				"members at this position now belong to a different group's spec")}
	case len(newDisks) == 0:
		return field.ErrorList{field.Forbidden(fldPath,
			"a local disk tier cannot be removed from the group at this position: whatever its "+
				"members have already written stays on their nodes with nothing addressing it. "+
				"Reordering members or removing an earlier group reaches this rule too, because "+
				"the position is what identifies a group")}
	case oldDisks[0].Path != newDisks[0].Path:
		return field.ErrorList{field.Forbidden(fldPath.Index(0).Child("path"),
			"the path is immutable: what the members at this position have already written stays "+
				"at the old one. Reordering members reaches this rule as well, since the position "+
				"is what identifies a group")}
	}

	return nil
}
