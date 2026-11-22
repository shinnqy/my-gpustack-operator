package worker

import (
	"context"
	"fmt"

	core "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrladmission "sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"gpustack.ai/gpustack/pkg/systemname"
	"gpustack.ai/gpustack/pkg/webhook"
	"gpustack.ai/gpustack/pkg/worker/kvcache/inject"
)

// Pod 用来请求 KV cache 的词汇表。触发器采用 LABEL，因为 objectSelector 只能匹配标签，
// 且它携带固定值，从而不会触及标签值 63 个字符的长度上限；所有可配置项均采用 ANNOTATION，
// 因为注解允许任意长度的名称。
const (
	// KVCacheInjectLabelKey 用于将 Pod 纳入注入范围。它是 mutating webhook 的 objectSelector，
	// 因此不带该标签的 Pod 根本不会到达此处理函数。
	KVCacheInjectLabelKey = "kvcache." + systemname.LabelPrefix + "inject"

	// KVCacheInjectLabelValue 是唯一表示启用的值。"false" 表示显式退出，移除该标签亦同。
	KVCacheInjectLabelValue = "true"

	// KVCacheBindingAnnotationKey 指定位于 Pod 自身命名空间中的 KVCachePoolBinding，
	// 该绑定在资源池上为此 Pod 配置授权——即容量和复用域，而非访问权限。不存在跨命名空间的形式。
	KVCacheBindingAnnotationKey = "kvcache." + systemname.LabelPrefix + "binding"

	// KVCacheEngineAnnotationKey 指定要配置的推理引擎。它是必填项，绝不根据镜像猜测：
	// 不同引擎接受不同的参数标志，错误的猜测会导致容器正常启动却完全不进行缓存。
	KVCacheEngineAnnotationKey = "kvcache." + systemname.LabelPrefix + "engine"

	// KVCacheManufacturerAnnotationKey 为 vLLM 选择厂商运行时变体。它是工作负载作者的声明，
	// 而非本 webhook 已对照 Pod 最终调度节点验证过的断言。目前仅接受经过实测的 Ascend 变体；
	// 错误的声明会被拒绝，而不会尝试猜测。
	KVCacheManufacturerAnnotationKey = "kvcache." + systemname.LabelPrefix + "manufacturer"

	// KVCacheRoleAnnotationKey 声明 prefill/decode 角色。不设置是合法的，表示调用方没有此类拆分。
	KVCacheRoleAnnotationKey = "kvcache." + systemname.LabelPrefix + "role"

	// KVCacheContainerAnnotationKey 指定要注入的目标容器名称。仅当 Pod 包含多个容器时才为必填，
	// 因为若默认选取第一个容器，在工作负载运行于其他容器时注入可能会误落入 sidecar 中。
	KVCacheContainerAnnotationKey = "kvcache." + systemname.LabelPrefix + "container"

	// KVCacheLaunchArgsForwardedAnnotationKey 允许容器作者声明某个未识别的启动器或脚本会将
	// 追加的参数转发给引擎。它不能覆盖 webhook 已知会丢失这些参数的启动方式，例如 shell 的命令模式。
	KVCacheLaunchArgsForwardedAnnotationKey = "kvcache." + systemname.LabelPrefix + "launch-args-forwarded"

	// KVCacheDomainAnnotationKey 是一个被拒绝（REFUSED）的键，而非未识别的键。复用域只能来自
	// Binding，因此在此处指定复用域的 Pod 会被拒绝，而不是静默忽略——针对不存在的后门机制
	// 编写的清单应当在作者可见的位置明确失败。
	KVCacheDomainAnnotationKey = "kvcache." + systemname.LabelPrefix + "domain"
)

// PodKVCacheWebhook 将推理引擎使用 KV cache 池所需的客户端配置注入到所有通过
// KVCacheInjectLabelKey 标签启用的 Pod 中。CREATE 阶段执行 Mutating：向指定的单个容器
// 追加 env、args、volume 和 mount，并在 Pod 上标记其执行的操作。这一半所做的每项检查
// 都是拒绝变更本身的理由，因此它从 Default 而非 validating 处理函数报告拒绝。
//
// UPDATE 阶段执行 Validating，这一半回答的是另一个问题。mutating 一半只做一次决策，
// 而承载该决策的键在 Pod 存续期间默认仍是可编辑的。validating 一半会冻结这些键——
// 即启用标签、注入记录以及容器投射的客户端配置——从而保证本 webhook 对 Pod 写入的内容
// 在 Pod 的整个生命周期内持续为真，而不仅仅在准入那一刻有效。这相同的三个键在 CREATE
// 阶段作为输入也会被 checkAnnotationVocabulary 和解析路径拒绝；此处则是将该契约延续到准入之后。
//
// ReceiveDeletionUpdate 使得"Pod 的生命周期"包含其终止过程。如果没有该标记，
// 共享配置会在校验器外层包裹一个守卫，只要 Pod 带有 deletionTimestamp 就直接返回成功，
// 从而导致这三个键在整个宽限期内都可被编辑——而在此期间容器仍在运行，kubelet 仍会依据
// 注解重新投射客户端配置。该守卫原本是为了防止依赖可能已消失状态的校验阻塞清除 finalizer 的
// 更新；而本校验器除传入的两个对象外不读取任何外部状态，且清除 finalizer 不会修改上述三个
// 键中的任何一个，因此无论如何都会被准入。
//
// UPDATE 路径无法保障的方面（每项缺口都是实际可达而非假设的）：
//
//   - 其 failurePolicy 为 Ignore（相对于 mutating 一半的 Fail），因此当 webhook 不可达时，
//     所有本应被拒绝的编辑都会被准入。这一选择是有意为之：该守卫用于保证记录真实，
//     绝不能成为阻塞在线工作负载更新或结束的原因。若设为 Fail，不可达的 webhook 会阻塞
//     所有已启用 Pod 的 finalizer 移除，而本特性自身的清理流程恰恰要等待这一点——
//     当 status.usedBy 非空时 Binding 的 finalizer 会保持锁定，而在其 Pod 无法完成删除前
//     status.usedBy 无法清空。漏掉一次拒绝最多导致一条错误记录；卡死删除则会拖垮整个资源池。
//   - 其 objectSelector 为启用标签，因此从未携带该标签的 Pod 不会被看到，且可以将两个
//     注解的伪造值写入此类 Pod。这些注解在那里不起作用——没有 volume 投射配置，也没有
//     容器读取它——但读取该记录的操作人员看到的将是伪造信息。
//   - 它仅冻结元数据。mutating 一半写入的 volume、mount、env 和 args 位于 Pod spec 中，
//     而此路径不会读取或比对它们。
//
// 它是独立于 PodWebhook 之外的第二对 Pod webhook，两者在设计上相互独立：
// PodWebhook 由 Kueue queue-name 标签触发并重写容器资源，而本 webhook 由自身标签触发且
// 从不修改资源。一个 Pod 可以同时携带两个标签、都不携带，或只携带其中一个。
//
// reinvocationPolicy 为 Never。本 webhook 会拒绝目标容器中已包含其所管理键的 Pod，
// 因此对其自身输出再执行一次调用将会拒绝刚刚配置好的 Pod。
//
// nolint: lll
// +k8s:webhook-gen:mutating:group="",version="v1",resource="pods",scope="Namespaced"
// +k8s:webhook-gen:mutating:operations=["CREATE"],failurePolicy="Fail",sideEffects="None",matchPolicy="Equivalent",reinvocationPolicy="Never",timeoutSeconds=10
// +k8s:webhook-gen:mutating:objectSelector={"matchExpressions":[{"key":"kvcache.gpustack.ai/inject","operator":"In","values":["true"]}]}
// +k8s:webhook-gen:mutating:namePrefix="gpustack-worker-kvcache"
// +k8s:webhook-gen:validating:group="",version="v1",resource="pods",scope="Namespaced"
// +k8s:webhook-gen:validating:operations=["UPDATE"],failurePolicy="Ignore",sideEffects="None",matchPolicy="Equivalent",timeoutSeconds=10
// +k8s:webhook-gen:validating:objectSelector={"matchExpressions":[{"key":"kvcache.gpustack.ai/inject","operator":"In","values":["true"]}]}
// +k8s:webhook-gen:validating:namePrefix="gpustack-worker-kvcache"
type PodKVCacheWebhook struct {
	Client    ctrlcli.Client
	APIReader ctrlcli.Reader
}

func (r *PodKVCacheWebhook) SetupWebhook(_ context.Context, opts webhook.SetupOptions) (runtime.Object, error) {
	r.Client = opts.Manager.GetClient()
	r.APIReader = opts.Manager.GetAPIReader()

	return &core.Pod{}, nil
}

var (
	_ ctrladmission.Defaulter[runtime.Object] = (*PodKVCacheWebhook)(nil)
	_ ctrladmission.Validator[runtime.Object] = (*PodKVCacheWebhook)(nil)
	_ webhook.ReceiveDeletionUpdate           = (*PodKVCacheWebhook)(nil)
)

// ReceiveDeletionUpdate 使该 webhook 在 Pod 正在被删除时仍能接收更新校验。详见类型注释：在默认保护机制
// 本应将其忽略的那一刻，该冻结机制反而最不该失效。
//
// 它也会让变更（mutating）部分绕过同一保护，但这在外部不会产生任何可观测变化。变更部分仅注册于
// CREATE 操作，而创建请求从不带有删除时间戳。
func (r *PodKVCacheWebhook) ReceiveDeletionUpdate() {}

func (r *PodKVCacheWebhook) Default(ctx context.Context, obj runtime.Object) error {
	pod, ok := obj.(*core.Pod)
	if !ok {
		return fmt.Errorf("expected a Pod, got %T", obj)
	}

	// 注入仅在 CREATE 时执行一次。防止其决策在事后被修改的是下方的 ValidateUpdate，而非此处逻辑。
	//
	// objectSelector 已将请求过滤为携带可接受值选择加入标签的 Pod，因此这里属于纵深防御而非主防线。
	// 保留此检查是因为放宽该选择器只需修改生成文件中的一行，且规范明确指出了此类回退的风险：
	// 若无此检查，放宽选择器会让集群中的每个 Pod 都进入解析流程，而 fail-closed 的 webhook 会导致整个集群停摆。
	// 有了此检查，这类失误的影响范围就被限制为空操作。
	if pod.Labels[KVCacheInjectLabelKey] != KVCacheInjectLabelValue {
		return nil
	}

	res, err := r.resolve(ctx, pod)
	if err != nil {
		return err
	}

	out, err := inject.Render(res.Input)
	if err != nil {
		return err
	}

	// 至此所有拒绝检查均已执行完毕，因此 Pod 要么被完整注入，要么保持原样。半注入状态的 Pod（例如
	// 已添加卷但缺少环境变量）启动后会产生任何字段都无法解释的行为。
	return r.injectPod(pod, res, out)
}

// ValidateCreate 未注册：CREATE 阶段由变更部分负责，它在 Default 中直接拒绝，因为其每项检查都是
// 不应执行变更的理由。
func (r *PodKVCacheWebhook) ValidateCreate(
	_ context.Context, _ runtime.Object,
) (ctrladmission.Warnings, error) {
	return nil, nil
}

func (r *PodKVCacheWebhook) ValidateUpdate(
	_ context.Context, oldObj, newObj runtime.Object,
) (ctrladmission.Warnings, error) {
	oldPod, ok := oldObj.(*core.Pod)
	if !ok {
		return nil, fmt.Errorf("expected a Pod, got %T", oldObj)
	}
	newPod, ok := newObj.(*core.Pod)
	if !ok {
		return nil, fmt.Errorf("expected a Pod, got %T", newObj)
	}

	if errs := validatePodKVCacheOwnedMetadata(oldPod, newPod); len(errs) > 0 {
		return nil, kerrors.NewInvalid(
			core.SchemeGroupVersion.WithKind("Pod").GroupKind(), newPod.Name, errs,
		)
	}

	return nil, nil
}

// ValidateDelete 未注册。Pod 删除时无需重新检查 KVCache 决策：授权归属于 Binding 且生命周期长于 Pod，
// 而在此拒绝删除反而可能导致工作负载无法正常结束。
func (r *PodKVCacheWebhook) ValidateDelete(
	_ context.Context, _ runtime.Object,
) (ctrladmission.Warnings, error) {
	return nil, nil
}

// podKVCacheOwnedAnnotations 是该 webhook 写入的两个键，以及修改它们各自的错误提示。
// 冻结它们的原因与在 CREATE 时拒绝传入值相同——它们用于记录已做出的决策而非承载输入——且两者的原因不同，
// 因此各自配有说明。
var podKVCacheOwnedAnnotations = []struct {
	key    string
	reason string
}{
	{
		key: KVCacheInjectedAnnotationKey,
		reason: "this annotation is written by this webhook and may not be edited: it is the record " +
			"of what admission decided, nothing re-derives it, and an edited value is a record of a " +
			"decision nobody made. Change the kvcache annotations where the Pod is defined and let " +
			"the Pod be replaced",
	},
	{
		key: inject.ClientConfigAnnotationKey,
		reason: "this annotation is written by this webhook and may not be edited: the container " +
			"reads it as a downwardAPI projection at " + inject.ConfigFilePath + ", so editing it " +
			"swaps the master address, transport and segment sizes under a running container with " +
			"nothing recording that they moved. It is rendered from the Binding this Pod names and " +
			"the pool behind it, so change those and let the Pod be replaced",
	},
}

// validatePodKVCacheOwnedMetadata 校验准入阶段为 Pod 整个生命周期决定的三个键。
//
// 它同时比较键是否存在以及键值是否变化，因此删除键与修改键会被同等拒绝。若仅读取键值，
// 删除操作会被误判为修改成空字符串而放行。
//
// 其他所有元数据修改均不干预，这是设计要求而非副作用：已选择加入的 Pod 是运行中的工作负载，
// 其标签、其他注解和 finalizer 归属于管理它的上层组件。
func validatePodKVCacheOwnedMetadata(oldPod, newPod *core.Pod) field.ErrorList {
	oldOptedIn := oldPod.Labels[KVCacheInjectLabelKey] == KVCacheInjectLabelValue
	newOptedIn := newPod.Labels[KVCacheInjectLabelKey] == KVCacheInjectLabelValue

	// 只要新旧对象中任一方携带该标签，objectSelector 就会匹配，因此任意方向的变更都会进入此处，
	// 而未使用该特性的 Pod 不会到达这里。新旧双方均未选择加入的情况只会在选择器被放宽时出现，
	// 其处理方式与变更部分一致。
	if !oldOptedIn && !newOptedIn {
		return nil
	}

	var errs field.ErrorList

	labelPath := field.NewPath("metadata", "labels").Key(KVCacheInjectLabelKey)
	switch {
	case !oldOptedIn && newOptedIn:
		errs = append(errs, field.Forbidden(labelPath,
			"the opt-in label is immutable: injection runs at CREATE and only there, so adding this "+
				"label to an existing Pod gives it the label that says it uses a cache and none of "+
				"the configuration - no volume, no mount, no environment. Add the label where the "+
				"Pod is defined and let the Pod be replaced"))
	case oldOptedIn && !newOptedIn:
		errs = append(errs, field.Forbidden(labelPath,
			"the opt-in label is immutable: nothing un-injects a Pod, so the volume, mount, "+
				"environment and arguments this webhook wrote are still on this one, and dropping "+
				"the label would leave the container configured for a cache while the Pod no longer "+
				"says it is. Remove the label where the Pod is defined and let the Pod be replaced"))
	}

	annotationsPath := field.NewPath("metadata", "annotations")
	for _, owned := range podKVCacheOwnedAnnotations {
		oldValue, hadOld := oldPod.Annotations[owned.key]
		newValue, hasNew := newPod.Annotations[owned.key]
		if hadOld == hasNew && oldValue == newValue {
			continue
		}
		errs = append(errs, field.Forbidden(annotationsPath.Key(owned.key), owned.reason))
	}

	return errs
}
