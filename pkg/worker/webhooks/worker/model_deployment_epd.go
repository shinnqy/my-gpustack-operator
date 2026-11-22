package worker

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation/field"

	worker "gpustack.ai/gpustack/api/worker/v1"
	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	workerctrl "gpustack.ai/gpustack/pkg/worker/controllers/worker"
	"gpustack.ai/gpustack/pkg/worker/kvcache/inject"
)

// modelDeploymentDeclaresEncode 报告部署是否声明了 Encode role；一旦声明，即构成
// encoder/prefill/decode 部署并触发下列规则。未声明该 role 的部署仍沿用原有全部规则。
func modelDeploymentDeclaresEncode(md *workercore.ModelDeployment) bool {
	return modelDeploymentRoleIndexOfKind(md, workercore.ModelDeploymentRoleKindEncode) != -1
}

// modelDeploymentRoleIndexOfKind 返回该类型唯一 role 所在的索引位置，若部署未声明则返回 -1。
// 拒绝错误会定位到该索引，因为它是用户实际编写的字段。
func modelDeploymentRoleIndexOfKind(
	md *workercore.ModelDeployment, kind workercore.ModelDeploymentRoleKind,
) int {
	for i := range md.Spec.Roles {
		if workerctrl.ModelDeploymentEffectiveRoleKind(&md.Spec.Roles[i]) == kind {
			return i
		}
	}

	return -1
}

// modelDeploymentRoleClaimMount 返回挂载声明（claim）的 additionalVolumes 条目，以及该 role 是否挂载了声明。
// 只需取出一个即可用于比对：挂载两个声明的 role 无法形成单一共享目录，
// 拒绝这种情况的正是校验编码器与预填充器挂载配置不一致的规则。
func modelDeploymentRoleClaimMount(
	role *workercore.ModelDeploymentRole,
) (*workercore.ModelDeploymentAdditionalVolume, bool) {
	for i := range role.AdditionalVolumes {
		if role.AdditionalVolumes[i].PersistentVolumeClaim != nil {
			return &role.AdditionalVolumes[i], true
		}
	}

	return nil, false
}

// modelDeploymentRouterName 返回部署指定的路由器名称，若未指定则返回空字符串。
func modelDeploymentRouterName(md *workercore.ModelDeployment) string {
	if md.Spec.Router == nil {
		return ""
	}

	return md.Spec.Router.Name
}

// validateModelDeploymentEPD 汇总仅适用于声明了 Encode role 的部署的所有规则。
// 未声明该 role 的部署不会返回任何错误，从而保证其他形态的渲染保持不变。
//
// 该校验在每次创建和每次更新时都会运行，不对这些规则出现前已存储的对象提供豁免。
// 其他后增规则之所以保留豁免，是因为当时规则尚不存在而对象已入库，拒绝其后续编辑会导致对象卡死。
// 此处不存在此类历史对象：在这些规则准入之前，Encode role 本就会被准入阶段拒绝，因此第一个到达这里的对象
// 必然已经能够满足这些规则。在每次更新时重新检查，也能防止编辑操作将已准入的部署移出支持矩阵，
// 因为平台、引擎和路由器都会根据提交的对象重新读取。
func (r *ModelDeploymentWebhook) validateModelDeploymentEPD(
	ctx context.Context, md *workercore.ModelDeployment,
) (field.ErrorList, error) {
	if md.DeletionTimestamp != nil || !modelDeploymentDeclaresEncode(md) {
		return nil, nil
	}

	errs := validateModelDeploymentEPDShape(md)

	platform, platformErrs, err := r.modelDeploymentEPDPlatform(ctx, md)
	if err != nil {
		return nil, err
	}
	if len(platformErrs) > 0 {
		// 支持矩阵按平台检索，因此未能确定平台时就无从查表，再报告第二条相关错误只是重复第一条的内容。
		return append(errs, platformErrs...), nil
	}

	router := modelDeploymentRouterName(md)
	if gap := workerctrl.ModelDeploymentEPDMatrixGap(router, md.Spec.Engine.Name, platform); gap != "" {
		return append(errs, field.Invalid(field.NewPath("spec", "engine", "name"), md.Spec.Engine.Name, fmt.Sprintf(
			"%s; an encoder role is admitted only for a combination in the admitted matrix, and the "+
				"admitted rows are %s", gap, workerctrl.ModelDeploymentEPDMatrixSummary(),
		))), nil
	}

	// 上方的缺口检查已经拒绝了所有无法适配的组合，因此此处必然能取到行。
	// 显式保留第二个返回值而非丢弃，是为了防止未来修改将缺口检查移走时，未察觉它已不再保证该行存在。
	row, admitted := workerctrl.ModelDeploymentEPDRowFor(router, md.Spec.Engine.Name, platform)
	if !admitted {
		return append(errs, field.Invalid(field.NewPath("spec", "engine", "name"),
			md.Spec.Engine.Name, "no admitted row for the combination the gap check reported one "+
				"for; this is the operator's own tables disagreeing, not a field to correct")), nil
	}

	if row.SharedVolume {
		errs = append(errs, validateModelDeploymentEPDSharedVolume(md, row)...)
	}
	errs = append(errs, validateModelDeploymentEPDSideChannelPort(md, row)...)

	return errs, nil
}

// validateModelDeploymentEPDShape 存放仅依赖对象本身即可判定的三个 role 及其周边字段规则。
//
// 每种类型最多一个 role 已在上方由禁止重复非 Server 类型的规则拒绝，Encode 与 Server 并存的情况亦然。
// 此处补充的是该规则看不到的组合：单独存在一个 Encode role，却没有任何下游 role 可以承接其输出。
func validateModelDeploymentEPDShape(md *workercore.ModelDeployment) field.ErrorList {
	var errs field.ErrorList

	rolesPath := field.NewPath("spec", "roles")
	encodeIdx := modelDeploymentRoleIndexOfKind(md, workercore.ModelDeploymentRoleKindEncode)
	encodePath := rolesPath.Index(encodeIdx).Child("kind")

	// 编码器只有一个读取方。部署若包含 Encode role 却没有 Prefill，编码器生成的嵌入向量将没有任何 role 消费；
	// 若没有 Decode，则预填充器的输出同样没有消费者，而这正是该形态所要扩展的 prefill/decode 链路。
	for _, kind := range []workercore.ModelDeploymentRoleKind{
		workercore.ModelDeploymentRoleKindPrefill, workercore.ModelDeploymentRoleKindDecode,
	} {
		if modelDeploymentRoleIndexOfKind(md, kind) >= 0 {
			continue
		}

		errs = append(errs, field.Required(encodePath, fmt.Sprintf(
			"an %q role needs a %q role beside it, and none is declared: %s",
			workercore.ModelDeploymentRoleKindEncode, kind, modelDeploymentEPDSiblingReason(kind),
		)))
	}

	if md.Spec.Router == nil {
		errs = append(errs, field.Required(field.NewPath("spec", "router"),
			"an Encode role requires a router: without one nothing pairs the roles, and the "+
				"operator does not render an unpaired shape"))
	}

	if md.Spec.KVCache != nil {
		errs = append(errs, field.Forbidden(field.NewPath("spec", "kvCache"), fmt.Sprintf(
			"spec.kvCache is refused on a deployment with an %q role: the field is deployment-wide "+
				"and would attach every role to one pool, but the encoder keeps no KV cache at all, "+
				"so it has nothing to place in that pool",
			workercore.ModelDeploymentRoleKindEncode,
		)))
	}

	for i := range md.Spec.Roles {
		role := &md.Spec.Roles[i]
		if len(role.Command) > 0 {
			errs = append(errs, field.Forbidden(rolesPath.Index(i).Child("command"), fmt.Sprintf(
				"role %q is one of three roles the operator pairs through a router, and a command "+
					"replaces the managed arguments and the routing sidecar: the roles would keep "+
					"their kinds and stop talking to each other", role.Name,
			)))
		}
	}

	errs = append(errs, validateModelDeploymentEPDEncodeShape(md, encodeIdx)...)

	return errs
}

// modelDeploymentEPDSiblingReason 说明为何必须与 Encode role 同时声明某种 role 类型。
// 每种类型都有各自独立的提示语，因为合用一句话会在提示缺少解码器时同时提及预填充器，
// 让读者自己去分辨究竟缺少了哪一半。
func modelDeploymentEPDSiblingReason(kind workercore.ModelDeploymentRoleKind) string {
	if kind == workercore.ModelDeploymentRoleKindPrefill {
		return "the encoder produces embeddings that only a prefiller reads"
	}

	return fmt.Sprintf("the prefiller's output needs a %q role to generate from",
		workercore.ModelDeploymentRoleKindDecode)
}

// validateModelDeploymentEPDEncodeShape 存放仅针对 Encode role 本身的规则。
func validateModelDeploymentEPDEncodeShape(md *workercore.ModelDeployment, encodeIdx int) field.ErrorList {
	var errs field.ErrorList

	role := &md.Spec.Roles[encodeIdx]
	rolePath := field.NewPath("spec", "roles").Index(encodeIdx)

	if role.ReplicaSize != 1 {
		errs = append(errs, field.Invalid(rolePath.Child("replicaSize"), role.ReplicaSize,
			fmt.Sprintf(
				"role %q runs one instance per Pod, so its replica size is 1: the operator places "+
					"one encoder per member and a second instance would produce embeddings for a "+
					"transfer key the prefiller never asks for", role.Name,
			)))
	}

	if role.ElasticEP != nil {
		errs = append(errs, field.Forbidden(rolePath.Child("elasticEp"),
			fmt.Sprintf(
				"role %q cannot carry the elastic-EP profile: that profile admits a single Server "+
					"role of one instance of one Pod, and scales the group holding it, while an "+
					"encoder is a fixed producer whose withdrawal would cut transfers already in "+
					"flight", role.Name,
			)))
	}

	// 外部数据并行（external data parallelism）会将响应请求的成员移至引擎进程之外，
	// 由 Operator 在准入后从 Node 中发现。编码器不存在这一问题：
	// 它生成的是嵌入向量而非最终响应，其输出由单个副本寻址。
	status := workerctrl.ReadModelDeploymentRoleParallelism(md.Spec.Engine.Name, role)
	if status.LoadBalance == workercore.ModelDeploymentLoadBalanceExternal ||
		status.LoadBalance == workercore.ModelDeploymentLoadBalanceHybrid {
		errs = append(errs, field.Forbidden(rolePath.Child("extraArgs"), fmt.Sprintf(
			"role %q declares a data parallelism the engine balances outside itself, and passes %v: "+
				"an encoder has one producer and one output key, so there is no per-member endpoint "+
				"to balance across", role.Name, role.ExtraArgs,
		)))
	}

	return errs
}

// modelDeploymentEPDPlatform 返回所有 role 运行所在的唯一加速卡平台，以及阻止确定平台的拒绝错误列表。
//
// 平台从每个 role 的 InstanceType 中读取而非直接声明，因为 role 指定了类型，而类型上报了厂商信息。
// 混合平台的结果会被拒绝而非自行折中，因为连接器和传输协议都跟随加速卡类型，单个部署无法在两条腿上同时混用两种平台。
// 厂商信息为空代表尚未同步的状态，而非一种独立平台，因此同样予以拒绝。
func (r *ModelDeploymentWebhook) modelDeploymentEPDPlatform(
	ctx context.Context, md *workercore.ModelDeployment,
) (string, field.ErrorList, error) {
	var errs field.ErrorList

	rolesPath := field.NewPath("spec", "roles")
	seen := make(map[string]*worker.InstanceType, len(md.Spec.Roles))

	platform := ""
	mixed := make([]string, 0, 2)
	for i := range md.Spec.Roles {
		role := &md.Spec.Roles[i]
		if role.InstanceType == "" {
			continue
		}

		instType, ok := seen[role.InstanceType]
		if !ok {
			var err error
			if instType, err = r.getInstanceType(ctx, role.InstanceType, i); err != nil {
				var fieldErr *field.Error
				if !errors.As(err, &fieldErr) || fieldErr.Type == field.ErrorTypeInternal {
					return "", nil, err
				}
				errs = append(errs, fieldErr)

				continue
			}
			seen[role.InstanceType] = instType
		}

		manufacturer := instType.Status.Detail.Manufacturer
		if manufacturer == "" {
			errs = append(errs, field.Invalid(rolesPath.Index(i).Child("instanceType"), role.InstanceType,
				fmt.Sprintf(
					"instance type %q reports no accelerator platform yet: an encoder role is admitted "+
						"per platform, because the connector it needs follows the accelerator, so the "+
						"platform has to be known before this deployment can be judged", role.InstanceType,
				)))

			continue
		}

		if platform == "" {
			platform = manufacturer

			continue
		}
		if platform != manufacturer && !slices.Contains(mixed, manufacturer) {
			mixed = append(mixed, manufacturer)
		}
	}

	if len(mixed) > 0 {
		return "", append(errs, field.Invalid(field.NewPath("spec", "roles"), md.Spec.Roles, fmt.Sprintf(
			"the roles of this deployment run on more than one accelerator platform: one reports %q "+
				"and another reports %q. The encoder leg follows the accelerator, so one deployment "+
				"cannot carry it across two",
			platform, strings.Join(mixed, ", "),
		))), nil
	}

	if platform == "" && len(errs) == 0 {
		return "", append(errs, field.Required(rolesPath,
			"no role names an instance type, so the accelerator platform the encoder admission needs cannot be read")), nil
	}

	return platform, errs, nil
}

// validateModelDeploymentEPDSideChannelPort 拒绝自身 HTTP 端口与其侧信道所需端口冲突的 encoder。
//
// 冲突发生在同一容器内的两个监听方之间。NIXL 生产者在提供 HTTP 服务的同一容器内绑定
// 侧信道端口上的 ZMQ ROUTER socket，因此若 encoder 在该端口提供服务，引擎自身的服务端
// 就会占据 consumer 用于获取 embeddings 的 socket。两者必有一方绑定失败，且哪一方
// 失败并不确定——表现为一个阶段始终无法启动，或 consumer 连上了 HTTP 服务，且双方
// 日志中都不会点明端口冲突。
//
// 两种写法都会被拒绝，因为一个 role 有两种指定服务端口的方式，而渲染逻辑两种都会采纳。
// 显式声明的 `ports` 条目是 operator 渲染到引擎 --port 参数中的值，且优先级更高：
// 声明了端口的 role 会被固定使用其中的第一个端口（`modelDeploymentTargetPort`），
// 而 role 仍可能同时传入 --port 并声明端口。若只检查启动参数，就会放过声明字段的写法，
// 导致拒绝报错时指向一个渲染阶段根本不会使用的端口号。
//
// 只有网络传输行可能发生冲突，因为侧信道仅由网络 connector 渲染。
// 共享目录行通过挂载路径传递 embeddings，不会绑定第二个 socket，因此其 encoder 可以
// 自由使用该端口。故此规则保留在矩阵校验旁，而不并入按设计对厂商无感知的
// `modelDeploymentRouterReservedPorts`，以免误伤共享目录行。
//
// 两者均未指定时 role 会使用默认端口，该端口不是此侧信道端口，因此无需拒绝。
func validateModelDeploymentEPDSideChannelPort(
	md *workercore.ModelDeployment, row workerctrl.ModelDeploymentEPDRow,
) field.ErrorList {
	if row.Connector != workerctrl.ModelDeploymentECConnectorCPU {
		return nil
	}

	encodeIdx := modelDeploymentRoleIndexOfKind(md, workercore.ModelDeploymentRoleKindEncode)
	if encodeIdx < 0 {
		return nil
	}

	role := &md.Spec.Roles[encodeIdx]
	rolesPath := field.NewPath("spec", "roles").Index(encodeIdx)
	refusal := func(path *field.Path, value any) field.ErrorList {
		return field.ErrorList{field.Invalid(path, value, fmt.Sprintf(
			"role %q serves HTTP on port %d, which is the port %s binds its side channel on: the "+
				"engine would serve on the socket every consumer dials for embeddings, and one of "+
				"the two fails to bind",
			role.Name, inject.VLLMECSideChannelPort, row.Connector,
		))}
	}

	// 优先读取显式声明的端口集合，因为渲染逻辑优先采用它：只要 role 声明了端口，
	// 启动参数就会受其约束而不再被单独采信。
	var errs field.ErrorList
	for _, port := range role.Ports {
		if port.Port == inject.VLLMECSideChannelPort {
			errs = append(errs, refusal(rolesPath.Child("ports"), port.Port)...)
		}
	}

	if listen, ok := workerctrl.ModelDeploymentRoleListenPort(md.Spec.Engine.Name, role); ok &&
		listen == inject.VLLMECSideChannelPort {
		errs = append(errs, refusal(rolesPath.Child("extraArgs"),
			fmt.Sprintf("--port=%d", listen))...)
	}

	return errs
}

// validateModelDeploymentEPDSharedVolume 拒绝无法同时被两个 role 读取的 encoder 链路。
//
// 这三个字段需要一并比较，因为 connector 只会被告知一个目录路径，两个 role 必须最终
// 指向同一目录。仅比较声明名称（claim）还不够：两个 role 可能将同一个 claim 挂载到
// 不同的子路径下，从而指向两个不同的目录，而 encoder 写入的文件只会落在其中之一。
//
// encoder 自身的挂载禁止设为只读，因为 encoder 需要写入被请求的 embeddings，只读挂载
// 会导致写入在运行时失败，而非在准入阶段被拦截。
func validateModelDeploymentEPDSharedVolume(
	md *workercore.ModelDeployment, row workerctrl.ModelDeploymentEPDRow,
) field.ErrorList {
	var errs field.ErrorList

	rolesPath := field.NewPath("spec", "roles")
	encodeIdx := modelDeploymentRoleIndexOfKind(md, workercore.ModelDeploymentRoleKindEncode)
	prefillIdx := modelDeploymentRoleIndexOfKind(md, workercore.ModelDeploymentRoleKindPrefill)

	// 该规则需要读取两个 role，因此当任一 role 缺失或 encoder 并非此行所描述的
	// 单实例时，没有可供比较的对象。上方的形态规则已经拒绝了这两种情况，在此再次拒绝
	// 只会对同一个缺失的 role 重复报错。
	if prefillIdx < 0 || md.Spec.Roles[encodeIdx].ReplicaSize != 1 {
		return nil
	}

	encodePath := rolesPath.Index(encodeIdx).Child("additionalVolumes")
	encode := &md.Spec.Roles[encodeIdx]

	encodeMount, ok := modelDeploymentRoleClaimMount(encode)
	if !ok {
		return append(errs, field.Required(encodePath, fmt.Sprintf(
			"role %q is the encoder-cache producer on %s, and %s carries its output through a "+
				"directory rather than over the network: attach the shared claim the prefiller mounts "+
				"as an additionalVolumes entry naming a persistentVolumeClaim",
			encode.Name, row.Platform, row.Connector,
		)))
	}

	if encodeMount.ReadOnly {
		errs = append(errs, field.Forbidden(encodePath.Child("readOnly"), fmt.Sprintf(
			"role %q writes the embeddings %s carries, so its mount must not be read-only: the "+
				"encoder's write would fail once the Pod is running, with nothing in the object to say "+
				"so", encode.Name, row.Connector,
		)))
	}

	prefillPath := rolesPath.Index(prefillIdx).Child("additionalVolumes")
	prefillMount, ok := modelDeploymentRoleClaimMount(&md.Spec.Roles[prefillIdx])
	if !ok {
		return append(errs, field.Required(prefillPath, fmt.Sprintf(
			"role %q reads the embeddings %s carries out of the directory role %q writes them to, "+
				"and mounts nothing: attach the same claim role %q mounts",
			md.Spec.Roles[prefillIdx].Name, row.Connector, encode.Name, encode.Name,
		)))
	}

	for _, mismatch := range []struct {
		// name 是两个 role 必须保持拼写一致的字段名。
		name string
		// value 是 encoder 自身条目在该字段上的取值。
		value string
		// equal 报告两个条目在该字段上是否一致。
		equal bool
	}{
		{
			"claimName", encodeMount.PersistentVolumeClaim.ClaimName,
			encodeMount.PersistentVolumeClaim.ClaimName == prefillMount.PersistentVolumeClaim.ClaimName,
		},
		{"subPath", encodeMount.SubPath, encodeMount.SubPath == prefillMount.SubPath},
		{"mountPath", encodeMount.MountPath, encodeMount.MountPath == prefillMount.MountPath},
	} {
		if mismatch.equal {
			continue
		}

		errs = append(errs, field.Invalid(
			modelDeploymentEPDVolumeMismatchField(encodePath, mismatch.name), mismatch.value, fmt.Sprintf(
				"role %q and role %q must name the same %s for %s to carry the encoder's output between "+
					"them, and they do not",
				encode.Name, md.Spec.Roles[prefillIdx].Name, mismatch.name, row.Connector,
			),
		))
	}

	return errs
}

// modelDeploymentEPDVolumeMismatchField 返回报告共享挂载不匹配时对应的字段路径。
//
// 在这三种不匹配情况下用户需要修改的字段各不相同，因此需逐一查找而非硬编码单个名称：
// claim 名称位于承载它的卷源之下，而另外两个挂载字段位于其旁，与上方 claim 源规则的
// 结构一致。若把三种错误都报在同一个字段上，会误导用户去修改本就正确的字段，而漏掉
// 真正出错的字段。
func modelDeploymentEPDVolumeMismatchField(volumesPath *field.Path, name string) *field.Path {
	if name == "claimName" {
		return volumesPath.Child("persistentVolumeClaim", "claimName")
	}

	return volumesPath.Child(name)
}
