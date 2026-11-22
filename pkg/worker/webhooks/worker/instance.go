package worker

import (
	"context"
	"fmt"
	"path"
	"slices"
	"strings"

	core "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrladmission "sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	worker "gpustack.ai/gpustack/api/worker/v1"
	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/kubemeta"
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/setting"
	"gpustack.ai/gpustack/pkg/utils/ctrlclix"
	"gpustack.ai/gpustack/pkg/utils/quantityx"
	"gpustack.ai/gpustack/pkg/webhook"
	workerctrl "gpustack.ai/gpustack/pkg/worker/controllers/worker"
	"gpustack.ai/gpustack/pkg/worker/kuberess"
	"gpustack.ai/gpustack/pkg/worker/settings"
)

// InstanceWebhook 拦截并处理 v1alpha1.Instance 对象。
//
// 它是当前包中唯一保留共享删除保护的处理器，原因在于其变更逻辑而非校验逻辑：
// Default 会读取所引用的 InstanceType，若后者已不存在则会拒绝请求；其注册配置的
// UPDATE failurePolicy 为 Fail，且 Instance 带有系统 finalizer——因此若某个
// InstanceType 先于其关联的 Instance 被删除，所有运行中的 Instance 都将无法删除，
// 因为清理 finalizer 的更新请求会因读取已不存在的对象而被拒绝。
//
// 禁止为该处理器添加 webhook.ReceiveDeletionUpdate。该标记会同时放开 defaulter
// 和 validator，无法在冻结字段的同时避免上述死锁。它本可冻结的字段在权衡中处于次要位置：
// 这些字段仅在 Instance 处于停止状态时可编辑，而处于停止状态的 Instance 并不需要排空。
//
// nolint: lll
// +k8s:webhook-gen:validating:group="worker.gpustack.ai",version="v1alpha1",resource="instances",scope="Namespaced"
// +k8s:webhook-gen:validating:operations=["CREATE","UPDATE"],failurePolicy="Fail",sideEffects="None",matchPolicy="Equivalent",timeoutSeconds=10
// +k8s:webhook-gen:mutating:group="worker.gpustack.ai",version="v1alpha1",resource="instances",scope="Namespaced"
// +k8s:webhook-gen:mutating:operations=["CREATE","UPDATE"],failurePolicy="Fail",sideEffects="None",matchPolicy="Equivalent",timeoutSeconds=10
type InstanceWebhook struct {
	Client    ctrlcli.Client
	APIReader ctrlcli.Reader
}

func (r *InstanceWebhook) SetupWebhook(_ context.Context, opts webhook.SetupOptions) (runtime.Object, error) {
	r.Client = opts.Manager.GetClient()
	r.APIReader = opts.Manager.GetAPIReader()

	return &workercore.Instance{}, nil
}

var (
	_ ctrladmission.Validator[runtime.Object] = (*InstanceWebhook)(nil)
	_ ctrladmission.Defaulter[runtime.Object] = (*InstanceWebhook)(nil)
)

func (r *InstanceWebhook) ValidateCreate(ctx context.Context, obj runtime.Object) (ctrladmission.Warnings, error) {
	inst := obj.(*workercore.Instance)

	if kuberess.IsReservedNamespace(inst.Namespace) {
		return nil, field.Invalid(
			field.NewPath("metadata.namespace"), inst.Namespace, "cannot create instance in reserved namespace",
		)
	}

	if inst.Spec.Type == "" {
		return nil, field.Required(
			field.NewPath("spec.type"), "type must be specified",
		)
	}

	// 只有需要运行的 Instance 才要求所引用的 InstanceType 存在并据此校验资源限制；
	// 已停止的 Instance 可以引用正在排空或已被移除的 InstanceType。
	var instType *worker.InstanceType
	if !inst.Spec.Stop {
		instType = &worker.InstanceType{
			ObjectMeta: meta.ObjectMeta{
				Name: inst.Spec.Type,
			},
		}
		err := r.Client.Get(ctx, ctrlcli.ObjectKeyFromObject(instType), instType)
		if err != nil {
			if !kerrors.IsNotFound(err) {
				return nil, field.InternalError(
					field.NewPath("spec.type"), fmt.Errorf("get instance type: %w", err),
				)
			}
			err = r.APIReader.Get(ctx, ctrlcli.ObjectKeyFromObject(instType), instType,
				ctrlclix.WithoutQuorum)
			if err != nil {
				return nil, field.NotFound(
					field.NewPath("spec.type"), inst.Spec.Type,
				)
			}
		}
	}

	var errs field.ErrorList
	if instRess := inst.Spec.Resources; instType != nil && instRess != nil {
		// 加速器 Detail 尚未计算完成的切分请求无法进行校验；
		// 应返回可重试的瞬态 error 而非永久性的 Invalid，以便 reconciler 填充 Status.Detail 后
		// 同一请求可以重试成功。
		if slicingRequestNotReady(instType, instRess) {
			return nil, kerrors.NewInternalError(fmt.Errorf("instance type %s is not ready yet; retry", instType.Name))
		}
		errs = append(errs, validateResourceRequests(instType, instRess)...)
	}
	nodeErr, err := r.validateNodePin(ctx, inst.Spec.NodeName)
	if err != nil {
		return nil, err
	}
	if nodeErr != nil {
		errs = append(errs, nodeErr)
	}
	errs = append(errs, validateAdditionalVolumes(inst)...)
	// 创建时 Instance 尚未持有任何权限，因此它请求的每项逃逸能力都视为新增获取。
	// 读取设置失败时一律拒绝而非放行，无论该设置的默认值为何。
	errs = append(errs, validateHostAccess(nil, inst,
		hostAccessAllowed(ctx, settings.InstancePrivilegedAllowed),
		hostAccessAllowed(ctx, settings.InstanceHostPathVolumeAllowed))...)
	switch {
	case inst.Spec.Volume.Ephemeral != nil && inst.Spec.Volume.Persistent != nil:
		errs = append(errs, field.Forbidden(
			field.NewPath("spec.volume"), "cannot specify both ephemeral and persistent of volume",
		))
	case inst.Spec.Volume.Ephemeral == nil && inst.Spec.Volume.Persistent == nil:
		errs = append(errs, field.Required(
			field.NewPath("spec.volume"), "either ephemeral or persistent of volume should be specified",
		))
	case inst.Spec.Volume.Ephemeral != nil:
		if inst.Spec.Volume.Ephemeral.Capacity.Sign() <= 0 {
			errs = append(errs, field.Invalid(
				field.NewPath("spec.volume.ephemeral.capacity"), inst.Spec.Volume.Ephemeral.Capacity.String(),
				"capacity of ephemeral volume must be positive",
			))
		}
	case inst.Spec.Volume.Persistent != nil:
		if inst.Spec.Volume.Persistent.Name == "" {
			errs = append(errs, field.Required(
				field.NewPath("spec.volume.persistent.name"), "name of the persistent volume must be specified",
			))
		}
	}
	if len(errs) > 0 {
		return nil, kerrors.NewInvalid(workercore.Kind("Instance"), inst.Name, errs)
	}

	return nil, nil
}

func (r *InstanceWebhook) ValidateUpdate(ctx context.Context, oldObj, newObj runtime.Object) (ctrladmission.Warnings, error) {
	instOld, inst := oldObj.(*workercore.Instance), newObj.(*workercore.Instance)

	stopped := instOld.Spec.Stop
	starting := stopped && !inst.Spec.Stop

	var errs field.ErrorList

	// 校验字段修改：除 "Volume" 外，其余字段仅可在 Instance 停止时修改。
	if !stopped {
		if inst.Spec.Type != instOld.Spec.Type {
			errs = append(errs, field.Forbidden(
				field.NewPath("spec.type"), "type is immutable",
			),
			)
		}
		if inst.Spec.Image != instOld.Spec.Image {
			errs = append(errs, field.Forbidden(
				field.NewPath("spec.image"), "image is immutable",
			),
			)
		}
		if inst.Spec.ImagePullPolicy != instOld.Spec.ImagePullPolicy {
			errs = append(errs, field.Forbidden(
				field.NewPath("spec.imagePullPolicy"), "imagePullPolicy is immutable",
			),
			)
		}
		if !kubemeta.DeepEqual(inst.Spec.Command, instOld.Spec.Command) {
			errs = append(errs, field.Forbidden(
				field.NewPath("spec.command"), "command is immutable",
			),
			)
		}
		if inst.Spec.Privileged != instOld.Spec.Privileged {
			errs = append(errs, field.Forbidden(
				field.NewPath("spec.privileged"), "privileged is immutable",
			),
			)
		}
		if !kubemeta.DeepEqual(inst.Spec.Ports, instOld.Spec.Ports) {
			errs = append(errs, field.Forbidden(
				field.NewPath("spec.ports"), "ports is immutable",
			),
			)
		}
		if !kubemeta.DeepEqual(inst.Spec.Env, instOld.Spec.Env) {
			errs = append(errs, field.Forbidden(
				field.NewPath("spec.env"), "env is immutable",
			),
			)
		}
		if inst.Spec.VolumeMount != instOld.Spec.VolumeMount {
			errs = append(errs, field.Forbidden(
				field.NewPath("spec.volumeMount"), "volumeMount is immutable",
			),
			)
		}
		if !kubemeta.DeepEqual(inst.Spec.ImagePullSecret, instOld.Spec.ImagePullSecret) {
			errs = append(errs, field.Forbidden(
				field.NewPath("spec.imagePullSecret"), "imagePullSecret is immutable",
			),
			)
		}
		if !kubemeta.DeepEqual(inst.Spec.Resources, instOld.Spec.Resources) {
			errs = append(errs, field.Forbidden(
				field.NewPath("spec.resources"), "resources is immutable",
			),
			)
		}
		if !kubemeta.DeepEqual(inst.Spec.SSHPublicKey, instOld.Spec.SSHPublicKey) {
			errs = append(errs, field.Forbidden(
				field.NewPath("spec.sshPublicKey"), "sshPublicKey is immutable",
			),
			)
		}
		if inst.Spec.NodeName != instOld.Spec.NodeName {
			errs = append(errs, field.Forbidden(
				field.NewPath("spec.nodeName"), "nodeName is immutable",
			),
			)
		}
		if !kubemeta.DeepEqual(inst.Spec.AdditionalVolumes, instOld.Spec.AdditionalVolumes) {
			errs = append(errs, field.Forbidden(
				field.NewPath("spec.additionalVolumes"), "additionalVolumes is immutable",
			),
			)
		}
	}
	if !kubemeta.DeepEqual(inst.Spec.Volume, instOld.Spec.Volume) {
		errs = append(errs, field.Forbidden(
			field.NewPath("spec.volume"), "volume is immutable",
		),
		)
	}

	// 无论 Instance 处于何种状态，均对本次更新新增获取的主机边界逃逸权限进行准入控制：
	// 当 Instance 停止时，上方的不可变性保护会被跳过，因此逃逸配置可在任意时刻而非仅创建时
	// 写入 spec。Instance 已经持有的逃逸权限会保持放行，从而避免后续关闭相关设置时使其陷入异常状态。
	errs = append(errs, validateHostAccess(instOld, inst,
		hostAccessAllowed(ctx, settings.InstancePrivilegedAllowed),
		hostAccessAllowed(ctx, settings.InstanceHostPathVolumeAllowed))...)

	// 校验状态转换。
	if starting {
		// 校验 Instance 确实处于已停止状态后，才允许将其启动。
		if instOld.Status.Phase != workerctrl.InstancePhaseStopped {
			errs = append(errs, field.Forbidden(
				field.NewPath("spec.stop"), "can only start stopped instance",
			))
		}
		// 启动已停止的 Instance 时，校验所引用的 InstanceType 是否存在。
		instType := &worker.InstanceType{
			ObjectMeta: meta.ObjectMeta{
				Name: inst.Spec.Type,
			},
		}
		err := r.Client.Get(ctx, ctrlcli.ObjectKeyFromObject(instType), instType)
		if err != nil {
			if !kerrors.IsNotFound(err) {
				return nil, field.InternalError(
					field.NewPath("spec.type"), fmt.Errorf("get instance type: %w", err),
				)
			}
			err = r.APIReader.Get(ctx, ctrlcli.ObjectKeyFromObject(instType), instType,
				ctrlclix.WithoutQuorum)
			if err != nil {
				errs = append(errs, field.NotFound(
					field.NewPath("spec.type"), inst.Spec.Type,
				))
			}
		}
		// 此处特意不对节点固定配置（node pin）重新校验：该配置仅在创建时校验一次，
		// 若对应节点之后已下线，Pod 只会因调度器自身的原因保持 Pending，而不会阻塞启动操作。
		//
		// 但附加卷（additional volumes）需要重新校验：与节点不同，它们属于 Instance 自身的 spec，
		// 在停止期间可被编辑，格式错误的条目会导致 Pod 在每次 reconcile 时都被 API server 拒绝，
		// 而不仅仅是停留在 Pending 状态。
		errs = append(errs, validateAdditionalVolumes(inst)...)
		// 使用与 ValidateCreate 相同的检查项（符号、加速器/CPU 上限、切分比例范围以及每单元内存/本地存储）
		// 重新校验启动时生效的资源配置，而不仅检查上限——因为 Instance 处于停止状态时
		// 上方的不可变性保护会被跳过，其资源配置可能已在停止期间被修改。
		if inst.Spec.Resources != nil {
			// 与创建时相同，若加速器 Detail 尚未计算完成，切分请求会以可重试的瞬态 error 拒绝，
			// 而非返回永久性的 Invalid。
			if slicingRequestNotReady(instType, inst.Spec.Resources) {
				return nil, kerrors.NewInternalError(fmt.Errorf("instance type %s is not ready yet; retry", instType.Name))
			}
			errs = append(errs, validateResourceRequests(instType, inst.Spec.Resources)...)
		}
	}

	if len(errs) > 0 {
		return nil, kerrors.NewInvalid(worker.Kind("Instance"), inst.Name, errs)
	}

	return nil, nil
}

// validateAdditionalVolumes 检查每个附加卷条目，并在字段路径中携带条目索引以便拒绝时定位具体项：
// 要求挂载路径为绝对路径且不与其他条目重复或遮蔽工作区、有且仅有一个来源、相对子路径不会逃逸出卷范围，
// 以及引用名非空。
//
// 本函数不会查询所引用的对象本身，这与 spec.volume.persistent 保持一致：对象缺失会表现为 Pod 自身的事件，
// 而不是直接拒绝请求（否则也会误拒合法的并发创建）。该函数由 ValidateCreate 和 ValidateUpdate 的
// 启动（恢复）路径共用，确保在 Instance 停止期间编辑过的卷列表不会在启动后生成被 API server 拒绝的 Pod。
func validateAdditionalVolumes(inst *workercore.Instance) field.ErrorList {
	var errs field.ErrorList

	seen := make(map[string]struct{}, len(inst.Spec.AdditionalVolumes))
	for i := range inst.Spec.AdditionalVolumes {
		av := &inst.Spec.AdditionalVolumes[i]
		fldPath := field.NewPath("spec.additionalVolumes").Index(i)

		mountPath := fldPath.Child("mountPath")
		switch {
		case av.MountPath == "":
			errs = append(errs, field.Required(mountPath, "mount path must be specified"))
		case !path.IsAbs(av.MountPath):
			errs = append(errs, field.Invalid(mountPath, av.MountPath, "mount path must be absolute"))
		case path.Clean(av.MountPath) != av.MountPath:
			// 非规范路径会绕过下方的两项检查，因为校验是按文本进行比较而 kubelet 会实际解析路径：
			// "/workspace/." 会静默覆盖工作区，而 "/tmp/../data" 会挂载到与 "/data" 相同的位置却不会被判为重复。
			errs = append(errs, field.Invalid(mountPath, av.MountPath,
				`mount path must be canonical, without a ".", ".." or trailing "/" element`))
		case av.MountPath == path.Clean(inst.Spec.VolumeMount):
			errs = append(errs, field.Invalid(mountPath, av.MountPath,
				"mount path must differ from the workspace volume mount"))
		default:
			if _, dup := seen[av.MountPath]; dup {
				errs = append(errs, field.Duplicate(mountPath, av.MountPath))
			}
			seen[av.MountPath] = struct{}{}
		}

		if av.SubPath != "" {
			subPath := fldPath.Child("subPath")
			switch {
			case path.IsAbs(av.SubPath):
				errs = append(errs, field.Invalid(subPath, av.SubPath, "sub path must be relative"))
			case slices.Contains(strings.Split(av.SubPath, "/"), ".."):
				errs = append(errs, field.Invalid(subPath, av.SubPath, "sub path must not contain \"..\""))
			}
		}

		errs = append(errs, validateAdditionalVolumeSource(av, fldPath)...)
	}

	return errs
}

// validateAdditionalVolumeSource 检查附加卷是否恰好设置了一个来源，且该来源实际指定了非空名称。
func validateAdditionalVolumeSource(av *workercore.InstanceAdditionalVolume, fldPath *field.Path) field.ErrorList {
	var (
		errs field.ErrorList
		set  []string
	)

	refs := []struct {
		name string
		ref  *core.LocalObjectReference
	}{
		{"persistent", av.Persistent},
		{"configMap", av.ConfigMap},
		{"secret", av.Secret},
	}
	if av.Model != nil {
		refs = append(refs, struct {
			name string
			ref  *core.LocalObjectReference
		}{"model.artifactRef", &av.Model.ArtifactRef})
		// artifact 自身的路径就是 subPath，再指定一层 subPath 会变成位于该条目编写者
		// 不可见路径内的子路径。
		if av.SubPath != "" {
			errs = append(errs, field.Forbidden(fldPath.Child("subPath"),
				"a model volume takes no subPath: the ModelArtifact's own path is the directory mounted"))
		}
	}
	for _, r := range refs {
		if r.ref == nil {
			continue
		}
		set = append(set, strings.Split(r.name, ".")[0])
		namePath := fldPath.Child(r.name, "name")
		if r.ref.Name == "" {
			errs = append(errs, field.Required(namePath,
				"name of the referenced object must be specified"))
			continue
		}
		// 对象本身就不可能使用的名称永远无法解析，这与仅仅尚不存在的对象不同。
		// Pod 校验不会捕获此类问题（volume source 中的对象引用在那里仅检查是否为空），
		// 因此 Pod 会被准入，随后在 ContainerCreating 状态因无法成功的挂载而永久卡住，
		// 贯穿整个生命周期，因为 Pod 只渲染一次且不会重新比对差异。准入阶段是编写者唯一能看到原因的地方。
		if msgs := validation.IsDNS1123Subdomain(r.ref.Name); len(msgs) > 0 {
			errs = append(errs, field.Invalid(namePath, r.ref.Name, strings.Join(msgs, "; ")))
		}
	}
	if av.HostPath != nil {
		set = append(set, "hostPath")
		errs = append(errs, validateHostPathSource(av.HostPath, fldPath.Child("hostPath"))...)
	}

	switch len(set) {
	case 1:
	case 0:
		errs = append(errs, field.Required(fldPath,
			"exactly one of persistent, configMap, secret, hostPath or model must be specified"))
	default:
		errs = append(errs, field.Forbidden(fldPath,
			fmt.Sprintf("cannot specify more than one source: %s", strings.Join(set, ", "))))
	}

	return errs
}

// hostAccessAllowed 读取宿主机访问开关，读取失败时默认拒绝。
//
// bool 类型的 Setting 在无法读取时会回退到默认值，但授予容器逃逸或节点文件系统访问权限的
// 开关绝不能这样：其默认值可能在首次部署时被初始化为 true 随后被管理员关闭，且该 webhook
// 仅在对象启用逃逸能力时进行裁决，因此在读取失败期间被准入的逃逸权限在读取恢复后仍会被保留。
func hostAccessAllowed(ctx context.Context, s setting.Setting) bool {
	allowed, err := s.ValueBool(ctx)
	return err == nil && allowed
}

// validateHostAccess 拒绝 Instance 跨越节点边界的两种方式——特权模式和 hostPath 附加卷——
// 除非管理员已明确允许对应方式。两者独立控制，因为特权模式授予的权限严格更大：
// 在节点文件系统之上还包括节点设备和内核接口。
//
// 受控的是获取逃逸权限的行为，而非持有该权限。instOld 是 Instance 当前的既有状态（创建时为 nil），
// 其已持有的逃逸权限绝不会被再次拒绝：关闭某个开关不能让在开启期间合法获得权限的 Instance 陷入困境。
// 若没有旧对象，该规则将退化为仅在创建时生效，从而形同虚设——以停止且空配置状态创建的 Instance
// 什么都不请求，之后只需一次 patch 就能同时拿到两种逃逸权限。
//
// 该规则直接接收已解析的设置值而非自行读取，以便测试所有组合情况，
// 避免写入一个会缓存 30 秒且无法刷新的值从而污染后续测试。
func validateHostAccess(instOld, inst *workercore.Instance, privilegedAllowed, hostPathAllowed bool) field.ErrorList {
	var errs field.ErrorList

	heldPrivileged := instOld != nil && instOld.Spec.Privileged
	if inst.Spec.Privileged && !privilegedAllowed && !heldPrivileged {
		errs = append(errs, field.Forbidden(field.NewPath("spec.privileged"),
			fmt.Sprintf("privileged mode is not allowed: enable the %q setting to allow it",
				settings.InstancePrivilegedAllowed.Name())))
	}

	if !hostPathAllowed {
		var held []*workercore.InstanceAdditionalVolume
		if instOld != nil {
			for i := range instOld.Spec.AdditionalVolumes {
				if instOld.Spec.AdditionalVolumes[i].HostPath != nil {
					held = append(held, &instOld.Spec.AdditionalVolumes[i])
				}
			}
		}
		for i := range inst.Spec.AdditionalVolumes {
			av := &inst.Spec.AdditionalVolumes[i]
			if av.HostPath == nil || slices.ContainsFunc(held, func(h *workercore.InstanceAdditionalVolume) bool {
				return coversHostAccess(h, av)
			}) {
				continue
			}
			errs = append(errs, field.Forbidden(
				field.NewPath("spec.additionalVolumes").Index(i).Child("hostPath"),
				fmt.Sprintf("mounting a host path is not allowed: enable the %q setting to allow it",
					settings.InstanceHostPathVolumeAllowed.Name()),
			))
		}
	}

	return errs
}

// coversHostAccess 返回 Instance 已持有的 hostPath 条目是否至少覆盖了另一个条目所请求的权限，
// 从而判断后者是否构成新的逃逸。
//
// 比较按与位置无关的值进行，因此调整列表顺序或编辑同级条目不会被误判为获取新的逃逸权限。
// 比较的是完整授权范围，而不仅仅是节点路径：扩大同一路径的权限（如去掉 subPath，
// 或将只读挂载改为可写）会赋予 Instance 原本没有的访问能力，因此必须像其他新逃逸一样经过开关校验。
// 相反方向的收窄则不受限制，因为暴露范围小于既有挂载的配置不会增加任何权限。
func coversHostAccess(held, want *workercore.InstanceAdditionalVolume) bool {
	if !kubemeta.DeepEqual(normalizeHostPath(held.HostPath), normalizeHostPath(want.HostPath)) {
		return false
	}
	// subPath 覆盖其自身及其下方的所有路径，而未设置 subPath 时则覆盖整个卷，
	// 因为它暴露了卷的根目录。双方已知均为相对路径且不含 ".."，
	// 因此此处的路径前缀确实代表一棵子树，而不只是表面相似的字符串。
	if held.SubPath != "" &&
		want.SubPath != held.SubPath &&
		!strings.HasPrefix(want.SubPath, held.SubPath+"/") {
		return false
	}
	// 可写挂载覆盖只读挂载，反之则不成立。
	return !held.ReadOnly || want.ReadOnly
}

// normalizeHostPath 返回将 Type 规范化后的源配置，因为 Kubernetes 将缺失的 Type
// 和空字符串视为同一种 "unset" 类型——直接比较原始指针会把写入 `type: ""` 的客户端
// 误判为与省略该字段时指定了不同的节点路径。
func normalizeHostPath(hp *core.HostPathVolumeSource) *core.HostPathVolumeSource {
	if hp == nil || hp.Type == nil || *hp.Type != core.HostPathUnset {
		return hp
	}
	return &core.HostPathVolumeSource{Path: hp.Path}
}

// _HostPathTypes 是 Kubernetes 在 hostPath 卷上接受的节点路径类型集合。
var _HostPathTypes = []core.HostPathType{
	core.HostPathUnset,
	core.HostPathDirectoryOrCreate,
	core.HostPathDirectory,
	core.HostPathFileOrCreate,
	core.HostPathFile,
	core.HostPathSocket,
	core.HostPathCharDev,
	core.HostPathBlockDev,
}

// validateHostPathSource 检查 hostPath 源是否满足节点实际可提供的条件：受支持类型的规范化绝对路径。
// Kubernetes 本身会拒绝包含 ".." 元素的路径和未知类型，若此处准入只会把拒绝推迟到 Pod 创建阶段，
// 导致 reconciler 无限重试；绝对路径与末尾路径元素规则则更进一步，因为 API server 接受但
// 节点无法解析的路径同样会永久卡住 Pod——Pod 只渲染一次且不会重新比对差异。
func validateHostPathSource(hp *core.HostPathVolumeSource, fldPath *field.Path) field.ErrorList {
	var errs field.ErrorList

	nodePath := fldPath.Child("path")
	switch {
	case hp.Path == "":
		errs = append(errs, field.Required(nodePath, "path on the node must be specified"))
	case !path.IsAbs(hp.Path):
		errs = append(errs, field.Invalid(nodePath, hp.Path, "path on the node must be absolute"))
	case path.Clean(hp.Path) != hp.Path:
		errs = append(errs, field.Invalid(nodePath, hp.Path,
			`path on the node must be canonical, without a ".", ".." or trailing "/" element`))
	}

	if hp.Type != nil && !slices.Contains(_HostPathTypes, *hp.Type) {
		errs = append(errs, field.NotSupported(fldPath.Child("type"), *hp.Type, _HostPathTypes))
	}

	return errs
}

// validateNodePin 检查被绑定的 Node 是否存在，以便在创建时捕获拼写错误，
// 避免 Instance 永久处于 Pending 状态。未绑定节点的 Instance 直接放行。
//
// 仅检查节点是否存在，且仅在创建时检查。该节点实际能否承载 Instance 的 InstanceType
// 交由调度器判断：不在池内 flavor 中的节点只是不会被调度到，而若在此处拒绝绑定则会阻止合法的
// 放置需求——例如将无卡 Instance（如模型下载任务）绑定到特定的加速节点上。
//
// 拒绝结果通过 *field.Error 返回；非单纯 "not found" 的读取失败则通过 error 返回，
// 从而确保瞬时 API 故障不会变成永久性拒绝。
func (r *InstanceWebhook) validateNodePin(ctx context.Context, nodeName string) (*field.Error, error) {
	if nodeName == "" {
		return nil, nil
	}

	fldPath := field.NewPath("spec.nodeName")
	nd := &core.Node{
		ObjectMeta: meta.ObjectMeta{
			Name: nodeName,
		},
	}
	err := r.Client.Get(ctx, ctrlcli.ObjectKeyFromObject(nd), nd)
	if err != nil {
		if !kerrors.IsNotFound(err) {
			return nil, field.InternalError(fldPath, fmt.Errorf("get node: %w", err))
		}
		// 缓存未命中可能是由于 informer 尚未同步，因此在拒绝前直接向 API server 查询——
		// 只有来自它的真实 "not found" 才是永久性判定，而超时或鉴权失败则保持为可重试错误，
		// 不会误判为节点不存在。与本包中的所有读取操作一样，此次读取由 API server 自身缓存提供
		// 而非经过 etcd quorum：其新鲜度足以弥补尚未追平的 informer，剩余时间窗口的代价至多是
		// 对刚创建的 Node 返回可重试的拒绝，而不会造成错误准入。
		err = r.APIReader.Get(ctx, ctrlcli.ObjectKeyFromObject(nd), nd,
			ctrlclix.WithoutQuorum)
		if err != nil {
			if !kerrors.IsNotFound(err) {
				return nil, field.InternalError(fldPath, fmt.Errorf("get node: %w", err))
			}
			return field.NotFound(fldPath, nodeName), nil
		}
	}

	return nil, nil
}

func (r *InstanceWebhook) ValidateDelete(ctx context.Context, obj runtime.Object) (ctrladmission.Warnings, error) {
	return nil, nil
}

func (r *InstanceWebhook) Default(ctx context.Context, obj runtime.Object) error {
	inst := obj.(*workercore.Instance)

	if inst.Spec.ImagePullPolicy == "" {
		inst.Spec.ImagePullPolicy = core.PullIfNotPresent
	}

	if inst.Spec.VolumeMount == "" {
		inst.Spec.VolumeMount = "/workspace"
	}

	if inst.Spec.Resources == nil {
		inst.Spec.Resources = &workercore.InstanceResources{}
	}

	// 如果未指定 type，
	// 跳过资源默认值填充，交由后续校验拦截。
	if inst.Spec.Type == "" {
		return nil
	}

	// 如果实例处于停止状态，
	// 跳过资源默认值填充。
	if inst.Spec.Stop {
		return nil
	}

	instType := &worker.InstanceType{
		ObjectMeta: meta.ObjectMeta{
			Name: inst.Spec.Type,
		},
	}
	err := r.Client.Get(ctx, ctrlcli.ObjectKeyFromObject(instType), instType)
	if err != nil {
		if !kerrors.IsNotFound(err) {
			return field.InternalError(
				field.NewPath("spec.type"), fmt.Errorf("get instance type: %w", err),
			)
		}
		err = r.APIReader.Get(ctx, ctrlcli.ObjectKeyFromObject(instType), instType,
			ctrlclix.WithoutQuorum)
		if err != nil {
			return field.NotFound(
				field.NewPath("spec.type"), inst.Spec.Type,
			)
		}
	}

	withGeneralOvercommit := settings.InstanceGeneralResourcesOvercommit.ShouldValueBool(ctx)

	instRess := inst.Spec.Resources
	// 在 reconciler 计算出加速卡 Detail 之前，无法确定切分请求的资源规格：
	// 其可切分性及单卡资源配比均来自 Status.Detail。此时应返回瞬时（可重试）错误予以拒绝，
	// 而不应回退到整卡规格计算（那会静默导致 Pod 规格错误）；一旦 Detail 填充完毕，同一请求即可成功。
	if slicingRequestNotReady(instType, instRess) {
		return kerrors.NewInternalError(fmt.Errorf("instance type %s is not ready yet; retry", instType.Name))
	}
	if instType.Spec.Acceleratable {
		if instRess.Accelerator == nil {
			// 在此处填充默认请求，
			// 若超出 InstanceType 的 CPU 容量，则交由后续调度拦截。
			instRess.Accelerator = resource.NewQuantity(1, resource.DecimalSI)
		}
		memPct := instRess.AcceleratorSlicedMemoryPercentage
		coresPct := instRess.AcceleratorSlicedCoresPercentage
		partitionPct, sizeable := workerctrl.PartitionProfileMemoryPercent(
			(*workercore.InstanceType)(instType), instRess.AcceleratorPartitionedProfile,
		)
		if !sizeable {
			// 资源池提供了该 profile，但尚无法计算其资源规格。以可重试错误拒绝，
			// 与 slicingRequestNotReady 对待未计算 Detail 的处理方式相同——此处的整卡规格计算会永久固化，
			// 因为一旦资源字段被设置，Default 就不会再次运行。
			return kerrors.NewInternalError(fmt.Errorf(
				"instance type %s is not ready yet (partition profile %q cannot be sized from the "+
					"observed accelerator detail); retry",
				instType.Name, instRess.AcceleratorPartitionedProfile,
			))
		}
		switch {
		case partitionPct > 0:
			// 硬件分区对应单卡上的一个实例，因此将缺失或显式设为 0 的加速卡数量默认设为 1
			// （nil 已在上方填充默认值）；否则后续校验会因数量不严格等于 1 而拒绝该请求。
			if instRess.Accelerator.Value() == 0 {
				instRess.Accelerator = resource.NewQuantity(1, resource.DecimalSI)
			}
			// UnitResources 对应整张卡的规格，因此按该 profile 所占显存的比例来计算宿主机 CPU/RAM——
			// 这与逻辑切分路径采用的基于显存的比例一致，从而确保相同规格的硬件分区与逻辑切分消耗相同的宿主机资源。
			if err = sizeAcceleratorUnitByPercent(instType, instRess, partitionPct, withGeneralOvercommit); err != nil {
				return err
			}
		case instType.Status.Detail.IsLogicallySliceable() && (memPct > 0 || coresPct > 0):
			// 逻辑切分对应单卡的一部分，因此将缺失或显式设为 0 的加速卡数量默认设为 1
			// （nil 已在上方填充默认值）；否则后续校验会因数量不严格等于 1 而拒绝该请求。
			if instRess.Accelerator.Value() == 0 {
				instRess.Accelerator = resource.NewQuantity(1, resource.DecimalSI)
			}
			// 对于可切分的 InstanceType，当仅设置了显存或算力切分百分比中的一项时，
			// 将其复制到另一项，使仅指定显存的请求获得同等比例的算力份额（反之亦然）。
			switch {
			case memPct > 0 && coresPct == 0:
				instRess.AcceleratorSlicedCoresPercentage = memPct
			case coresPct > 0 && memPct == 0:
				instRess.AcceleratorSlicedMemoryPercentage = coresPct
			}
			// 切分请求占用单卡的一部分（其加速卡数量在校验中固定为 1）。UnitResources 对应整张卡的规格，
			// 因此按显存百分比（即卡上实际预留的份额）计算宿主机 CPU/RAM。算力百分比仅限制 GPU 核心使用率，
			// 不影响宿主机资源。此处的显存百分比一定非零（当仅设置算力百分比时，已在上方复制填充）。
			if err = sizeAcceleratorUnitByPercent(instType, instRess,
				int64(instRess.AcceleratorSlicedMemoryPercentage), withGeneralOvercommit); err != nil {
				return err
			}
		default:
			// 整卡请求（不可切分类型，或可切分类型上百分比为 0 的请求）按加速卡数量等比缩放单位 CPU/RAM。
			// 允许数量为 0 或缺失，但在计算其他资源请求时将其视为 1。
			accC := instRess.Accelerator.Value()
			if accC == 0 {
				accC = 1
			}
			if withGeneralOvercommit || instRess.CPU.IsZero() {
				instRess.CPU, err = quantityx.StringMultiply(instType.Spec.UnitResources.CPU, accC)
				if err != nil {
					return field.InternalError(
						field.NewPath("spec.resources.cpu"),
						fmt.Errorf("invalid CPU unit of instance type %s: %w", instType.Name, err),
					)
				}
			}
			if withGeneralOvercommit || instRess.RAM.IsZero() {
				instRess.RAM, err = quantityx.StringMultiply(instType.Spec.UnitResources.RAM, accC)
				if err != nil {
					return field.InternalError(
						field.NewPath("spec.resources.ram"),
						fmt.Errorf("invalid RAM unit of instance type %s: %w", instType.Name, err),
					)
				}
			}
		}
	} else {
		cpuC := int64(1)
		if instRess.CPU.IsZero() {
			// 在此处填充默认请求，
			// 若超出 InstanceType 的 CPU 容量，则交由后续调度拦截。
			instRess.CPU = *resource.NewQuantity(1, resource.DecimalSI)
		} else {
			cpuC = instRess.CPU.Value()
		}
		if withGeneralOvercommit || instRess.RAM.IsZero() {
			instRess.RAM, err = quantityx.StringMultiply(instType.Spec.UnitResources.RAM, cpuC)
			if err != nil {
				return field.InternalError(
					field.NewPath("spec.resources.ram"),
					fmt.Errorf("invalid RAM unit of instance type %s: %w", instType.Name, err),
				)
			}
		}
	}
	if instRess.LocalStorage.IsZero() {
		// 将本地存储请求默认设为 15Gi，但绝不超过 InstanceType 的 LocalStorage
		// （validating webhook 也会将显式请求上限约束在该值）。
		def := resource.NewQuantity(15<<30, resource.BinarySI) // 15Gi
		if instType.Spec.LocalStorage != "" {
			if maxStg, perr := resource.ParseQuantity(instType.Spec.LocalStorage); perr == nil && def.Cmp(maxStg) > 0 {
				def = &maxStg
			}
		}
		instRess.LocalStorage = *def
	}

	return nil
}

// sizeAcceleratorUnitByPercent 为占用单卡一部分的请求（逻辑切分或硬件分区）计算宿主机 CPU/RAM，
// 取 InstanceType 整卡单位资源的对应百分比。除非开启超分，否则保留显式指定的请求值，与整卡路径保持一致。
func sizeAcceleratorUnitByPercent(
	instType *worker.InstanceType, instRess *workercore.InstanceResources,
	unitPct int64, withGeneralOvercommit bool,
) error {
	var err error
	if withGeneralOvercommit || instRess.CPU.IsZero() {
		instRess.CPU, err = quantityx.StringPercentMultiply(instType.Spec.UnitResources.CPU, unitPct)
		if err != nil {
			return field.InternalError(
				field.NewPath("spec.resources.cpu"),
				fmt.Errorf("invalid CPU unit of instance type %s: %w", instType.Name, err),
			)
		}
	}
	if withGeneralOvercommit || instRess.RAM.IsZero() {
		instRess.RAM, err = quantityx.StringPercentMultiply(instType.Spec.UnitResources.RAM, unitPct)
		if err != nil {
			return field.InternalError(
				field.NewPath("spec.resources.ram"),
				fmt.Errorf("invalid RAM unit of instance type %s: %w", instType.Name, err),
			)
		}
	}
	return nil
}

// slicingRequestNotReady 返回当前请求是否向加速型 InstanceType 申请单卡份额——即逻辑切分
// （非零的显存或算力百分比）或硬件分区（非空 profile）——而该 InstanceType 的观测加速卡 Detail 尚未计算完成。
// 可切分性、分区 profile 清单及单卡规格均从 Status.Detail 读取，因此在就绪前此类请求既无法计算规格也无法校验，
// 必须作为可重试错误予以拒绝——绝不能将其当作整卡请求（那会静默准入规格错误的 Pod），
// 也不能当作未知 profile 处理（那会永久拒绝合法请求）。
func slicingRequestNotReady(instType *worker.InstanceType, instRess *workercore.InstanceResources) bool {
	if instRess == nil || !instType.Spec.Acceleratable {
		return false
	}
	// 非零百分比（包括负数，负数会在 Detail 就绪后由范围校验拒绝）即属于切分请求：
	// 在未就绪时直接拦截，确保空的 Detail 绝不会回退到整卡规格计算。
	slice := instRess.AcceleratorSlicedMemoryPercentage != 0 || instRess.AcceleratorSlicedCoresPercentage != 0
	partition := instRess.AcceleratorPartitionedProfile != ""
	return (slice || partition) && !instType.Status.Detail.AcceleratorReady()
}

// validateResourceRequests 根据 InstanceType 的资源配额校验 Instance 的资源请求：正负号、加速卡/CPU 上限、
// 分区 profile 清单、切分百分比范围，以及（通过 capResourcesToInstanceType）每单位 RAM / 本地存储限制。
// 该函数由 ValidateCreate 与 ValidateUpdate 的启动（恢复）路径共用，使修改过资源的已停止 Instance
// 无法携带在创建阶段本会被拒绝的请求重新启动——此前启动路径仅重新检查了上限（capResourcesToInstanceType），
// 放行了负数、超上限或越界的切分请求。
func validateResourceRequests(instType *worker.InstanceType, instRess *workercore.InstanceResources) field.ErrorList {
	var errs field.ErrorList
	// 先校验加速卡请求，因为它可能决定其他资源请求的校验逻辑。
	if instType.Spec.Acceleratable {
		memPct := int64(instRess.AcceleratorSlicedMemoryPercentage)
		coresPct := int64(instRess.AcceleratorSlicedCoresPercentage)
		switch {
		case instRess.AcceleratorPartitionedProfile != "":
			errs = append(errs, validatePartitionedAcceleratorRequest(instType, instRess)...)
		case memPct != 0 || coresPct != 0:
			// 逻辑切分通过 [0,100] 范围内的显存/算力百分比来请求；两个配额相互独立。
			// 没有逻辑可切分卡的资源池完全无法服务此类请求——在全分区资源池上若准入该请求，
			// 它将永久处于 Pending 状态——因此此处直接拒绝，而非改写为整卡请求。
			if !instType.Status.Detail.IsLogicallySliceable() {
				errs = append(errs, field.Forbidden(
					field.NewPath("spec.resources.acceleratorSlicedMemoryPercentage"),
					fmt.Sprintf("instance type %s does not offer logical slicing", instType.Name),
				))
				break
			}
			memPath := field.NewPath("spec.resources.acceleratorSlicedMemoryPercentage")
			coresPath := field.NewPath("spec.resources.acceleratorSlicedCoresPercentage")
			if memPct < 0 || memPct > 100 {
				errs = append(errs, field.Invalid(memPath, instRess.AcceleratorSlicedMemoryPercentage,
					"must be between 0 and 100"))
			}
			if coresPct < 0 || coresPct > 100 {
				errs = append(errs, field.Invalid(coresPath, instRess.AcceleratorSlicedCoresPercentage,
					"must be between 0 and 100"))
			}
			// 切分请求（非零百分比）占用单卡的一部分：切分份额通过百分比表达，
			// 而非卡数，因此加速卡数量必须严格为 1。
			errs = append(errs, validateSingleCardRequest(instRess, "sliced")...)
		default:
			// 零百分比请求（无论是否在可切分类型上）均视为整卡独占请求，可以跨越多张卡。
			errs = append(errs, validateExclusiveAcceleratorRequest(instType, instRess)...)
		}
	} else if instRess.Accelerator != nil && !instRess.Accelerator.IsZero() {
		errs = append(errs, field.Invalid(
			field.NewPath("spec.resources.accelerator"), instRess.Accelerator.String(),
			"accelerator request must not be specified for non-acceleratable instance type",
		))
	}
	if instRess.CPU.Sign() < 0 {
		errs = append(errs, field.Invalid(
			field.NewPath("spec.resources.cpu"), instRess.CPU.String(),
			"CPU request cannot be negative",
		))
	} else if !instType.Spec.Acceleratable &&
		instRess.CPU.Cmp(instType.Status.CPU.Capacity) > 0 {
		// 只有非加速类型具有 CPU 容量视图；加速类型的 Status.CPU 为零
		// （其 CPU 由 unitCPU × 数量推导，并在他处限制上限）。
		//
		// 此处上限取 Capacity 而非 OnceMaxRequest，因为后者受限于当前尚未被请求的核数：
		// 若按后者限制，资源池繁忙时提交的每个 Instance 都会被直接拒绝，而它们本应进入队列等待。
		// Capacity 统计整个资源池的总量，因此大于最大单节点但小于总量的请求会被准入，
		// 并等待足够大的节点加入后才能运行；status 中未携带单节点容量以施加更严格的上限。
		errs = append(errs, field.Invalid(
			field.NewPath("spec.resources.cpu"), instRess.CPU.String(),
			cpuRequestRejection(instType),
		))
	}
	// 负的 RAM 或本地存储请求会直接被拒绝；正值请求必须位于 InstanceType 的单单位 RAM 配额
	// 及其本地存储上限之内（在下方依据 UnitResources / LocalStorage 进行校验）。
	if instRess.RAM.Sign() < 0 {
		errs = append(errs, field.Invalid(
			field.NewPath("spec.resources.ram"), instRess.RAM.String(),
			"RAM request cannot be negative",
		))
	}
	if instRess.LocalStorage.Sign() < 0 {
		errs = append(errs, field.Invalid(
			field.NewPath("spec.resources.localStorage"), instRess.LocalStorage.String(),
			"local storage request cannot be negative",
		))
	}
	errs = append(errs, capResourcesToInstanceType(instType, instRess)...)
	return errs
}

// cpuRequestRejection 说明为何 CPU 请求不适用于某个非加速 InstanceType。
// 容量为零并不意味着上限很小：当资源池中的所有节点均被驱逐（drained）或处于未纳管状态时，
// 其 ClusterQueue 仍保持准入——因此它报告 Active 阶段且容量为零——此时仅提示超出最大值
// 会让排查者去寻找一个根本不存在的限额。应改为明确指出该状态，而在常规情况下则像 RAM
// 和本地存储错误信息那样附带具体容量。
func cpuRequestRejection(instType *worker.InstanceType) string {
	cpu := instType.Status.CPU
	if cpu.Capacity.IsZero() {
		return fmt.Sprintf("instance type %s has no CPU capacity: no managed node currently backs it",
			instType.Name)
	}
	return fmt.Sprintf("exceeds the maximum CPU request %s of instance type %s",
		cpu.Capacity.String(), instType.Name)
}

// validateSingleCardRequest 检查占用单卡一部分的请求（逻辑切分或硬件分区）是否严格请求了 1 张卡。
// 使用 Cmp 进行比较（而非 Value()，后者会将 "1m" 这类分数量向上取整为 1），以确保只有真正的 1 才能通过。
// 参数 kind 用于在错误消息中指明请求类型。
func validateSingleCardRequest(instRess *workercore.InstanceResources, kind string) field.ErrorList {
	one := resource.NewQuantity(1, resource.DecimalSI)
	if instRess.Accelerator != nil && instRess.Accelerator.Cmp(*one) == 0 {
		return nil
	}
	got := "0"
	if instRess.Accelerator != nil {
		got = instRess.Accelerator.String()
	}
	return field.ErrorList{field.Invalid(
		field.NewPath("spec.resources.accelerator"), got,
		fmt.Sprintf("accelerator request must be exactly 1 for a %s request", kind),
	)}
}

// validatePartitionedAcceleratorRequest 校验硬件分区请求：它与两个逻辑切分百分比互斥（硬件分区与
// 软件切分不能同时作用于同一张卡）；前端资源池必须提供硬件分区能力；所指定的 profile 必须是该池
// 实际提供的（并在拒绝信息中列出已提供的集合，否则池无法创建的 profile 会导致 Instance 永久处于 Pending）；
// 且数量必须严格为单卡单实例。不支持硬件分区的厂商根本不会生成分区资源键，因此此处直接拒绝该请求，
// 避免 controller 将其拼成空键。
func validatePartitionedAcceleratorRequest(
	instType *worker.InstanceType, instRess *workercore.InstanceResources,
) field.ErrorList {
	var errs field.ErrorList
	profile := instRess.AcceleratorPartitionedProfile
	profilePath := field.NewPath("spec.resources.acceleratorPartitionedProfile")

	if instRess.AcceleratorSlicedMemoryPercentage != 0 || instRess.AcceleratorSlicedCoresPercentage != 0 {
		errs = append(errs, field.Forbidden(profilePath,
			"a hardware partition and a logical slice percentage are mutually exclusive; "+
				"clear acceleratorSlicedMemoryPercentage and acceleratorSlicedCoresPercentage"))
	}

	manufacturer := instType.Status.Detail.Manufacturer
	if nodefeature.GetAcceleratableResourceName(manufacturer, workercore.DeviceAllocationModePartitioned) == "" {
		errs = append(errs, field.Invalid(profilePath, profile,
			fmt.Sprintf("manufacturer %s does not support hardware partitioning", manufacturer)))
		return errs
	}

	// 该厂商支持分区，但此资源池中的卡可能未处于分区模式。
	// 应在查找 profile 之前先因缺少该能力而拒绝，否则查找会报告空的可用集合，使错误看起来像是 profile 拼写有误。
	if !instType.Status.Detail.IsPhysicallySliceable() {
		errs = append(errs, field.Forbidden(profilePath,
			fmt.Sprintf("instance type %s does not offer hardware partitioning", instType.Name)))
		return errs
	}

	offered := make([]string, 0, len(instType.Status.Detail.SlicedDetail.Physical.Profiles))
	var found bool
	for _, p := range instType.Status.Detail.SlicedDetail.Physical.Profiles {
		offered = append(offered, p.Name)
		if p.Name == profile {
			found = true
		}
	}
	switch {
	case !found:
		errs = append(errs, field.Invalid(profilePath, profile,
			fmt.Sprintf("instance type %s does not offer this partition profile; offered: %v",
				instType.Name, offered)))
	case nodefeature.GetAcceleratablePartitionedProfileResourceName(manufacturer, profile) == "":
		// 名称不是合法资源名片段的可用 profile 无法被请求：它生成的资源键无法被寻址。
		errs = append(errs, field.Invalid(profilePath, profile,
			"partition profile name does not yield a valid resource name"))
	}

	// 与逻辑切分相同，硬件分区对应单卡上的一个实例（规则 3）。
	errs = append(errs, validateSingleCardRequest(instRess, "partitioned")...)

	return errs
}

// validateExclusiveAcceleratorRequest 校验整卡（独占）加速卡请求：不能为负数，必须是整数张卡，
// 且不能超过 InstanceType 的整卡 Capacity——对于没有未分区卡的资源池，该值为零，因此全分区资源池
// 在此会拒绝正数请求。nil 请求留待默认值处理。该函数由不可切分类型和可切分类型上的零百分比（整卡）请求共用。
//
// 上限取 Capacity 而非 OnceMaxRequest，因为后者统计的是当前空闲卡数：若按后者限制，
// 资源池繁忙时提交的每个 Instance 都会被拒绝，而它们本应在队列中等待。Capacity 统计整个资源池的总量，
// 因此大于最大单节点但小于总量的请求会被准入并保持排队；status 中未携带单节点容量以施加更严格的上限。
func validateExclusiveAcceleratorRequest(
	instType *worker.InstanceType, instRess *workercore.InstanceResources,
) field.ErrorList {
	if instRess.Accelerator == nil {
		return nil
	}
	if instRess.Accelerator.Sign() < 0 {
		return field.ErrorList{field.Invalid(
			field.NewPath("spec.resources.accelerator"), instRess.Accelerator.String(),
			"accelerator request cannot be negative",
		)}
	}
	// 整卡请求会原样输出为 Pod 的扩展资源数量，而 Kubernetes 要求其必须为整数，
	// 因此在此处拒绝分数量（例如 "1m"），而非等到 Pod 准入阶段再失败。
	if _, ok := instRess.Accelerator.AsInt64(); !ok {
		return field.ErrorList{field.Invalid(
			field.NewPath("spec.resources.accelerator"), instRess.Accelerator.String(),
			"accelerator request must be a whole number of cards",
		)}
	}
	if instRess.Accelerator.Cmp(instType.Status.Accelerator.Capacity) > 0 {
		return field.ErrorList{field.Invalid(
			field.NewPath("spec.resources.accelerator"), instRess.Accelerator.String(),
			fmt.Sprintf("exceeds the maximum accelerator request of instance type %s", instType.Name),
		)}
	}
	return nil
}

// capResourcesToInstanceType 拒绝 CPU 或 RAM 超出 InstanceType 单单位配额（unitCPU/unitRAM × 单位数），
// 或本地存储超出 InstanceType 的 LocalStorage 的 Instance。单位数的推导与 Default 一致：
// 可加速类型取加速卡数量，否则取 CPU 数量。CPU 上限仅适用于可加速类型——
// 非加速类型的 CPU 在 ValidateCreate 中已受其 ClusterQueue 容量（Status.CPU）限制，
// 且若从 CPU 请求本身推导单位数，会使 unitCPU × 数量的上限校验陷入循环。
func capResourcesToInstanceType(
	instType *worker.InstanceType, instRess *workercore.InstanceResources,
) field.ErrorList {
	var errs field.ErrorList

	unitCount := int64(1)
	if instType.Spec.Acceleratable {
		if instRess.Accelerator != nil && instRess.Accelerator.Value() > 1 {
			unitCount = instRess.Accelerator.Value()
		}
	} else if instRess.CPU.Value() > 1 {
		unitCount = instRess.CPU.Value()
	}

	if instType.Spec.Acceleratable {
		if maxCPU, err := quantityx.StringMultiply(instType.Spec.UnitResources.CPU, unitCount); err == nil &&
			instRess.CPU.Cmp(maxCPU) > 0 {
			errs = append(errs, field.Invalid(
				field.NewPath("spec.resources.cpu"), instRess.CPU.String(),
				fmt.Sprintf("exceeds the maximum CPU %s of instance type %s", maxCPU.String(), instType.Name),
			))
		}
	}
	if maxRAM, err := quantityx.StringMultiply(instType.Spec.UnitResources.RAM, unitCount); err == nil &&
		instRess.RAM.Cmp(maxRAM) > 0 {
		errs = append(errs, field.Invalid(
			field.NewPath("spec.resources.ram"), instRess.RAM.String(),
			fmt.Sprintf("exceeds the maximum RAM %s of instance type %s", maxRAM.String(), instType.Name),
		))
	}
	if instType.Spec.LocalStorage != "" {
		if maxStg, err := resource.ParseQuantity(instType.Spec.LocalStorage); err == nil &&
			instRess.LocalStorage.Cmp(maxStg) > 0 {
			errs = append(errs, field.Invalid(
				field.NewPath("spec.resources.localStorage"), instRess.LocalStorage.String(),
				fmt.Sprintf("exceeds the local storage %s of instance type %s", maxStg.String(), instType.Name),
			))
		}
	}
	return errs
}
