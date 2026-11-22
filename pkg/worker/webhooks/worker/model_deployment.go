package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"

	core "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/authentication/serviceaccount"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrladmission "sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	worker "gpustack.ai/gpustack/api/worker/v1"
	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/kubemeta"
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/systemname"
	"gpustack.ai/gpustack/pkg/webhook"
	workerctrl "gpustack.ai/gpustack/pkg/worker/controllers/worker"
	"gpustack.ai/gpustack/pkg/worker/kuberess"
	"gpustack.ai/gpustack/pkg/worker/kvcache/inject"
	"gpustack.ai/gpustack/pkg/worker/kvcache/mooncake"
	"gpustack.ai/gpustack/pkg/worker/kvcache/router"
	"gpustack.ai/gpustack/pkg/worker/settings"
)

// ModelDeploymentWebhook 校验 v1alpha1.ModelDeployment，并为 schema 无法处理的唯一字段填充默认值。
//
// 该 API 的其他所有默认值（connector 鉴别字段与副本数）均为 CRD schema 默认值，且所有枚举均为 CRD schema 枚举。
// 变更（mutating）部分仅针对唯一依赖于其他对象来确定默认值的字段：role 的加速器数量——在可加速的 InstanceType 上为 1，
// 在其他类型上为空，而任何 schema 默认值都无法区分这两类类型。至于枚举，即使编写 webhook 也无能为力：
// 结构化 schema 校验在验证准入链之前执行，因此超出枚举范围的值在到达此处理程序之前即会被拒绝。
//
// 留在此处处理的，是 schema 无法表达的约束：必须附带可操作错误信息的边界条件、列表内两个条目之间的比较，以及用户输入与 operator 托管字段之间的冲突。
//
// 大多数校验规则仅基于对象本身即可判定。资源模式与加速卡数量取决于 InstanceType。新建缓存绑定的传输方式取决于其 Binding、pool 与 backend。
//
// 凡需读取其他对象的逻辑，均直接从 API server 读取而绝不走缓存，包括默认值填充与传输方式检查。
// 若缓存滞后，会在正反两个方向上误判结果：刚创建不久的 InstanceType 在缓存中表现为不存在，导致引用该有效类型的部署被拒绝；
// 同名重建的 InstanceType 则可能读到旧的 acceleratable 标志并据此写入错误的数量。后者危害更大，因为错误的值会被持久化。
//
// InstanceType 的读取在每条规则内按名称缓存（memoize），而非按 role 缓存。变更与校验属于两次独立调用，
// 且新建绑定还需读取其 pool 与 backend。这些读取仅在用户发起写入时触发，而非在 reconcile 循环中执行。
//
// nolint: lll
// +k8s:webhook-gen:validating:group="worker.gpustack.ai",version="v1alpha1",resource="modeldeployments",scope="Namespaced"
// +k8s:webhook-gen:validating:operations=["CREATE","UPDATE"],failurePolicy="Fail",sideEffects="None",matchPolicy="Equivalent",timeoutSeconds=10
// +k8s:webhook-gen:mutating:group="worker.gpustack.ai",version="v1alpha1",resource="modeldeployments",scope="Namespaced"
// +k8s:webhook-gen:mutating:operations=["CREATE","UPDATE"],failurePolicy="Fail",sideEffects="None",matchPolicy="Equivalent",timeoutSeconds=10
type ModelDeploymentWebhook struct {
	// APIReader 直接穿透读取 API server。此处刻意不并列使用带缓存的客户端：
	// 本处理程序唯一填充默认值的字段需从其他对象读取，若缓存滞后将导致误判。
	APIReader ctrlcli.Reader

	// Namespace 和 ServiceAccount 指定了本 operator 运行的命名空间及所使用的服务账号。
	//
	// 两者共同标识了已绑定部署的副本数唯一合法写入方。未携带该身份的请求即为用户请求，而用户不得修改由 scaler 管理的副本数。
	Namespace      string
	ServiceAccount string
}

func (r *ModelDeploymentWebhook) SetupWebhook(_ context.Context, opts webhook.SetupOptions) (runtime.Object, error) {
	r.APIReader = opts.Manager.GetAPIReader()
	r.Namespace = systemname.NamespaceName
	r.ServiceAccount = kuberess.SystemRoutingServiceName

	return &workercore.ModelDeployment{}, nil
}

var (
	_ ctrladmission.Validator[runtime.Object] = (*ModelDeploymentWebhook)(nil)
	_ ctrladmission.Defaulter[runtime.Object] = (*ModelDeploymentWebhook)(nil)
	_ webhook.ReceiveDeletionUpdate           = (*ModelDeploymentWebhook)(nil)
)

// ReceiveDeletionUpdate 使本 webhook 在部署处于删除过程中时仍持续校验其更新请求。
// 其自身的 finalizer 会释放所持有的 Binding，因此对象会维持删除中状态直至释放完成，而 role 校验规则可防止在此窗口期内的编辑产生无法被消费的配置形态。
//
// 该标记对变更与校验两部分生效，因此启用删除期校验的同时也会启用删除期默认值填充。在删除窗口期内两部分各自的行为取决于是否需要读取其他对象，
// 这条分界线并非位于变更与校验之间，而是贯穿于校验规则内部。
//
// 在删除期间，凡需要读取 InstanceType 的规则在两部分中均跳过执行。此类规则在 InstanceType 缺失时会拒绝请求，因此若 InstanceType 先于部署被删除，清理 finalizer 的更新将被拒绝，进而导致对象无法完成自身销毁。
// Default 会整体跳过，因为为即将销毁的对象填充数量毫无意义。校验部分会保留所有仅凭对象自身即可判定的规则，仅略过依赖 InstanceType 的两条规则，
// 因为只有这两条规则会锁死对象，而其余规则仍负责防止该窗口期内的编辑产生无效配置。
//
// 这样做带来的代价是明确而非隐晦的：在部署删除期间进行的编辑可能会将某个 role 切换到其 InstanceType 不提供的模式。
// 由于部署正在销毁，不会再有组件渲染该结果；而另一种选择——拒绝该编辑——则会导致上述死锁问题。
func (r *ModelDeploymentWebhook) ReceiveDeletionUpdate() {}

// Default 为 role 中未设置的加速器数量填充默认值：在可加速的 InstanceType 上为 1 张卡，在其他类型上则不填充。
//
// 这与 Instance webhook 已采用的规则一致，此前仅本资源类型尚未实施该规则。未指定数量的 role 通常即希望请求常规资源（1 张卡），
// 而此前该情况下生成的副本却完全未请求任何加速器。
//
// 留空的代价并非得到一个更小的副本，而是使副本游离于本应受约束的配额之外。加速资源池的 ClusterQueue 仅涵盖对应厂商的资源额度，
// 因此未请求加速器的 role 不会向队列声明任何配额需求——且管理器已配置为忽略队列未声明的资源。在集群上的实测表现是：此类副本的 Workload 会预留资源并报告 Admitted，
// 其 Pod 正常运行，但队列中该加速卡额度的使用量却始终为零，且全程没有任何报错。
//
// 相较于它所取代的旧故障，这是一种更隐蔽的失效模式，也是该规则在其最初成因消失后依然保留的原因。此前每个 role 是其同组共享 Workload 中的一个 PodSet 时，
// 相同的配置形态会导致调度器写入少于 PodSet 数量的分配结果，API 拒绝该更新后 Workload 会无限重新入队——大约每秒触发上百个调度周期，持续空转并产生大量噪声。
// 如今每个副本各自拥有独立的单 PodSet Workload，上述冲突已不复存在；但仍会遗留副本占用加速器却未被计费的问题。
//
// 显式指定的 0 值会保持不变。0 是用户明确写入的值，静默替换它会使请求偏离用户原意。校验阶段会在与其他 role 共享的可加速类型上拒绝 0 值，并提示纯 CPU 副本应改用纯 CPU 的 InstanceType。
//
// 该逻辑在更新和创建时均会执行，因为已有 role 仍可能存在未设置的数量。校验阶段会拒绝新增或移除 role。
// 默认值填充函数仅能获取传入的新对象而无法看到旧对象，因此只要发现未设置的数量就会填充默认值；对于在此规则生效前已存储的对象，这会改变其先前已通过准入的请求。
// 这一行为之所以可接受，仅因为该 API 尚未进入任何正式发布版本，因此分支之外并不存在此类历史对象。
func (r *ModelDeploymentWebhook) Default(ctx context.Context, obj runtime.Object) error {
	md := obj.(*workercore.ModelDeployment)

	// 正在删除的对象不执行默认值填充。本处理程序绕过了共享的删除守卫以在删除窗口期内继续校验更新，而该设置同时作用于默认值填充。
	// 下方的读取逻辑在 InstanceType 不存在时会拒绝请求，因此若无此判断，当 InstanceType 先被删除时，清理 finalizer 的更新将被拒绝，对象也将永远无法释放。
	if md.DeletionTimestamp != nil {
		return nil
	}

	// 各个 role 可以各自指定不同的 InstanceType，因此每个 role 均依据其自身指定的类型填充默认值，且读取结果按名称缓存，而非假定全对象共用同一类型。
	// 这不再只是为了防范在校验之前运行而做的防御措施，而是本来的数据模型：部署中的各个 role 作为一个集合跨类型整体准入（每个副本一组），因此单个对象中包含多种类型属于正常用法，而非后续规则会拒绝的状态。
	seen := make(map[string]*worker.InstanceType, 1)
	for i := range md.Spec.Roles {
		role := &md.Spec.Roles[i]
		if role.Resources != nil && role.Resources.Accelerator != nil {
			continue
		}

		// 此时 schema 校验尚未执行，因此本 API 本应拒绝的名称仍会传入此处。对于空名称，将其留给后续规则按字段错误报告：
		// 若在此处查询空名称，会导致查询请求本身失败而非报错在对象字段上，从而返回“读取集群失败”而非“名称无效”的拒绝信息。
		if role.InstanceType == "" {
			continue
		}

		instType, ok := seen[role.InstanceType]
		if !ok {
			var err error
			if instType, err = r.getInstanceType(ctx, role.InstanceType, i); err != nil {
				return err
			}
			seen[role.InstanceType] = instType
		}
		if !instType.Spec.Acceleratable {
			continue
		}

		if role.Resources == nil {
			role.Resources = new(workercore.ModelDeploymentRoleResources)
		}
		tp, err := workerctrl.ModelDeploymentElasticTensorParallelSize(role)
		if err != nil {
			return err
		}
		role.Resources.Accelerator = resource.NewQuantity(int64(tp), resource.DecimalSI)
	}

	return nil
}

// getInstanceType 直接从 API server 读取单个 InstanceType，绝不走缓存。
//
// 法定人数（quorum）直读正是安全引入此默认值填充逻辑的全部前提。若 informer 滞后，缓存读取会在正反两个方向上误判结果：
// 刚创建的 InstanceType 在缓存中表现为不存在，导致引用有效类型的部署被拒绝；
// 而同名删除重建的 InstanceType 则可能读到旧的 acceleratable 标志并据此写入错误的数量——后者危害更大，因为错误的值会被持久化。
// 指定 ResourceVersion "0" 同样不可行：该请求由 API server 的 watch 缓存响应，同样会因缓存滞后而产生相同问题。
//
// 代价是每次准入请求读取一次，而非每个 role 读取一次。合法部署中的所有 role 通常指向同一类型，且调用方按名称缓存结果，因此读取会被合并。
// 本处理程序仅在用户发起单对象写入时运行，而非在 reconcile 循环中运行，因此在此处为一致性付出一次直读成本是更划算的权衡。
//
// 查询未命中会拒绝写入，这意味着只要所引用的类型缺失，就会在更新时阻塞对现有部署的一切编辑。
// 限制该影响范围的是：查询使用的是传入对象中的名称——将 role 指向一个确实存在的类型即可放行，且正在删除的对象根本不会触发默认值填充。
// 若改为放行缺失类型的写入，将会存入一个渲染器无法构建的部署，而该失败只会以 Event 形式暴露给用户，同时状态条件却仍提示尚未创建任何副本（与慢启动时的表现完全相同）。
//
// 错误信息直接指明 InstanceType 而非读取操作，因为两者会引导排查人员走向截然不同的方向。
// “instance type not found”提示去检查名称拼写；“could not read instance type”则提示去排查集群故障，若把前者报成后者，会让运维人员白白耗费时间去查找一个拼写错误。
func (r *ModelDeploymentWebhook) getInstanceType(
	ctx context.Context, name string, roleIdx int,
) (*worker.InstanceType, error) {
	instType := &worker.InstanceType{ObjectMeta: meta.ObjectMeta{Name: name}}
	rolePath := field.NewPath("spec.roles").Index(roleIdx).Child("instanceType")

	if err := r.APIReader.Get(ctx, ctrlcli.ObjectKeyFromObject(instType), instType); err != nil {
		if kerrors.IsNotFound(err) {
			return nil, field.NotFound(rolePath, name)
		}

		return nil, field.InternalError(rolePath, fmt.Errorf("get instance type: %w", err))
	}

	return instType, nil
}

// validateModelDeploymentHostAccess 将 Instance 的宿主机访问开关（gate）应用到每个 role。当管理员关闭某个开关时，role 此前已持有的访问权限依然有效。
func validateModelDeploymentHostAccess(
	old, md *workercore.ModelDeployment, privilegedAllowed, hostPathAllowed bool,
) field.ErrorList {
	var errs field.ErrorList
	oldRoles := make(map[string]*workercore.ModelDeploymentRole)
	if old != nil {
		for i := range old.Spec.Roles {
			oldRoles[old.Spec.Roles[i].Name] = &old.Spec.Roles[i]
		}
	}
	for i := range md.Spec.Roles {
		role := &md.Spec.Roles[i]
		held := oldRoles[role.Name]
		rolePath := field.NewPath("spec", "roles").Index(i)
		if role.Privileged && !privilegedAllowed && (held == nil || !held.Privileged) {
			errs = append(errs, field.Forbidden(rolePath.Child("privileged"),
				fmt.Sprintf("privileged mode is not allowed: enable the %q setting to allow it",
					settings.InstancePrivilegedAllowed.Name())))
		}
		if hostPathAllowed {
			continue
		}
		for j := range role.AdditionalVolumes {
			want := &role.AdditionalVolumes[j]
			if want.HostPath == nil {
				continue
			}
			covered := false
			if held != nil {
				for k := range held.AdditionalVolumes {
					prev := &held.AdditionalVolumes[k]
					if prev.HostPath != nil && coversHostAccess(
						&workercore.InstanceAdditionalVolume{HostPath: prev.HostPath, SubPath: prev.SubPath, ReadOnly: prev.ReadOnly},
						&workercore.InstanceAdditionalVolume{HostPath: want.HostPath, SubPath: want.SubPath, ReadOnly: want.ReadOnly},
					) {
						covered = true
						break
					}
				}
			}
			if !covered {
				errs = append(errs, field.Forbidden(rolePath.Child("additionalVolumes").Index(j).Child("hostPath"),
					fmt.Sprintf("mounting a host path is not allowed: enable the %q setting to allow it",
						settings.InstanceHostPathVolumeAllowed.Name())))
			}
		}
	}
	return errs
}

// validateModelDeploymentRoleListenPorts 拒绝“既声明了 ports 又在参数中传入了指向不同端口的 --port（以引擎能识别的任意拼写形式）”的 role。
// 仅当 role 未传入 --port 时，operator 才会将第一个声明的端口渲染为引擎的 --port，且 Service 会将流量指向该声明端口；
// 因此 role 自行传入指向另一端口的 --port 会使引擎脱离所有请求实际发往的端口。两者不可能同时正确，拒绝信息会同时列出这两个端口。
//
// 直连（direct）模式下的 decoder 角色不受此限制，因为在该模式下两者本就设计为不同：路由代理占用声明的端口，而 role 的 --port 将引擎置于其后，
// 这部分端口关系已由 validateModelDeploymentRouterPorts 负责校验。
//
// 对象原先已存在且 ports 与参数均未发生变化的 role 不予拦截。该规则在部分违规对象存储之后才引入，若因此拒绝后续所有编辑，
// 会使对象因未被触碰的字段而陷入僵死状态；只有当编辑修改了 ports 或参数任一字段时，才按本规则校验。
func validateModelDeploymentRoleListenPorts(old, md *workercore.ModelDeployment) field.ErrorList {
	held := make(map[string]*workercore.ModelDeploymentRole)
	if old != nil {
		for i := range old.Spec.Roles {
			held[old.Spec.Roles[i].Name] = &old.Spec.Roles[i]
		}
	}

	var errs field.ErrorList
	for i := range md.Spec.Roles {
		role := &md.Spec.Roles[i]
		if len(role.Ports) == 0 || workerctrl.ModelDeploymentRoleFrontedByProxy(md, role) {
			continue
		}
		if prev := held[role.Name]; prev != nil && kubemeta.DeepEqual(prev.Ports, role.Ports) &&
			slices.Equal(workerctrl.ModelDeploymentRoleArgs(prev), workerctrl.ModelDeploymentRoleArgs(role)) {
			continue
		}
		port, ok := workerctrl.ModelDeploymentRoleListenPort(md.Spec.Engine.Name, role)
		if !ok || port == role.Ports[0].Port {
			continue
		}

		rolePath := field.NewPath("spec", "roles").Index(i)
		argsPath := rolePath.Child("extraArgs")
		if len(role.Command) > 0 {
			argsPath = rolePath.Child("command")
		}
		errs = append(errs, field.Invalid(argsPath, fmt.Sprintf("--port=%d", port),
			fmt.Sprintf("role %q passes --port=%d in %s but declares %d in %s; the engine listens on "+
				"the one and the Service targets the other, so they must name the same port: drop "+
				"--port, or make the two equal",
				role.Name, port, argsPath, role.Ports[0].Port,
				rolePath.Child("ports").Index(0).Child("port"))))
	}

	return errs
}

// validateModelDeploymentRoleReservedListenPorts 拒绝“未声明 ports、却通过任意引擎可识别的 --port 拼写传入了 operator 为该 role 保留的端口”的 role。
// 渲染阶段会将该端口与自动生成的监听器端口一同放置到容器上，两个端口占用同一端口号会导致每次渲染均失败。
// 未传入 --port 的 role 将使用默认端口提供服务，该端口不被任何监听器占用。take-over 模式的 role 不受此限制，因为渲染阶段不会为其生成监听器。
//
// 与 validateModelDeploymentRolePorts 相同，当存在 router 时，拒绝错误会定位到 router 字段；否则定位到对象实际携带的 arguments 字段，而非其未声明的 ports 字段。
//
// 同一 role 原先已存在的端口冲突不予拦截，以免后续编辑锁死在本规则引入前已存储的对象。
// 此处直接比对“冲突状态本身”而非比对 role 的 ports 和 arguments，因为保留端口集合也会随 router 配置而变化：
// 例如向一个已传入事件端口的 role 所在对象添加 router，即使未触动该 role 本身，也会引入占用该端口的监听器。
func validateModelDeploymentRoleReservedListenPorts(old, md *workercore.ModelDeployment) field.ErrorList {
	held := make(map[string]int32)
	if old != nil {
		for i := range old.Spec.Roles {
			if port, _, ok := modelDeploymentRoleReservedListenPort(old, &old.Spec.Roles[i]); ok {
				held[old.Spec.Roles[i].Name] = port
			}
		}
	}

	var errs field.ErrorList
	for i := range md.Spec.Roles {
		role := &md.Spec.Roles[i]
		port, reserved, ok := modelDeploymentRoleReservedListenPort(md, role)
		if !ok {
			continue
		}
		if prev, stored := held[role.Name]; stored && prev == port {
			continue
		}

		argsPath := field.NewPath("spec", "roles").Index(i).Child("extraArgs")
		if md.Spec.Router != nil {
			errs = append(errs, field.Invalid(field.NewPath("spec", "router"), md.Spec.Router,
				fmt.Sprintf("role %q passes --port=%d in %s, which the operator reserves for a listener "+
					"it synthesizes onto this role under engine %q and router %q; reserved here are %v",
					role.Name, port, argsPath, md.Spec.Engine.Name, md.Spec.Router.Name, reserved)))
		} else {
			errs = append(errs, field.Invalid(argsPath, fmt.Sprintf("--port=%d", port),
				fmt.Sprintf("role %q passes --port=%d, which the operator reserves for a listener it "+
					"synthesizes onto this role under engine %q; reserved here are %v",
					role.Name, port, md.Spec.Engine.Name, reserved)))
		}
	}

	return errs
}

// modelDeploymentRoleReservedListenPort 返回未声明 ports 的 role 通过自身 --port 传入的端口号、该 role 上的保留端口集合，以及前者是否命中保留端口的布尔值。
func modelDeploymentRoleReservedListenPort(
	md *workercore.ModelDeployment, role *workercore.ModelDeploymentRole,
) (int32, []int32, bool) {
	if len(role.Ports) > 0 {
		return 0, nil, false
	}
	port, ok := workerctrl.ModelDeploymentRoleListenPort(md.Spec.Engine.Name, role)
	if !ok {
		return 0, nil, false
	}
	reserved := modelDeploymentRouterReservedPorts(md, role)

	return port, reserved, slices.Contains(reserved, port)
}

// validateModelDeploymentPoolTransport 针对每个 role 的引擎检查新的缓存绑定。
// 未变更的绑定视为已持有的既有权限（held access）：如同上述宿主机访问开关一样，若后端后续更改了传输方式，现有部署仍保持可编辑状态。
// 更新时，仅对切换了 instanceType 的 role 重新执行该规则，因为其读取的引擎信息由该类型的厂商决定；instanceType 未变的 role 则保留既有权限。
func (r *ModelDeploymentWebhook) validateModelDeploymentPoolTransport(
	ctx context.Context, old, md *workercore.ModelDeployment,
) (field.ErrorList, error) {
	if md.Spec.KVCache == nil || md.DeletionTimestamp != nil ||
		old != nil && kubemeta.DeepEqual(old.Spec.KVCache, md.Spec.KVCache) &&
			!modelDeploymentPoolTransportInputsChanged(old, md) {
		return nil, nil
	}
	path := field.NewPath("spec", "kvCache", "poolRef", "name")
	binding := &workercore.KVCachePoolBinding{}
	if err := r.APIReader.Get(ctx, ctrlcli.ObjectKey{
		Namespace: md.Namespace, Name: md.Spec.KVCache.PoolRef.Name,
	}, binding); err != nil {
		if kerrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, field.InternalError(path, fmt.Errorf("get kv cache pool binding: %w", err))
	}
	pool := &workercore.KVCachePool{}
	if err := r.APIReader.Get(ctx, ctrlcli.ObjectKey{Name: binding.Spec.PoolRef.Name}, pool); err != nil {
		if kerrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, field.InternalError(path, fmt.Errorf("get kv cache pool: %w", err))
	}
	if len(pool.Spec.Backends) == 0 {
		return nil, nil
	}
	backend := &workercore.KVCacheBackend{}
	if err := r.APIReader.Get(ctx, ctrlcli.ObjectKey{Name: pool.Spec.Backends[0]}, backend); err != nil {
		if kerrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, field.InternalError(path, fmt.Errorf("get kv cache backend: %w", err))
	}
	offers := mooncake.MemberProtocols(backend)
	nonempty := sets.New[string]()
	for _, offer := range offers {
		if offer != "" {
			nonempty.Insert(offer)
		}
	}
	if nonempty.Len() < 2 {
		return nil, nil
	}
	// 创建时会校验所有角色；更新时，类型未变更的角色保留原有访问权限，
	// 变更后的绑定则会整体对照新的资源池重新校验。
	judgeAll := old == nil || !kubemeta.DeepEqual(old.Spec.KVCache, md.Spec.KVCache)
	stored := make(map[string]*workercore.ModelDeploymentRole, len(md.Spec.Roles))
	if !judgeAll {
		for i := range old.Spec.Roles {
			stored[old.Spec.Roles[i].Name] = &old.Spec.Roles[i]
		}
	}
	seen := make(map[string]struct{}, len(md.Spec.Roles))
	for i := range md.Spec.Roles {
		role := &md.Spec.Roles[i]
		if !judgeAll {
			if was := stored[role.Name]; was != nil && was.InstanceType == role.InstanceType {
				continue
			}
		}
		if _, ok := seen[role.InstanceType]; ok {
			continue
		}
		seen[role.InstanceType] = struct{}{}
		manufacturer := ""
		if md.Spec.Engine.Name == workercore.ModelDeploymentEngineVLLM {
			instType, err := r.getInstanceType(ctx, role.InstanceType, i)
			if err != nil {
				return nil, err
			}
			manufacturer = instType.Status.Detail.Manufacturer
		}
		engine, err := workerctrl.ModelDeploymentInjectEngine(md.Spec.Engine.Name, manufacturer)
		if err != nil {
			return nil, field.InternalError(path, err)
		}
		if err := inject.ValidateBindingTransport(engine, offers); err != nil {
			return field.ErrorList{field.Forbidden(path, err.Error())}, nil
		}
	}
	return nil, nil
}

// modelDeploymentPoolTransportInputsChanged 返回更新操作是否将某个角色切换到了不同的
// instanceType。这是对象自身携带的唯一传输输入：每个角色运行的引擎取决于该类型的
// 厂商属性，因此切换类型可能会推翻已有配置此前通过的校验结论。
func modelDeploymentPoolTransportInputsChanged(old, md *workercore.ModelDeployment) bool {
	if len(old.Spec.Roles) != len(md.Spec.Roles) {
		return true
	}
	stored := make(map[string]string, len(old.Spec.Roles))
	for i := range old.Spec.Roles {
		stored[old.Spec.Roles[i].Name] = old.Spec.Roles[i].InstanceType
	}
	for i := range md.Spec.Roles {
		if stored[md.Spec.Roles[i].Name] != md.Spec.Roles[i].InstanceType {
			return true
		}
	}

	return false
}

// modelDeploymentDirectLegProbe 按非 Ascend 且具有已计算状态的类型的处理方式，渲染单个角色的
// 直连传输链路。厂商属性只会强制置空该链路或原样返回声明的协议，因此只要探针结果相等，
// 无论该角色的类型如何上报，其传输链路都保持不变。
func modelDeploymentDirectLegProbe(md *workercore.ModelDeployment, role *workercore.ModelDeploymentRole) string {
	return workerctrl.ModelDeploymentDirectInterfaceProtocol(md, role, nodefeature.ManufacturerNVIDIA)
}

// modelDeploymentRoleInterfaceInputsChanged 返回更新操作是否修改了接口请求校验规则针对单个
// 角色读取的输入：接口请求量、InstanceType 以及是否配置了 command。原对象中不存在的
// 角色视为已变更。角色按名称匹配，资源数量按值比较。
//
// 未变更的角色不会重新校验：该规则会解析集群侧对象，而这些对象在已保存的部署下可能发生漂移，
// 若对输入未变动的角色重新执行校验，会因非本次编辑引起的漂移而拒绝该角色。
func modelDeploymentRoleInterfaceInputsChanged(was, role *workercore.ModelDeploymentRole) bool {
	return was == nil || role.InstanceType != was.InstanceType ||
		(len(role.Command) > 0) != (len(was.Command) > 0) ||
		!kubemeta.DeepEqual(interfaceQuantity(role.Resources), interfaceQuantity(was.Resources))
}

// interfaceQuantity 返回角色的接口请求量，路径上任一层级未设置时返回 nil。
func interfaceQuantity(ress *workercore.ModelDeploymentRoleResources) *resource.Quantity {
	if ress == nil {
		return nil
	}
	return ress.Interface
}

// modelDeploymentRoleNeedsInterfaceRejudge 返回本次写入是否必须重新校验某个请求接口的角色。
// 创建时会校验所有角色；更新时仅校验自身输入发生变动的角色。router 和传输声明仅通过
// 生产辅助函数渲染的直连链路影响角色，因此直连链路值保持不变的角色保留其已保存校验结论
// 提供的豁免路径。
func (r *ModelDeploymentWebhook) modelDeploymentRoleNeedsInterfaceRejudge(
	ctx context.Context, old, md *workercore.ModelDeployment, was, role *workercore.ModelDeploymentRole, i int,
) (bool, error) {
	if modelDeploymentRoleInterfaceInputsChanged(was, role) {
		return true, nil
	}
	if modelDeploymentDirectLegProbe(old, was) == modelDeploymentDirectLegProbe(md, role) {
		return false, nil
	}
	// 探针结果不同，但只有非 Ascend 厂商才会将其转化为实际的链路变更，
	// 因此在重新校验前，会先按该类型自身的厂商属性比较渲染出的链路。
	manufacturer := ""
	if md.Spec.Engine.Name == workercore.ModelDeploymentEngineVLLM {
		instType, err := r.getInstanceType(ctx, role.InstanceType, i)
		if err != nil {
			return false, err
		}
		manufacturer = instType.Status.Detail.Manufacturer
	}
	return workerctrl.ModelDeploymentDirectInterfaceProtocol(old, was, manufacturer) !=
		workerctrl.ModelDeploymentDirectInterfaceProtocol(md, role, manufacturer), nil
}

// validateModelDeploymentInterfaceRequests 根据已解析的缓存后端以及各受检角色实际渲染的
// 直连链路，校验设备键。未解析的缓存绑定不能作为正值设备请求的依据，因为它后续可能解析为
// 其他网络架构。
//
// 创建时会校验所有请求接口的角色；更新时会校验自身输入发生变动的角色，以及渲染后的直连传输
// 链路随 router 或传输声明变动的角色；链路值未变的角色保留其已保存校验结论给出的豁免路径。
func (r *ModelDeploymentWebhook) validateModelDeploymentInterfaceRequests(
	ctx context.Context, old, md *workercore.ModelDeployment,
) (field.ErrorList, error) {
	stored := make(map[string]*workercore.ModelDeploymentRole, len(md.Spec.Roles))
	if old != nil {
		for i := range old.Spec.Roles {
			stored[old.Spec.Roles[i].Name] = &old.Spec.Roles[i]
		}
	}

	var requested []int
	for i := range md.Spec.Roles {
		ress := md.Spec.Roles[i].Resources
		if ress == nil || ress.Interface == nil || ress.Interface.Sign() <= 0 {
			continue
		}
		if old != nil {
			judge, err := r.modelDeploymentRoleNeedsInterfaceRejudge(
				ctx, old, md, stored[md.Spec.Roles[i].Name], &md.Spec.Roles[i], i,
			)
			if err != nil {
				return nil, err
			}
			if !judge {
				continue
			}
		}
		requested = append(requested, i)
	}
	if len(requested) == 0 {
		return nil, nil
	}

	firstPath := field.NewPath("spec", "roles").Index(requested[0]).Child("resources", "interface")
	var protocols []string
	if md.Spec.KVCache != nil {
		binding := &workercore.KVCachePoolBinding{}
		if err := r.APIReader.Get(ctx, ctrlcli.ObjectKey{
			Namespace: md.Namespace, Name: md.Spec.KVCache.PoolRef.Name,
		}, binding); err != nil {
			if kerrors.IsNotFound(err) {
				return field.ErrorList{field.Forbidden(firstPath, "cache binding must resolve before requesting interfaces")}, nil
			}
			return nil, field.InternalError(firstPath, fmt.Errorf("get kv cache pool binding: %w", err))
		}
		pool := &workercore.KVCachePool{}
		if err := r.APIReader.Get(ctx, ctrlcli.ObjectKey{Name: binding.Spec.PoolRef.Name}, pool); err != nil {
			if kerrors.IsNotFound(err) {
				return field.ErrorList{field.Forbidden(firstPath, "cache pool must resolve before requesting interfaces")}, nil
			}
			return nil, field.InternalError(firstPath, fmt.Errorf("get kv cache pool: %w", err))
		}
		if len(pool.Spec.Backends) == 0 {
			return field.ErrorList{field.Forbidden(firstPath, "cache backend must resolve before requesting interfaces")}, nil
		}
		backend := &workercore.KVCacheBackend{}
		if err := r.APIReader.Get(ctx, ctrlcli.ObjectKey{Name: pool.Spec.Backends[0]}, backend); err != nil {
			if kerrors.IsNotFound(err) {
				return field.ErrorList{field.Forbidden(firstPath, "cache backend must resolve before requesting interfaces")}, nil
			}
			return nil, field.InternalError(firstPath, fmt.Errorf("get kv cache backend: %w", err))
		}
		protocols = mooncake.MemberProtocols(backend)
	}

	var errs field.ErrorList
	for _, i := range requested {
		role := &md.Spec.Roles[i]
		count, whole := role.Resources.Interface.AsInt64()
		if !whole {
			continue
		}
		manufacturer := ""
		if md.Spec.Engine.Name == workercore.ModelDeploymentEngineVLLM {
			instType, err := r.getInstanceType(ctx, role.InstanceType, i)
			if err != nil {
				return nil, err
			}
			manufacturer = instType.Status.Detail.Manufacturer
		}
		roleProtocols := protocols
		if len(role.Command) > 0 {
			roleProtocols = nil
		}
		_, err := workerctrl.ModelDeploymentInterfaceResource(roleProtocols,
			workerctrl.ModelDeploymentDirectInterfaceProtocol(md, role, manufacturer), count)
		if err != nil {
			errs = append(errs, field.Invalid(field.NewPath("spec", "roles").Index(i).Child("resources", "interface"),
				role.Resources.Interface.String(), err.Error()))
		}
	}
	return errs, nil
}

func (r *ModelDeploymentWebhook) ValidateCreate(
	ctx context.Context, obj runtime.Object,
) (ctrladmission.Warnings, error) {
	md := obj.(*workercore.ModelDeployment)

	errs := validateModelDeployment(md, nil)
	errs = append(errs, r.validateModelDeploymentScalerFence(ctx, nil, md)...)
	errs = append(errs, r.ValidateModelDeploymentElasticEP(ctx, md, nil)...)
	errs = append(errs, validateModelDeploymentHostAccess(nil, md,
		hostAccessAllowed(ctx, settings.InstancePrivilegedAllowed),
		hostAccessAllowed(ctx, settings.InstanceHostPathVolumeAllowed))...)
	errs = append(errs, validateModelDeploymentRoleListenPorts(nil, md)...)
	errs = append(errs, validateModelDeploymentRoleReservedListenPorts(nil, md)...)
	errs = append(errs, validateModelDeploymentServedModelNames(nil, md)...)
	errs = append(errs, validateModelDeploymentKVCacheDtype(md, workerctrl.ModelDeploymentOwnsKVCacheDtype(ctx))...)

	typeErrs, err := r.validateRoleResourcesAgainstInstanceTypes(ctx, md)
	if err != nil {
		return nil, err
	}
	errs = append(errs, typeErrs...)

	epdErrs, err := r.validateModelDeploymentEPD(ctx, md)
	if err != nil {
		return nil, err
	}
	errs = append(errs, epdErrs...)

	if len(errs) == 0 {
		interfaceErrs, err := r.validateModelDeploymentInterfaceRequests(ctx, nil, md)
		if err != nil {
			return nil, err
		}
		errs = append(errs, interfaceErrs...)
	}
	if len(errs) == 0 {
		transportErrs, err := r.validateModelDeploymentPoolTransport(ctx, nil, md)
		if err != nil {
			return nil, err
		}
		errs = append(errs, transportErrs...)
	}

	if len(errs) > 0 {
		return nil, kerrors.NewInvalid(md.GroupVersionKind().GroupKind(), md.Name, errs)
	}

	return nil, nil
}

func (r *ModelDeploymentWebhook) ValidateUpdate(
	ctx context.Context, oldObj, newObj runtime.Object,
) (ctrladmission.Warnings, error) {
	md := newObj.(*workercore.ModelDeployment)
	old, _ := oldObj.(*workercore.ModelDeployment)

	// 部署“是什么”不可编辑，而“当前如何运行”可以编辑。回答前一个问题的字段在此处被拒绝修改，
	// 其划分标准直接写在 validateModelDeploymentIdentity 上，而非仅留下由此产出的字段列表，
	// 以便后续新增字段时有明确的判断依据。
	//
	// 此处传入了对象已有的角色名称集合，原因是在对象持久化之后才加入本处理器的规则不能导致
	// 该对象陷入无法更新的境地。Service 名称长度规则就有此风险：部署自身的名称不可变，因此
	// 组合名称过长的角色永远无法缩短，从而导致后续每一次编辑（包括移除该违规角色的编辑）都会被
	// 拒绝。这比该规则原本要防止的调谐失败更加严重。
	errs := validateModelDeployment(md, modelDeploymentRoleNames(oldObj))
	errs = append(errs, r.validateModelDeploymentScalerFence(ctx, old, md)...)
	errs = append(errs, r.ValidateModelDeploymentElasticEP(ctx, md, old)...)
	errs = append(errs, validateModelDeploymentHostAccess(old, md,
		hostAccessAllowed(ctx, settings.InstancePrivilegedAllowed),
		hostAccessAllowed(ctx, settings.InstanceHostPathVolumeAllowed))...)
	errs = append(errs, validateModelDeploymentRoleListenPorts(old, md)...)
	errs = append(errs, validateModelDeploymentRoleReservedListenPorts(old, md)...)
	errs = append(errs, validateModelDeploymentServedModelNames(old, md)...)
	errs = append(errs, validateModelDeploymentKVCacheDtype(md, workerctrl.ModelDeploymentOwnsKVCacheDtype(ctx))...)
	errs = append(errs, validateModelDeploymentIdentity(md, old)...)
	errs = append(errs, validateModelDeploymentRouterName(md, old)...)

	typeErrs, err := r.validateRoleResourcesAgainstInstanceTypes(ctx, md)
	if err != nil {
		return nil, err
	}
	errs = append(errs, typeErrs...)

	epdErrs, err := r.validateModelDeploymentEPD(ctx, md)
	if err != nil {
		return nil, err
	}
	errs = append(errs, epdErrs...)

	if len(errs) == 0 && old != nil {
		interfaceErrs, err := r.validateModelDeploymentInterfaceRequests(ctx, old, md)
		if err != nil {
			return nil, err
		}
		errs = append(errs, interfaceErrs...)
	}
	if len(errs) == 0 {
		transportErrs, err := r.validateModelDeploymentPoolTransport(ctx, old, md)
		if err != nil {
			return nil, err
		}
		errs = append(errs, transportErrs...)
	}

	if len(errs) > 0 {
		return nil, kerrors.NewInvalid(md.GroupVersionKind().GroupKind(), md.Name, errs)
	}

	return nil, nil
}

// modelDeploymentIdentityMessage 是所有身份字段变更拒绝信息共用的原因说明。
//
// 它陈述的是规则本身而非实现机制，二者的区别在于会引导读者走向何处。该不可变约束早期的
// 表述是“缩小两个写入方可能产生分歧的范围”；若错误消息携带这种表述，会让运维人员去排查一个
// 根本不存在的锁问题。他们真正需要的是该规则所回答的问题——该值决定了对象之所以成为当前部署
// 的身份特征，因此不同的值描述的是另一个不同的部署，应当新建而非编辑。
const modelDeploymentIdentityMessage = "this is part of what makes this deployment the deployment " +
	"it is: a different value describes a different deployment, which is created rather than edited"

// validateModelDeploymentRouterName 允许添加或移除 router，但不允许就地变更其名称。
// 更换 router 实现应先删除再创建，并在两者之间保留间隔。
func validateModelDeploymentRouterName(md, old *workercore.ModelDeployment) field.ErrorList {
	if old == nil || old.Spec.Router == nil || md.Spec.Router == nil ||
		old.Spec.Router.Name == md.Spec.Router.Name {
		return nil
	}

	return field.ErrorList{field.Invalid(
		field.NewPath("spec", "router", "name"), md.Spec.Router.Name, "field is immutable",
	)}
}

// validateModelDeploymentIdentity 拒绝改变部署“是什么”的更新，仅放行改变“当前如何运行”的更新。
//
// 评判标准是下方这条原则，而非字段清单本身。如果一个字段回答的是“这是哪一个部署”——
// 提供什么服务、由什么提供服务、与谁共享缓存，则该字段冻结不可改；如果它回答的是“该部署
// 当前如何运行”——多少副本与多大规格、使用哪个资源池和卡型、哪个构建版本、如何拉取和调优
// 以及如何启动，则该字段可编辑。评估新增字段时应追问上述问题，而非机械地往清单里追加；
// 单纯靠先例堆砌的清单最终会失去语义。角色的规格形态字段属于可编辑一侧，而 kind 保持冻结，
// 因为它定义了角色的本质。
//
// 角色按名称匹配，绝不按位置匹配。该字段是以名称为键的 listType=map，因此重排后的列表
// 仍代表同一组角色，API 也本就如此看待它；若按索引比较，就会在声明式 apply 未作实质修改时
// 错误拒绝请求，且该字段恰恰最容易在序列化后以不同顺序返回。
//
// 比较前无需对已存储对象再执行默认值填充，这是本 Webhook 自身的机制保障而非假设。变更
// Webhook 同时注册了 CREATE 与 UPDATE，因此每个入库对象在写入时都已填充默认值，携带了与
// 新请求对象相同的加速器数量。此前可能存在的缺口——即默认值引入前入库的对象会被视作 nil
// 变为 1 并在无人修改时遭到拒绝——因该 API 尚未在任何版本中发布而自然关闭，这与 Default
// 所依据的前提一致。
//
// spec 之外的内容一概不作比较。Labels、annotations 和 finalizers 保持可编辑，因为本
// operator 等控制器会写入它们；而 status 本就不由用户提交。
func validateModelDeploymentIdentity(md, old *workercore.ModelDeployment) field.ErrorList {
	if old == nil {
		return nil
	}

	specPath := field.NewPath("spec")

	var errs field.ErrorList
	if !kubemeta.DeepEqual(md.Spec.Model, old.Spec.Model) {
		errs = append(errs, field.Invalid(
			specPath.Child("model"), md.Spec.Model, modelDeploymentIdentityMessage,
		))
	}
	if md.Spec.Engine.Name != old.Spec.Engine.Name {
		errs = append(errs, field.Invalid(
			specPath.Child("engine", "name"), md.Spec.Engine.Name, modelDeploymentIdentityMessage,
		))
	}
	if !kubemeta.DeepEqual(md.Spec.KVCache, old.Spec.KVCache) {
		errs = append(errs, field.Invalid(
			specPath.Child("kvCache"), md.Spec.KVCache, modelDeploymentIdentityMessage,
		))
	}

	return append(errs, validateModelDeploymentRoleIdentity(md, old)...)
}

// validateModelDeploymentRoleIdentity 拒绝变更角色集合，或修改对象已有角色的冻结字段。
//
// 角色集合属于部署形态的一部分，因此新增和移除角色都会被拒绝，并在读者可定位的位置
// 报错：新增角色定位到其所在索引，移除角色定位到角色列表路径。此处遍历两个切片而非 map，
// 以确保拒绝信息按稳定顺序输出，从而使测试能够断言具体出错的字段，而不只是验证存在错误。
func validateModelDeploymentRoleIdentity(md, old *workercore.ModelDeployment) field.ErrorList {
	rolesPath := field.NewPath("spec", "roles")

	stored := make(map[string]*workercore.ModelDeploymentRole, len(old.Spec.Roles))
	for i := range old.Spec.Roles {
		stored[old.Spec.Roles[i].Name] = &old.Spec.Roles[i]
	}

	var errs field.ErrorList
	incoming := sets.New[string]()
	for i := range md.Spec.Roles {
		role := &md.Spec.Roles[i]
		incoming.Insert(role.Name)

		was, ok := stored[role.Name]
		if !ok {
			errs = append(errs, field.Invalid(
				rolesPath.Index(i).Child("name"), role.Name, modelDeploymentIdentityMessage,
			))

			continue
		}

		errs = append(errs, validateModelDeploymentRoleIdentityFields(rolesPath.Index(i), role, was)...)
	}

	for i := range old.Spec.Roles {
		if name := old.Spec.Roles[i].Name; !incoming.Has(name) {
			errs = append(errs, field.Invalid(rolesPath, name, modelDeploymentIdentityMessage))
		}
	}

	return errs
}

// validateModelDeploymentRoleIdentityFields 将单个角色的冻结字段与已存储的同名角色进行比较。
//
// Kind 是角色唯一的冻结字段：它定义了角色的本质，因此将 Server 角色重新声明为 Decode
// 相当于顶着同一名称的另一个部署。size、instanceType、resources 和 command 描述角色当前
// 如何运行，均支持编辑，并通过整组替换生效。elastic-EP 配置通过
// validateModelDeploymentElasticIdentity 锁定其自身角色。
func validateModelDeploymentRoleIdentityFields(
	rolePath *field.Path, role, was *workercore.ModelDeploymentRole,
) field.ErrorList {
	var errs field.ErrorList
	if role.Kind != was.Kind {
		errs = append(errs, field.Invalid(
			rolePath.Child("kind"), role.Kind, modelDeploymentIdentityMessage,
		))
	}

	return errs
}

// modelDeploymentRoleNames 返回对象已有的角色名称集合，创建时返回 nil。
func modelDeploymentRoleNames(obj runtime.Object) sets.Set[string] {
	md, ok := obj.(*workercore.ModelDeployment)
	if !ok {
		return nil
	}

	names := sets.New[string]()
	for i := range md.Spec.Roles {
		names.Insert(md.Spec.Roles[i].Name)
	}

	return names
}

func (r *ModelDeploymentWebhook) ValidateDelete(
	_ context.Context, _ runtime.Object,
) (ctrladmission.Warnings, error) {
	// 此处不拒绝删除操作。阻止部署被直接删除的是其自身的 finalizer（负责释放持有的
	// Binding），该过程依赖本处理器无法感知的消费方。
	return nil, nil
}

// validateModelDeployment 包含所有仅凭对象本身即可判断的校验规则。
//
// 单角色版本此前在此预留的接缝——将长期有效的规则与当时临时性的单角色限制分离开——
// 现已完成使命：validateModelDeploymentSingleRole 已移除并由下方的分组规则取代，
// 解除该限制确实只需删除一次调用。
func validateModelDeployment(
	md *workercore.ModelDeployment, existingRoles sets.Set[string],
) field.ErrorList {
	errs := validateModelDeploymentRoles(md)
	errs = append(errs, validateModelDeploymentKVCache(md)...)
	errs = append(errs, validateModelDeploymentRolesCount(md)...)
	errs = append(errs, validateModelDeploymentRoleNames(md)...)
	errs = append(errs, validateModelDeploymentRoleServiceNames(md, existingRoles)...)
	errs = append(errs, validateModelDeploymentServiceNamesAreDistinct(md)...)
	errs = append(errs, validateModelDeploymentRoleMemberNames(md)...)
	errs = append(errs, validateModelDeploymentRoleKinds(md)...)
	errs = append(errs, validateModelDeploymentRoleTopology(md)...)
	errs = append(errs, validateModelDeploymentRouter(md)...)
	errs = append(errs, validateModelDeploymentExternalDPRouter(md)...)

	return errs
}

func validateModelDeploymentRoleTopology(md *workercore.ModelDeployment) field.ErrorList {
	rolesPath := field.NewPath("spec", "roles")
	var errs field.ErrorList
	for i := range md.Spec.Roles {
		topology := md.Spec.Roles[i].Topology
		if topology == nil || topology.RequiredLevel == "" {
			continue
		}
		path := rolesPath.Index(i).Child("topology", "requiredLevel")
		if msgs := validation.IsQualifiedName(topology.RequiredLevel); len(msgs) > 0 {
			errs = append(errs, field.Invalid(path, topology.RequiredLevel,
				"is not a topology label key: "+strings.Join(msgs, "; ")))
		}
		if topology.RequiredLevel == core.LabelHostname {
			errs = append(errs, field.Forbidden(path,
				"kubernetes.io/hostname is implicit and cannot be required; "+
					"omitting requiredLevel leaves placement unconstrained, it does not keep the Pods on one Node"))
		}
	}
	return errs
}

// modelDeploymentRouterOwnedArgs 按 router 分类，列出由 router 渲染逻辑接管推导的参数标志。
//
// 这些条目分为两类，它们之所以共用相同的拒绝提示，仅仅因为对用户而言结果一致。其中大多数
// 被拒绝是因为 operator 已自行计算了对应的值——选择器、目标端口和配置文件路径都只是
// 在重复表达本部署自身对象已决定的内容，同一设置不能出现两个值而无法区分。
//
// --secure-serving 和 --metrics-endpoint-auth 被拒绝则是出于另一个原因：它们并非推导值，
// 而是职责边界。router 管理集群内部的东西向流量；传输安全与调用方鉴权属于南北向职责，
// 归负责接入流量的网关所有。这两项在渲染时均被显式关闭（与上游默认开启相反），且渲染代码
// 说明了原因。若将它们排除在此列表之外，并不会使其获得有意义的可配置性，反而会把架构边界
// 变成按对象随意选择的开关——这正是此处要拒绝的。
var modelDeploymentRouterOwnedArgs = map[string][]string{
	workercore.ModelDeploymentRouterLLMD: {
		"--endpoint-selector",
		"--endpoint-target-ports",
		"--config-file",
		"--secure-serving",
		"--grpc-health-port",
		"--metrics-endpoint-auth",
	},
	// 绑定地址和连接器参数之所以由 operator 接管，正是因为它们需要被渲染出来：这三项各自覆盖了
	// 上游会静默失效的默认值，如果允许用户值生效，就会重新引入渲染逻辑本要防止的故障——要么
	// router 完全不可达，要么能够正确路由却从不执行传输。其余参数则是在重复表达本部署自身对象
	// 已决定的内容。
	workercore.ModelDeploymentRouterVLLM: {
		"--host",
		"--port",
		"--prometheus-host",
		"--prometheus-port",
		"--kv-connector",
		"--service-discovery",
		"--service-discovery-namespace",
		"--service-discovery-port",
		"--selector",
		"--prefill-selector",
		"--decode-selector",
		"--vllm-pd-disaggregation",
		"--epd-config",
	},
	// 与上一行条目清单相同，但减去该项目不存在的两个参数：它的传输后端即引擎自身的后端，
	// 且其分离式部署开关的拼写不包含引擎名称。此处直接显式列出而非相互推导，因为决定条目
	// 是否正确的依据是该参数确实存在于某一个上游程序中，两个程序今天碰巧写法一致并不意味着
	// 它们是同一个程序。
	workercore.ModelDeploymentRouterSGLang: {
		"--host",
		"--port",
		"--prometheus-host",
		"--prometheus-port",
		"--service-discovery",
		"--service-discovery-namespace",
		"--service-discovery-port",
		"--selector",
		"--prefill-selector",
		"--decode-selector",
		"--pd-disaggregation",
	},
}

// modelDeploymentRouterEngines 定义了每种 router 允许前置代理的引擎。
//
// 该映射是按引擎严格匹配的而非开放通用，且各条目之间并不对称。"llm-d-router" 支持两种
// 引擎，因为上游为每种引擎都提供了对应的握手连接器和指标配置。其余取值则是特定项目针对
// 自身引擎的 router，若允许其代理其他引擎，就等同于做出本仓库尚未实测验证的兼容性承诺；
// 以后放宽限制属于向后扩展，而事后收紧限制则会破坏已存在的对象。
var modelDeploymentRouterEngines = map[string][]string{
	workercore.ModelDeploymentRouterLLMD: {
		workercore.ModelDeploymentEngineVLLM,
		workercore.ModelDeploymentEngineSGLang,
	},
	workercore.ModelDeploymentRouterVLLM: {
		workercore.ModelDeploymentEngineVLLM,
	},
	workercore.ModelDeploymentRouterSGLang: {
		workercore.ModelDeploymentEngineSGLang,
	},
}

func validateModelDeploymentRouter(md *workercore.ModelDeployment) field.ErrorList {
	if md.Spec.Router == nil {
		return nil
	}

	var errs field.ErrorList
	argsPath := field.NewPath("spec", "router", "extraArgs")
	for i, arg := range md.Spec.Router.ExtraArgs {
		name := workerctrl.ModelDeploymentArgName(arg)
		if !slices.Contains(modelDeploymentRouterOwnedArgs[md.Spec.Router.Name], name) {
			continue
		}
		errs = append(errs, field.Invalid(argsPath.Index(i), arg, fmt.Sprintf(
			"%q is set by the operator for router %q and must not be supplied here, because two "+
				"values for it cannot be told apart", name, md.Spec.Router.Name,
		)))
	}

	// 对于没有该概念的 router，该阈值会被直接拒绝而非静默忽略。本 API 此前一直拒绝“允许填写
	// 却不渲染任何内容”的字段设计；并且由于 router 名称在创建后即冻结，这一拒绝规则是稳定的——
	// 对象不会因为后续修改其他字段而突然变得无效。
	if md.Spec.Router.DisaggregationThresholdTokens != nil &&
		md.Spec.Router.Name != workercore.ModelDeploymentRouterLLMD {
		errs = append(errs, field.Invalid(
			field.NewPath("spec", "router", "disaggregationThresholdTokens"),
			*md.Spec.Router.DisaggregationThresholdTokens,
			fmt.Sprintf("router %q has no prompt-token threshold to set; it is meaningful under %q "+
				"alone, whose scheduler decides per request whether to split one",
				md.Spec.Router.Name, workercore.ModelDeploymentRouterLLMD),
		))
	}

	if engines, known := modelDeploymentRouterEngines[md.Spec.Router.Name]; known &&
		!slices.Contains(engines, md.Spec.Engine.Name) {
		errs = append(errs, field.Invalid(field.NewPath("spec", "router", "name"), md.Spec.Router.Name,
			fmt.Sprintf("router %q does not front engine %q; it accepts %s",
				md.Spec.Router.Name, md.Spec.Engine.Name, strings.Join(engines, ", "))))
	}

	// 引擎指标表仅作为该选择器（picker）的前置条件。它只用于向该 router 的指标提取器提供
	// 数据；另外两种 router 基于各自观测到的信息进行评分，完全不读取该表。此处限制并非偏好：
	// 拒绝信息中的 router 名称取自本包自身常量而非对象字段，如果漏加条件判断，就会向请求
	// 某种 router 的用户错误地提示另一种 router 需要某项指标。
	if md.Spec.Router.Name == workercore.ModelDeploymentRouterLLMD {
		engine := md.Spec.Engine.Name
		if mapped, ok := workerctrl.ModelDeploymentEngineUpstreamName(engine); ok {
			engine = mapped
		}
		if _, err := router.MetricsForEngine(engine); err != nil {
			errs = append(errs, field.Invalid(field.NewPath("spec", "engine"), md.Spec.Engine.Name, err.Error()))
		}
	}

	ports := make([]int32, 0, len(md.Spec.Roles))
	for i := range md.Spec.Roles {
		role := &md.Spec.Roles[i]
		if port := workerctrl.ModelDeploymentRoleServingProtocol(role); port != core.ProtocolTCP {
			errs = append(errs, field.Invalid(field.NewPath("spec", "router"), md.Spec.Router,
				fmt.Sprintf("every routed role must use TCP serving ports; role %q uses %s",
					role.Name, port)))
		}
		if workerctrl.ModelDeploymentRoleServingScheme(md.Spec.Engine.Name, role) == core.URISchemeHTTPS {
			errs = append(errs, field.Invalid(field.NewPath("spec", "router"), md.Spec.Router,
				fmt.Sprintf("managed router supports plaintext engine endpoints only; role %q uses HTTPS",
					role.Name)))
		}
		ports = append(ports, workerctrl.ModelDeploymentRoleServingPort(md, role))
		errs = append(errs, validateModelDeploymentRouterPorts(md, role)...)
	}
	slices.Sort(ports)
	ports = slices.Compact(ports)
	if len(ports) != 1 {
		errs = append(errs, field.Invalid(field.NewPath("spec", "router"), md.Spec.Router,
			fmt.Sprintf("every routed role must use the same serving port; got %v", ports)))
	}

	return errs
}

// validateModelDeploymentRolePorts 拒绝与 operator 为副本合成的监听器发生冲突的端口配置。
// 该校验对所有角色（无论是否配置了 router）都会执行，因为不依赖 router 的监听器——即
// 随显式声明的 PD 配对角色而启用的 SGLang bootstrap registry——同样会渲染到挂接共享
// 资源池且未配置 router 的 prefiller 上。该规则放在准入阶段，是因为它所防范的冲突在运行期
// 是不可恢复的：副本完成渲染后，进程就会尝试绑定一个已被其他组件占用的端口。
func validateModelDeploymentRolePorts(
	md *workercore.ModelDeployment, role *workercore.ModelDeploymentRole, rolePath *field.Path,
) field.ErrorList {
	var errs field.ErrorList

	// 渲染逻辑会将这些监听器放在角色声明端口所在的同一容器上；对于显式声明了端口的角色，该声明
	// 集合就是全部冲突面——除不保留任何端口的直连 decoder 外，其自身的 --port 参数在所有情况下
	// 都绑定到声明端口中的第一个。未声明任何端口的角色则通过自身 --port 指定的端口提供服务，
	// 该情形由 validateModelDeploymentRoleReservedListenPorts 校验，因为区分已有冲突与新增
	// 冲突需要依赖旧对象，而本规则并不持有旧对象。
	//
	// 当存在 router 时，拒绝信息会指明该 router，因为正是 router 引入了这两个 vLLM 监听器；
	// 若不存在 router，该角色上的监听器便仅来自引擎自身（即仅为该角色渲染的 SGLang registry），
	// 此时拒绝信息将落在用户声明的端口字段上，而非对象中并不存在的字段上。
	if reserved := modelDeploymentRouterReservedPorts(md, role); len(reserved) > 0 {
		for _, port := range role.Ports {
			if !slices.Contains(reserved, port.Port) {
				continue
			}
			if md.Spec.Router != nil {
				errs = append(errs, field.Invalid(field.NewPath("spec", "router"), md.Spec.Router,
					fmt.Sprintf("role %q declares port %d, which the operator reserves for a listener it "+
						"synthesizes onto this role under engine %q and router %q; reserved here are %v",
						role.Name, port.Port, md.Spec.Engine.Name, md.Spec.Router.Name, reserved)))
			} else {
				errs = append(errs, field.Invalid(rolePath.Child("ports"), port.Port,
					fmt.Sprintf("role %q declares port %d, which the operator reserves for a listener it "+
						"synthesizes onto this role under engine %q; reserved here are %v",
						role.Name, port.Port, md.Spec.Engine.Name, reserved)))
			}
		}
	}

	return errs
}

// validateModelDeploymentRouterPorts 拒绝与托管 router 接管的服务端口发生冲突的端口配置。
// 两条规则均置于准入阶段，是因为它们所防范的冲突在运行期是不可恢复的：副本完成渲染后，
// 进程就会尝试绑定一个已被其他组件占用的端口。
func validateModelDeploymentRouterPorts(
	md *workercore.ModelDeployment, role *workercore.ModelDeploymentRole,
) field.ErrorList {
	var errs field.ErrorList

	// 直连 decode 角色前置有一个占用服务端口的路由代理，因此模型服务器必须让出该端口。渲染逻辑
	// 仅在由 operator 注入该标志时才会自动移开端口；若角色自行传入 --port，则会保留用户值，
	// 进而导致渲染在每次调谐时都因端口冲突而失败。此处无法获知加速器厂商，因此只要存在渲染
	// 该代理的可能性就会触发此规则——在 Ascend 上虽然不会渲染该代理，但在此处拒绝该标志并无
	// 代价，因为默认值填充本就会将其设为同一个值。
	if md.Spec.Router.Name != workercore.ModelDeploymentRouterLLMD ||
		workerctrl.ModelDeploymentEffectiveRoleKind(role) != workercore.ModelDeploymentRoleKindDecode ||
		len(role.Command) > 0 {
		return errs
	}
	if port, ok := workerctrl.ModelDeploymentRoleListenPort(md.Spec.Engine.Name, role); ok &&
		port == workerctrl.ModelDeploymentRoleServingPort(md, role) {
		errs = append(errs, field.Invalid(field.NewPath("spec", "router"), md.Spec.Router,
			fmt.Sprintf("role %q passes --port=%d, the port its Service publishes; the routing proxy "+
				"a direct decode role is fronted by takes that port, so the model server must leave it",
				role.Name, port)))
	}

	return errs
}

// modelDeploymentRouterReservedPorts 返回在本部署指定的引擎与 router 下，operator 为当前
// 角色合成的容器端口集合。这些端口直接引用 inject 包的常量而非重复硬编码数字，从而保证
// 准入拒绝逻辑与渲染逻辑不会产生偏差。
//
// 该集合按角色单独计算而非使用单一固定列表，因为这些监听器并不属于同一个集合：每个监听器
// 都有各自不同的渲染条件，使用单一列表会同时在两个方向上出错。它既会误拒当前角色根本不会
// 使用的端口（decoder 从不发布事件，通过 argv 配置的 router 下的任何角色也都不发布），又会
// 漏放实际使用的端口（SGLang 的 bootstrap registry 会在任何 prefiller 上渲染，而旧列表
// 仅针对 vLLM）。
//
// 该函数同样适用于未配置 router 的部署，因为 SGLang 的 registry 跟随角色而非 router：
// 挂接在共享资源池上的显式配对 prefiller 即使没有 router 在两端之间路由，也会绑定该端口。
// 两个 vLLM 监听器则恰恰相反——两者都仅在存在 router 时渲染，其中事件发布器仅在特定一种
// router 下渲染，而 bootstrap 服务器在任一指定 router 下都会渲染——因此在未声明 router 时
// 不会保留任何端口。
//
// 此处按与渲染逻辑相同的维度判断是否构成配对，因为与资源池厂商不同，角色列表就在 spec 中，
// 在此是已知的。各个 bootstrap 监听器仅在显式配对的 prefiller 上渲染——传输链路以及
// SGLang 的分离式参数都遵循与 router 相同的配对规则——因此端口保留也遵循这一规则，
// 向共享资源池供数据的单独 prefiller 不会占用这些端口。
//
// 资源池的加速器厂商在准入阶段不可知（而正是厂商属性会排除 Ascend 后端的事件发布），
// 因此只要某监听器存在被渲染的可能，就会保留对应端口。在此拒绝某个端口并不会造成无法解决的
// 代价，用户只需改用其他端口即可。
func modelDeploymentRouterReservedPorts(
	md *workercore.ModelDeployment, role *workercore.ModelDeploymentRole,
) []int32 {
	// 自定义 command 的角色可豁免：渲染逻辑不会为其合成任何上述监听器，因此不存在端口冲突。
	if len(role.Command) > 0 {
		return nil
	}

	kind := workerctrl.ModelDeploymentEffectiveRoleKind(role)
	var reserved []int32

	switch md.Spec.Engine.Name {
	case workercore.ModelDeploymentEngineVLLM:
		// 下方两个监听器仅在配置了 router 时渲染，因此未配置 router 的 vLLM 角色不保留任何端口：
		// 事件发布器服务于特定 router 的数据层，而承载 bootstrap 服务器的传输链路则随路由配对
		// 启用。
		if md.Spec.Router == nil {
			break
		}
		// 事件发布器及其回放 socket 用于向选择器（picker）的数据层供数，因此仅在该 router 下
		// 存在——且不会出现在 decoder（仅消费而不发布）或 encoder（根本不维护可发布事件的
		// KVCache）上。
		if md.Spec.Router.Name == workercore.ModelDeploymentRouterLLMD &&
			kind != workercore.ModelDeploymentRoleKindDecode &&
			kind != workercore.ModelDeploymentRoleKindEncode {
			reserved = append(reserved, inject.VLLMKVEventsPort, inject.VLLMKVEventsReplayPort)
		}
		// Mooncake bootstrap 服务器仅运行在 prefiller 上，且在所有 router 下均适用，而非仅限某一种。
		// 绑定该服务的传输链路读取 router 名称，仅仅是为了排除位于无法驱动它的 router 之后的
		// Ascend 配对；在其他所有厂商平台上，无论指定哪种 router，该链路都会被渲染。本规则无法
		// 感知厂商信息，因此如果在此处限定单一 router，就会在另外两种 router 下放行该端口，而
		// 渲染阶段仍会绑定它——这正是上文所述“双向同时出错”的永久性冲突。渲染器也从自身角度陈述了
		// 同一规则，即防止用户端口与此端口冲突的保留机制与厂商无关。此处同样检查配对条件，因为
		// 该信息在此是可获取的：该传输链路随显式配对启用，即使在 router 下，单独的 prefiller 也
		// 不会绑定 bootstrap 服务器。
		if kind == workercore.ModelDeploymentRoleKindPrefill &&
			workerctrl.ModelDeploymentDeclaresBothHalves(md) {
			reserved = append(reserved, inject.VLLMMooncakeBootstrapPort)
		}
	case workercore.ModelDeploymentEngineSGLang:
		// SGLang 的 bootstrap registry 跟随角色本身而非任何 router，因为在该引擎上声明拆分即意味着
		// 作为配对的一端运行。它仅在显式配对上渲染，因此端口保留逻辑采用相同规则。此外，它也仅在
		// 渲染器确实能够合成连接器时才会渲染（复用控制渲染的同一判断谓词）：既没有 router 也没有
		// 缓存的配对不会渲染任何连接器，其 prefiller 也不会占用该端口。
		//
		// 声明 encoder 角色会在同一个 prefiller 上增加第二个监听器。纯语言引擎除了自身端口外还会
		// 绑定 encoder bootstrap 端口，因此声明了 encoder 的部署会同时保留这两个端口。不含
		// encoder 的配对仅保留第一个端口，从而使从不运行 encoder 的部署可以自由使用 8997 端口。
		if kind == workercore.ModelDeploymentRoleKindPrefill &&
			workerctrl.ModelDeploymentDeclaresBothHalves(md) &&
			workerctrl.ModelDeploymentMaySynthesizeConnector(md) {
			reserved = append(reserved, inject.SGLangBootstrapPort)
			if modelDeploymentDeclaresEncode(md) {
				reserved = append(reserved, inject.SGLangEncoderBootstrapPort)
			}
		}
	}

	return reserved
}

// validateModelDeploymentKVCache 拒绝名称为空的 Binding 引用。
//
// 该规则放在此处仅因为 Schema 无法表达它。spec 中其他所有必填字符串都通过 minLength 标记约束
// 下限，这样更早、开销更低且适用于所有客户端；而该字段的类型来自上游 core.LocalObjectReference，
// 无法为非本 API 所有的结构体附加标记。因此这一例外是由类型所迫，而非主动选择。
//
// 该约束本身是必要的，因为 `required` 只保证键存在，不保证值非空：携带 `poolRef: {name: ""}`
// 的对象完全符合 Schema 要求。
func validateModelDeploymentKVCache(md *workercore.ModelDeployment) field.ErrorList {
	if md.Spec.KVCache == nil {
		return nil
	}

	if md.Spec.KVCache.PoolRef.Name != "" {
		return nil
	}

	return field.ErrorList{field.Required(
		field.NewPath("spec", "kvCache", "poolRef", "name"),
		"the Binding name must not be empty: the Binding is the authorization point for reaching "+
			"the pool, and an empty reference names none",
	)}
}

// modelDeploymentMaxRoles 表示单个部署可声明的 role 数量上限。
//
// 该上限过去由 Kueue 决定，现在属于本项目的产品约束，这就是全部变化所在。此前每个 role 对应
// 单个 Workload 中的一个 PodSet，因此该值取自 Workload.spec.podSets 自身的 maxItems，且必须
// 以实际运行的 Kueue 为准，而非本模块编译所依赖的类型库。如今每个副本都作为只含一个 PodSet 的
// 独立 Workload 进行准入，Kueue 不再限制部署声明的 role 数量；10 是产品侧设定的上限：
// prefill/decode 部署仅声明 2 个，此处渲染的任何内容都用不到 10 个，而不设上限会让单个对象在
// 准入阶段无从约束地扇出任意数量的 Workload、Service 和队列引用。
//
// 因此不再需要对照集群去核实该数值（原注释曾要求读者这样做）。调高该值属于产品决策，而不再
// 依赖 Kueue 升级。
const modelDeploymentMaxRoles = 10

// validateModelDeploymentRolesCount 限制 role 的数量上限。
//
// 该约束放在此处而非作为 Schema 的 maxItems，原因有二（均沿袭自其所替代的旧规则）：
// 拒绝信息可以明确指出限制来源，这决定了用户是向本 operator 提交 bug 还是去查阅 Kueue 文档；
// 此外，若需跟进上游数值变化，只需修改 Webhook，而无需变更每个已存储对象都必须兼容的 CRD Schema。
//
// 此处仅检查上限。下限由 Schema 的 minItems 约束，该校验在到达此处理器前已执行，因此在此处
// 再做一次检查只会成为永远无法进入的分支。
func validateModelDeploymentRolesCount(md *workercore.ModelDeployment) field.ErrorList {
	if len(md.Spec.Roles) <= modelDeploymentMaxRoles {
		return nil
	}

	return field.ErrorList{field.Invalid(
		field.NewPath("spec", "roles"), len(md.Spec.Roles),
		fmt.Sprintf(
			"at most %d roles: every role renders its own replicas, Services and queue references, "+
				"and a deployment naming more than that is a shape this operator does not serve — a "+
				"prefill/decode deployment names two",
			modelDeploymentMaxRoles,
		),
	)}
}

// validateModelDeploymentRoleNames 拒绝两个 role 共用同一名称。
//
// 副本的 Kueue 分组由 role 名称和副本序号推导得出，因此两个同名 role 会把相同序号的副本放进
// 同一个分组——一个声明总数为 1 却包含 2 个成员的分组。Kueue 对超额成员的处理是删除其中较新的
// 一个，从而导致一个 role 的副本因另一个 role 而被反复删除，且没有任何提示说明原因。
//
// 名称的格式约束（Kueue 的 PodSetReference 模式及 63 个字符的长度上限）由 Schema 负责，此处不再
// 重复。结构校验在本处理器之前运行，因此这里的格式检查永远不会触发。
//
// 本函数通过 API Server 运行时同样不会触发。`Roles` 带有 `+listType=map +listMapKey=name` 标记，
// API Server 会在任何 Webhook 之前于校验阶段强制保证该键的唯一性。在真实集群上的实测结果为：
// 两个名为 `worker` 的 role 会由 Schema 返回 `spec.roles[1]: Duplicate value`，永远不会看到
// 本函数的错误消息。保留它是为了在标记被意外移除时兜底（否则将静默失去该保证）；验证拒绝行为的
// 测试用例必须断言 Schema 的错误信息，因为实践中由 Schema 层负责该约束。
func validateModelDeploymentRoleNames(md *workercore.ModelDeployment) field.ErrorList {
	var errs field.ErrorList

	rolesPath := field.NewPath("spec", "roles")
	seen := make(map[string]int, len(md.Spec.Roles))
	for i := range md.Spec.Roles {
		name := md.Spec.Roles[i].Name
		first, dup := seen[name]
		if !dup {
			seen[name] = i
			continue
		}

		// 第二个参数是重复的值本身，API Server 会将其原样拼接在 "Duplicate value: " 之后。如果在这里
		// 传入解释性文本，返回时整句话都会被加上引号，看起来像用户输入的名称。解释性说明应放在旁边的
		// Invalid 错误中，其 Detail 字段才是用于编写说明文本的地方。
		errs = append(errs,
			field.Duplicate(rolesPath.Index(i).Child("name"), name),
			field.Invalid(rolesPath.Index(i).Child("name"), name, fmt.Sprintf(
				"a role's name is part of the Kueue group each of its replicas joins, so this does "+
					"not collide with %s — it puts the two roles' same-numbered replicas into one "+
					"group that declares a single member, and Kueue deletes whichever arrived later",
				rolesPath.Index(first).Child("name"),
			)))
	}

	return errs
}

// validateModelDeploymentRoleServiceNames 拒绝 Service 名称不合法的 role。
//
// 每个 role 前面都有一个名为 `<deployment>-<role>` 的 Service，而 Service 名称必须是 DNS-1035
// 标签：最多 63 个字符，但对象名称最长可达 253 个字符。前后两半各自都合法，拼接后却可能超长
// （例如 40 字符的 deployment 加上 30 字符的 role 共 71 字符），因此这是两个字段各自无法单独
// 承担的校验规则。
//
// 这里拦截的是一类本会静默循环的故障。deployment 自身名称过长时，其全局 Service 本就无法创建，
// 问题会立刻暴露；而此处的情形是全局 Service 正常、单 role 的 Service 每次创建都会被 API Server
// 拒绝，导致协调器不断重试，而报错对象与真正引发问题的字段隔了两层对象。
//
// 此处校验的是完整形态，而不仅仅是长度。早期版本仅检查长度，理由是两半都已经符合 DNS 标签格式——
// 但这对 deployment 那一半并不成立：对象名称是 DNS 子域，因此 `team.model.serving` 是合法的
// ModelDeployment 名称，但组合后的 Service 名称带有小数点，即使远短于 63 个字符也会在每次创建时
// 被拒绝。IsDNS1035Label 同时覆盖了长度和字符集检查。
//
// 对象已有的 role 予以豁免。由于本规则是在历史对象创建之后才加入的，deployment 自身名称不可变，
// 且后续每次编辑都会经过此处——如果拒绝已存储的 role，对象将被彻底锁死，包括删除该 role 在内的
// 任何编辑都无法挽救。新建和重命名的 role 仍会被拒绝，这也正是错误实际发生的位置。
func validateModelDeploymentRoleServiceNames(
	md *workercore.ModelDeployment, existingRoles sets.Set[string],
) field.ErrorList {
	var errs field.ErrorList

	rolesPath := field.NewPath("spec", "roles")
	for i := range md.Spec.Roles {
		role := md.Spec.Roles[i].Name
		if existingRoles.Has(role) {
			continue
		}

		name := md.Name + "-" + role
		why := validation.IsDNS1035Label(name)
		if len(why) == 0 {
			continue
		}

		errs = append(errs, field.Invalid(
			rolesPath.Index(i).Child("name"), role, fmt.Sprintf(
				"this role is fronted by a Service named %q (%d characters), which is not a valid "+
					"Service name: %s. Shorten or rename this role, or the deployment",
				name, len(name), strings.Join(why, "; "),
			),
		))
	}

	return errs
}

// validateModelDeploymentServiceNamesAreDistinct 拒绝会导致两个 Service 重名的部署。
//
// 冲突源于两套不同的命名公式，因此仅靠 role 名称互不相同并不足以避免。role 对应的 Service 名为
// `<deployment>-<role>`，而包含多个成员的 role 还会为每个实例创建 `<deployment>-<role>-r<ordinal>`
// ——因此一个名为 `x` 且每实例含多个 Pod 的 role 会生成 `<deployment>-x-r0`，而同级名为 `x-r0` 的
// role 也会生成相同名称。两个 role 本身都合法、不重名，且本处理器中也没有其他规则对比它们。
//
// 该规则防范的是静默故障，因此选择直接拒绝而非自行消解。协调器会按列表逐项对齐集群中的 Service；
// 若列表中有两项指向同一个对象名称，每次调谐都会把该对象改写成另一种形态。实例的无头 Service
// 与 role 的 ClusterIP Service 不可互换——前者发布无集群 IP 的单 Pod 记录，后者负责负载均衡——
// 因此在调谐中落败的一方会让其使用方失效：要么实例成员间无法互相解析，要么 role 停止响应。
// 整个过程没有任何报错，两个对象都存在，只是其中一个变成了错误类型的 Service。
//
// 该校验在每次请求时都会执行，而非仅针对新增 role。`replicas` 是预期由用户修改的字段，它决定了
// role 会派生出多少个实例 Service，因此扩容操作恰恰可能将原本合法的部署推入名称冲突。
func validateModelDeploymentServiceNamesAreDistinct(md *workercore.ModelDeployment) field.ErrorList {
	var errs field.ErrorList

	// 部署自身的 Service 以部署名命名，在检查任何 role 之前先占用该名称。
	claimedBy := map[string]string{md.Name: "the deployment's own Service"}

	// 托管路由器同样由一个名为 `<deployment>-router` 的 Service 暴露，而名为 "router" 的 role 也会
	// 推导出相同名称。无论 router 配置为何值，其对象都统一渲染在该名称下，因此只要启用了 router
	// 就占位，而不必区分具体配置值。
	if md.Spec.Router != nil {
		claimedBy[md.Name+"-router"] = "the Service fronting the managed router"
	}

	rolesPath := field.NewPath("spec", "roles")
	for i := range md.Spec.Roles {
		role := &md.Spec.Roles[i]

		derived := []string{md.Name + "-" + role.Name}
		describes := []string{fmt.Sprintf("the Service fronting role %q", role.Name)}
		// 仅在成员数大于 1 时占位：单 Pod 实例没有需要互相寻址的对等成员，也不会为其渲染无头 Service，
		// 若在此枚举并不实际创建的名称，反而会误拒本可正常工作的部署。
		if role.ReplicaSize > 1 {
			for ordinal := range int(role.Replicas) {
				derived = append(derived,
					fmt.Sprintf("%s-%s-r%d", md.Name, role.Name, ordinal))
				describes = append(describes,
					fmt.Sprintf("the headless Service publishing instance %d of role %q",
						ordinal, role.Name))
			}
		}

		for j, name := range derived {
			if by, taken := claimedBy[name]; taken {
				errs = append(errs, field.Invalid(
					rolesPath.Index(i).Child("name"), role.Name, fmt.Sprintf(
						"%s would be named %q, and so would %s. One name is one object, and the "+
							"two need different shapes, so whichever is written last leaves the "+
							"other without the addresses it publishes. Rename this role",
						describes[j], name, by,
					),
				))

				continue
			}
			claimedBy[name] = describes[j]
		}
	}

	return errs
}

// validateModelDeploymentRoleMemberNames 拒绝成员名称无法被 Kubernetes 用作主机名的 role。
//
// 这是与上述 Service 名称校验相互独立的规则，而非其扩展，因为两者的校验对象和触发时机均不同。
// 前一条规则针对 `<deployment>-<role>`，更新操作不会改变它，因此仅在 role 出现时检查一次；本规则
// 针对 `<deployment>-<role>-r<ordinal>-m<member>`，其长度取决于 `replicas` 和 `size`——这两个都是
// 允许用户修改的字段——因此必须在每次更新时（包括已存在的 role）都执行检查。若将两者合并，
// 要么会对不可变组合反复重检，要么会放任编辑把合法部署改坏。
//
// 它防范的同样是无限重试而非显式报错。成员名称同时用作其主机名，即最长 63 个字符的 DNS-1123
// 标签，而上面的 Service 名称只对较短的组合检查 63 字符上限——因此 61 字符的 `<deployment>-<role>`
// 在上面合法，到此处却会生成 67 字符的成员名。若无本规则，部署虽能准入，但每次创建 Pod 都会被
// API Server 拒绝，协调器将无限重试，而真正出错的字段隔了两层对象。当成员数大于 1 时，这是
// 唯一会发生的情况：由于分组根本无法组装，外界看不到任何部分成功的状态。
//
// 此处衡量的是当前 spec 能产生的最长名称，而非按字段类型的最坏情况估算。若按 10 位序号预留空间，
// 会白白占用每个部署 24 个字符的预算以应对无人使用的规模；按当前声明的数量测算毫无开销，且能
// 在请求会破坏限制的规模时当场拒绝。
func validateModelDeploymentRoleMemberNames(md *workercore.ModelDeployment) field.ErrorList {
	var errs field.ErrorList

	rolesPath := field.NewPath("spec", "roles")
	for i := range md.Spec.Roles {
		role := &md.Spec.Roles[i]
		// 每副本仅一个成员时，渲染器不会显式命名：Pod 沿用一贯的自动生成名称，也不设置主机名，
		// 因此无需检查长度预算。
		if role.ReplicaSize <= 1 {
			continue
		}

		// 最大序号与最大成员索引共同构成该 role 能生成的最长名称。两者均以 0 为下限，即使 role 声明了
		// 零副本（Schema 会拒绝该情况，但本规则不应依赖这一点），依然能测算出有效的名称。
		name := fmt.Sprintf("%s-%s-r%d-m%d", md.Name, role.Name,
			max(int(role.Replicas)-1, 0), int(role.ReplicaSize)-1)
		why := validation.IsDNS1123Label(name)
		if len(why) == 0 {
			continue
		}

		// 该错误关联到 role 本身而非某个具体字段，因为拼出该名称需要四个输入——部署名称、role 名称、
		// `replicas` 和 `size`——仅从待校验对象中无法判断究竟是哪一项发生了变化。两个名称作为身份标识
		// 不可变更，只有数量可以编辑，因此错误中会列出所有输入，而不是猜测其中某一个。
		errs = append(errs, field.Invalid(
			rolesPath.Index(i), name, fmt.Sprintf(
				"a replica of this role is addressed by naming each of its Pods, and the longest "+
					"such name would be %q (%d characters), which cannot be a hostname: %s. "+
					"Shorten the role or the deployment, or declare fewer replicas or members",
				name, len(name), strings.Join(why, "; "),
			),
		))
	}

	return errs
}

// validateModelDeploymentRoleKinds 包含针对 role 类型声明的三条规则。
//
// 第一条关于集合组合：server 本身独立处理完整请求，因此“一个普通 server 加上一个 prefiller”
// 不对应任何下游能消费的拓扑形态，若放行会导致渲染出语义未定义的传输配置。
//
// 第二条关于多重性，且有意设计为非对称规则。多个 server 类型的 role 构成一组对等节点，前置组件
// 可以在它们之间选择调度；但多个 prefiller 则不然，因为消费这些 role 的组件无法表达第二个同类
// role，多余的 role 只会渲染出下游无法触及的配置。因此本规则拒绝重复的非 server 类型，而非禁止
// 一切重复；server 的豁免正是基于这一原因，而非遗漏。
//
// 第三条关于引擎支持：仅当引擎的渲染逻辑认识该类型时，该类型才有效。在准入阶段拒绝正是意义所在——
// 否则容器虽能启动、看似配置完成，实际行为却如同未声明该 role，或者在引擎启动时报错且提示与
// 此毫无关联。
func validateModelDeploymentRoleKinds(md *workercore.ModelDeployment) field.ErrorList {
	var errs field.ErrorList

	rolesPath := field.NewPath("spec", "roles")

	hasServer, hasOther := false, false
	for i := range md.Spec.Roles {
		if workerctrl.ModelDeploymentEffectiveRoleKind(&md.Spec.Roles[i]) == workercore.ModelDeploymentRoleKindServer {
			hasServer = true
		} else {
			hasOther = true
		}
	}

	// firstOfKind 记录非 server 类型首次声明的位置，以便拒绝信息能指出第二个 role 与哪个已有 role
	// 冲突，而不仅仅是给出其所在下标。
	firstOfKind := make(map[workercore.ModelDeploymentRoleKind]int, len(md.Spec.Roles))

	for i := range md.Spec.Roles {
		role, kindPath := &md.Spec.Roles[i], rolesPath.Index(i).Child("kind")
		kind := workerctrl.ModelDeploymentEffectiveRoleKind(role)

		if hasServer && hasOther {
			errs = append(errs, field.Invalid(kindPath, kind, fmt.Sprintf(
				"%q cannot be combined with another kind in one deployment: a %q role serves whole "+
					"requests by itself, so a deployment holding one beside a %q or %q role "+
					"describes no arrangement the engines' KV transfer configuration can express",
				workercore.ModelDeploymentRoleKindServer, workercore.ModelDeploymentRoleKindServer,
				workercore.ModelDeploymentRoleKindPrefill, workercore.ModelDeploymentRoleKindDecode,
			)))

			continue
		}

		if kind != workercore.ModelDeploymentRoleKindServer {
			if first, seen := firstOfKind[kind]; seen {
				errs = append(errs, field.Invalid(kindPath, kind, fmt.Sprintf(
					"kind %q is already declared by role %q: only %q may be declared by more than one "+
						"role, because a set of servers is a set of equals, whereas nothing consuming "+
						"these roles expresses a second %q and the extra role would render into a "+
						"configuration nothing downstream can reach",
					kind, md.Spec.Roles[first].Name, workercore.ModelDeploymentRoleKindServer, kind,
				)))

				continue
			}

			firstOfKind[kind] = i
		}

		// 本 API 目前接受的所有引擎都不会触发此规则拒绝，但除了一种类型外，仍保留该规则。现有两个引擎
		// 均支持渲染 Server、Prefill 和 Decode，因此唯一会触发它的情况要么是未建立映射的类型，要么是
		// 渲染器新增了引擎却遗漏了支持表条目——而后者正是值得长期保留该规则的原因。支持表是渲染侧自身的
		// 权威来源；未录入表中的引擎对所有类型都会报告“无对应术语”，而此处是唯一能将该结果转化为用户可
		// 感知并处理的准入拒绝的地方。缺少它时，这类部署会先被准入，然后在用户无法直接触及的协调器中
		// 渲染失败。
		//
		// Encode 是唯一例外，且在调用前而非调用内部将其排除。该函数回答的是 KV 分离相关能力，而编码器
		// 不参与其中：向渲染器询问是否支持某 role 时，得到的其实是渲染器对 KV 消费方的回答。Encode role
		// 改由编码器矩阵负责准入，它关注不同的问题并能指出具体缺失项。
		if kind != workercore.ModelDeploymentRoleKindEncode &&
			!workerctrl.ModelDeploymentSupportsRoleKind(md.Spec.Engine.Name, kind) {
			errs = append(errs, field.Invalid(kindPath, kind, fmt.Sprintf(
				"engine %q has no rendering term for kind %q; accepting it would leave the container "+
					"looking configured and behaving as though the role were never declared",
				md.Spec.Engine.Name, kind,
			)))
		}
	}

	return errs
}

// validateModelDeploymentRoles 包含所有后续版本均保留的单 role 校验规则。
func validateModelDeploymentRoles(md *workercore.ModelDeployment) field.ErrorList {
	errs := make(field.ErrorList, 0, len(md.Spec.Roles))

	rolesPath := field.NewPath("spec", "roles")
	for i := range md.Spec.Roles {
		role, rolePath := &md.Spec.Roles[i], rolesPath.Index(i)
		if role.ShmSize != nil && role.ShmSize.Sign() <= 0 {
			errs = append(errs, field.Invalid(rolePath.Child("shmSize"), role.ShmSize.String(), "must be greater than zero"))
		}

		errs = append(errs, validateModelDeploymentRoleExtraArgs(md.Spec.Engine.Name, role, rolePath)...)
		errs = append(errs, validateModelDeploymentRoleParallelWidth(md.Spec.Engine.Name, role, rolePath)...)
		errs = append(errs, validateModelDeploymentRoleLoadBalance(md.Spec.Engine.Name, role, rolePath)...)
		errs = append(errs, validateModelDeploymentRoleEnv(md.Spec.Engine.Name, role, rolePath)...)
		errs = append(errs, validateModelDeploymentRoleResources(role, rolePath)...)
		errs = append(errs, validateModelDeploymentRoleAdditionalVolumes(md.Namespace, role, rolePath)...)
		errs = append(errs, validateModelDeploymentRoleArtifactKeys(md, role, rolePath)...)
		errs = append(errs, validateModelDeploymentRoleReservedMountPaths(md, role, rolePath)...)
		// 此处无条件执行而非放在 router 专属规则内，因为需要避让的监听端口并非全部属于 router：
		// SGLang 的 bootstrap 注册表随 role 渲染在未接入路由的 prefiller 上，而仅在启用 router 时
		// 运行的遍历逻辑不会检查该情形。
		errs = append(errs, validateModelDeploymentRolePorts(md, role, rolePath)...)
	}

	return errs
}

// validateModelDeploymentRoleAdditionalVolumes 强制执行字段文档中声明但 Schema 无法表达的三条规则。
//
// 这些问题在此处拒绝，而不是留待后续由 API Server 拦截。若没有本校验，任何路径错误的最早表现
// 都是渲染出的 Pod 在每次调谐时被拒绝创建，在原本顺利准入的对象上抛出一条引用了用户从未编写过的
// volumeMount 的创建失败信息。
//
// 未指定任何卷源的条目比路径错误更隐蔽，因为根本没有任何组件会拒绝它：渲染器没有卷源可构建而
// 跳过该条目，容器会在缺少所请求挂载的情况下照常启动，且没有任何错误、Event 或 Condition 提示。
// 卷源由多个独立的可选指针构成，这种结构无法在 Schema 中约束为“有且仅有一个”。
//
// ".." 路径段按目录段而非子串检查：诸如 "a..b" 的 subPath 虽然包含这两个字符，却是完全正常的
// 目录名，而 "a/../b" 才是本规则防范的路径穿越。
//
// 唯一性校验仅覆盖这些条目，而不涵盖 operator 自身的挂载，这是能力边界而非疏漏。渲染器会向同一
// 容器追加连接器的卷挂载，这些挂载来自部署在调谐阶段解析出的 KVCache 后端——准入阶段无法看到该
// 对象，因为解析它是控制器的职责，且解析结果可能在本对象未变更的情况下发生改变。因此，与
// operator 渲染的挂载冲突的路径在此处仍会被准入，并在渲染时静默覆盖或分歧。要彻底解决该问题，
// 需要在已知全部挂载的位置（即渲染器而非本校验器）执行检查。
func validateModelDeploymentRoleAdditionalVolumes(
	namespace string, role *workercore.ModelDeploymentRole, rolePath *field.Path,
) field.ErrorList {
	var errs field.ErrorList

	avsPath := rolePath.Child("additionalVolumes")
	seen := make(map[string]int, len(role.AdditionalVolumes))
	for i := range role.AdditionalVolumes {
		av, avPath := &role.AdditionalVolumes[i], avsPath.Index(i)

		// claim 卷源与原有的三种卷源一同参与校验，因此未指定任何卷源的条目会基于相同原因在此被拒绝：
		// 渲染时会被跳过，导致容器在缺少该卷的情况下启动。针对空 claimName 或在 persistentVolumeClaim
		// 之外又指定第二种卷源的拒绝逻辑适用于所有携带该字段的条目：claim 卷源是新增字段，早于它存储的
		// 对象不可能持有该字段；若将该检查限制在 Encode role 上，则会导致不含 Encode role 的部署可以在
		// 同一条目上挂载空的 claimName 或同时指定两个卷源。
		if av.ConfigMap == nil && av.Secret == nil && av.HostPath == nil && av.PersistentVolumeClaim == nil {
			errs = append(errs, field.Required(avPath, fmt.Sprintf(
				"one of configMap, secret, hostPath or persistentVolumeClaim, so that %q names "+
					"something to mount; an entry naming none is skipped at render and the container "+
					"starts without it",
				av.MountPath,
			)))
		}
		if av.PersistentVolumeClaim != nil {
			if av.PersistentVolumeClaim.ClaimName == "" {
				errs = append(errs, field.Required(avPath.Child("persistentVolumeClaim", "claimName"),
					fmt.Sprintf(
						"the claim name must not be empty: it names a claim in namespace %q, and an "+
							"empty reference names none", namespace,
					)))
			}
			for _, other := range []struct {
				field string
				set   bool
			}{
				{"configMap", av.ConfigMap != nil},
				{"secret", av.Secret != nil},
				{"hostPath", av.HostPath != nil},
			} {
				if other.set {
					errs = append(errs, field.Forbidden(avPath.Child(other.field),
						fmt.Sprintf(
							"%q is set beside persistentVolumeClaim on the same entry: one entry "+
								"mounts one volume, and two sources leave the renderer to pick",
							other.field,
						)))
				}
			}
		}

		if first, duplicate := seen[av.MountPath]; duplicate {
			errs = append(errs, field.Duplicate(avPath.Child("mountPath"), fmt.Sprintf(
				"%q is already mounted by %s; two volumes cannot occupy one path, and which of them "+
					"the container would see is not something this API decides",
				av.MountPath, avsPath.Index(first).Child("mountPath"),
			)))
		} else {
			seen[av.MountPath] = i
		}

		if av.SubPath == "" {
			continue
		}
		if slices.Contains(strings.Split(av.SubPath, "/"), "..") {
			errs = append(errs, field.Invalid(avPath.Child("subPath"), av.SubPath,
				"must not contain a \"..\" element: the path is resolved inside the volume, and an "+
					"element that climbs out of it reaches the host filesystem of whatever backs the "+
					"volume"))
		}
	}

	return errs
}

// validateModelDeploymentServedModelNames 对每个 role 执行 validateModelDeploymentRoleServedModelName，
// 但在更新时若某个 role 保留了原有的服务模型名称则予以豁免：本规则是在对象存储后才引入的，原本
// 会被其拒绝的对象仍须允许修改其他字段（如扩缩容、修改标签），而不能被锁死并迫使在同一请求中
// 修复其启动参数。
func validateModelDeploymentServedModelNames(old, md *workercore.ModelDeployment) field.ErrorList {
	var errs field.ErrorList
	rolesPath := field.NewPath("spec", "roles")
	for i := range md.Spec.Roles {
		role := &md.Spec.Roles[i]
		if old != nil {
			if j := slices.IndexFunc(old.Spec.Roles, func(r workercore.ModelDeploymentRole) bool {
				return r.Name == role.Name
			}); j >= 0 {
				was, _ := workerctrl.ModelDeploymentServedModelNames(old.Spec.Engine.Name, old.Spec.Roles[j].ExtraArgs)
				now, _ := workerctrl.ModelDeploymentServedModelNames(md.Spec.Engine.Name, role.ExtraArgs)
				if slices.Equal(was, now) && old.Spec.Model.Name == md.Spec.Model.Name &&
					len(old.Spec.Roles[j].Command) == len(role.Command) {
					continue
				}
			}
		}
		errs = append(errs, validateModelDeploymentRoleServedModelName(md, role, rolesPath.Index(i))...)
	}

	return errs
}

// validateModelDeploymentRoleServedModelName 拒绝将自身 --served-model-name 设为非
// spec.model.name 值的托管角色。
//
// 本规则不可放宽，无论是否使用 artifact 均适用。Router、Router 的 tokenizer 调用以及指标中的
// model 标签全部基于 spec.model.name 进行匹配。实测表明，若角色以其他名称提供服务，会静默触发双重故障：
// 通过 Router 按 spec.model.name 发起的请求会返回 404，而按角色名称发起的请求虽能成功，
// 但 Router 的 token 生成器每次都会失败且前缀缓存评分降为零——与此同时部署仍报告 Ready。
// 除了在此拒绝外，没有任何机制能阻止这一问题。
//
// 同样拒绝提供第二个名称：vLLM 虽支持多个名称，但其指标仅上报第一个。
//
// 已替换命令行的角色不受此检查约束：其作者自行负责全部参数，这正是该层级的定位。
func validateModelDeploymentRoleServedModelName(
	md *workercore.ModelDeployment, role *workercore.ModelDeploymentRole, rolePath *field.Path,
) field.ErrorList {
	if len(role.Command) > 0 {
		return nil
	}
	names, found := workerctrl.ModelDeploymentServedModelNames(md.Spec.Engine.Name, role.ExtraArgs)
	if !found || (len(names) == 1 && names[0] == md.Spec.Model.Name) {
		return nil
	}

	return field.ErrorList{field.Invalid(rolePath.Child("extraArgs"), names, fmt.Sprintf(
		"%s must be exactly %q, the name spec.model.name serves under: a router, its tokenizer "+
			"calls and the metrics' model label all match on that name, so any other value, or a "+
			"second one, makes them miss while the deployment reports Ready; leave the flag out "+
			"to have the operator render it",
		workerctrl.ModelDeploymentServedModelNameArg, md.Spec.Model.Name,
	))}
}

// validateModelDeploymentRoleArtifactKeys 在设置了 spec.model.artifactRef 时，拒绝包含由
// operator 渲染用于交付权重的参数或环境变量条目：包括引擎加载的模型、模型版本锁定、下载目标路径
// 以及引擎访问 Hub 的方式。
//
// 后续同名的参数标志会静默覆盖 operator 渲染的值，导致部署实际加载并非该 artifact 的权重，
// 而状态却仍显示该 artifact。已替换命令行的角色不会获得这些渲染项，因此不受此检查约束。
func validateModelDeploymentRoleArtifactKeys(
	md *workercore.ModelDeployment, role *workercore.ModelDeploymentRole, rolePath *field.Path,
) field.ErrorList {
	if md.Spec.Model.ArtifactRef == nil || len(role.Command) > 0 {
		return nil
	}

	engine := md.Spec.Engine.Name
	var errs field.ErrorList
	for i, arg := range role.ExtraArgs {
		owned, ok := workerctrl.ModelDeploymentArtifactOwnedArg(engine, arg)
		if !ok {
			continue
		}
		errs = append(errs, field.Invalid(rolePath.Child("extraArgs").Index(i), arg, fmt.Sprintf(
			"%q is set by the operator while spec.model.artifactRef names the weights, and a second "+
				"value would silently replace the artifact's; replace the whole command line through "+
				"%s to own it instead", owned, rolePath.Child("command"),
		)))
	}
	for i := range role.Env {
		name := role.Env[i].Name
		if !workerctrl.ModelDeploymentArtifactOwnsEnv(engine, name) {
			continue
		}
		errs = append(errs, field.Invalid(rolePath.Child("env").Index(i), name, fmt.Sprintf(
			"%q is set by the operator while spec.model.artifactRef names the weights: it is how an "+
				"engine downloading them reaches the Hub with the artifact's credential; replace the "+
				"whole command line through %s to own it instead", name, rolePath.Child("command"),
		)))
	}

	return errs
}

// validateModelDeploymentKVCacheDtype 在设置了 spec.kvCache 且 operator 拥有该参数控制权时，
// 拒绝包含 --kv-cache-dtype 的角色参数：operator 会在此处渲染 Binding 的 dtype，若出现后续值
// 会静默将其覆盖，而状态却仍回显 Binding 的配置。
//
// 已替换命令行的角色不会获得缓存参数渲染，因此不受此检查约束。正在删除中的部署同样豁免：
// 本规则晚于已存有该标志的历史对象引入，清除其 finalizer 的更新不得被拒绝。此类对象会保留其自身
// 的值（因其在命令行中靠后而生效），直到后续更新在此被拒绝为止。
func validateModelDeploymentKVCacheDtype(md *workercore.ModelDeployment, owned bool) field.ErrorList {
	if !owned || md.Spec.KVCache == nil || md.DeletionTimestamp != nil {
		return nil
	}

	engine := md.Spec.Engine.Name
	rolesPath := field.NewPath("spec", "roles")
	var errs field.ErrorList
	for i := range md.Spec.Roles {
		role := &md.Spec.Roles[i]
		if len(role.Command) > 0 {
			continue
		}
		rolePath := rolesPath.Index(i)
		for j, arg := range role.ExtraArgs {
			flag, ok := workerctrl.ModelDeploymentKVCacheOwnedArg(engine, arg)
			if !ok {
				continue
			}
			errs = append(errs, field.Invalid(rolePath.Child("extraArgs").Index(j), arg, fmt.Sprintf(
				"%q is set by the operator while spec.kvCache names a pool: it carries the Binding's "+
					"spec.domain.dtype, because the store key does not and engines writing two dtypes "+
					"into one domain read each other's blocks wrong; a different dtype needs a Binding "+
					"declaring it, or replace the whole command line through %s to own it instead",
				flag, rolePath.Child("command"),
			)))
		}
	}

	return errs
}

// validateModelDeploymentRoleReservedMountPaths 在设置了 spec.model.artifactRef 时，拒绝
// 挂载在权重挂载路径本身、其内部或包含其上层路径的角色卷：挂载在内部会遮蔽部分权重，
// 挂载在外部则会遮蔽全部权重。
func validateModelDeploymentRoleReservedMountPaths(
	md *workercore.ModelDeployment, role *workercore.ModelDeploymentRole, rolePath *field.Path,
) field.ErrorList {
	if md.Spec.Model.ArtifactRef == nil {
		return nil
	}

	var errs field.ErrorList
	for i, av := range role.AdditionalVolumes {
		for _, reserved := range []string{workerctrl.ModelDeploymentModelMountPath, workerctrl.ModelDeploymentModelCachePath} {
			if !pathsOverlap(av.MountPath, reserved) {
				continue
			}
			errs = append(errs, field.Invalid(rolePath.Child("additionalVolumes").Index(i).Child("mountPath"),
				av.MountPath, fmt.Sprintf(
					"overlaps %q, where the operator mounts the weights spec.model.artifactRef names", reserved,
				)))
			break
		}
	}

	return errs
}

// validateModelDeploymentRoleExtraArgs 拒绝属于追加层级但由 operator 拥有的参数。
//
// 此举旨在防止静默合并，原因在于可诊断性而非代码整洁：同一连接器参数出现两个值时无法判断哪个生效，
// 且编写第二个值的用户也无从得知第一个值的存在。
//
// 无法被解析器识别的并行度声明也会在此被拒绝，因为 KV 传输配置文档正是依据同一份参数来源渲染的：
// 面对引擎自身在启动时就会拒绝的声明，operator 写入文档的任何默认值都只能是导致错误启动的错误答案。
// 拒绝信息会通过解析器自身的报错指出对应标志。参数来源即角色的单一参数流——其 ExtraArgs，或在替换
// 命令行时的 Command——两者中的未知标志仍与之前一样允许通过：拒绝引擎不认识的参数是引擎自身的职责。
// 而在接管型 Command 旁闲置的 ExtraArgs 不构成参数流，因此不会被任何人解析，也不会因此被拒绝。
func validateModelDeploymentRoleExtraArgs(
	engine string, role *workercore.ModelDeploymentRole, rolePath *field.Path,
) field.ErrorList {
	var errs field.ErrorList

	argsPath := rolePath.Child("extraArgs")
	for i, arg := range role.ExtraArgs {
		owned, ok := workerctrl.ModelDeploymentOwnedArg(engine, arg)
		if !ok {
			continue
		}

		// 若引擎将某种拼写视为被拥有的键，则需一并指出该键对应的实际写法，否则拒绝信息会引用一个用户从未写过的标志。
		subject := fmt.Sprintf("%q", owned)
		if name := workerctrl.ModelDeploymentArgName(arg); name != owned {
			subject = fmt.Sprintf("%q is %q, which", name, owned)
		}
		errs = append(errs, field.Invalid(argsPath.Index(i), arg, fmt.Sprintf(
			"%s is set by the operator for engine %q and must not be supplied here, because two "+
				"values for it cannot be told apart; replace the whole command line through "+
				"%s to own it instead",
			subject, engine, rolePath.Child("command"),
		)))
	}

	if _, err := workerctrl.ParseModelDeploymentDeclaredParallelism(
		engine, workerctrl.ModelDeploymentRoleArgs(role), role.Env,
	); err != nil {
		errs = append(errs, field.Invalid(rolePath, role.Name, fmt.Sprintf(
			"declares parallelism the KV transfer document must follow but cannot read: %s", err,
		)))
	}

	return errs
}

// validateModelDeploymentRoleLoadBalance 确保托管角色保持每个 Pod 仅暴露一个 HTTP 端点。
func validateModelDeploymentRoleLoadBalance(
	engine string, role *workercore.ModelDeploymentRole, rolePath *field.Path,
) field.ErrorList {
	if len(role.Command) > 0 {
		return nil
	}
	status := workerctrl.ReadModelDeploymentRoleParallelism(engine, role)
	if status.Source.Kind != workercore.ModelDeploymentParallelismSourceKindExtraArgs {
		return nil // 并行度解析器会单独报告无法识别的参数。
	}
	if status.LoadBalance == workercore.ModelDeploymentLoadBalanceUnknown {
		return field.ErrorList{field.Invalid(rolePath.Child("extraArgs"), role.ExtraArgs,
			status.Source.UnreadableReason)}
	}
	if status.LoadBalance == workercore.ModelDeploymentLoadBalanceMultiPort {
		return field.ErrorList{field.Invalid(rolePath.Child("extraArgs"), role.ExtraArgs,
			"MultiPort load balancing is unsupported; managed roles require one HTTP endpoint per Pod")}
	}
	return nil
}

// validateModelDeploymentExternalDPRouter 拒绝外部数据并行（external DP）缺少 Router 进行监管的角色。
//
// 此处拒绝的依据是“监管”而非“可达性”。无论 Router 如何工作，成员 Pod 始终可通过其 Pod IP 访问，
// 因此称端点只能通过 Router 寻址会夸大 Pod 网络的限制。Router 真正提供的是分组故障与恢复屏障所需的
// 监管能力：当分组不健康时，operator 会将其成员从 Router 的选择列表中撤出，而 Router 对该选择列表
// 的视图正是判断该分组是否真正处于服务状态的依据。
//
// 并行形态源自并行度解析器，它是角色声明的单一事实来源。Unknown 和 MultiPort 交由上述规则处理
// （上述规则已将其拒绝）：由其他规则负责的缺陷不会重复拒绝。
func validateModelDeploymentExternalDPRouter(md *workercore.ModelDeployment) field.ErrorList {
	if md.Spec.Router != nil {
		return nil
	}

	// 这里会收集所有违规角色而非遇到第一个就返回，以便单次 apply 即可指出用户需要修复的全部角色，
	// 而不是每次只暴露一个错误。
	offenders := make([]string, 0)

	for i := range md.Spec.Roles {
		role := &md.Spec.Roles[i]
		// 本规则自身的命令行接管跳过逻辑。已替换命令行的角色没有受管并行形态：整行命令均由用户自行编写，
		// 因此引擎实际运行的并行度由用户自行决定，而非受管标志所声明的值。status 同样不会为这类角色读取形态，
		// 所以针对受管形态的规则对此类角色无从约束。
		if modelDeploymentRoleOwnsItsCommandLine(role) {
			continue
		}
		status := workerctrl.ReadModelDeploymentRoleParallelism(md.Spec.Engine.Name, role)
		// 本规则自身的来源跳过逻辑。并行度读数必须来自角色自身的 ExtraArgs，因为这是本 operator 唯一
		// 会参与渲染的参数来源。来自其他地方的读数描述的是非本 operator 编写的 argv，而无法读取的参数流
		// 应由并行度解析器拒绝，而非本规则。
		if !modelDeploymentRoleShapeCameFromExtraArgs(status) {
			continue
		}
		if status.LoadBalance != workercore.ModelDeploymentLoadBalanceExternal &&
			status.LoadBalance != workercore.ModelDeploymentLoadBalanceHybrid {
			continue
		}

		offenders = append(offenders, fmt.Sprintf("%q", role.Name))
	}

	if len(offenders) == 0 {
		return nil
	}

	subject, verb := "the role", "declares"
	if len(offenders) > 1 {
		subject, verb = "the roles", "declare"
	}

	return field.ErrorList{field.Required(
		field.NewPath("spec", "router"),
		fmt.Sprintf("RouterRequired: %s %s %s external data parallelism, whose groups "+
			"are served through a Router and need it to supervise them; declare one under spec.router",
			subject, strings.Join(offenders, ", "), verb),
	)}
}

// modelDeploymentRoleOwnsItsCommandLine 报告用户是否替换了角色的命令行。
//
// 已替换命令行的角色没有受管并行形态：整行命令均由用户自行编写，因此引擎实际运行的并行度由用户自己
// 决定，而非受管标志所声明的值。status 同样不会为这类角色读取形态，所以针对受管形态的规则对此类
// 角色无从约束。
func modelDeploymentRoleOwnsItsCommandLine(role *workercore.ModelDeploymentRole) bool {
	return len(role.Command) > 0
}

// modelDeploymentRoleShapeCameFromExtraArgs 报告角色并行度的读数是否来自角色自身的 ExtraArgs。
//
// 来自其他任何地方的读数均不由本规则处理：此时引擎实际运行的并行度由用户自行决定，而无法读取的
// 参数流应由并行度解析器拒绝，而非本规则。
func modelDeploymentRoleShapeCameFromExtraArgs(
	status workercore.ModelDeploymentRoleParallelismStatus,
) bool {
	return status.Source.Kind == workercore.ModelDeploymentParallelismSourceKindExtraArgs
}

// validateModelDeploymentRoleParallelWidth 是首个将角色声明的并行度与其资源请求关联起来的检查：
// 每个成员的引擎并行宽度必须能容纳于该成员请求的加速卡数量之内。
//
// 本检查刻意保持克制。它仅根据作者已填写的数值进行计算，只拒绝绝对无法启动的配置；任何存在歧义的
// 情况均保持静默——包括连线标志（部分宽度可能位于其他成员上，而本表不对放置拓扑建模）、无法读取或
// 为零的卡数，以及解析阶段已拒绝的声明。它从不推算合适大小，也不读取资源池：仅凭作者自身的配置做
// 算术校验，且只在算术上无法成立时才拒绝。参数来源即角色的单一参数流，因此接管型 Command 旁闲置的
// ExtraArgs 不计入宽度——该 Command 即为该角色的声明。
//
// 宽度公式遵循各引擎自身的规则。vLLM 在每个数据并行（DP）rank 的成员上放置
// tensor x pipeline x prefill-context 个 rank，并在其上乘以数据并行宽度：若声明的本地（LOCAL）
// 份额大于零（且无连线标志），该份额即为此成员实际运行的 rank 数；否则使用完整宽度，声明为零视作
// 未声明，因为这是引擎自身用于表示外部指定 DP 的哨兵值。Decode-context 复用 tensor rank，而
// expert-parallel 是一种模式，因此两者均不计入乘积。SGLang 放置 tensor x pipeline x data-parallel，
// 当启用 DP attention、DWDP、MoE-DP 或 attention-CP 将数据并行宽度移入 tensor 域内时，则缩减为
// tensor x pipeline。
func validateModelDeploymentRoleParallelWidth(
	engine string, role *workercore.ModelDeploymentRole, rolePath *field.Path,
) field.ErrorList {
	if role.Resources == nil || role.Resources.Accelerator == nil {
		return nil
	}

	declared, err := workerctrl.ParseModelDeploymentDeclaredParallelism(
		engine, workerctrl.ModelDeploymentRoleArgs(role), role.Env,
	)
	if err != nil || len(declared.Wiring) > 0 {
		return nil
	}

	cards, ok := role.Resources.Accelerator.AsInt64()
	if !ok || cards <= 0 {
		return nil
	}

	// multiply 将一个声明的并行度乘入当前累计宽度。每个并行度至少为 1，因此乘积只会递增；一旦乘积
	// 即将溢出 int64，即可直接判定为拒绝（卡数同样在 int64 范围内），此时宽度会被钳位到比卡数大 1
	// 的值而非发生回绕，随后的拒绝信息也不会给出具体数字。
	width, pinned := int64(1), false
	multiply := func(degree int) {
		if pinned {
			return
		}
		if width > math.MaxInt64/int64(degree) {
			width, pinned = cards+1, true
			return
		}
		width *= int64(degree)
	}

	var factors []string
	multiply(declared.TensorParallel)
	multiply(declared.PipelineParallel)
	if declared.TensorParallel > 1 {
		factors = append(factors, fmt.Sprintf("tensor-parallel %d", declared.TensorParallel))
	}
	if declared.PipelineParallel > 1 {
		factors = append(factors, fmt.Sprintf("pipeline-parallel %d", declared.PipelineParallel))
	}

	switch engine {
	case workercore.ModelDeploymentEngineVLLM:
		if declared.PrefillContextParallel > 1 {
			multiply(declared.PrefillContextParallel)
			factors = append(factors, fmt.Sprintf("prefill-context-parallel %d", declared.PrefillContextParallel))
		}
		// 存在任何连线标志的情况已在上方提前返回，因此此处声明的本地份额恰好就是该成员运行的 DP rank 数：
		// 它本身就是单成员的数据并行宽度，直接参与相乘。
		if declared.DataParallelLocal > 1 {
			multiply(declared.DataParallelLocal)
			factors = append(factors, fmt.Sprintf("data-parallel-local %d", declared.DataParallelLocal))
		} else if declared.DataParallelLocal == 0 {
			multiply(declared.DataParallel)
			if declared.DataParallel > 1 {
				factors = append(factors, fmt.Sprintf("data-parallel %d", declared.DataParallel))
			}
		}
	case workercore.ModelDeploymentEngineSGLang:
		narrows := slices.Contains(declared.Modes, "--enable-dp-attention") ||
			declared.DWDPSize > 1 || declared.MoEDataParallel > 1 || declared.AttentionContextParallel > 1
		if !narrows {
			multiply(declared.DataParallel)
			if declared.DataParallel > 1 {
				factors = append(factors, fmt.Sprintf("data-parallel %d", declared.DataParallel))
			}
		}
	default:
		return nil
	}

	if width <= cards {
		return nil
	}

	needs := fmt.Sprintf("%d", width)
	if pinned {
		needs = fmt.Sprintf("more than %d", cards)
	}

	return field.ErrorList{field.Invalid(
		rolePath.Child("resources", "accelerator"), cards, fmt.Sprintf(
			"%d cards cannot hold the width the role declares (%s): the engine needs %s per "+
				"member, and the width is per member, so %s does not rescue it",
			cards, strings.Join(factors, ", "), needs, rolePath.Child("size"),
		),
	)}
}

// validateModelDeploymentRoleEnv 拒绝由 operator 拥有的环境变量条目。
//
// 此处的所有权取决于某个键会破坏什么，而非它重复了什么：配置路径变量是指向 operator 所写文件的
// 唯一指针，重新指向它会把整份客户端配置替换为另一个文件的内容，使所有故障表象与根因隔开一层。
// operator 仅提供默认值的键并不归其所有，因此用户设置的值在那里会直接生效而不会被拒绝。
//
// 此处拒绝的集合必须与渲染器丢弃的集合完全一致。渲染器会无条件丢弃这些键（包括角色接管命令行时），
// 因此此处也无条件拒绝：两个集合只要无论向哪一侧出现偏差，都会导致静默丢弃。
func validateModelDeploymentRoleEnv(
	engine string, role *workercore.ModelDeploymentRole, rolePath *field.Path,
) field.ErrorList {
	var errs field.ErrorList

	for i := range role.Env {
		name := role.Env[i].Name
		if !workerctrl.ModelDeploymentOwnsEnv(engine, name) {
			continue
		}

		errs = append(errs, field.Invalid(rolePath.Child("env").Index(i), name, fmt.Sprintf(
			"%q is set by the operator for engine %q and must not be supplied here, because it "+
				"selects the client configuration the operator rendered; replace the whole "+
				"command line through %s to own it instead",
			name, engine, rolePath.Child("command"),
		)))
	}

	return errs
}

// validateModelDeploymentRoleResources 拒绝同时请求硬件分区和软件切分的资源请求。
//
// 单块加速器无法同时提供这两种模式，因此同时指定两者的请求不存在合理的解释——而 operator 的渲染器
// 会按优先级裁决这对配置，从而静默授予分区 profile 并丢弃切分百分比。
//
// 需要读取 InstanceType 的另外两条规则不在此处，这是由它们所读取的对象决定的，而非主题拆分。
// 本规则仅凭请求本身即可判定，因此对正在删除的部署以及尚未读取到 InstanceType 的部署均有效；
// 另外两条规则位于 validateRoleResourcesAgainstInstanceType 中，仅当成功读取到 InstanceType 时才会执行。
func validateModelDeploymentRoleResources(
	role *workercore.ModelDeploymentRole, rolePath *field.Path,
) field.ErrorList {
	ress := role.Resources
	if ress == nil {
		return nil
	}
	var errs field.ErrorList
	if ress.Interface != nil {
		count, whole := ress.Interface.AsInt64()
		if !whole || count < 0 {
			errs = append(errs, field.Invalid(rolePath.Child("resources", "interface"),
				ress.Interface.String(), "interface count must be a non-negative whole number"))
		}
	}
	if ress.AcceleratorPartitionedProfile == "" {
		return errs
	}

	if ress.AcceleratorSlicedMemoryPercentage == 0 && ress.AcceleratorSlicedCoresPercentage == 0 {
		return errs
	}

	ressPath := rolePath.Child("resources")

	return append(errs, field.Invalid(
		ressPath.Child("acceleratorPartitionedProfile"), ress.AcceleratorPartitionedProfile,
		fmt.Sprintf(
			"a partition profile cannot be combined with %s or %s: hardware partitioning and "+
				"software slicing cannot both apply to one accelerator",
			ressPath.Child("acceleratorSlicedMemoryPercentage"),
			ressPath.Child("acceleratorSlicedCoresPercentage"),
		),
	))
}

// validateRoleResourcesAgainstInstanceTypes 应用需要读取角色所指定 InstanceType 的校验规则，
// 也是此处唯一需要读取其他对象的校验路径。
//
// 这些规则的价值在于将拒绝提前到 API 入口处，而非滞后到调度链深处。若没有它们，不可行的请求最终
// 仍会被调度链自身的准入检查拒绝，但报错会落在 Workload 上，既不指明对应的部署也不指出用户填写的
// 字段。最终结果本身并没有错，错在错误信息不可操作，用户不得不耗费时间排查本应直接指出的问题。
//
// 正在删除的对象不受这些规则约束。读取 InstanceType 的规则会在类型缺失时拒绝请求，若仍启用这些
// 规则，已删除的 InstanceType 将阻塞清除本部署 finalizer 的更新。这与 Default 方法整体跳过删除中
// 对象的理由相同，此处应用于引入了同样外部依赖的那部分校验逻辑。
//
// 未指定 InstanceType 或未指定 Resources 的角色会直接跳过而不会发起查询。空名称属于单对象规则
// 已能报告的字段错误，若去查询不仅会失败，还会返回关于集群读取失败而非名称本身的问题提示。
//
// 无法读取 InstanceType 时会直接中止本轮校验，而不是将其转化为第二条字段拒绝。getInstanceType
// 已将“名称不存在”与“集群无响应”区分开来，且默认化阶段在所有访问存储的路径上都会先抛出同样的错误。
func (r *ModelDeploymentWebhook) validateRoleResourcesAgainstInstanceTypes(
	ctx context.Context, md *workercore.ModelDeployment,
) (field.ErrorList, error) {
	if md.DeletionTimestamp != nil {
		return nil, nil
	}

	rolesPath := field.NewPath("spec", "roles")

	var errs field.ErrorList
	seen := make(map[string]*worker.InstanceType, 1)
	for i := range md.Spec.Roles {
		role := &md.Spec.Roles[i]
		if role.InstanceType == "" || role.Resources == nil {
			continue
		}

		instType, ok := seen[role.InstanceType]
		if !ok {
			var err error
			if instType, err = r.getInstanceType(ctx, role.InstanceType, i); err != nil {
				// 字段错误会与其他错误合并，而不是直接结束本轮校验。指定的 InstanceType 不存在属于当前对象字段
				// 本身的问题，直接裸返回该错误会带来两个问题：一是响应会变为普通拒绝而非指明字段路径的 Invalid
				// 错误；二是会导致短路——一个拼写错的 instanceType 会掩盖对象上的所有其他校验错误，迫使用户每次
				// apply 只能修复一处。
				//
				// 传输层故障同样通过字段错误承载，但绝不能合并到错误列表中。getInstanceType 会用
				// field.InternalError 报告 API 不可读的问题，它虽然与“未找到”一样都是 *field.Error，含义却
				// 截然不同：对象本身可能完全合法，只是无法查询集群。若将其合并，会仅因一次读取失败就告知用户其
				// 对象无效；因此这类错误仍应直接结束校验，交由失败策略决定后续行为。
				var fieldErr *field.Error
				if !errors.As(err, &fieldErr) || fieldErr.Type == field.ErrorTypeInternal {
					return nil, err
				}
				errs = append(errs, fieldErr)

				continue
			}
			seen[role.InstanceType] = instType
		}
		if !instType.Spec.Acceleratable {
			continue
		}

		// 尚未计算完成的详情属于瞬态拒绝，而非永久拒绝。空的详情表示“尚未同步”的状态，而不是“该类型
		// 不提供任何模式”；两者的区分由 Instance webhook 所调用的同一个辅助函数负责——若另写一套空值
		// 判断逻辑，两者迟早会产生语义漂移。若不使用该判断，资源池刚出现后几秒内提交的合法部署就会因该类型
		// 实际上支持的模式而被拒绝，而用户能做的只有等待，字段错误并不会传达这一信息。
		if slicingRequestNotReady(instType, roleInstanceResources(role.Resources)) {
			return nil, kerrors.NewInternalError(
				fmt.Errorf("instance type %s is not ready yet; retry", instType.Name),
			)
		}

		errs = append(errs, validateRoleResourcesAgainstInstanceType(
			instType, role.Resources, rolesPath.Index(i).Child("resources"),
		)...)
	}

	errs = append(errs, validateModelDeploymentZeroAcceleratorInMultiRoleGroup(md, seen, rolesPath)...)

	return append(errs, validateModelDeploymentPairCannotShareOneAccelerator(md, seen, rolesPath)...), nil
}

// validateModelDeploymentZeroAcceleratorInMultiRoleGroup 拒绝与另一角色共享同一个 InstanceType、
// 却未请求任何受加速队列计费资源的角色。
//
// 本规则防止的是副本在未计费的情况下运行。队列仅覆盖厂商的加速卡额度，管理器会忽略队列未声明的
// 资源，因此这类副本能够完成预留、报告 Admitted 并正常启动，而队列中对应额度的用量却保持为零——
// 这一点已在集群上实测确认，且全程没有任何报错。同一 InstanceType 上的其他兄弟角色每持有一张卡
// 都会被计费，从而与这个不计入配额却消耗同一资源池的角色产生竞争。
//
// 本规则限定于共享同一 InstanceType 的场景，而非禁止所有零请求：因为独占可加速 InstanceType 的
// 单角色会让整个部署的资源池完全不计费（表现为部署从不消耗配额，易于察觉），而混杂在计费兄弟角色
// 中的未计费角色则完全隐蔽。
// NOTE: 上述原因并非当初编写此规则的初衷。最初的原因是 Kueue 会拒绝 PodSets 数量多于其分配项的
// Workload 的部分分配，而在如今每个副本各自携带单个 PodSet 的 Workload 后，该情况已不会发生。
func validateModelDeploymentZeroAcceleratorInMultiRoleGroup(
	md *workercore.ModelDeployment,
	instanceTypes map[string]*worker.InstanceType,
	rolesPath *field.Path,
) field.ErrorList {
	groupSizes := make(map[string]int, len(instanceTypes))
	for i := range md.Spec.Roles {
		groupSizes[md.Spec.Roles[i].InstanceType]++
	}

	var errs field.ErrorList
	for i := range md.Spec.Roles {
		role := &md.Spec.Roles[i]
		instType := instanceTypes[role.InstanceType]
		if instType == nil || !instType.Spec.Acceleratable || groupSizes[role.InstanceType] < 2 ||
			role.Resources == nil || role.Resources.Accelerator == nil || role.Resources.Accelerator.Sign() != 0 {
			continue
		}

		errs = append(errs, field.Invalid(
			rolesPath.Index(i).Child("resources", "accelerator"), role.Resources.Accelerator.String(),
			fmt.Sprintf(
				"must be greater than zero because the queue behind acceleratable instance type %q "+
					"accounts only in accelerator credits: this role's replicas would be admitted and "+
					"run while that queue charges them nothing, spending from the pool its sibling "+
					"roles on the same type are charged for; request at least one accelerator or use "+
					"a non-acceleratable instance type for this CPU-only role",
				role.InstanceType,
			),
		))
	}

	return errs
}

// validateModelDeploymentPairCannotShareOneAccelerator 拒绝以下组合：一个 prefill 角色和一个
// decode 角色各自请求两者的 InstanceType 均可选中同一块加速卡的逻辑切分（slice）。
//
// 拆分这两类角色的目的本是隔离相互争用的工作负载，因此允许两者分别持有同一张卡上的一个切分，
// 会使对象表面上看似分离部署，实则丧失了隔离效果。运行过程中不会有任何报错：副本会正常启动并
// 提供服务，但 prefill 阶段的突发负载会落在 decode 阶段正在流式输出的同一块芯片上。
//
// 判定前提是两者对应互不相交的加速器集合，而两个不同的 InstanceType 名称并不足以证明这一点。
// 管理员可以基于某个派生类型已覆盖的 acceleratorGroup 再创建一个 InstanceType，此时两者实际上是
// 同一批加速器的两种视图。若规则仅以名称不同作为放行条件，恰恰会漏过它原本要堵住的场景；而若完全
// 不加限定条件，又会过度拒绝：它会阻断双 instanceType 角色本欲实现的异构部署形态。
//
// 即使落在同一张卡上，硬件分区（partition）组合也会被接受。硬件分区由设备本身提供彼此隔离，而这正是
// 本规则所关注的属性；逻辑切分则不具备这种隔离，这正是两者的根本区别。整卡分配组合出于同样原因在
// 更粗粒度上也被接受。
//
// 单角色部署不受影响。本规则约束的是角色对，单个角色与自身共享同一张卡正是逻辑切分的本来用途。
//
// 每一个 prefill 角色都会与每一个 decode 角色逐一比较，因为一个部署中任一类型都可能声明多个角色：
// 角色类型规则仅禁止将 server 角色与其他类型混用，并不禁止两个 prefill 角色共存。若每种类型只
// 保留一个角色，就只会比较最后声明的那一对，从而放行了声明在前面的违规组合。
func validateModelDeploymentPairCannotShareOneAccelerator(
	md *workercore.ModelDeployment,
	types map[string]*worker.InstanceType,
	rolesPath *field.Path,
) field.ErrorList {
	var prefills, decodes []*workercore.ModelDeploymentRole
	for i := range md.Spec.Roles {
		role := &md.Spec.Roles[i]
		if !roleRequestsALogicalSlice(role) {
			continue
		}
		switch role.Kind {
		case workercore.ModelDeploymentRoleKindPrefill:
			prefills = append(prefills, role)
		case workercore.ModelDeploymentRoleKindDecode:
			decodes = append(decodes, role)
		}
	}

	var errs field.ErrorList
	for _, prefill := range prefills {
		prefillType := types[prefill.InstanceType]
		if prefillType == nil {
			// 对于无法读取的类型或未指定类型的角色：专门负责报告这些问题的规则已经处理过了，
			// 仅凭配对中的一半信息来回答此问题无异于猜测。
			continue
		}
		for _, decode := range decodes {
			decodeType := types[decode.InstanceType]
			// 未设置的分组表示未知，而非两个类型共享同一分组。InstanceType webhook 要求可加速类型必须填写
			// 该字段，但只有通过该规则的对象才会带有它；若将两个空字符串判为相等，就会仅凭两个类型都未填写的
			// 信息而拒绝一组配置。因字段为空而拒绝合法部署属于本规则的错误兜底方式：它会阻断拆分部署本欲
			// 支持的形态，且由于 instanceType 不可变，用户也无从修改。
			if decodeType == nil ||
				prefillType.Spec.AcceleratorGroup == "" || decodeType.Spec.AcceleratorGroup == "" ||
				prefillType.Spec.AcceleratorGroup != decodeType.Spec.AcceleratorGroup {
				continue
			}

			errs = append(errs, field.Invalid(
				rolesPath, prefill.Name+", "+decode.Name,
				fmt.Sprintf(
					"roles %q and %q both request a logical slice, and instance types %q and %q draw from "+
						"the same accelerator group %q — so the two can land on one card, which is what the "+
						"prefill/decode split exists to prevent. Give one of them an instance type over a "+
						"different accelerator group, or request whole cards or hardware partition profiles, "+
						"which are isolated by the device",
					prefill.Name, decode.Name,
					prefill.InstanceType, decode.InstanceType, prefillType.Spec.AcceleratorGroup,
				),
			))
		}
	}

	return errs
}

// roleRequestsALogicalSlice 报告 role 是否在软件层面申请了单卡的部分切片。
//
// 只要指定了任一百分比即视为切片请求：默认值填充逻辑会将其中一项复制到另一项，因此仅检查内存百分比的规则会完全漏掉只指定算力百分比的请求。
func roleRequestsALogicalSlice(role *workercore.ModelDeploymentRole) bool {
	ress := role.Resources

	return ress != nil &&
		(ress.AcceleratorSlicedMemoryPercentage != 0 || ress.AcceleratorSlicedCoresPercentage != 0)
}

// roleInstanceResources 将 role 的资源请求投影到 InstanceResources 的加速卡字段。
//
// 这样做是为了通过已负责回答 Instance 就绪性判断的辅助函数来进行检查，而不是另写一套可能出现分歧的判空逻辑。
// ModelDeploymentRoleResources 在字段名和语义上均与这些字段一致，因此这里只是重命名而非转换；投影中不包含 CPU、内存或本地存储，是因为 role 本身不声明这些资源。
func roleInstanceResources(ress *workercore.ModelDeploymentRoleResources) *workercore.InstanceResources {
	return &workercore.InstanceResources{
		Accelerator:                       ress.Accelerator,
		AcceleratorSlicedMemoryPercentage: ress.AcceleratorSlicedMemoryPercentage,
		AcceleratorSlicedCoresPercentage:  ress.AcceleratorSlicedCoresPercentage,
		AcceleratorPartitionedProfile:     ress.AcceleratorPartitionedProfile,
	}
}

// validateRoleResourcesAgainstInstanceType 拒绝所指定的 InstanceType 无法满足的请求：
// 包括请求了该类型不提供的模式，以及请求的整卡数量超过了资源池容量。
//
// 模式由请求指定的字段决定，而非由类型决定。指定切分配置文件（partition profile）使请求成为切分模式，
// 非零百分比使请求成为切片模式，两者均未指定则为所有可加速类型都提供的整卡模式。
// 同时指定两者的情况在此之前已由专门处理该冲突的规则拒绝，因此此处的分支互不重叠。
//
// 两种拒绝错误均需指明 InstanceType，因为仅靠资源字段本身无法定位问题：相同的请求在另一个类型下可能是正确的，
// 运维人员需要修改的是两者之一。
func validateRoleResourcesAgainstInstanceType(
	instType *worker.InstanceType,
	ress *workercore.ModelDeploymentRoleResources,
	ressPath *field.Path,
) field.ErrorList {
	var errs field.ErrorList

	switch {
	case ress.AcceleratorPartitionedProfile != "":
		errs = append(errs, validateRolePartitionProfileOffered(instType, ress, ressPath)...)
		errs = append(errs, validateRoleSingleCardRequest(ress, "partitioned", ressPath)...)
	case ress.AcceleratorSlicedMemoryPercentage != 0 || ress.AcceleratorSlicedCoresPercentage != 0:
		// 没有可逻辑切片卡的资源池完全无法服务该请求：若予以准入，此类 role 会一直处于 Pending 状态，而不会被改写为整卡请求。
		if !instType.Status.Detail.IsLogicallySliceable() {
			errs = append(errs, field.Forbidden(
				ressPath.Child("acceleratorSlicedMemoryPercentage"),
				fmt.Sprintf("instance type %s does not offer logical slicing", instType.Name),
			))
		}
		errs = append(errs, validateRoleSingleCardRequest(ress, "sliced", ressPath)...)
	default:
		// 整卡上限仅约束整卡请求，不约束其他模式，这也是它位于此分支内的原因。status.accelerator 是整卡视图，
		// 仅统计未切分的卡，因此全部卡都已切分的资源池在该视图下会报告 0，但其切分视图仍可正常服务请求。
		// 若将该上限应用于所有模式，就会以 0 为上限拒绝合法的切分或切片 role——这实际上是在错误描述类型而非请求。
		//
		// 此外，被拒绝的值可能正是当前 Webhook 写入的。未指定卡数的 role 会由变更阶段默认设为 1 张卡，
		// 运维人员从未输入过该数字，也就无从修正。让用户无从下手的拒绝规则比没有规则更糟。
		//
		// Instance Webhook 已经划清了这条界线：validateExclusiveAcceleratorRequest 仅在整卡路径上执行。
		//
		// 上限取自视图的 Capacity，而非 onceMaxRequest。onceMaxRequest 表示单个节点上最大的空闲卡数，
		// 资源池繁忙时会降为 0；若在此处检查它，所有在卡被占用期间提交的部署都会被拒绝。
		// 这是队列需要考虑的问题，而非准入控制的问题：只要资源池在卡释放后能够服务该部署，就应当准入并在队列中等待。
		// Capacity 不会随占用情况变化。
		//
		// 这是一个宽松上限，属于已知限制而非严格保证。Capacity 汇总整个资源池的容量，而单个 Pod 的卡必须来自同一节点，
		// 因此在多节点资源池中，大于最大单节点容量但仍在池总量内的请求会被准入，随后保持排队状态。
		// status 中不包含单节点容量信息，无法做更紧的约束。
		//
		// 上限数值直接包含在错误信息中，而不是让读者自己去查。只说“超出最大值”仅指出了请求有误，
		// 给出具体数值则说明了正确范围，两者的区别在于下一次尝试是否还要靠猜。
		if ress.Accelerator != nil {
			if ceiling := instType.Status.Accelerator.Capacity; ress.Accelerator.Cmp(ceiling) > 0 {
				errs = append(errs, field.Invalid(
					ressPath.Child("accelerator"), ress.Accelerator.String(),
					fmt.Sprintf("instance type %s has at most %s whole accelerator(s) in its pool",
						instType.Name, ceiling.String()),
				))
			}
		}
	}

	return errs
}

// validateRoleSingleCardRequest 拒绝申请超过一张卡的切片或切分 role。
//
// 切片是单卡的一部分，切分是单卡上的一个实例，因此请求规模由百分比或配置文件名称表达，卡数始终为 1。
// “两张各占 50% 的卡”无法被调度器放置，且控制器为此生成的资源键无论如何都是单卡维度的。
//
// 在整卡上限不再覆盖这两种模式后，由本规则负责约束它们。Instance Webhook 一直通过
// validateSingleCardRequest 在这两条路径上执行此限制；此处此前只是缺少该规则，
// 且由于整卡上限恰巧会以错误原因拒绝同样过大的请求（同时也拒绝了合法的单卡请求），这一缺失被掩盖了。
func validateRoleSingleCardRequest(
	ress *workercore.ModelDeploymentRoleResources, kind string, ressPath *field.Path,
) field.ErrorList {
	one := resource.NewQuantity(1, resource.DecimalSI)
	if ress.Accelerator != nil && ress.Accelerator.Cmp(*one) == 0 {
		return nil
	}

	got := "0"
	if ress.Accelerator != nil {
		got = ress.Accelerator.String()
	}

	return field.ErrorList{field.Invalid(
		ressPath.Child("accelerator"), got,
		fmt.Sprintf("accelerator request must be exactly 1 for a %s request", kind),
	)}
}

// validateRolePartitionProfileOffered 检查切分请求是否匹配资源池上报的配置文件。
//
// 缺少切分能力会在查找配置文件之前报告。卡未处于切分模式的资源池提供的配置文件集合为空，
// 若将其报为“不提供此配置文件”，会让人误以为是拼写错误，从而引导运维人员去修改本没有问题的名称。
func validateRolePartitionProfileOffered(
	instType *worker.InstanceType,
	ress *workercore.ModelDeploymentRoleResources,
	ressPath *field.Path,
) field.ErrorList {
	profilePath := ressPath.Child("acceleratorPartitionedProfile")

	if !instType.Status.Detail.IsPhysicallySliceable() {
		return field.ErrorList{field.Forbidden(profilePath,
			fmt.Sprintf("instance type %s does not offer hardware partitioning", instType.Name))}
	}

	profiles := instType.Status.Detail.SlicedDetail.Physical.Profiles
	offered := make([]string, 0, len(profiles))
	for _, p := range profiles {
		if p.Name == ress.AcceleratorPartitionedProfile {
			return nil
		}
		offered = append(offered, p.Name)
	}

	return field.ErrorList{field.Invalid(profilePath, ress.AcceleratorPartitionedProfile,
		fmt.Sprintf("instance type %s does not offer this partition profile; offered: %v",
			instType.Name, offered))}
}

// 单写者围栏（single-writer fence）拒绝编辑时使用的规则常量，将其写入拒绝信息中以便测试和读者识别具体触发了哪条规则。
const (
	modelDeploymentFenceRuleReplicaCount = "the replica count of a bound deployment belongs to the scaler that owns it"
	modelDeploymentFenceRuleRoleSet      = "the roles of a bound deployment are planned by the scaler " +
		"and cannot be added, removed, or renamed while bound"
	modelDeploymentFenceRuleAnnotation = "the binding and decision annotations of a bound deployment belong to the scaler that owns it"
	modelDeploymentFenceRuleTerm       = "a binding term is never given back"
	modelDeploymentFenceRuleDecision   = "a replica count that changed under a binding carries the decision that changed it"
	modelDeploymentFenceRuleOrphan     = "the scaler that owns this deployment still exists"
)

// validateModelDeploymentScalerFence 维护单写者规则。
//
// 已绑定到扩缩器的部署，其副本数只能有一个写者，即该扩缩器。
// 该规则在准入阶段而非协调器（reconciler）中强制执行，因为协调器只能拒绝采取行动，而准入阶段可以直接拒绝写入本身；
// 阻止第二个写者的写入正是此机制要防止的故障。
//
// 该规则分为两侧，因为两类调用方的语义不同。非本 Operator 的调用方视为普通用户，
// 用户可以修改已绑定部署的任意其他内容，但不能修改副本数以及记录所有权的两个注解。
// 本 Operator 可以写入这些字段，但必须以事后可校验的形式写入：绑定关系携带只增不减的任期（term），
// 且在绑定下发生变更的副本数必须附带产生该变更的决策（decision）。
func (r *ModelDeploymentWebhook) validateModelDeploymentScalerFence(
	ctx context.Context, old, md *workercore.ModelDeployment,
) field.ErrorList {
	var errs field.ErrorList

	replicasChanged := modelDeploymentScalerReplicaCountsChanged(old, md)

	// 没有历史状态的部署不属于已绑定部署。该规则针对已经携带绑定的部署，尚未携带绑定的对象没有需要保护的所有者。
	if old == nil {
		old = &workercore.ModelDeployment{}
	}

	oldBinding, oldHasBinding, oldErr := modelDeploymentScalerBindingOf(old)
	newBinding, newHasBinding, newErr := modelDeploymentScalerBindingOf(md)
	if newErr != nil {
		return append(errs, field.Invalid(
			field.NewPath("metadata", "annotations").Key(workercore.ModelDeploymentScalerBindingAnnotationKey),
			md.Annotations[workercore.ModelDeploymentScalerBindingAnnotationKey],
			"the binding of this deployment cannot be read, so nothing may be written under it: "+
				newErr.Error(),
		))
	}

	bindingChanged := modelDeploymentScalerBindingChanged(old, md)

	if r.isOperatorRequest(ctx) {
		return append(errs, r.validateModelDeploymentScalerOperatorFence(
			oldBinding, oldHasBinding, newBinding, newHasBinding, bindingChanged,
			replicasChanged, md,
		)...)
	}

	if oldErr != nil {
		return append(errs, field.Invalid(
			field.NewPath("metadata", "annotations").Key(workercore.ModelDeploymentScalerBindingAnnotationKey),
			old.Annotations[workercore.ModelDeploymentScalerBindingAnnotationKey],
			"the binding of this deployment cannot be read, so nothing may be written under it: "+
				oldErr.Error(),
		))
	}

	return append(errs, r.validateModelDeploymentScalerUserFence(
		ctx, oldBinding, oldHasBinding, newHasBinding, replicasChanged, old, md,
	)...)
}

// validateModelDeploymentScalerOperatorFence 维护适用于本 Operator 自身写入的规则。
//
// 任期（term）绝不回退，因为读方依赖任期判断哪次绑定更新。
// 若任期回退，旧决策看起来会比新决策更新，从而使读方接受本已被围栏拒绝的写入。
//
// 发生变更的副本数必须携带产生该变更的决策，因为没有对应决策的副本数无法追溯回发起请求的观测结果，
// 读方也无法区分这是自动扩容还是手动修改。
func (r *ModelDeploymentWebhook) validateModelDeploymentScalerOperatorFence(
	oldBinding workercore.ModelDeploymentScalerBinding, oldHasBinding bool,
	newBinding workercore.ModelDeploymentScalerBinding, newHasBinding bool,
	bindingChanged, replicasChanged bool, md *workercore.ModelDeployment,
) field.ErrorList {
	var errs field.ErrorList

	annotationsPath := field.NewPath("metadata", "annotations")

	if newHasBinding && (!oldHasBinding || bindingChanged) {
		if newBinding.ScalerName == "" || newBinding.ScalerUID == "" {
			errs = append(errs, field.Invalid(
				annotationsPath.Key(workercore.ModelDeploymentScalerBindingAnnotationKey),
				md.Annotations[workercore.ModelDeploymentScalerBindingAnnotationKey],
				"a binding must name both scalerName and scalerUID",
			))
		}
		if newBinding.Term < 1 {
			errs = append(errs, field.Invalid(
				annotationsPath.Key(workercore.ModelDeploymentScalerBindingAnnotationKey),
				newBinding.Term,
				modelDeploymentFenceRuleTerm+": a binding term starts at one",
			))
		}
		if oldHasBinding && newBinding.Term < oldBinding.Term {
			errs = append(errs, field.Invalid(
				annotationsPath.Key(workercore.ModelDeploymentScalerBindingAnnotationKey),
				newBinding.Term,
				fmt.Sprintf("%s: the term of this binding is %d, and the one it replaces is %d",
					modelDeploymentFenceRuleTerm, newBinding.Term, oldBinding.Term),
			))
		}
	}

	if replicasChanged && (oldHasBinding || newHasBinding) {
		if !newHasBinding {
			errs = append(errs, field.Invalid(
				annotationsPath.Key(workercore.ModelDeploymentScalerBindingAnnotationKey), "",
				modelDeploymentFenceRuleDecision,
			))
			return errs
		}
		decision, hasDecision, err := modelDeploymentScalerDecisionOf(md)
		if err != nil {
			errs = append(errs, field.Invalid(
				annotationsPath.Key(workercore.ModelDeploymentScalerDecisionAnnotationKey),
				md.Annotations[workercore.ModelDeploymentScalerDecisionAnnotationKey],
				"the decision of this deployment cannot be read: "+err.Error(),
			))
			return errs
		}
		if !hasDecision {
			errs = append(errs, field.Invalid(
				annotationsPath.Key(workercore.ModelDeploymentScalerDecisionAnnotationKey), "",
				modelDeploymentFenceRuleDecision,
			))
			return errs
		}
		if decision.Term != newBinding.Term {
			errs = append(errs, field.Invalid(
				annotationsPath.Key(workercore.ModelDeploymentScalerDecisionAnnotationKey),
				decision.Term,
				fmt.Sprintf("%s: the decision is written under term %d, and the binding is under "+
					"term %d", modelDeploymentFenceRuleDecision, decision.Term, newBinding.Term),
			))
		}
	}

	return errs
}

// validateModelDeploymentScalerUserFence 维护适用于非本 Operator 调用方的规则。
//
// 已绑定部署的其余部分仍可自由修改。用户依然可以更改扩缩器所管理部署的镜像、参数或标签：
// 扩缩器拥有的只是副本数量以及谁有权决定数量，而非这些副本运行什么内容。
//
// 逃生通道（escape hatch）的存在是为了处理扩缩器被删除后遗留绑定信息的情况。
// 没有它，该部署将永远无法移交给其他扩缩器，其副本数也再也无法修改，因为绑定中指向的不存在的扩缩器已经无法释放它。
// 该通道范围很窄：仅当扩缩器确实已不存在时才允许移除绑定，且绝不允许同时修改副本数。
func (r *ModelDeploymentWebhook) validateModelDeploymentScalerUserFence(
	ctx context.Context,
	oldBinding workercore.ModelDeploymentScalerBinding, oldHasBinding bool,
	newHasBinding, replicasChanged bool,
	old, md *workercore.ModelDeployment,
) field.ErrorList {
	var errs field.ErrorList

	annotationsPath := field.NewPath("metadata", "annotations")
	bindingKey := annotationsPath.Key(workercore.ModelDeploymentScalerBindingAnnotationKey)
	decisionKey := annotationsPath.Key(workercore.ModelDeploymentScalerDecisionAnnotationKey)

	bindingChanged := old.Annotations[workercore.ModelDeploymentScalerBindingAnnotationKey] !=
		md.Annotations[workercore.ModelDeploymentScalerBindingAnnotationKey]
	decisionChanged := old.Annotations[workercore.ModelDeploymentScalerDecisionAnnotationKey] !=
		md.Annotations[workercore.ModelDeploymentScalerDecisionAnnotationKey]
	decisionRemoved := old.Annotations[workercore.ModelDeploymentScalerDecisionAnnotationKey] != "" &&
		md.Annotations[workercore.ModelDeploymentScalerDecisionAnnotationKey] == ""

	if !oldHasBinding {
		// 当前没有绑定关系，因此没有需要保护的所有者。但用户仍然不能写入这些注解，因为写入注解等同于宣称所有权。
		if bindingChanged {
			errs = append(errs, field.Forbidden(bindingKey, modelDeploymentFenceRuleAnnotation))
		}
		if decisionChanged && !decisionRemoved {
			errs = append(errs, field.Forbidden(decisionKey, modelDeploymentFenceRuleAnnotation))
		}
		return errs
	}

	// 正在放弃绑定关系。下面的逃生通道逻辑决定是否允许这样做，关于注解的其他所有规则也都由此判定。
	if oldHasBinding && !newHasBinding {
		if modelDeploymentScalerRoleSetChanged(old, md) {
			errs = append(errs, field.Forbidden(
				field.NewPath("spec", "roles"), modelDeploymentFenceRuleRoleSet,
			))
		} else if replicasChanged {
			errs = append(errs, field.Forbidden(
				field.NewPath("spec", "roles"), modelDeploymentFenceRuleReplicaCount,
			))
		}
		if decisionChanged && !decisionRemoved {
			errs = append(errs, field.Forbidden(decisionKey, modelDeploymentFenceRuleAnnotation))
		}
		if orphan, fErr := r.modelDeploymentScalerBindingIsOrphan(ctx, md.Namespace, oldBinding); !orphan {
			if fErr != nil {
				errs = append(errs, fErr)
			}
		}
		return errs
	}

	if bindingChanged {
		errs = append(errs, field.Forbidden(bindingKey, modelDeploymentFenceRuleAnnotation))
	}
	if decisionChanged {
		errs = append(errs, field.Forbidden(decisionKey, modelDeploymentFenceRuleAnnotation))
	}
	if modelDeploymentScalerRoleSetChanged(old, md) {
		errs = append(errs, field.Forbidden(
			field.NewPath("spec", "roles"), modelDeploymentFenceRuleRoleSet,
		))
	} else if replicasChanged {
		errs = append(errs, field.Forbidden(
			field.NewPath("spec", "roles"), modelDeploymentFenceRuleReplicaCount,
		))
	}

	return errs
}

// isOperatorRequest 报告请求是否来自本 Operator。
//
// 身份依据是 ServiceAccount，因为 API Server 会为使用绑定令牌发起的请求记录该身份。
// 不具备该身份的请求均被视为用户请求，即触发拒绝的一侧。RBAC 必须限制 Operator 命名空间内的
// ServiceAccount 创建与令牌投影，以确保只有 Operator 工作负载能以该 ServiceAccount 身份执行操作。
func (r *ModelDeploymentWebhook) isOperatorRequest(ctx context.Context) bool {
	req, err := ctrladmission.RequestFromContext(ctx)
	if err != nil {
		return false
	}

	return req.UserInfo.Username == serviceaccount.MakeUsername(r.Namespace, r.ServiceAccount)
}

// modelDeploymentScalerBindingIsOrphan 报告拥有该部署的扩缩器是否确实已不存在。
//
// 扩缩器会实时读取，因为判断的是集群当前的实际状态，而非部署在绑定时记录的快照。
// 被删除后又以同名重建的扩缩器会拥有不同的 UID，此前绑定到旧对象的部署即不再受其约束。
func (r *ModelDeploymentWebhook) modelDeploymentScalerBindingIsOrphan(
	ctx context.Context, namespace string, binding workercore.ModelDeploymentScalerBinding,
) (bool, *field.Error) {
	bindingKey := field.NewPath("metadata", "annotations").Key(workercore.ModelDeploymentScalerBindingAnnotationKey)
	reader := r.APIReader
	if reader == nil {
		return false, field.InternalError(bindingKey,
			fmt.Errorf("the scaler cannot be read, because this handler has no reader"))
	}

	scaler := new(workercore.ModelDeploymentScaler)
	err := reader.Get(ctx, ctrlcli.ObjectKey{Namespace: namespace, Name: binding.ScalerName}, scaler)
	if err != nil {
		if kerrors.IsNotFound(err) {
			return true, nil
		}
		return false, field.InternalError(bindingKey,
			fmt.Errorf("the scaler %q could not be read: %w", binding.ScalerName, err))
	}

	if string(scaler.UID) != binding.ScalerUID {
		return true, nil
	}

	return false, field.Forbidden(bindingKey,
		fmt.Sprintf("%s: the scaler %q still exists", modelDeploymentFenceRuleOrphan, binding.ScalerName))
}

// modelDeploymentScalerBindingOf 读取部署携带的绑定信息。
func modelDeploymentScalerBindingOf(
	md *workercore.ModelDeployment,
) (workercore.ModelDeploymentScalerBinding, bool, error) {
	raw, ok := md.Annotations[workercore.ModelDeploymentScalerBindingAnnotationKey]
	if !ok || raw == "" {
		return workercore.ModelDeploymentScalerBinding{}, false, nil
	}

	binding := workercore.ModelDeploymentScalerBinding{}
	if err := json.Unmarshal([]byte(raw), &binding); err != nil {
		return workercore.ModelDeploymentScalerBinding{}, false, fmt.Errorf(
			"the binding annotation is not a binding: %w", err,
		)
	}

	return binding, true, nil
}

// modelDeploymentScalerDecisionOf 读取部署携带的决策信息。
func modelDeploymentScalerDecisionOf(
	md *workercore.ModelDeployment,
) (workercore.ModelDeploymentScalerDecision, bool, error) {
	raw, ok := md.Annotations[workercore.ModelDeploymentScalerDecisionAnnotationKey]
	if !ok || raw == "" {
		return workercore.ModelDeploymentScalerDecision{}, false, nil
	}

	decision := workercore.ModelDeploymentScalerDecision{}
	if err := json.Unmarshal([]byte(raw), &decision); err != nil {
		return workercore.ModelDeploymentScalerDecision{}, false, fmt.Errorf(
			"the decision annotation is not a decision: %w", err,
		)
	}

	return decision, true, nil
}

// modelDeploymentScalerBindingChanged 报告部署的绑定注解是否发生变化。
func modelDeploymentScalerBindingChanged(old, md *workercore.ModelDeployment) bool {
	return old.Annotations[workercore.ModelDeploymentScalerBindingAnnotationKey] !=
		md.Annotations[workercore.ModelDeploymentScalerBindingAnnotationKey]
}

// modelDeploymentScalerRoleSetChanged 报告 role 集合是否有新增、删除或重命名。
func modelDeploymentScalerRoleSetChanged(old, md *workercore.ModelDeployment) bool {
	if old == nil || md == nil {
		return false
	}
	if len(old.Spec.Roles) != len(md.Spec.Roles) {
		return true
	}

	names := make(map[string]bool, len(old.Spec.Roles))
	for i := range old.Spec.Roles {
		names[old.Spec.Roles[i].Name] = true
	}

	for i := range md.Spec.Roles {
		if !names[md.Spec.Roles[i].Name] {
			return true
		}
	}

	return false
}

// modelDeploymentScalerReplicaCountsChanged 报告是否有任意 role 的副本数发生变化。
//
// role 按名称匹配，因为部署通过名称标识它们；在另一个名称下变化的副本数属于不同的 role，而非同一 role 的副本数变更。
func modelDeploymentScalerReplicaCountsChanged(old, md *workercore.ModelDeployment) bool {
	if old == nil {
		return false
	}
	if len(old.Spec.Roles) != len(md.Spec.Roles) {
		return true
	}

	counts := make(map[string]int32, len(old.Spec.Roles))
	for i := range old.Spec.Roles {
		counts[old.Spec.Roles[i].Name] = old.Spec.Roles[i].Replicas
	}

	for i := range md.Spec.Roles {
		was, ok := counts[md.Spec.Roles[i].Name]
		if !ok || was != md.Spec.Roles[i].Replicas {
			return true
		}
	}

	return false
}
