package mooncake

import (
	"fmt"
	"math"
	"slices"

	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/kubemeta"
	"gpustack.ai/gpustack/pkg/systemmeta"
	"gpustack.ai/gpustack/pkg/worker/kuberess"
	"gpustack.ai/gpustack/pkg/worker/kvcache"
)

const (
	// leaderResourceNoteRole 用于将 leader 的对象与 member 组的对象区分开来；
	// 二者采用相同的记录方式，并部署在同一个 Namespace 中。
	leaderResourceNoteRole = "leader"

	// leader 的对象和 Pod 所携带的身份标签。它们定义为常量，是因为对 leader Pod
	// 的 watch 必须准确读回渲染器写入的内容；而 Pod 是此处唯一不携带资源记录的对象：
	// 它由 Deployment 自身的控制器根据模板创建，资源记录位于 Deployment 上而非模板内部。
	labelKeyName      = "app.kubernetes.io/name"
	labelKeyInstance  = "app.kubernetes.io/instance"
	labelKeyComponent = "app.kubernetes.io/component"
	labelValueName    = "kv-cache-backend"
	labelValueLeader  = "leader"
	// 位于选择器之外；在此定义为常量而非内联编写，是因为 HA 账户也携带该标签，
	// 若出现第二种拼写，这两组对象将无法被一并检索到。
	labelKeyPartOf   = "app.kubernetes.io/part-of"
	labelValuePartOf = "gpustack-operator-worker"

	// leaderRPCPortName 和 leaderAdminPortName 为两个对外发布的端口命名。
	// 在 Service 上使用名称而非端口号，既能让调用方按角色请求端口，也因为这两个端口
	// 面向完全不同的使用方：一个供推理引擎连接，另一个供本 operator 抓取指标。
	leaderRPCPortName   = "rpc"
	leaderAdminPortName = "admin"

	// leaderReadinessPath 是一个受门控的路由：构件用服务平面检查将其包裹，在服务平面
	// 就绪前会返回 503。这恰好对应就绪检查所关心的问题——流量能否进入——因此探针直接
	// 请求该路径，而无需间接推断。
	leaderReadinessPath = "/get_all_segments"
	// leaderLivenessPath 是一个非门控路由：无论服务平面处于何种状态，只要进程能响应
	// 就返回 200。这对应存活检查所关心的问题，也是存活探针绝不能复用就绪路径的原因：
	// 否则仅仅是激活较慢的 leader 会因启动慢而被杀掉。
	leaderLivenessPath = "/health"

	// 配额策略所需的两个卷，以及桥接它们的容器。它们被拆分为两个独立卷是因为二者
	// 满足相反的需求：一个必须可写且不能是 ConfigMap，另一个必须承载 operator 的
	// 期望状态且只能是 ConfigMap。
	quotaPolicyVolumeName     = "tenant-quota-policy"
	quotaPolicySeedVolumeName = "tenant-quota-policy-seed"
	quotaPolicyInitName       = "seed-tenant-quota-policy"

	// 种子初始化命令所读取的环境变量。命令本身是固定字符串，它涉及的每个值都通过
	// 环境变量传入——将渲染后的 YAML 文档直接插值拼接进 shell 命令，会让策略内容
	// 受到 shell 引号转义规则的影响。
	quotaPolicySeedEnv  = "POLICY_SEED"
	quotaPolicyFileEnv  = "POLICY_FILE"
	quotaPolicyEmptyEnv = "POLICY_EMPTY"

	// quotaPolicySeedCommand 将 operator 的期望策略复制到可写卷中；当没有可复制的
	// 内容时则写入一个空策略文档。它读取的变量即上述三个常量；测试会将命令与常量两端
	// 绑定到相同的字面量名称。
	//
	// 该回退逻辑并非防御性冗余：ConfigMap 由 POOL 协调器渲染，因此开启了多租户但
	// 尚未绑定任何池的后端没有组件为其写入该 ConfigMap。对应的卷是可选的，未就绪时
	// 会挂载为空目录，复制找不到源文件；而若此处不写入有效文档，master 将拒绝启动——
	// 空文件同样无法通过其解析器校验。
	//
	// 这正是回退逻辑以源文件是否存在为条件，而不是在复制失败时回退的原因。
	// `cp || fallback` 无法区分预期的无池场景与其他原因导致的复制失败（如权限不足、
	// 挂载截断、瞬态 IO 错误），而 `2>/dev/null` 还会抹掉最后的错误痕迹。若被伪装成
	// 无池场景，真正的故障会写入一份空的账本，init 容器正常退出（返回 0），master 在
	// 没有租户配额的情况下启动，而池在配额已丢失的情况下仍继续报告 Ready。以文件存在性
	// 测试为条件后，此类故障会以 cp 自身的错误信息使 init 容器失败，这也是该错误唯一
	// 仍能被观测到的地方。
	//
	// 该逻辑在每次 Pod 启动时运行，因此会覆盖 master 上一次通过管理 API 写入的内容。
	// 这正是预期的数据流向：ConfigMap 代表期望状态，文件是 master 持有的副本，因此
	// 重启会将副本重置为 operator 期望的值，随后下一次协调过程会重新使账本收敛一致。
	quotaPolicySeedCommand = `if [ -f "$POLICY_SEED" ]; then ` +
		`cp "$POLICY_SEED" "$POLICY_FILE"; else ` +
		`printf '%s' "$POLICY_EMPTY" > "$POLICY_FILE"; fi`
)

// 两个参数 leader.memberAddressing 的取值。空值会触发 Service 的显示，这是通过审核的对象的默认模式。
const (
	MemberAddressingLease = "Lease"
)

// LeaderObjectNameSuffix 是 LeaderObjectName 追加的后缀。将其导出是因为
// 有一个使用该名称的对象由存储本身（而非本 operator）创建——即用于选举的 Lease——
// 因此从该对象反推后端的唯一方式就是剥离此后缀；若出现第二处字面量拼写，反向解析
// 就会与正向命名不再一致。
const LeaderObjectNameSuffix = "-leader"

// LeaderObjectName 返回为后端的 leader 渲染的所有对象的名称。
//
// 后端是集群作用域的，而其下属对象不是，因此名称必须在落入同一个共享 Namespace 后
// 依然可区分：它由后端自身名称加上角色后缀组成，既能隔离不同后端，又能将同一后端
// 自身的对象归集在一起。
func LeaderObjectName(kvcb *workercore.KVCacheBackend) string {
	return kvcb.Name + LeaderObjectNameSuffix
}

// LeaderServiceHost 返回集群内用来访问 leader 的 Service DNS 名称。
func LeaderServiceHost(kvcb *workercore.KVCacheBackend) string {
	return fmt.Sprintf("%s.%s.svc", LeaderObjectName(kvcb), kuberess.SystemNamespaceName)
}

// LeaderEndpoints 是 status.endpoints 为托管后端发布的内容：同一个主机使用两个端口，每个端口的命名规则取决
// 于由谁进行读取。
func LeaderEndpoints(kvcb *workercore.KVCacheBackend) []workercore.KVCacheBackendEndpoint {
	host := LeaderServiceHost(kvcb)
	return []workercore.KVCacheBackendEndpoint{
		{
			Name:    workercore.KVCacheBackendEndpointNameClient,
			Address: fmt.Sprintf("%s:%d", host, LeaderRPCPort),
		},
		{
			Name:    workercore.KVCacheBackendEndpointNameAdmin,
			Address: fmt.Sprintf("%s:%d", host, LeaderMetricsPort),
		},
	}
}

// LeaderReplicas 返回后端期望运行的 leader 进程数。
//
// 要求：此限制机制是“双脑模式”，而非防御性整洁性。该模式的 `maximum=5` 限制了数值，但无法表达跨领域配对情况；
// 且该模式是文档中规定的权威规则，正好在 Webhook 未安装的位置——因此该集群允许 `replicas: 3` 且
// `electionBackend: None`，三个主节点在彼此之间无选举行为，各自服务于一个池子。只有一个进程可以安全读取该对
// 象，与 spec.replicas 的偏差在 `kubectl get deploy` 中可见，而非日志行中。
//
// 副本数量所决定的所有其他事项——更新策略、滚动更新截止日期——都是从这里读取的，而不是从字段中读取的，因此三
// 人对于是否存在选举这一问题不会产生分歧。
func LeaderReplicas(leader workercore.KVCacheBackendLeader) int32 {
	if !leaderNeedsAPIAccess(leader) {
		return 1
	}

	if leader.Replicas != nil {
		return *leader.Replicas
	}

	return 1
}

// leaderElectionFlag 是启动选举的标识。其命名源于 argv 也是可读取的：即关于实时模板是否进行选举的回答，是从
// 渲染器写入该标识中的内容中获得的。
const leaderElectionFlag = "-enable_ha=true"

// LeaderTemplateElects 判断 leader Pod 模板是否启用了选主。
//
// 它读取 argv，而不是账户或令牌挂载，因为标志决定了进程的行为模式；另外两种方式则仅能实现这一功能。准入将标
// 志作为通过参数处理，因此渲染器是唯一的写入者。
func LeaderTemplateElects(template core.PodTemplateSpec) bool {
	for _, c := range template.Spec.Containers {
		if slices.Contains(c.Args, leaderElectionFlag) {
			return true
		}
	}
	return false
}

// LeaderDeploymentAtOneReplica 是一个已渲染的主节点 Deployment，包含单个副本的数量、策略及截止时间，模板也
// 保持已渲染状态。
//
// 未选举的单副本 Deployment 扩到多副本时，先保持一个副本并使用 Recreate 切换到选举模板，待旧进程退出且新
// ReplicaSet 成为唯一活动组后，再扩容备用副本。若同时修改模板和副本数，Deployment 控制器会先扩容旧
// ReplicaSet，使多个未参与选举的 master 同时运行。
func LeaderDeploymentAtOneReplica(deploy *apps.Deployment) *apps.Deployment {
	held := deploy.DeepCopy()
	held.Spec.Replicas = ptr.To[int32](1)
	held.Spec.Strategy = leaderUpdateStrategy(1)
	held.Spec.ProgressDeadlineSeconds = leaderProgressDeadlineSeconds(1)
	return held
}

// leaderUpdateStrategy 决定了主节点的 Deployment 如何更新，这两种方案是相反而非变体。
//
// SINGLE REPLICA：重新创建。RollingUpdate 的 maxSurge 默认值为 25%，四舍五入后等于 1。如果禁用选举机制，旧
// 主节点和新主节点将同时运行。如果启用选举机制，新的备用节点在旧主节点释放 Lease 之前无法准备就绪，因此默认
// 的 maxUnavailable 为 0，可能导致部署延迟。重新创建会首先停止旧进程。
//
// 多个复制品：RollingUpdate，且两个参数均被反转。
//   - maxSurge 可以超过零，因为租约允许一个主节点下运行多个进程。此处的分裂现象导致的激增是安全的。
//   - maxUnavailable 是副本数，而非 replicas-1，这种差异会导致永久死锁，而非缓慢的部署过程。控制器仅在
//     `availablePodCount > replicas - maxUnavailable` 时才会缩减旧的 ReplicaSet，因为此处始终只有 1 个副本
//     可用，因为待机节点故意未准备就绪，因此 replicas-1 会导致 `1 > 1`，旧主节点也永远不会被移除——而新副本
//     在释放 Lease 之前无法准备就绪。只有当 maxUnavailable 等于 replicas 时，才会降至零，从而让旧主节点消失。
//
// 这种激增现象在那种设置中依然具有其地位：新的复制品已经启动，并在旧主节点被移除之前争夺 Lease 资源，因此这
// 种状态属于选举过程，而非 Pod 的启动。
func leaderUpdateStrategy(replicas int32) apps.DeploymentStrategy {
	if replicas <= 1 {
		return apps.DeploymentStrategy{Type: apps.RecreateDeploymentStrategyType}
	}

	return apps.DeploymentStrategy{
		Type: apps.RollingUpdateDeploymentStrategyType,
		RollingUpdate: &apps.RollingUpdateDeployment{
			MaxSurge:       ptr.To(intstr.FromInt32(1)),
			MaxUnavailable: ptr.To(intstr.FromInt32(replicas)),
		},
	}
}

// leaderProgressDeadlineSeconds 在存在备用 leader 时禁用滚动更新超时，其他情况沿用 API Server 的默认期限。
//
// 该超时机制无法针对此工作负载进行区分。DeploymentComplete 要求 availableReplicas 等于 spec.replicas，但在
// 仅有一个主节点准备就绪的情况下，这一条件始终不成立，因此滚动更新完成后，Progressing 条件将停止推进——这就
// 像镜像无法拉取时的情况一样。这两种情况都会表现为过时的时间戳，而两个答案均相同的检查并不属于有效的检查。
//
// 要求：移除此项的人必须同时带上替代项。用于区分的谓词为 DeploymentComplete，且不包含 availableReplicas 子
// 句：在此处，updatedReplicas 和 replicas 都会正常达到 spec.replicas 的数量。
func leaderProgressDeadlineSeconds(replicas int32) *int32 {
	if replicas <= 1 {
		return nil
	}

	return ptr.To(int32(math.MaxInt32))
}

// leaderEnv 是渲染后的 argv 所引用的所有值。如果在此处定义的变量 argv 无法读取，那将毫无意义；如果有一个标
// 志在这里未定义，它就会以字面形式“$(NAME)”的形式传递给进程。
//
// Pod 的名称、命名空间和 IP 是相关的，因为指向它们的标志仅在高可用性模式下才会被显示——关于地址的决定机制，
// 请参阅 leader_flags.go 中的选举组相关代码；关于为何其他两个标志不是无条件存在的，可参考该文件中的 Pod 身份标志。
func leaderEnv(kvcb *workercore.KVCacheBackend) []core.EnvVar {
	var env []core.EnvVar

	if leaderNeedsAPIAccess(kvcb.Spec.Connection.Managed.Leader) {
		env = append(env,
			core.EnvVar{
				Name: LeaderPodNameEnv,
				ValueFrom: &core.EnvVarSource{
					FieldRef: &core.ObjectFieldSelector{FieldPath: "metadata.name"},
				},
			},
			core.EnvVar{
				Name: LeaderPodNamespaceEnv,
				ValueFrom: &core.EnvVarSource{
					FieldRef: &core.ObjectFieldSelector{FieldPath: "metadata.namespace"},
				},
			},
			core.EnvVar{
				Name: LeaderPodIPEnv,
				ValueFrom: &core.EnvVarSource{
					FieldRef: &core.ObjectFieldSelector{FieldPath: "status.podIP"},
				},
			})
	}

	// 最后进行准入流程，按照所写的顺序进行，与成员端保持一致：准入拒绝某个名称，任何上述行都已经生效，
	// 这就能避免一个变量同时持有两个值，从而让获胜者能够在运行时发挥作用。
	for _, e := range kvcb.Spec.Connection.Managed.Leader.ExtraEnv {
		env = append(env, core.EnvVar{Name: e.Name, Value: e.Value})
	}

	return env
}

// leaderSelectorLabels 是 Deployment 和 Service 共同用于选择 leader Pod 的标签。
//
// Deployment 的 spec.selector 是不可变的。包含不同值的更新会被拒绝，此时需要手动删除该对象。因此，这种更新
// 仅包含身份信息——即后端、角色等信息，而不会包含 spec 更新所能涉及的任何内容：无论是镜像、传输方式，还是标志都不包含。
func leaderSelectorLabels(kvcb *workercore.KVCacheBackend) map[string]string {
	return map[string]string{
		labelKeyName:      labelValueName,
		labelKeyInstance:  kvcb.Name,
		labelKeyComponent: labelValueLeader,
	}
}

// LeaderPodBackendName 返回 Pod 所属的后端名称；对于非 leader 的 Pod 则返回 ""。
//
// 它读取身份标签而非资源记录，因为 leader 的 Pod 不携带任何资源记录——参见
// 上方的标签常量。这些标签正是 Deployment 自身不可变的选择器，因此按其匹配的
// watch 不会与渲染器写入的内容产生偏离。
func LeaderPodBackendName(labels map[string]string) string {
	if labels[labelKeyName] != labelValueName || labels[labelKeyComponent] != labelValueLeader {
		return ""
	}
	return labels[labelKeyInstance]
}

// leaderPodLabels 在选择器上补充所有受管对象共有的 part-of 标签。该标签仅用于展示，避免把它加入创建后不能修改的选择器。
func leaderPodLabels(kvcb *workercore.KVCacheBackend) map[string]string {
	labels := leaderSelectorLabels(kvcb)
	labels[labelKeyPartOf] = labelValuePartOf
	return labels
}

// RenderLeaderDeployment 根据后端声明和选定镜像生成 leader 的 Deployment。
//
// 由控制器先在 spec.image 和集群 Setting 之间解析镜像，再作为参数传入。这样渲染函数无需读取集群，也可单独验
// 证镜像回退行为。
//
// 相同输入须产生相同模板。否则每轮调谐都会重写 Deployment 并使 leader 重启。
func RenderLeaderDeployment(kvcb *workercore.KVCacheBackend, image string) *apps.Deployment {
	leader := kvcb.Spec.Connection.Managed.Leader

	deploy := &apps.Deployment{
		ObjectMeta: meta.ObjectMeta{
			Name:      LeaderObjectName(kvcb),
			Namespace: kuberess.SystemNamespaceName,
			Labels:    leaderPodLabels(kvcb),
		},
		Spec: apps.DeploymentSpec{
			Replicas:                ptr.To(LeaderReplicas(leader)),
			Strategy:                leaderUpdateStrategy(LeaderReplicas(leader)),
			ProgressDeadlineSeconds: leaderProgressDeadlineSeconds(LeaderReplicas(leader)),
			Selector:                &meta.LabelSelector{MatchLabels: leaderSelectorLabels(kvcb)},
			Template: core.PodTemplateSpec{
				ObjectMeta: meta.ObjectMeta{Labels: leaderPodLabels(kvcb)},
				Spec: core.PodSpec{
					// 关闭 HA 时，Mooncake leader 不访问 API Server，显式关闭自动挂载
					// ServiceAccount 令牌。若省略该字段，API Server 会默认启用它，控制
					// 器便无法收敛到期望值。
					//
					// 启用 HA 后，leader 通过 Lease 选主并从挂载的令牌读取身份。镜像中
					// 没有可供回退的 kubeconfig，所以要同时设置 ServiceAccount 和令牌挂
					// 载，否则进程每秒重试却一直无法就绪。
					AutomountServiceAccountToken: ptr.To(leaderNeedsAPIAccess(leader)),
					ServiceAccountName:           leaderServiceAccountName(kvcb),
					ImagePullSecrets:             kvcb.Spec.ImagePullSecrets,
					Containers: []core.Container{
						leaderContainerSpec(kvcb, image, kvcache.EffectivePullPolicy(kvcb, image)),
					},
				},
			},
		},
	}

	podSpec := &deploy.Spec.Template.Spec
	if leader.MultiTenancyEnabled() {
		podSpec.Volumes = append(podSpec.Volumes, quotaPolicyVolumes(kvcb)...)
		podSpec.InitContainers = []core.Container{
			quotaPolicySeedContainer(image, kvcache.EffectivePullPolicy(kvcb, image)),
		}
	}

	systemmeta.NoteResource(deploy, kvcache.ResourceType, map[string]string{
		kvcache.ResourceNoteBackend: kvcb.Name,
		"role":                      leaderResourceNoteRole,
	})
	kubemeta.ControlOnWithoutBlock(deploy, kvcb,
		workercore.SchemeGroupVersion.WithKind("KVCacheBackend"))

	return deploy
}

// leaderContainerSpec 构造 leader Deployment 实际运行的容器配置。
//
// 它不要求的是承载功能：没有 hostNetwork，没有设备，没有权限，也没有超出已声明功能范围的卷，即租户配额政策
// 的可写入副本。主节点是一个不持有缓存字节的元数据服务——成员方是需要主机的一方——因此这里的任何内容都是无人要求的权限。
func leaderContainerSpec(
	kvcb *workercore.KVCacheBackend, image string, pullPolicy core.PullPolicy,
) core.Container {
	leader := kvcb.Spec.Connection.Managed.Leader

	var volumeMounts []core.VolumeMount
	if leader.MultiTenancyEnabled() {
		// 不是只读的，这正是其特点：Master 会在每次管理 API 更新时，将临时文件写入此目录，并根据策略将其重命名。
		//
		// leader 容器只读取可写策略文件。ConfigMap 种子卷仅由初始化容器使用，若连接器 URI 直接指向
		// 这个只读卷，启动检查可能通过，第一次写配额时才会失败。
		volumeMounts = append(volumeMounts, core.VolumeMount{
			Name: quotaPolicyVolumeName, MountPath: QuotaPolicyDir,
		})
	}

	return core.Container{
		Name:            "leader",
		Image:           image,
		ImagePullPolicy: pullPolicy,
		// 这个组件从不写入 /dev/termination-log，因此默认策略会将每次死亡事件显示为空字符串 "Error"。
		TerminationMessagePolicy: core.TerminationMessageFallbackToLogsOnError,
		// 入口点具有自定义名称，而非从镜像中继承而来。因此，在无需进入容器的情况下，通过
		// `kubectl get deploy -o yaml` 即可查看运行内容——这也是为什么标志位为 argv 而非环境变量。
		Command: []string{"mooncake_master"},
		// 将后端设置为其主节点进程运行的 argv 环境
		Args:         RenderLeaderFlags(kvcb),
		VolumeMounts: volumeMounts,
		Ports: []core.ContainerPort{
			{Name: leaderRPCPortName, ContainerPort: LeaderRPCPort, Protocol: core.ProtocolTCP},
			{Name: leaderAdminPortName, ContainerPort: LeaderMetricsPort, Protocol: core.ProtocolTCP},
		},
		// 渲染后的 argv 对应每一个实例。若缺少其中一个，标志值会作为字面量“$(KUBERNETES_POD_NAME)”
		// 传递给进程，而主节点会将其作为自己的名称。
		Env: leaderEnv(kvcb),
		ReadinessProbe: &core.Probe{
			ProbeHandler: core.ProbeHandler{
				HTTPGet: &core.HTTPGetAction{
					Path: leaderReadinessPath,
					Port: intstr.FromInt32(LeaderMetricsPort),
				},
			},
			PeriodSeconds:    5,
			FailureThreshold: 3,
		},
		LivenessProbe: &core.Probe{
			ProbeHandler: core.ProbeHandler{
				HTTPGet: &core.HTTPGetAction{
					Path: leaderLivenessPath,
					Port: intstr.FromInt32(LeaderMetricsPort),
				},
			},
			PeriodSeconds: 10,
			// 故意将性能设置高于就绪状态。拒绝处理不提供推理服务的主节点流量毫无成本；杀死那些
			// 仅启动缓慢的节点则会导致重启，并且所有挂载在该节点上的内存段也会受到影响。
			FailureThreshold: 6,
		},
	}
}

// quotaPolicyVolumes 是租户配额策略所需的配对对象：一个由 Master 运行的可写入卷，另一个则是其初始加载的只读卷。
//
// ConfigMap 是可选的，因为这两个对象由不同的作者创建——资源池控制器会生成它，而那个尚未绑定资源池的多租户后
// 端则没有人负责生成它。如果必须使用 ConfigMap，会导致该后端的 Pod 处于等待状态，因为根本无法创建任何内容；
// 如果选择使用 ConfigMap 作为可选选项，它只会挂载为一个空目录，此时种子命令会回退到默认行为。
func quotaPolicyVolumes(kvcb *workercore.KVCacheBackend) []core.Volume {
	return []core.Volume{
		{
			Name:         quotaPolicyVolumeName,
			VolumeSource: core.VolumeSource{EmptyDir: &core.EmptyDirVolumeSource{}},
		},
		{
			Name: quotaPolicySeedVolumeName,
			VolumeSource: core.VolumeSource{
				ConfigMap: &core.ConfigMapVolumeSource{
					LocalObjectReference: core.LocalObjectReference{
						Name: QuotaPolicyObjectName(kvcb),
					},
					Optional: ptr.To(true),
					// 虽然这是 API 服务器默认设置的内容，但被明确写出，因为对齐器将此列
					// 表视为一个整体进行比较。留给服务器填充的字段与渲染结果存在差异，
					// 因此每次比较后都会进行更新——在 Deployment 上的每次重新同步操作都
					// 是一次写入操作，无需更改。
					DefaultMode: ptr.To[int32](0o644),
				},
			},
		},
	}
}

// quotaPolicySeedContainer 会在 Master 寻找政策文档之前，将政策文档放入可写卷中。
//
// 它运行的是主节点的自身镜像，而非复制工具，因此隔离安装时不会引入任何额外内容，且 pull 策略问题也有一个答
// 案，而不是两个。该镜像能够运行的内容取决于镜像本身，而非此生成过程，并且在端到端场景中针对真实镜像进行了验证。
func quotaPolicySeedContainer(image string, pullPolicy core.PullPolicy) core.Container {
	// 错误被丢弃，而非尝试解决：RenderQuotaPolicy 仅在它拒绝的租户上会失败，而空集合则没有此类问题。如
	// 果某个渲染器在此处出现错误，那么从底层到控制器整个流程都必须是有缺陷的，因为这种情况根本不可能发生。
	emptyPolicy, _ := RenderQuotaPolicy(nil)

	return core.Container{
		Name:                     quotaPolicyInitName,
		Image:                    image,
		ImagePullPolicy:          pullPolicy,
		TerminationMessagePolicy: core.TerminationMessageFallbackToLogsOnError,
		Command:                  []string{"sh", "-c", quotaPolicySeedCommand},
		Env: []core.EnvVar{
			{Name: quotaPolicySeedEnv, Value: QuotaPolicySeedFilePath},
			{Name: quotaPolicyFileEnv, Value: QuotaPolicyFilePath},
			{Name: quotaPolicyEmptyEnv, Value: string(emptyPolicy)},
		},
		VolumeMounts: []core.VolumeMount{
			{Name: quotaPolicyVolumeName, MountPath: QuotaPolicyDir},
			{Name: quotaPolicySeedVolumeName, MountPath: QuotaPolicySeedDir, ReadOnly: true},
		},
	}
}

// RenderLeaderService 为 leader 生成供客户端和管理端访问的 ClusterIP Service。
//
// Service 对外公布两个端口。引擎连接 RPC 端口，控制器从 Admin 端口读取健康、容量和内存段列表，两个消费者都需有对应地址。
func RenderLeaderService(kvcb *workercore.KVCacheBackend) *core.Service {
	svc := &core.Service{
		ObjectMeta: meta.ObjectMeta{
			Name:      LeaderObjectName(kvcb),
			Namespace: kuberess.SystemNamespaceName,
			Labels:    leaderPodLabels(kvcb),
		},
		Spec: core.ServiceSpec{
			// 仅可从集群内部访问。此范围内的任何组件都不会向外部发布 KV 缓存，而后端是一种具有
			// 特权的物理资源。
			Type:     core.ServiceTypeClusterIP,
			Selector: leaderSelectorLabels(kvcb),
			Ports: []core.ServicePort{
				{
					Name:       leaderRPCPortName,
					Port:       LeaderRPCPort,
					TargetPort: intstr.FromString(leaderRPCPortName),
					Protocol:   core.ProtocolTCP,
				},
				{
					Name:       leaderAdminPortName,
					Port:       LeaderMetricsPort,
					TargetPort: intstr.FromString(leaderAdminPortName),
					Protocol:   core.ProtocolTCP,
				},
			},
		},
	}

	systemmeta.NoteResource(svc, kvcache.ResourceType, map[string]string{
		kvcache.ResourceNoteBackend: kvcb.Name,
		"role":                      leaderResourceNoteRole,
	})
	kubemeta.ControlOnWithoutBlock(svc, kvcb,
		workercore.SchemeGroupVersion.WithKind("KVCacheBackend"))

	return svc
}
