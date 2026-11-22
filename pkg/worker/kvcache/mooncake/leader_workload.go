package mooncake

import (
	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
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
