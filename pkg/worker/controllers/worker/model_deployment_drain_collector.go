package worker

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
)

// 此处采用 exec 而非 scrape 作为传输方式，因为这是针对单个成员的查询，只有该成员自身能够回答。
// scrape 会经由路由器使用的同一 Service 抵达，而 Service 会在各成员间做负载均衡，因此无法判断
// 究竟哪一个成员处于空闲。exec 则直接运行在目标容器内部，访问该容器自身的回环地址，其响应明确
// 来自该成员本身而非其他成员。
//
// 它是 preStop hook 的只读孪生实现，且有意对同一组 gauge 做第二次独立读取，而非复用 hook：
// hook 在删除触发后按 kubelet 的宽限期时钟运行，此时再做决策已为时过晚；且 hook 始终以零状态退出，
// 其结果刻意设计为不可上报。而本次读取的结果会直接用于决策，因此它自带响应封装与完整性校验，
// 解析失败也必须显式暴露而非被静默吞掉。
const (
	// modelDeploymentDrainHTTPTimeoutSeconds 限制引擎自身响应的超时时间。preStop hook 在 kubelet 的
	// 时钟下只允许 1 秒；控制器侧的读取不受该时钟限制，因此可以多留出 1 秒，同时仍能在单次 reconcile
	// 周期内返回。
	modelDeploymentDrainHTTPTimeoutSeconds = 2

	// modelDeploymentDrainExecTimeoutSeconds 限制包含建立连接在内的整次读取的超时时间。对单个成员执行
	// 两次读取最多耗时该值的两倍，且两次失败均视为保持等待（hold），因此该轮检查会及时返回而不会阻塞。
	modelDeploymentDrainExecTimeoutSeconds = 5

	// modelDeploymentDrainMaxBodyBytes 限制单次读取可拉取的最大响应体字节数。即使引擎的指标端点行为异常，
	// 其影响也会被约束在此上限内，而不取决于它返回多大的内容。
	modelDeploymentDrainMaxBodyBytes = 1 << 20
)

// modelDeploymentDrainIndexPodUID 按 Pod 自身的 UID 建立索引。
//
// 查找目标是 UID 而非名称，因为在此场景下名称不能作为唯一身份标识。副本创建时仅设置了
// GenerateName（见 model_deployment_render.go），其名称由 API server 自动生成；若一个 Pod 被删除并
// 以相同名称重建，按名称查询就会命中一个从未受当前预留约束的新成员。UID 是唯一不可复用的句柄。
const modelDeploymentDrainIndexPodUID = "gpustack.ai/model-deployment-drain-pod-uid"

// modelDeploymentDrainExec 在指定容器内执行一条命令并返回其标准输出。
//
// 将其定义为函数而非由该读取器直接发起网络连接，其原因与通过接口隔离（seam）相同：需要确保
// 正确处理的结果都是各种故障情形，而真实的 SPDY 流无法按需模拟出 exec 被拒绝、响应体被截断或
// 容器在读取中途变更身份等情况。
type modelDeploymentDrainExec func(
	ctx context.Context, pod *corev1.Pod, container string, argv []string,
) (string, error)

// modelDeploymentDrainCollector 是生产环境使用的读取器：它定位目标成员的 Pod，校验该成员是否
// 允许被当前协议度量，并读取其所在引擎自身的 gauge 指标。
//
// 它在两次读取之间不保留任何状态。身份连续性直接通过实时查询 Pod 来证明，而非依赖缓存的采样值，
// 因此控制器重启不会丢失状态，也不存在可能出错的互斥锁。这一选择的代价会在下方的重启检查处说明。
type modelDeploymentDrainCollector struct {
	// client 用于定位目标 Pod。其字段索引仅作为定位器而非权威数据源：它只回答哪个命名空间和名称
	// 对应目标成员的 UID，而其返回的对象是控制器缓存最后一次看到的快照，该快照可能早于一次重启、
	// 替换或重新打标。
	client ctrlcli.Client

	// fresh 是用于校验定位器结果的权威数据源。此次读取所依赖的每一项事实都取自实时查询而非缓存，
	// 且会查询两次——exec 前一次、exec 后一次——因为即使 Pod 本身未变，容器也可能在流打开期间
	// 被替换。若 fresh reader 为 nil，则直接保持等待（hold）而非回退到缓存：无法证明自身正在度量谁的
	// 读取等同于尚未完成度量。
	fresh ctrlcli.Reader

	// restConfig 与 core 用于承载 exec 执行。二者分开传入，因为 REST client 负责构造子资源 URL，
	// 而 executor 负责将其升级为流。
	// core 使用类型化的核心客户端而非完整的 clientset，因为该读取器唯一访问的子资源是 pods/exec。
	// 仅为一个动词引入完整的 clientset 会增加本无需由 operator 承担的依赖面。
	core       corev1client.CoreV1Interface
	restConfig *rest.Config

	exec modelDeploymentDrainExec
}

// newModelDeploymentDrainCollector 构造生产环境读取器。当 exec 为 nil 时会安装真实的实现，
// 从而使测试可以注入自己的模拟实现，同时生产调用方也不会意外得到 nil。
func newModelDeploymentDrainCollector(
	client ctrlcli.Client, fresh ctrlcli.Reader, restConfig *rest.Config,
	core corev1client.CoreV1Interface,
) *modelDeploymentDrainCollector {
	collector := &modelDeploymentDrainCollector{
		client:     client,
		fresh:      fresh,
		restConfig: restConfig,
		core:       core,
	}

	collector.exec = collector.execInto

	return collector
}

// Drain 报告单个成员当前仍有在途的工作量。
//
// 检查的顺序就是各项条件必须得到证明的顺序，且任一检查失败都会返回 hold 而非直接给出结论：
// 目标确实是该成员、该成员总体上可被度量、该成员属于本协议可操作的对象、其运行的引擎拥有 gauge 列表、
// exec 前后为同一容器实例——只有全部满足，才会读取并采信返回的数值。
//
// 并非所有失败都是 error。无法给出结果的传输层异常会返回一个携带故障形态的响应，因为协议的
// reason 字符串是告知运维人员副本为何被挂起保留的依据。若改为直接返回 error，所有情况都会坍缩为
// 一行错误信息，导致无法解释为何继续保留。
func (c *modelDeploymentDrainCollector) Drain(
	ctx context.Context, target modelDeploymentDrainTarget,
) (modelDeploymentDrainAnswer, error) {
	if target.PodUID == "" {
		// 空 UID 不会匹配任何成员；更糟的是，如果查询被写成过滤而非精确相等，它甚至可能匹配所有成员。
		// 因此此处采用 fail-closed 策略直接拒绝。
		return modelDeploymentDrainHold(modelDeploymentDrainUnknown,
			"the drain target names no member"), nil
	}

	if target.Container == "" {
		return modelDeploymentDrainHold(modelDeploymentDrainUnknown,
			"the drain target names no container to read"), nil
	}

	expected, known := modelDeploymentInFlightMetrics[target.Engine]
	if !known {
		// 与协议自身做出的拒绝保持一致，原因也相同：没有 gauge 列表的引擎就无法进行度量，
		// 若在此处自行编造列表，就违背了设置 Unsupported 状态来拒绝未知引擎的初衷。
		return modelDeploymentDrainHold(modelDeploymentDrainUnsupported,
			"engine "+target.Engine+" has no in-flight gauge list"), nil
	}

	pod, hold := c.resolveMember(ctx, target, "resolved")
	if hold != nil {
		return *hold, nil
	}

	container, found := modelDeploymentDrainContainerOf(pod, target.Container)
	if !found {
		// 该扩展点直接携带了容器名而非现场查找，但携带的名称本身仍只是一份声明：Pod 才是其拥有哪些
		// 容器的权威依据。与 Pod 实际记录相矛盾的容器名不允许进入读取。
		return modelDeploymentDrainHold(modelDeploymentDrainUnknown,
			fmt.Sprintf("member %s has no container %q", pod.Name, target.Container)), nil
	}

	if reason, unsupported := modelDeploymentDrainParallelismUnreadable(target.Engine,
		modelDeploymentRoleKindFromUpstream(pod.Labels[modelDeploymentLabelKeyRoleKind]),
		container); unsupported {
		return modelDeploymentDrainHold(modelDeploymentDrainUnsupported, reason), nil
	}

	if drift := modelDeploymentDrainExpectedIdentity(pod, target, target.Container); drift != "" {
		return modelDeploymentDrainHold(modelDeploymentDrainUnknown, drift), nil
	}

	// 所属的部署对象必须实时重新读取，而不能轻信 Pod 的 owner reference。
	// owner reference 是创建 Pod 时写入的声明；若对象被删除后以相同名称重建，所有引用仍会指向该名称，
	// 而其背后的 UID 已经改变——缓存的身份信息就会误认为当前操作仍持有该成员。该检查位于前述
	// 捕获身份检查之后，从而使根本未指定部署对象的目标直接因该原因被拒绝，而非因一次它从未请求的
	// 读取而报错。
	if drift := c.verifyModelDeploymentIdentity(ctx, pod, target); drift != "" {
		return modelDeploymentDrainHold(modelDeploymentDrainUnknown, drift), nil
	}

	// 第一次实时读取保留完整 Pod 对象，而非仅提取容器状态，因为 exec 后的比对针对的是成员当时的
	// 整体状态，而非其中某一个字段。
	beforePod := pod
	before, ok := modelDeploymentDrainContainerStatus(beforePod, target.Container)
	if !ok {
		return modelDeploymentDrainHold(modelDeploymentDrainUnknown,
			fmt.Sprintf("member %s reports no status for container %q", pod.Name, target.Container)), nil
	}

	// 容器重启并不意味着无限等待，也并非没有代价。当前提供指标的引擎在协议开始观察时还不存在，
	// 因此它的数据无法代表协议原本要核算的在途工作。挂起保留受排空（drain）预算限制，超时后会将
	// 此次下线标记为 Aborted 并保留该成员而非删除，从而使持续崩溃的成员既不会被误删，也不会永久
	// 阻塞队列。
	if before.RestartCount > 0 {
		return modelDeploymentDrainHold(modelDeploymentDrainUnknown,
			fmt.Sprintf("member %s container %q has restarted %d time(s) since it was created",
				pod.Name, target.Container, before.RestartCount)), nil
	}

	url, ok := modelDeploymentDrainURL(pod)
	if !ok {
		return modelDeploymentDrainHold(modelDeploymentDrainUnknown,
			fmt.Sprintf("member %s carries no usable metrics endpoint annotation", pod.Name)), nil
	}

	body, err := c.read(ctx, pod, target.Container, url, expected)
	if err != nil {
		return modelDeploymentDrainHold(modelDeploymentDrainUnknown, err.Error()), nil
	}

	// 后置检查才是让这次读取真正对应当前成员的关键。在定位与 exec 之间，容器可能在稳定不变的 Pod
	// 内部被替换，因此必须重新读取并比对身份，而不能假设其一直未变。
	//
	// 此处不再比对成员自身的 UID，因为重新定位本来就是通过基于各 Pod 自身 UID 构建的索引来查找的：
	// 若成员已被删除并重建，新成员会带有新的 UID，查找结果将为空，从而读取早已被判定为未观测到。
	// 在此处再做一次不可能失败的比对，只会营造出一种并不存在的保护假象。
	after, hold := c.resolveMember(ctx, target, "re-resolved after the read")
	if hold != nil {
		return *hold, nil
	}
	afterStatus, ok := modelDeploymentDrainContainerStatus(after, target.Container)
	if !ok {
		return modelDeploymentDrainHold(modelDeploymentDrainUnknown,
			fmt.Sprintf("member %s reported no status for container %q after the read",
				after.Name, target.Container)), nil
	}
	if afterStatus.ContainerID != before.ContainerID {
		return modelDeploymentDrainHold(modelDeploymentDrainUnknown,
			fmt.Sprintf("container %q was replaced while it was being read", target.Container)), nil
	}

	// 在读取期间停止运行的容器回答的是另一个问题。指标来自一个随后已被 kubelet 观察到退出的进程，
	// 因此响应体是否格式完整已无关紧要：该成员已不再有正在运行的引擎可供度量。exec 前也施加了
	// 同样要求，从而在读取前后形成闭环校验。
	if running, why := modelDeploymentDrainContainerRunning(after, target.Container); !running {
		return modelDeploymentDrainHold(modelDeploymentDrainUnknown, why), nil
	}

	// 前后两次读取必须指向同一个成员，而容器 ID 只是其中最小的一部分。此次读取所依赖的其他所有
	// 状态也一并在此比对，因为其中任何一项变化都意味着被读取的成员已不再是预留所绑定的成员：
	// 属主变更说明 Pod 已被重新接管，角色变更说明成员被重新安置或重新打标，地址变更说明成员已迁移，
	// 指标端点变更则说明正在读取的是另一个进程。主语在读取期间发生变化的读数，无论数字是多少，
	// 都不能用来证明协议所核算的工作量。
	if drift := modelDeploymentDrainIdentityDrift(beforePod, after, target.Container); drift != "" {
		return modelDeploymentDrainHold(modelDeploymentDrainUnknown, drift), nil
	}

	// 身份信息不仅在 exec 前校验，在 exec 后也会再次读取。前文的漂移检查能指明具体哪一项发生了变化，
	// 在触发时是更清晰的答案，而此处则是其兜底保障：若流打开期间 Pod 的控制器被重新指向，或所属部署
	// 被删除并以同名重建，前文比对的所有容器级身份信息都可能保持不变，但其底层已变成另一个成员。
	// 此时指标所反映的将是一个当前操作不再持有的成员，因此必须在实时对象上再次核验捕获的身份。
	if drift := modelDeploymentDrainExpectedIdentity(after, target, target.Container); drift != "" {
		return modelDeploymentDrainHold(modelDeploymentDrainUnknown, fmt.Sprintf(
			"member is no longer the member this read began on: %s", drift,
		)), nil
	}
	if drift := c.verifyModelDeploymentIdentity(ctx, after, target); drift != "" {
		return modelDeploymentDrainHold(modelDeploymentDrainUnknown, fmt.Sprintf(
			"the operation's deployment changed while it was being read: %s", drift,
		)), nil
	}

	return modelDeploymentDrainAnswerFromBody(body, expected), nil
}

// modelDeploymentDrainIdentityDrift 返回两次实时读取之间成员发生变化的项，若读取所依赖的一切
// 均未改变，则返回空字符串。
//
// 角色不仅要与自身前后对比，还要与目标声明对比，因为从一个值变为另一个值固然是变化，
// 而与预留所指定角色不符的成员根本就不是本次操作绑定的对象。角色标签通过代码库统一的访问器读取，
// 而非在此硬编码键名，从而保证整个代码库对“该 Pod 属于哪个角色”只有唯一权威判断。
func modelDeploymentDrainIdentityDrift(
	before, after *corev1.Pod, container string,
) string {
	if before.UID != after.UID {
		return fmt.Sprintf("member %s was replaced while it was being read", after.Name)
	}

	if owner := modelDeploymentDrainControllerOf(before); owner != modelDeploymentDrainControllerOf(after) {
		return fmt.Sprintf("member %s changed controller while it was being read", after.Name)
	}

	if role := modelDeploymentPodRole(after); role != modelDeploymentPodRole(before) {
		return fmt.Sprintf("member %s changed role from %q to %q while it was being read",
			after.Name, modelDeploymentPodRole(before), role)
	}

	if before.Status.PodIP != after.Status.PodIP {
		return fmt.Sprintf("member %s changed address while it was being read", after.Name)
	}

	if before.Annotations["prometheus.io/port"] != after.Annotations["prometheus.io/port"] ||
		before.Annotations["prometheus.io/scheme"] != after.Annotations["prometheus.io/scheme"] ||
		before.Annotations["prometheus.io/path"] != after.Annotations["prometheus.io/path"] {
		return fmt.Sprintf("member %s changed its metrics endpoint while it was being read", after.Name)
	}

	beforeStatus, beforeFound := modelDeploymentDrainContainerStatus(before, container)
	afterStatus, afterFound := modelDeploymentDrainContainerStatus(after, container)
	if !beforeFound || !afterFound {
		return fmt.Sprintf("member %s reported no status for container %q across the read",
			after.Name, container)
	}

	// 重启计数发生变化，说明读取期间发生过重启，此时提供指标的进程在读取开始时并不存在。
	if beforeStatus.RestartCount != afterStatus.RestartCount {
		return fmt.Sprintf("container %q restarted while it was being read", container)
	}

	return ""
}

// modelDeploymentDrainControllerOf 返回 Pod 的控制属主，正是它将副本绑定到声明它的部署对象。
// modelDeploymentDrainControllerOf 返回作为身份标识的 Pod 控制属主。
//
// UID 是其中不可或缺的部分；若省略 UID，比对将在两个方面同时失效：属主被删除并以同名重建的
// Pod 会与自身判等，而没有任何属主的两个 Pod 也会彼此判等。名称在这里不能充当身份标识，原因
// 与成员名称不能充当身份标识完全相同。
func modelDeploymentDrainControllerOf(pod *corev1.Pod) string {
	owner := metav1.GetControllerOf(pod)
	if owner == nil {
		return ""
	}

	return owner.Kind + "/" + owner.Name + "/" + string(owner.UID)
}

// modelDeploymentDrainExpectedIdentity 校验实时 Pod 是否仍符合预留所绑定的成员身份。
//
// 每一个字段都必须检查，因为每一项都可能缺失。预期值为空且观测值也为空并不算达成一致，因此
// 未指定部署对象的目标，或没有控制器的实时 Pod，都会被直接拒绝而非放行。连自己正在度量哪个
// 部署都说不清的读取，度量的是一个毫无确定指向的对象。
func modelDeploymentDrainExpectedIdentity(
	pod *corev1.Pod, target modelDeploymentDrainTarget, container string,
) string {
	if target.DeploymentUID == "" {
		return "the read names no deployment to check the member against"
	}

	controller := metav1.GetControllerOf(pod)
	if controller == nil {
		return fmt.Sprintf("member %s has no controlling owner", pod.Name)
	}

	// 属主必须是 ModelDeployment 本身，因为这正是渲染逻辑写入的结构：普通引擎成员直接由其
	// ModelDeployment 控制（见 model_deployment_render.go），中间不存在 ReplicaSet 或工作负载 Deployment。
	// 若检查逻辑沿着 ReplicaSet 和 Deployment 向上追溯，就会拒绝所有正常成员，而拒绝它们的代价是
	// 没有任何成员能够被优雅排空。
	//
	// 匹配必须同时校验 Kind、API 版本和 UID，绝不能仅凭拼接字符串的后缀判断。后缀匹配会放行
	// 任何 UID 恰好相同的任意类型属主，从而把被无关同名对象接管的 Pod 当作当前操作的成员来读取。
	if controller.Kind != workercore.SchemeGroupVersionKind("ModelDeployment").Kind {
		return fmt.Sprintf("member %s is controlled by %q rather than a ModelDeployment",
			pod.Name, controller.Kind)
	}
	if controller.APIVersion != workercore.SchemeGroupVersion.String() {
		return fmt.Sprintf("member %s is controlled by a ModelDeployment of API version %q rather than %q",
			pod.Name, controller.APIVersion, workercore.SchemeGroupVersion.String())
	}
	if controller.UID != target.DeploymentUID {
		return fmt.Sprintf("member %s is owned by %s/%s rather than the operation's deployment %s",
			pod.Name, controller.Kind, controller.Name, target.DeploymentUID)
	}

	// 角色通过代码库统一的访问器读取，以确保对 Pod 属于哪个角色只有唯一判断口径，避免两处实现
	// 之间产生漂移。
	if role := modelDeploymentPodRole(pod); role != target.Role {
		return fmt.Sprintf("member %s is role %q rather than %q", pod.Name, role, target.Role)
	}

	status, found := modelDeploymentDrainContainerStatus(pod, container)
	if !found {
		return fmt.Sprintf("member %s reports no status for container %q", pod.Name, container)
	}
	// 空的容器身份并不构成有效匹配。这说明 kubelet 尚未上报该容器的标识，而两个空标识彼此相等
	// 却没有任何实质意义。
	if status.ContainerID == "" {
		return fmt.Sprintf("member %s reports no container identity for %q", pod.Name, container)
	}
	// 容器必须处于运行中（running），而不仅仅是存在。已终止或等待中的容器没有引擎在提供指标端点，
	// 从中读取到的响应要么来自不属于该成员的进程，要么来自某个先前进程遗留的陈旧套接字。
	if running, why := modelDeploymentDrainContainerRunning(pod, container); !running {
		return why
	}

	return ""
}

// modelDeploymentDrainContainerRunning 报告指定容器是否正在运行；若未运行，则返回原因。
//
// 运行状态必须直接读取而非推断。State 为 nil 的容器状态表示 kubelet 尚未上报任何相关信息，
// 它既不处于运行中也不处于停止状态；若将其视作正在运行，就会让从未启动过的成员错误地确认
// 读数为零。
func modelDeploymentDrainContainerRunning(pod *corev1.Pod, name string) (bool, string) {
	status, found := modelDeploymentDrainContainerStatus(pod, name)
	if !found {
		return false, fmt.Sprintf("member %s reports no status for container %q", pod.Name, name)
	}
	if status.State.Running == nil {
		return false, fmt.Sprintf(
			"member %s reports container %q in state %q rather than running",
			pod.Name, name, status.State,
		)
	}

	return true, ""
}

// verifyModelDeploymentIdentity 通过实时读取器重新读取成员所指明的部署，并拒绝除预留阶段所捕获对象以外的任何对象。
//
// Pod 的 OwnerReference 并非凭证。它只是创建 Pod 时写入的声明；如果某部署被删除并以相同名称重建，所有引用仍会解析到该名称，但其背后的 UID 已属于另一个对象。只有对对象本身执行实时读取并按 UID 比对，才能确定当前操作是否仍持有其成员所属的那个部署。
func (c *modelDeploymentDrainCollector) verifyModelDeploymentIdentity(
	ctx context.Context, pod *corev1.Pod, target modelDeploymentDrainTarget,
) string {
	controller := metav1.GetControllerOf(pod)
	if controller == nil {
		return fmt.Sprintf(
			"member %s has no controlling owner to read the operation's deployment from", pod.Name,
		)
	}

	if c.fresh == nil {
		return "no live reader is configured to confirm the operation's deployment"
	}

	live := new(workercore.ModelDeployment)
	if err := c.fresh.Get(ctx,
		ctrlcli.ObjectKey{Namespace: pod.Namespace, Name: controller.Name}, live); err != nil {
		return fmt.Sprintf("the operation's deployment could not be read live: %v", err)
	}
	if live.UID != target.DeploymentUID {
		return fmt.Sprintf("the operation's deployment is now %s rather than the captured %s",
			live.UID, target.DeploymentUID)
	}

	return ""
}

// read 运行采集器脚本并返回它实际生成的信封数据。
//
// 响应体被截断并非错误，因为脚本在未发现任何内容时也允许以零状态码退出。不完整的信封正对应着缺失或读取了一半的结果，调用方会将其转为挂起（hold）状态；在此处将其视为传输错误反而会丢失具体语义。
func (c *modelDeploymentDrainCollector) read(
	ctx context.Context, pod *corev1.Pod, container, url string, expected []string,
) (string, error) {
	readCtx, cancel := context.WithTimeout(ctx, modelDeploymentDrainExecTimeoutSeconds*time.Second)
	defer cancel()

	return c.exec(readCtx, pod, container, modelDeploymentDrainArgv(url, expected))
}

// resolveMember 是唯一的查找入口，它返回 Pod 或解释其缺失原因的挂起（hold）信息，而不是直接返回 error。因为找不到成员并非传输失败：这是本次读取预期给出的结果之一，必须保留原因并传递到 operator 的状态中。
//
// 查找通过字段索引按 UID 进行，而非 list 全量扫描，因此一次读取只需一次缓存查找，而不必遍历集群中的所有 Pod。如果同一个 UID 对应多个 Pod，则会拒绝处理（这种情况本不应发生，拒绝而非取其中第一个）。
// 缓存负责定位，API server 负责鉴权确认。索引用于让本读取器获知哪个 Namespace 和名称承载了成员 UID——因为 UID 才是身份标识而名称不是；但它不直接用于回答读取结果，因为缓存对象只是 watch 上次推送的快照，自那时起成员可能已重启、被替换或被重新打标。因此需要用定位到的名称去实时读取对象，再将实时对象的 UID 与冻结的目标进行比对：以相同名称删除并重建的 Pod 会返回新的 UID，属于不同的成员。
//
// 使用 Pod UID 字段选择器虽然读取成本更低，但会被拒绝：API server 并未对该字段建立索引，依赖它的读取器将无法请求所需的扫描。
func (c *modelDeploymentDrainCollector) resolveMember(
	ctx context.Context, target modelDeploymentDrainTarget, phase string,
) (*corev1.Pod, *modelDeploymentDrainAnswer) {
	pods := &corev1.PodList{}
	if err := c.client.List(ctx, pods,
		ctrlcli.MatchingFields{modelDeploymentDrainIndexPodUID: string(target.PodUID)},
	); err != nil {
		hold := modelDeploymentDrainHold(modelDeploymentDrainUnknown,
			fmt.Sprintf("the member could not be %s: the lookup failed: %v", phase, err))

		return nil, &hold
	}

	switch len(pods.Items) {
	case 0:
		hold := modelDeploymentDrainHold(modelDeploymentDrainUnknown,
			fmt.Sprintf("member could not be %s: no pod carries member uid %s", phase, target.PodUID))

		return nil, &hold
	default:
		if len(pods.Items) > 1 {
			hold := modelDeploymentDrainHold(modelDeploymentDrainUnknown,
				fmt.Sprintf("member could not be %s: %d pods carry member uid %s",
					phase, len(pods.Items), target.PodUID))

			return nil, &hold
		}
	}

	if c.fresh == nil {
		hold := modelDeploymentDrainHold(modelDeploymentDrainUnknown,
			fmt.Sprintf("member could not be %s: no live reader is configured to confirm it", phase))

		return nil, &hold
	}

	key := ctrlcli.ObjectKeyFromObject(&pods.Items[0])
	pod := new(corev1.Pod)
	if err := c.fresh.Get(ctx, key, pod); err != nil {
		hold := modelDeploymentDrainHold(modelDeploymentDrainUnknown,
			fmt.Sprintf("the member could not be %s: the live read failed: %v", phase, err))

		return nil, &hold
	}

	// 定位器上报的名称如今可能已完全属于另一个成员。未携带冻结 UID 的实时读取结果无论名称如何，都是另一个 Pod。
	if pod.UID != target.PodUID {
		hold := modelDeploymentDrainHold(modelDeploymentDrainUnknown,
			fmt.Sprintf("member could not be %s: %s/%s now carries uid %s rather than %s",
				phase, pod.Namespace, pod.Name, pod.UID, target.PodUID))

		return nil, &hold
	}

	return pod, nil
}

// execInto 是实际的传输通道：进入容器的 SPDY 流，同时受时间和大小限制。
//
// 命令使用的是镜像自带的 python3，理由与 preStop hook 相同：两种引擎都是 Python 程序，因此解释器必然存在，而为一次读取引入第二个二进制依赖会带来新的信任成本。标准错误输出已声明并丢弃，这样即使引擎输出日志也会被读取而不会阻塞，同时仍不会使受限响应体积膨胀。
func (c *modelDeploymentDrainCollector) execInto(
	ctx context.Context, pod *corev1.Pod, container string, argv []string,
) (string, error) {
	if c.core == nil || c.restConfig == nil {
		return "", fmt.Errorf("no exec transport is configured")
	}

	request := c.core.RESTClient().
		Post().
		Resource("pods").
		Name(pod.Name).
		Namespace(pod.Namespace).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: container,
			Command:   argv,
			// 声明 stdout，以便 exec 流在下方传递受限响应。
			Stdout: true,
			// 声明 stderr，因为客户端在下方会排空该流。
			Stderr: true,
			TTY:    false,
		}, scheme.ParameterCodec)

	executor, err := remotecommand.NewSPDYExecutor(c.restConfig, "POST", request.URL())
	if err != nil {
		return "", fmt.Errorf("prepare exec into %s/%s: %w", pod.Namespace, pod.Name, err)
	}

	// 大小限制在流运行期间而非结束后强制执行，因此持续写入的端点会在达到上限时被截断，而不会先放任响应增长再做计量。
	//
	// 排空 stderr，但不将其计入受限的 stdout 响应。
	stdout := &modelDeploymentDrainBounded{limit: modelDeploymentDrainMaxBodyBytes}
	if err := executor.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdout: stdout,
		Stderr: io.Discard,
	}); err != nil {
		return "", fmt.Errorf("exec into %s/%s: %w", pod.Namespace, pod.Name, err)
	}

	if stdout.exceeded {
		return "", fmt.Errorf("member %s served more than the %d byte bound",
			pod.Name, modelDeploymentDrainMaxBodyBytes)
	}

	return stdout.body.String(), nil
}

// modelDeploymentDrainBounded 在固定大小内累积流数据，并记录是否发生溢出。它在达到上限后停止接收字节而非直接截断，因为被截断的 Prometheus 响应体可能在行中结束，从而被解析成一个更短但格式看似正常的错误结果。
type modelDeploymentDrainBounded struct {
	body     bytes.Buffer
	limit    int
	exceeded bool
}

// Write 持续累积数据直至达到上限，并拒绝超出上限后的所有数据。
func (b *modelDeploymentDrainBounded) Write(p []byte) (int, error) {
	if b.body.Len()+len(p) > b.limit {
		b.exceeded = true
		return 0, fmt.Errorf("metrics body exceeds %d bytes", b.limit)
	}

	return b.body.Write(p)
}

// modelDeploymentDrainArgv 是本传输通道唯一会执行的命令：使用镜像自带的 python3 运行一段生成的程序。不存在第二种命令形式，也没有用户可触及的参数，这使得只读保证只需阅读本函数即可验证。
func modelDeploymentDrainArgv(url string, expected []string) []string {
	names := make([]string, 0, len(expected))
	for _, name := range expected {
		names = append(names, strconv.Quote(name))
	}

	// 该脚本仅跨标签集求和，绝不跨响应体求和。引擎按标签拆分的指标序列（两种引擎均会这样做）代表同一个成员的工作在多行中的汇报，属于它的行都在这单个响应体中。读取多个响应体正是将单成员零值误算为组零值的根源，而这里没有第二次读取可供混用。
	//
	// 无法解析的样本会被报告为不可读而非直接丢弃，这样本次读取请求了但无法理解的指标名称会作为损坏的信封到达调用方，而不是表现为缺失指标。直接丢弃会导致部分求和结果看起来像是一个完整的零。
	// 必需样本绝不会被丢弃，也绝不会被抵消。引擎按标签拆分的 gauge 指标包含多行，脚本无法读取的某一行依然代表该 gauge 正在汇报且被遗漏；仅对其可读的兄弟行求和会得出一个引擎从未写出的数字，而放任兄弟行保留则会报出一个引擎本意之外的总数。非计数、非有限值或小于零的值会以与不可读行相同的方式拒绝该 gauge，从而杜绝任何将无效证据转化为零的路径。
	script := fmt.Sprintf(`import math, sys, urllib.request
names = [%s]
try:
    body = urllib.request.urlopen(%s, timeout=%d).read(%d).decode("utf-8", "replace")
except Exception:
    sys.exit(0)
totals = {}
refused = set()
for line in body.splitlines():
    if not line or line.startswith("#"):
        continue
    name = line.split("{", 1)[0].split(" ", 1)[0]
    if name not in names:
        continue
    sample = line.rsplit("}", 1)[1] if "{" in line else line[len(name):]
    fields = sample.split()
    if not fields:
        refused.add(name)
        continue
    try:
        value = float(fields[0])
    except ValueError:
        refused.add(name)
        continue
    if not math.isfinite(value) or value < 0.0:
        refused.add(name)
        continue
    total = totals.get(name, 0.0) + value
    if not math.isfinite(total):
        refused.add(name)
        continue
    totals[name] = total
for name in sorted(refused):
    sys.stdout.write("%%s\tunreadable\n" %% name)
for name in sorted(totals):
    if name in refused:
        continue
    # repr round-trips the value exactly. A fixed format would round a count that is small but
    # real down to a zero, and a queue holding one request would be reported as holding none.
    sys.stdout.write("%%s\t%%s\n" %% (name, repr(totals[name])))
sys.exit(0)
`,
		strings.Join(names, ", "),
		strconv.Quote(url),
		modelDeploymentDrainHTTPTimeoutSeconds,
		modelDeploymentDrainMaxBodyBytes,
	)

	return []string{"python3", "-c", script}
}

// modelDeploymentDrainAnswerFromBody 将信封数据转换为四种状态之一。
//
// 完整性必须在数值求和之前判定，缺失的 gauge 应导致挂起（hold）而非零。这一顺序正是核心所在：如果读取器只抓取了四个 SGLang gauge 中的两个，否则就会直接对已找到的值求和，从而将未读到的序列报告为空，进而触发协议误操作。
//
// 协议层还会对照树中唯一映射表独立复核同一份清单，因此本函数是第一道闸门而非唯一一道。
func modelDeploymentDrainAnswerFromBody(
	body string, expected []string,
) modelDeploymentDrainAnswer {
	series := make(map[string]float64, len(expected))
	unparsable := make([]string, 0, len(expected))

	for _, line := range strings.Split(body, "\n") {
		if line == "" {
			continue
		}

		name, sample, found := strings.Cut(line, "\t")
		if !found {
			continue
		}
		value, err := strconv.ParseFloat(strings.TrimSpace(sample), 64)
		if err != nil || !modelDeploymentDrainActivityValue(value) {
			// 传输层请求了某个名称，但其携带的不是可用计数值，这属于损坏的信封而非缺失的指标。将其记录下来，以便原因中能指出具体是哪一个。
			// 解析为浮点数时 NaN 和无穷大也会解析成功，因此解析后还需校验数值本身；负计数值也因同样理由被拒绝。
			if modelDeploymentDrainExpected(name, expected) {
				unparsable = append(unparsable, name)
			}
			continue
		}
		series[name] = value
	}

	// 返回了非数字内容的指标名称会在完全未返回的指标名称之前报告。两者都会触发挂起，但属于不同故障：返回了不可读值的引擎至少给出了响应，而什么都没返回的引擎则没有。
	if len(unparsable) > 0 {
		sort.Strings(unparsable)
		return modelDeploymentDrainAnswer{
			State:  modelDeploymentDrainUnknown,
			Reason: "the engine served unreadable values for " + strings.Join(unparsable, ", "),
		}
	}

	if missing := modelDeploymentDrainAbsentSeries(expected, series); len(missing) > 0 {
		return modelDeploymentDrainAnswer{
			State:  modelDeploymentDrainUnknown,
			Reason: "the engine served no " + strings.Join(missing, ", "),
		}
	}

	total := 0.0
	for _, value := range series {
		total += value
	}
	// 每个值都是有限且非负的，因此总和不再是有效计数的唯一可能是发生溢出。此时应拒绝而非上报，因为给出如此巨大计数的引擎所描述的队列已超出本协议的推理范围。
	if math.IsInf(total, 0) || math.IsNaN(total) {
		return modelDeploymentDrainAnswer{
			State:  modelDeploymentDrainUnknown,
			Reason: "the engine's in-flight total is not a finite count",
		}
	}
	if total > 0 {
		return modelDeploymentDrainAnswer{
			State:    modelDeploymentDrainBusy,
			Reason:   fmt.Sprintf("the engine holds %.0f in flight", total),
			Complete: true,
			Series:   series,
		}
	}

	return modelDeploymentDrainAnswer{
		State:    modelDeploymentDrainIdle,
		Reason:   "the engine holds nothing in flight",
		Complete: true,
		Series:   series,
	}
}

// modelDeploymentDrainAbsentSeries 列出信封中未携带的 gauge 名称。
func modelDeploymentDrainAbsentSeries(expected []string, series map[string]float64) []string {
	missing := make([]string, 0, len(expected))
	for _, name := range expected {
		if _, present := series[name]; !present {
			missing = append(missing, name)
		}
	}

	return missing
}

// modelDeploymentDrainExpected 报告某个名称是否属于本次读取请求关注的指标之一。
func modelDeploymentDrainExpected(name string, expected []string) bool {
	for _, candidate := range expected {
		if candidate == name {
			return true
		}
	}

	return false
}

// modelDeploymentDrainHold 表示携带原因的挂起（hold）结果，其 completeness 设为 false，使协议无论收到何种状态都无法从中读取数值。
func modelDeploymentDrainHold(
	state modelDeploymentDrainState, reason string,
) modelDeploymentDrainAnswer {
	return modelDeploymentDrainAnswer{State: state, Reason: reason}
}

// modelDeploymentDrainContainerOf 查找本次读取对应的容器。
//
// 虽然容器名由 target 携带而非重新查询，但携带的名称只是声明，Pod 才是权威依据，因此在此处而非调用点校验该声明。
func modelDeploymentDrainContainerOf(
	pod *corev1.Pod, name string,
) (*corev1.Container, bool) {
	for i := range pod.Spec.Containers {
		if pod.Spec.Containers[i].Name == name {
			return &pod.Spec.Containers[i], true
		}
	}

	return nil, false
}

// modelDeploymentDrainContainerStatus 返回 kubelet 最近一次观测到的容器状态身份信息。
func modelDeploymentDrainContainerStatus(
	pod *corev1.Pod, name string,
) (*corev1.ContainerStatus, bool) {
	for i := range pod.Status.ContainerStatuses {
		if pod.Status.ContainerStatuses[i].Name == name {
			return &pod.Status.ContainerStatuses[i], true
		}
	}

	return nil, false
}

// modelDeploymentDrainURL 返回 Pod 中记录的引擎自身指标端点。
//
// 该地址从 Pod 读取而非从规格推导，原因与从部署而非 role 读取引擎一致：即便 spec 已移除某个副本的 role，该副本仍可排空，而 role 恰恰可能已经不存在。注解在渲染时写入，记录了引擎实际绑定的端口，而 role 自身的端口声明可能与之不一致。
func modelDeploymentDrainURL(pod *corev1.Pod) (string, bool) {
	port, err := strconv.Atoi(pod.Annotations["prometheus.io/port"])
	if err != nil || port <= 0 || port > 65535 {
		return "", false
	}

	schemeName := pod.Annotations["prometheus.io/scheme"]
	if schemeName != "http" && schemeName != "https" {
		return "", false
	}

	path := pod.Annotations["prometheus.io/path"]
	if path == "" {
		path = "/metrics"
	}
	if !strings.HasPrefix(path, "/") {
		return "", false
	}

	return fmt.Sprintf("%s://127.0.0.1:%d%s", schemeName, port, path), true
}

// modelDeploymentDrainParallelismUnreadable 拒绝控制器无法读取的并行度声明。
func modelDeploymentDrainParallelismUnreadable(
	engine string, kind workercore.ModelDeploymentRoleKind, container *corev1.Container,
) (string, bool) {
	reading, err := scanModelDeploymentParallelism(engine, container.Command,
		modelDeploymentContainerEnv(container))
	if err != nil {
		return fmt.Sprintf("the member's parallelism could not be read: %v", err), true
	}

	if shape, reason := modelDeploymentLoadBalance(engine, kind,
		modelDeploymentContainerEnv(container), reading.declared); shape == workercore.ModelDeploymentLoadBalanceUnknown {
		return reason, true
	}

	return "", false
}

// IMPORTANT: SGLang 分离模式识别
//
// modelDeploymentDrainSGLangDisaggregationMode 负责分离判定中的 SGLang 一侧，直接从命令行读取，
// 而不经过面向 vLLM 的负载均衡形态表。
//
// 负载均衡表仅根据 vLLM 的数据并行标志判定形态，因此跨节点启动的 SGLang 成员会被推导为 Internal
// 并通过常规检查。SGLang 改为在命令行上自行声明形态：该参数可取 null、prefill 或 decode，而
// prefill 或 decode 成员正是协议不能仅凭自身指标直接退场的形态。此处直接读取 Pod 自身命令行，
// 因此即使对应 role 已从 spec 删除依然有效。
func modelDeploymentDrainSGLangDisaggregationMode(command []string) string {
	for i, token := range command {
		value := ""
		switch {
		case token == "--disaggregation-mode" && i+1 < len(command):
			value = command[i+1]
		case strings.HasPrefix(token, "--disaggregation-mode="):
			value = strings.TrimPrefix(token, "--disaggregation-mode=")
		default:
			continue
		}

		if value == "prefill" || value == "decode" {
			return value
		}
	}

	return ""
}
