package worker

import (
	"fmt"
	"strconv"
	"strings"

	core "k8s.io/api/core/v1"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
)

const (
	// modelDeploymentDefaultTerminationGracePeriodSeconds 是当角色未设置 terminationGracePeriodSeconds 时，
	// 正在退出的副本从收到删除请求到被强制终止所能获得的全部时间。这是 Kubernetes 的默认值，此处显式列出
	// 是因为下方的排空（drain）预算基于它计算：kubelet 会在执行 preStop hook 之前启动该倒计时，
	// 并且仅在 hook 返回后才发送 SIGTERM，因此 hook 与引擎自身的退出过程共享这段时间。
	//
	// 角色可以调高该值，但默认值有意维持在下限：更长的宽限期会在每次删除时更久地占用副本的加速器和配额；
	// 而在滚动更新期间，每个角色每次只替换一个副本并等待旧副本完全退出，因此宽限期会成倍叠加到每次滚动
	// 更新中。只有退出耗时更长的引擎所在角色才需要调高该值，而非所有角色。
	modelDeploymentDefaultTerminationGracePeriodSeconds int64 = 30

	// modelDeploymentDrainSettleSeconds 是引擎在收到删除请求后、preStop hook 开始等待其变为空闲之前，
	// 继续正常提供服务的时长。
	//
	// 所有受支持的路由器都会依据副本的 DeletionTimestamp（而非就绪状态）将其摘除——llm-d endpoint picker、
	// vllm-router 和 SGLang gateway 均以该时间戳为准——但路由器仍可能在感知到删除前的瞬间将请求路由到
	// 该副本。这段静置窗口正是用来容纳此类在途请求的。若立即读取在途计数，可能会误判为零并提前结束
	// hook，从而导致该请求被发往已开始关闭的引擎。
	modelDeploymentDrainSettleSeconds int64 = 5

	// modelDeploymentDrainExitSeconds 是宽限期中为引擎预留的退出时间，用于 preStop hook 返回且
	// SIGTERM 送达之后。该阶段不再承载任何请求：此时副本要么已经空闲，要么 hook 已放弃等待仍在运行的请求。
	modelDeploymentDrainExitSeconds int64 = 5
)

// modelDeploymentTerminationGracePeriodSeconds 返回为该角色副本渲染的终止宽限期：
// 若角色自身设置了该值则使用角色的值，否则使用默认值。
func modelDeploymentTerminationGracePeriodSeconds(role *workercore.ModelDeploymentRole) int64 {
	if role.TerminationGracePeriodSeconds != nil {
		return *role.TerminationGracePeriodSeconds
	}

	return modelDeploymentDefaultTerminationGracePeriodSeconds
}

// modelDeploymentDrainDeadlineSeconds 是 preStop hook 从自身启动起算的最晚返回时刻。
//
// 该值由终止宽限期直接推导而非独立配置，避免两者配置失配导致 kubelet 在等待中途强杀引擎。
// 最后一次读取最多可能超出该时刻两秒（一次读超时加一次轮询间隔），由退出预留窗口吸收。
//
// 到达此时刻仍在运行的请求将被切断，这是可接受的上限：vLLM 在收到 SIGTERM 时立即中止请求，
// SGLang 则自行排空直到被强杀。
func modelDeploymentDrainDeadlineSeconds(graceSeconds int64) int64 {
	return graceSeconds - modelDeploymentDrainExitSeconds
}

// modelDeploymentInFlightMetrics 按引擎列出所有用于统计副本当前在途工作量的 gauge 指标名（求和后即总在途量）。
// 这些指标读取自引擎自身的 /metrics 接口，两个引擎均在其 API 端口提供该接口：vLLM 默认开启，
// SGLang 则因渲染的启动命令包含 --enable-metrics 而开启。
//
// decoder 仍在等待接收 KV 的请求也包含在这些总和之中，而朴素的“正在运行请求数”读取方式会漏掉这类请求。
// vLLM 将其状态标记为 WAITING_FOR_REMOTE_KVS 并计入 num_requests_waiting；SGLang 将其保留在
// prealloc 和 transfer 队列中并作为独立 gauge 上报；而在配对的另一侧，prefiller 的 bootstrap 和
// 在途传输队列也是同样的原理。
var modelDeploymentInFlightMetrics = map[string][]string{
	workercore.ModelDeploymentEngineVLLM: {
		"vllm:num_requests_running",
		"vllm:num_requests_waiting",
	},
	workercore.ModelDeploymentEngineSGLang: {
		"sglang:num_running_reqs",
		"sglang:num_queue_reqs",
		"sglang:num_prefill_bootstrap_queue_reqs",
		"sglang:num_prefill_inflight_queue_reqs",
		"sglang:num_decode_prealloc_queue_reqs",
		"sglang:num_decode_transfer_queue_reqs",
	},
}

// modelDeploymentDrainHook 生成副本引擎容器的 preStop hook：在静置窗口期间保持引擎正常服务，
// 随后等待引擎的在途指标归零，最晚在宽限期允许的截止时间前返回，随后 kubelet 发送 SIGTERM。
//
// 等待必须发生在 SIGTERM 之前，因为两个引擎在收到信号后都无法妥善排空。vLLM 的 --shutdown-timeout
// 默认为 0，会立即中止所有运行中请求；调大后虽会排空，但会拒绝期间到达的请求，包括删除前瞬间
// 路由过来的请求。SGLang 虽会在收到信号后排空，但每五秒才检查一次信号且没有自身上限。在此处等待
// 时引擎保持原状：迟到的请求仍可被服务，直到无请求可被中止时才送达信号。
//
// hook 读取与抓取注解相同的端口和协议，即引擎自身的监听器：在 direct-decode 形态下，那是位于
// 路由代理之后的端口，从而统计引擎而非代理的工作量。未声明端口的角色通过 --port 移动端口时，hook
// 会随之跟随；若声明端口与角色自身 --port 不一致（仅可能出现在准入拦截生效前存储的对象中），声明
// 端口上没有监听，hook 会在静置期结束后返回。
//
// hook 通过镜像内置的 python3 执行，与 KV 缓存成员的 hook 一致：两个引擎都是 Python 程序，镜像内
// 必然存在解释器。不能使用 httpGet hook，因为两个引擎都没有仅在空闲时才响应的路由。
func modelDeploymentDrainHook(
	engine string, port int32, scheme core.URIScheme, graceSeconds int64,
) *core.LifecycleHandler {
	url := fmt.Sprintf("%s://127.0.0.1:%d/metrics", strings.ToLower(string(scheme)), port)
	script := modelDeploymentDrainScript(url, modelDeploymentInFlightMetrics[engine],
		modelDeploymentDrainSettleSeconds, modelDeploymentDrainDeadlineSeconds(graceSeconds))

	return &core.LifecycleHandler{
		Exec: &core.ExecAction{Command: []string{"python3", "-c", script}},
	}
}

// modelDeploymentDrainScript 渲染 preStop hook 的执行脚本。时间参数通过入参传入以便测试在秒级
// 尺度运行，生产 hook 传入静置窗口与截止时间。
//
// 引擎的响应决定下一步行为，不同情形区分处理：
//   - 连接被拒、HTTP 错误或 TLS 失败说明此处无法度量（引擎已退出、仍在加载或监听他处），直接返回；
//   - 读取超时说明引擎过忙无法响应，按繁忙处理并继续等待；同时匹配 socket.timeout 与 TimeoutError，
//     因为在 Python 3.10 之前前者不属于后者，较旧镜像可能携带该版本解释器；
//   - 无法解析的采样按繁忙处理，因为无法证明副本已空闲。
//
// 脚本在连续第二次读到空闲（间隔一次轮询）时返回。prefiller 的指标在 prefill 计算完成瞬间下降，
// 而 decoder 拉取对应 KV 紧随其后开始，第二次读取正是为该拉取提供留白。
//
// 脚本始终以状态码 0 退出，这仅关乎退出码本身：非零退出只会产生 Failed 生命周期事件且不会阻止
// kubelet 在 hook 返回后发送 SIGTERM。hook 的返回并非退场副本的最终裁决——删除由退场协议拦截，
// 协议会在 Draining 阶段一直等待直到排空读取器连续两次确认成员空闲——因此此处提前返回最多缩小
// hook 自身的观测范围，不会扩大副本允许丢失的请求范围。
func modelDeploymentDrainScript(url string, metrics []string, settleSeconds, deadlineSeconds int64) string {
	names := make([]string, 0, len(metrics))
	for _, m := range metrics {
		names = append(names, strconv.Quote(m))
	}

	// 数值均为整数和由 strconv 转义的常量；对于其包含的 ASCII 内容，Go 引号字符串同样是合法的 Python 字符串字面量。
	return fmt.Sprintf(`import socket, ssl, sys, time, urllib.request
start = time.monotonic()
time.sleep(%[1]d)
names = [%[3]s]
context = ssl.create_default_context()
context.check_hostname = False
context.verify_mode = ssl.CERT_NONE
idle = 0
while time.monotonic() - start < %[2]d:
    try:
        body = urllib.request.urlopen(%[4]s, timeout=1, context=context).read().decode()
    except Exception as err:
        if not isinstance(getattr(err, "reason", err), (TimeoutError, socket.timeout)):
            sys.exit(0)
        body = None
    busy = 1.0 if body is None else 0.0
    for line in (body or "").splitlines():
        name = line.split("{", 1)[0].split(" ", 1)[0]
        if name not in names:
            continue
        sample = line.rsplit("}", 1)[1] if "{" in line else line[len(name):]
        try:
            busy += float(sample.split()[0])
        except (IndexError, ValueError):
            busy += 1.0
    idle = idle + 1 if busy == 0 else 0
    if idle == 2:
        sys.exit(0)
    time.sleep(1)
sys.exit(0)
`, settleSeconds, deadlineSeconds, strings.Join(names, ", "), strconv.Quote(url))
}
