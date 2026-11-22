// 按 Router 维度的服务观测：Feature 2 的 operator 侧实现。
//
// 两个 Rust router 暴露了只读的成员视图（即 pack/llm-router 下的 observer 补丁）。
// 本文件作为消费端，会严格按 schema 解析该视图，并在采集时基于 Kubernetes 状态
// （而非 router 自身声明的任何身份信息）将每个提供服务的 worker 绑定到其地址所解析的
// 存活 Pod，最终将绑定后的视图聚合为部署的服务结果。
//
// 规范对该结果施加的三项铁律及其在本文件中的落点：
//
//   - 按进程直连访问：视图始终从指定的单个 Router Pod 读取，绝不通过 Router 的
//     Service 采样；fetch 由调用方传入，因此请求目标在读取任何字节前便已确定。
//   - 采集时绑定身份：端点的 Pod UID 来自其地址所解析的存活 Pod 对象，且该 Pod
//     必须处于运行中且未处于终止状态。无法解析到任何存活对象的 URL 属于不确定绑定，
//     只要存在一处不确定绑定就会导致整个视图不可用——基于部分绑定得出的数字不过是
//     披着确认外衣的猜测。
//   - 绝不伪造零值：不可用、过期、缺失或存在分歧的一组视图应判定为 Unknown 或
//     NotConverged；NotConfigured 表示部署完全未声明 Router；Confirmed（包括
//     明确的零值）则要求每个视图都新鲜、完整、在单个 Router 进程内代数一致，且彼此一致。
package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/systemname"
	"gpustack.ai/gpustack/pkg/utils/json"
)

// RouterObservedWorker 表示 observer 载荷中的单个 worker 条目。observer 的选择
// 字段读取自 router 请求路径所查询的同一组访问器，因此 `in_selection` 即为
// 请求路径自身对“该 worker 是否会接收新请求”的回答。
// RouterObservedSelection 是 router 按 worker 维度对“该 worker 是否会接收
// 新请求”的自身回答，即请求路径所求值的合取条件。
type RouterObservedSelection struct {
	Registered  bool `json:"registered"`
	Healthy     bool `json:"healthy"`
	CircuitOpen bool `json:"circuit_open"`
	InSelection bool `json:"in_selection"`
}

type RouterObservedWorker struct {
	URL       string                  `json:"url"`
	Port      string                  `json:"port,omitempty"`
	Role      string                  `json:"role"`
	PodHint   string                  `json:"pod_hint"`
	WorkerID  string                  `json:"worker_id"`
	Selection RouterObservedSelection `json:"selection"`
	// Lane 和 LastJob 是网关的控制面运行状态读取字段（按 URL 划分的发现通道以及
	// 最近一次记录的控制面任务）。对它们进行解析仅为了让严格解码器能接受网关原样
	// 提供的视图，除此之外不会进一步读取：绑定器与聚合逻辑不从这两个字段获取任何输入。
	// LastJob 保留原始格式，因为其结构属于网关的 JobStatus，而非本契约的一部分。
	Lane *struct {
		Busy    bool `json:"busy"`
		Waiting int  `json:"waiting"`
	} `json:"lane,omitempty"`
	LastJob json.RawMessage `json:"last_job,omitempty"`
}

// RouterServingView 表示一条已解析且通过 schema 校验的 observer 载荷。其 schema
// 非常严格：未知字段意味着该载荷无法被本 operator 理解，而无法理解的载荷不能作为
// 任何有效视图。基于同样的原因，缺失 router 字段块也会被拒绝——启动代次为零会
// 导致重启与首次启动无法区分。
type RouterServingView struct {
	// RouterPodUID 是读取该视图的 Router Pod 身份标识，由采集器进行绑定；它并非
	// router 所提供的传输载荷的组成部分。
	RouterPodUID         types.UID `json:"-"`
	RouterBootGeneration uint64
	RouterNowMS          uint64
	RegistryRevision     uint64
	Workers              []RouterObservedWorker
}

// routerObservedSelectionWire 将四个选择布尔值作为指针携带，以便解码器区分缺失
// 字段与显式的 false，以及 null 与有效值。此前缺失的布尔字段会解码为 false，而
// 这与 router 拒绝某 worker 时发送的值相同，从而使截断或残缺的载荷无法与拒绝
// 区分，并错误地确认了零值。
type routerObservedSelectionWire struct {
	Registered  *bool `json:"registered"`
	Healthy     *bool `json:"healthy"`
	CircuitOpen *bool `json:"circuit_open"`
	InSelection *bool `json:"in_selection"`
}

// routerObservedWorkerWire 表示传输线上的 worker。Selection 采用指针类型，以便将
// null 或缺失的 selection 块与完整块区分开来。
type routerObservedWorkerWire struct {
	URL       string                       `json:"url"`
	Port      string                       `json:"port,omitempty"`
	Role      string                       `json:"role"`
	PodHint   string                       `json:"pod_hint"`
	WorkerID  string                       `json:"worker_id"`
	Selection *routerObservedSelectionWire `json:"selection"`
	Lane      *struct {
		Busy    bool `json:"busy"`
		Waiting int  `json:"waiting"`
	} `json:"lane,omitempty"`
	LastJob json.RawMessage `json:"last_job,omitempty"`
}

type routerObservationPayload struct {
	Router struct {
		BootGeneration uint64 `json:"boot_generation"`
		NowMS          uint64 `json:"now_ms"`
	} `json:"router"`
	RegistryRevision uint64 `json:"registry_revision"`
	// Workers 采用指针类型，以便将缺失或 null 的数组与显式数组区分开来。两个 router
	// 均序列化非 Option 的向量，因此传输载荷中始终存在该数组；不包含该数组的载荷
	// 并未声明任何成员，若将其视为空注册表，就会对正在提供服务的 router 错误确认零值。
	Workers *[]routerObservedWorkerWire `json:"workers"`
}

// parseRouterObservationView 严格解码单条 observer 载荷。解码器禁止未知字段，
// 因此如果某个服务在该端点返回了其他文档（即规范中警示的按插件调试转储结构），
// 会在这一步直接失败，而不会解析成一个看似可靠实则为空的视图。视图还必须显式携带
// workers 数组：显式的 [] 表示真实的空注册表并产生有效的零值，而省略或为 null
// 的数组则是从未说明其服务对象的载荷。
func parseRouterObservationView(data []byte) (RouterServingView, error) {
	var payload routerObservationPayload
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&payload); err != nil {
		return RouterServingView{}, fmt.Errorf("observer payload is not a membership view: %w", err)
	}
	if payload.Router.BootGeneration == 0 {
		return RouterServingView{}, fmt.Errorf("observer payload carries no boot_generation")
	}
	if payload.Workers == nil {
		return RouterServingView{}, fmt.Errorf("observer payload carries no workers array")
	}

	// 在载荷之后还带有第二个 JSON 值的文档不属于合法载荷。解码器读取第一个值后，
	// 若不加校验便会直接忽略后续内容。
	if err := decoder.Decode(new(json.RawMessage)); !errors.Is(err, io.EOF) {
		return RouterServingView{}, fmt.Errorf("observer payload carries a trailing JSON value")
	}

	workers := make([]RouterObservedWorker, 0, len(*payload.Workers))
	for i, wire := range *payload.Workers {
		worker, err := routerObservedWorkerOf(wire)
		if err != nil {
			return RouterServingView{}, fmt.Errorf("observer payload worker %d: %w", i, err)
		}
		workers = append(workers, worker)
	}

	return RouterServingView{
		RouterBootGeneration: payload.Router.BootGeneration,
		RouterNowMS:          payload.Router.NowMS,
		RegistryRevision:     payload.RegistryRevision,
		Workers:              workers,
	}, nil
}

// routerObservedWorkerOf 转换单条传输行，并拒绝任何未完整声明其选择状态的数据。
//
// 所有选择布尔字段都必须存在、非 null 且为布尔类型，该行才具有语义。原因在于协议
// 所依据的结果是零值，而零值声明 router 未向任何对象提供服务。若某一行从未说明
// 其是否已注册、是否健康、是否熔断或是否处于选择集合中，就不能认为它声明了未提供服务；
// 把这种沉默当作默认肯定，正是残缺载荷导致对正在服务的 router 错误确认零值的原因。
// 显式的 false 则恰恰相反，代表 router 自身作出的拒绝服务答复。
func routerObservedWorkerOf(wire routerObservedWorkerWire) (RouterObservedWorker, error) {
	if wire.Selection == nil {
		return RouterObservedWorker{}, fmt.Errorf("carries no selection")
	}

	selection, err := routerObservedSelectionOf(*wire.Selection)
	if err != nil {
		return RouterObservedWorker{}, err
	}

	return RouterObservedWorker{
		URL:       wire.URL,
		Port:      wire.Port,
		Role:      wire.Role,
		PodHint:   wire.PodHint,
		WorkerID:  wire.WorkerID,
		Selection: selection,
		Lane:      wire.Lane,
		LastJob:   wire.LastJob,
	}, nil
}

// routerObservedSelectionOf 要求全部四个布尔字段均明确给出。导出的 selection 保留
// 普通 bool 类型，供代码库其余部分读取；字段是否存在在此处一次性判定，下游无需
// 再次检查。
func routerObservedSelectionOf(wire routerObservedSelectionWire) (RouterObservedSelection, error) {
	if wire.Registered == nil {
		return RouterObservedSelection{}, fmt.Errorf("selection states no registered")
	}
	if wire.Healthy == nil {
		return RouterObservedSelection{}, fmt.Errorf("selection states no healthy")
	}
	if wire.CircuitOpen == nil {
		return RouterObservedSelection{}, fmt.Errorf("selection states no circuit_open")
	}
	if wire.InSelection == nil {
		return RouterObservedSelection{}, fmt.Errorf("selection states no in_selection")
	}

	return RouterObservedSelection{
		Registered:  *wire.Registered,
		Healthy:     *wire.Healthy,
		CircuitOpen: *wire.CircuitOpen,
		InSelection: *wire.InSelection,
	}, nil
}

// ServingEndpoint 表示一个已绑定的服务中 worker：视图声明其处于选择集合中，且
// operator 在采集时已将该 worker 的地址解析到存活的 Pod。
type ServingEndpoint struct {
	PodUID types.UID
	Member string
	// Role 和 Rank 来源于所绑定 Pod 自身的工作负载身份：即渲染阶段写入的组件标签与
	// 十进制成员索引。observer 注册表中的 role 和 worker_id 属于注册表身份，绝不会
	// 复制到此处，因为 router 可以任意命名端点，而工作负载才是定义端点身份的权威来源。
	Role string
	Rank string
	// Listener 是所绑定 URL 指向的已声明容器端口。共享同一物理端点的虚拟 rank 会
	// 共享该 listener 及上述 PodUID，因此按角色统计不同的物理 listener 本质上就能
	// 将它们合并计数。
	Listener int32
	URL      string
	Port     string
	Serving  bool
}

// RouterServingObservation 是单个 Router 进程的观测结果：包含已解析的视图及其
// 服务中 worker 绑定的端点列表，或者导致该视图不可用的错误。
type RouterServingObservation struct {
	View RouterServingView
	// CollectedAt 是 operator 读取该视图的时间；新鲜度依据此时间并按 operator 的时钟
	// 进行判断，而非 router 的时钟。
	CollectedAt time.Time
	Bound       []ServingEndpoint
	// RecentDispatch 是该 router 最近曾向其分发过请求的 worker 日志，绑定到与 Bound
	// 相同的 Pod 身份，但刻意未合入 Bound 中。这些 worker 已不再处于选择集合内，
	// 将其合并进 Bound 会虚高 router 已停止选择的 worker 的服务计数。该日志回答的是
	// 另一个问题，仅退役残余检查会关注：最近是否有分发请求到达过该目标，以至于
	// 现在释放它等同于释放一个可能仍在提供服务的成员。
	RecentDispatch []ServingEndpoint
	Err            error
}

// Empty 报告视图是否从未被读取。传输失败会产生一个带有 Err 的 Empty 观测结果——
// 聚合逻辑会读取 Err，而 Empty 可避免将未读视图误认为空注册表视图。
func (o RouterServingObservation) Empty() bool {
	return o.View.RouterBootGeneration == 0 && o.View.Workers == nil && o.Bound == nil
}

// observerURL 是 observer 补丁在 router 现有监听端口上提供的端点路径。
const observerURLPath = "/observer/endpoints"

// modelDeploymentRouterObserverPort 返回所属 profile 提供 observer 服务的监听端口。
//
// llm-d 将 observer 挂载到其已有的管理处理器上，即指标所在的管理监听端口。
// 另外两个 profile 则在各自 Service 已指向的请求端口上提供该端点，因此其契约
// 保持不变。该端口值直接从 profile 读取而非通过 Pod 发现，因为 Pod 端口只是
// 容器碰巧开放的端口，并非调用方可用于读取视图的约定端口。
func modelDeploymentRouterObserverPort(profile string) int32 {
	if profile == workercore.ModelDeploymentRouterLLMD {
		return modelDeploymentRouterMetricsPort
	}

	return modelDeploymentRouterHTTPPort
}

// modelDeploymentObserverMaxBodyBytes 限制单个 observer 响应体的最大字节数。对于
// 包含数百个 worker 的成员视图而言该上限足够宽松，同时又足够小，可防止异常
// Router 的响应无限增长并耗尽 reconciler 的内存。
const modelDeploymentObserverMaxBodyBytes = 4 << 20

// modelDeploymentObservationBudget 是单次采集所能消耗的总时间预算，涵盖所有
// Router 的读取及其后的绑定过程。
//
// 这是单次总预算，而非每个 Router 各一份预算。每次读取虽自带调用方的截止时间，
// 但无截止时间的调用方相当于未设限，在包含多个 Router 的部署中，这种无限制
// 会叠加成总耗时等于各 Router 实际耗时之和的漫长过程。在此处派生总预算，意味着
// 无论调用方传入何种上下文、也无论有多少个 Router 响应，单次采集的耗时始终有界。
//
// 该预算有意短于视图必须满足的新鲜度窗口。若视图恰在新鲜度临界点采集完成，到
// 聚合逻辑校验时便已过期，因此与新鲜度等长的读取预算只会导致采集到的结果被聚合器
// 拒绝。下方留出的余量即用于覆盖发现、最后一次读取以及绑定过程在时间窗口内的开销。
const modelDeploymentObservationBudget = modelDeploymentServingFreshness - time.Second

// modelDeploymentServingFetchTimeout 限制单次读取 Router 视图的超时时间。
//
// 该超时特意短于整次采集的总预算。一次采集由多次读取组成，若单次读取仅继承采集
// 总预算，就可能在某个无响应的 Router 上耗尽全部预算，使其他 Router 无时间可用。
// 采集自身的总预算仍限制整体上限，且早于此超时的调用方截止时间依然优先于两者。
const modelDeploymentServingFetchTimeout = 2 * time.Second

// modelDeploymentObservationContext 派生单次读取所使用的上下文，使自带截止时间的
// 调用方保留其限制，而无限制的调用方也能获得有限的超时控制。
//
// 它添加的是单次拉取的截止时间，而非整次采集的总预算：该传输层可被独立调用，
// 若单次读取等待整个采集预算，就可能拖垮整轮采集流程。
func modelDeploymentObservationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, modelDeploymentServingFetchTimeout)
}

// modelDeploymentRouterObservationContext 将采集过程控制在当前有效的退役预算之内。
func (r *ModelDeploymentReconciler) modelDeploymentRouterObservationContext(
	ctx context.Context, md *workercore.ModelDeployment,
) (context.Context, context.CancelFunc) {
	budget := modelDeploymentObservationBudget
	if reservation := md.Status.Retirement; reservation != nil {
		phaseBudget := time.Duration(0)
		switch reservation.State {
		case workercore.ModelDeploymentRetirementStateAdmitted,
			workercore.ModelDeploymentRetirementStateDisqualified,
			workercore.ModelDeploymentRetirementStateWithdrawing:
			phaseBudget = modelDeploymentRetirementWithdrawalBudget
		case workercore.ModelDeploymentRetirementStateDraining:
			phaseBudget = modelDeploymentRetirementDrainBudget
		case workercore.ModelDeploymentRetirementStateSettling:
			phaseBudget = modelDeploymentRetirementSettleBudget
		case workercore.ModelDeploymentRetirementStateDeleting:
			phaseBudget = modelDeploymentRetirementOverallBudget
		}
		if phaseBudget != 0 {
			now := r.modelDeploymentNow()
			budget = min(budget, reservation.Deadline.Sub(now),
				reservation.PhaseStartedAt.Add(phaseBudget).Sub(now))
		}
	}

	return context.WithTimeout(ctx, budget)
}

type routerViewFetch func(ctx context.Context, url string) ([]byte, error)

// collectRouterServingView 通过传入的 fetch 读取单个 Router 进程的视图，并携带
// Router Pod 的 UID 作为该视图的身份标识。将 fetch 作为参数传入可使传输方式由
// 调用方决定——生产环境调用方直接拨号连接 Pod IP，而测试则可在无网络环境下替代。
func collectRouterServingView(
	ctx context.Context, fetch routerViewFetch, routerPodUID types.UID,
) (RouterServingObservation, error) {
	view, err := fetch(ctx, observerURLPath)
	if err != nil {
		return RouterServingObservation{Err: fmt.Errorf("router view fetch failed: %w", err)}, err
	}
	parsed, err := parseRouterObservationView(view)
	if err != nil {
		return RouterServingObservation{Err: err}, err
	}
	parsed.RouterPodUID = routerPodUID

	return RouterServingObservation{View: parsed}, nil
}

// bindRouterObservationView 将每个服务中 worker 的地址解析到存活的 Pod，并返回
// 绑定后的端点列表。该视图遵循“全有或全无”原则：只要存在一个服务中 worker 的地址
// 无法解析到存活 Pod、解析到多个 Pod，或仅解析到处于终止中或非运行状态的 Pod，
// 即构成不确定绑定——operator 既不能断言该 worker 在提供服务，也不能断言其未提供，
// 因此会拒绝整个视图，而不会带着缺口进行统计。位于选择集合之外的 worker 不会被
// 绑定，也不会导致视图被拒绝：它们本身就是 router 已经拒绝的 worker。
func bindRouterObservationView(
	view RouterServingView, pods []*corev1.Pod, journal bool,
) ([]ServingEndpoint, []ServingEndpoint, error) {
	byIP := make(map[string][]*corev1.Pod, len(pods))
	for _, pod := range pods {
		if ip := pod.Status.PodIP; ip != "" {
			byIP[ip] = append(byIP[ip], pod)
		}
	}

	bound := make([]ServingEndpoint, 0)
	dispatched := make([]ServingEndpoint, 0)
	for _, worker := range view.Workers {
		// 无论是否出现在日志中，处于选择集合之外的 worker 都不会计入服务中数量。日志是
		// 独立证据，不会将其重新提升回 Bound 集合。
		inSelection := worker.Selection.InSelection

		// 仅向发布日志的 profile 查询该日志。另外两个 profile 在同一键下发布网关自身的
		// last_job 结构，本 operator 不对其进行解析；对于无法读取的字段块，绝不能将其
		// 误读为“近期无分发”。
		recent := false
		if !inSelection && journal {
			failure := error(nil)
			recent, failure = recentDispatchFromLastJob(worker.LastJob)
			if failure != nil {
				return nil, nil, failure
			}
		}

		endpoint, err := bindRouterObservationWorker(worker, byIP)
		if err != nil {
			// 任何处于可选状态但 operator 无法定位的 worker 都属于不确定绑定，而只要存在
			// 一处不确定绑定就会使整个视图不可用：基于部分绑定得出的数字只是披着确认外衣的猜测。
			//
			// router 自身已拒绝的 worker 则不同。无论如何它都不会被计入数量，因此无法
			// 定位的此类 worker 只是不提供任何证据，而非拒绝整个视图的理由，视图自身的
			// 矛盾检查也仅将其忽略。
			if inSelection {
				return nil, nil, err
			}

			// 无法定位的日志条目属于不可用证据，而非“无证据”。分发日志是区分近期有分发
			// 的目标与可能仍在处理该分发的成员的唯一依据，因此当某条日志对应的成员无法
			// 解析时必须采取失效关闭策略：直接丢弃它会等效于日志已清空，从而依据一条
			// operator 无法定位的记录过早释放成员。
			if !inSelection && recent {
				return nil, nil, err
			}

			continue
		}

		// 两种选择状态均予以保留，因为聚合逻辑唯一的矛盾检查建立在并集之上：若一个
		// 物理端点被某个 router 选中却被另一个拒绝，或在同一视图中既被选中又被拒绝，
		// 这种分歧如果在入口处就丢弃被拒绝的行便会无从察觉。下方的 Serving 标志用于
		// 区分这两者。
		bound = append(bound, endpoint)
		if !inSelection && recent {
			dispatched = append(dispatched, endpoint)
		}
	}

	return bound, dispatched, nil
}

// bindRouterObservationWorker 将单条 worker 记录解析为其地址所指代的存活 Pod，
// 并推导出其计入统计时使用的工作负载身份。该身份完全由地址和存活 Pod 决定，
// 注册表中关于该记录的任何声明均不参与其中。
func bindRouterObservationWorker(
	worker RouterObservedWorker, byIP map[string][]*corev1.Pod,
) (ServingEndpoint, error) {
	host := workerHost(worker.URL)
	if host == "" {
		return ServingEndpoint{},
			fmt.Errorf("binding indeterminate: worker URL %q has no address", worker.URL)
	}
	candidates := byIP[host]
	if len(candidates) != 1 {
		return ServingEndpoint{}, fmt.Errorf(
			"binding indeterminate: %s resolves to %d live pods, not one", host, len(candidates),
		)
	}
	pod := candidates[0]
	if pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning {
		return ServingEndpoint{}, fmt.Errorf(
			"binding indeterminate: %s is pod %s which is not live", host, pod.Name,
		)
	}
	identity, err := modelDeploymentEndpointIdentityOf(pod, worker.Port)
	if err != nil {
		return ServingEndpoint{}, err
	}

	return ServingEndpoint{
		PodUID:   pod.UID,
		Member:   pod.Name,
		Role:     identity.role,
		Rank:     identity.rank,
		Listener: identity.listener,
		URL:      worker.URL,
		Port:     worker.Port,
		Serving:  worker.Selection.InSelection,
	}, nil
}

// modelDeploymentEndpointIdentity 是单个已绑定端点在计数时使用的工作负载身份。
// 全部三个字段均来自所绑定 Pod 自身渲染的标签和端口，且缺一不可，因为基于身份
// 不完整的端点得到的计数无法归属到具体成员。
type modelDeploymentEndpointIdentity struct {
	role     string
	rank     string
	listener int32
}

// modelDeploymentEndpointIdentityOf 从存活 Pod 上读取该身份信息。
//
// 此处绝不采用 observer 自身的 worker_id。它是 router 为自身记账生成的不透明原生
// 注册表标识，在数据并行成员上会枚举共享同一个物理监听端口的多个虚拟 rank。若将
// 其当作 member rank 读取，就会把单个引擎拆分成与分片数相等的多个端点，这反映的是
// router 注册表的数量而非资源池的真实状态。rank 来自渲染阶段打上的 member-index
// 标签，它才代表本 operator 对 Pod 所属副本成员序号的定义。
//
// 每个字段都会经过校验而非直接存储，缺少其中任一字段的端点将被拒绝而不计入统计。
// 缺失 role 无法指代工作负载，非十进制 rank 不是渲染写入的序号，Pod 未声明的端口
// 也不是其可提供服务的监听端口：将这些包含进计数相当于上报了 Pod 自身并未声明的状态。
func modelDeploymentEndpointIdentityOf(pod *corev1.Pod, urlPort string) (modelDeploymentEndpointIdentity, error) {
	role := modelDeploymentPodRole(pod)
	if role == "" {
		return modelDeploymentEndpointIdentity{}, fmt.Errorf(
			"binding indeterminate: member %s carries no workload role", pod.Name,
		)
	}

	rank := pod.Labels[modelDeploymentMemberIndexLabel]
	if rank == "" {
		return modelDeploymentEndpointIdentity{}, fmt.Errorf(
			"binding indeterminate: member %s carries no member rank", pod.Name,
		)
	}
	// 成员索引从零开始计数，因此按无符号十进制整数解析。ParseUint 会拒绝正负号、
	// 进制前缀以及前后空白字符，渲染逻辑绝不会写入这些格式，若误读则会产生截然
	// 不同的语义。
	if _, err := strconv.ParseUint(rank, 10, 32); err != nil {
		return modelDeploymentEndpointIdentity{}, fmt.Errorf(
			"binding indeterminate: member %s carries member rank %q rather than a decimal index",
			pod.Name, rank,
		)
	}

	listener := modelDeploymentDeclaredListenerOf(pod, urlPort)
	if listener == 0 {
		return modelDeploymentEndpointIdentity{}, fmt.Errorf(
			"binding indeterminate: member %s declares no container port for %q", pod.Name, urlPort,
		)
	}

	return modelDeploymentEndpointIdentity{role: role, rank: rank, listener: listener}, nil
}

// modelDeploymentDeclaredListenerOf 返回所绑定 URL 指向的已声明容器端口；若 Pod
// 未声明该端口则返回 0。已声明端口是渲染逻辑写入的监听契约，Pod 从未声明的端口
// 不能作为本 operator 计数的监听端口。
func modelDeploymentDeclaredListenerOf(pod *corev1.Pod, urlPort string) int32 {
	port, err := strconv.Atoi(urlPort)
	if err != nil {
		return 0
	}
	for i := range pod.Spec.Containers {
		for _, declared := range pod.Spec.Containers[i].Ports {
			if int(declared.ContainerPort) == port {
				return declared.ContainerPort
			}
		}
	}

	return 0
}

// recentDispatchFromLastJob 读取随附 llm-d observer 写入的日志字段。
//
// 对于从未向其分发过请求的 worker，生成端会完全省略 last_job，因此缺失该字段块
// 代表近期无分发，而非缺少必需字段；若强制要求该字段存在，会导致所有未处理过请求
// 的 worker 均报错。当该字段块存在时，必须以布尔值形式携带 recent_dispatch，因为
// 生成端仅在标记分发时才输出它。缺少该字段或字段类型不符均属于本 operator 无法
// 理解的结构，将被拒绝而不会按 false 处理：解码器默认值会把无法读取的日志误判为
// 已确认无分发，这是读取该字段时绝不允许出现的结果。
//
// 对于处于选择集合外且无日志块的 worker，由调用方自行处理。另外两个 router 在该
// 字段下发布的是网关自身的 last_job 结构，本 operator 不对其进行解析。
func recentDispatchFromLastJob(raw json.RawMessage) (bool, error) {
	if len(raw) == 0 {
		return false, nil
	}
	var job struct {
		Recent *bool `json:"recent_dispatch"`
	}
	if err := json.Unmarshal(raw, &job); err != nil {
		return false, fmt.Errorf("the dispatch journal is not readable: %w", err)
	}
	if job.Recent == nil {
		return false, fmt.Errorf(
			"the dispatch journal carries no recent_dispatch boolean, so it cannot be read",
		)
	}

	return *job.Recent, nil
}

// workerHost 提取 worker URL 中的地址部分（去除 scheme 与路径）。注册表中的
// URL 格式为 `scheme://host:port`；非预期格式的 URL 会返回空 host，绑定器
// 将直接拒绝而不会强行猜测。
func workerHost(url string) string {
	withoutScheme := url
	if scheme, rest, found := strings.Cut(url, "://"); found {
		_ = scheme
		withoutScheme = rest
	}
	host := withoutScheme
	if cut := strings.IndexAny(host, "/?"); cut >= 0 {
		host = host[:cut]
	}
	if hostPort, _, err := net.SplitHostPort(host); err == nil {
		return hostPort
	}

	// 不带端口的 host 仍然是一个有效地址；SplitHostPort 对该格式返回错误符合预期，
	// 调用方所需的正是 host 本身。
	return host
}

// ServingAnswer 是 Feature 5 传输格式下的 Feature 2 结果：始终包含状态，仅当
// 状态为 Confirmed 时才包含数值。
type ServingAnswer struct {
	State  workercore.ModelDeploymentServingState
	Value  *int32
	Reason string
	// ByRole 是对 Value 所汇总的同一批端点按工作负载角色分别统计的并集计数。其存在
	// 是为了避免用整个部署的总数填充单个角色自身的状态字段：若 decode 角色有两个
	// 服务中端点而导致 prefill 角色上报“2”，就会让人误以为该数字属于 prefill 自己的
	// 资源池。没有端点的角色对应显式的零值，因此该 map 会为并集中出现的每个角色
	// 构建条目，其余部分由读取方补充。
	ByRole map[string]int32
	// Groups 是按 GROUP 而非按整个部署读取的同一份采集结果。部署级别的结果无法区分
	// 同一角色下的两个 group，并且视图残缺的 group 必须能与仅仅为空的 group 区分开来。
	Groups map[modelDeploymentGroupKey]modelDeploymentGroupServing
	// Members 记录本次采集读取到的各个成员所携带的事实信息，代表最新读取结果，
	// 而非调用方缓存的 Pod 列表。
	Members map[types.UID]modelDeploymentMemberServing
	// CollectedAt 是此结果中最早一条证据被读取的时间。整个结果的新鲜度取决于其最早证据的新鲜度，因此恢复决策会将该时间与新鲜度窗口进行比较。
	CollectedAt time.Time
}

// modelDeploymentGroupKey 标识一个分组：所属角色、所占用的副本序号，以及该分组当前的成员代次。
//
// 该代次是分组当前的成员集合，而非 Router 恰好列出的成员。若根据观测到的成员构造键，在部分视图下将无法匹配其所属分组，从而导致该分组静默地永远得不到结果——这是最糟糕的情况，因为部分视图恰恰是不应发布已确认服务数量的时候。
type modelDeploymentGroupKey struct {
	Role       string
	Ordinal    int
	Generation modelDeploymentMembershipGeneration
}

// modelDeploymentGroupServing 表示从 Router 视图中了解到的单个分组的状况。
//
// 这些计数代表两类不同的概念。Endpoints 统计被选中的该分组 Router 端点数量，而 Members 和 Expected 统计成员数量：一个包含三个成员的普通分组有一个预期端点和三个成员，如果将两者混淆，就会把一个三成员分组错误地发布为三个端点。
type modelDeploymentGroupServing struct {
	// SnapshotValid 表示当前 operator 是否持有该分组新鲜、完整且一致的观测结果。它是恢复门控的输入，但本身不是确认状态。
	SnapshotValid bool
	// InvalidReason 以便于排查的表述说明快照无效的原因。
	InvalidReason string
	// State 是该分组的服务状态。
	State workercore.ModelDeploymentServingState
	// Endpoints 统计被选中的该分组 Router 端点数量。
	Endpoints int32
	// Members 统计该分组当前的成员数量。
	Members int32
	// Expected 统计该分组的预期成员数量。
	Expected int32
	// Retained 表示该分组是否同时具备资格且代次未变：每个应答成员均已带有资格标签，且这些成员上的资格注解标明了该分组当前完整的成员代次。
	//
	// 这是区分状态必须分辨的两种情况的唯一事实。当观测丢失时，已保留的分组会发布 Eligible，而被屏障阻挡的分组会发布 AwaitingObservation；如果使用复合标志，恰恰会抹除这一区分。
	Retained bool
	// MemberNotReady 表示在最新成员列表中发现该分组存在未就绪的成员。
	//
	// 这是一个已知事实而非缺失信息，因此后续的不确定性不会抹除它：即使之后的采集未能读取 Router 视图，系统仍然知道该成员当时未就绪。状态必须指明阻挡该分组的环节，而指明该成员的原因也会随此事实一同传递。
	MemberNotReady bool
}

// modelDeploymentMemberServing 是采集时读取到的单个成员所携带的事实。
//
// 这些信息是在采集时实时读取的，而非取自调用方传入的 Pod 列表。调用方的列表来自 informer 缓存对部署的视图，滞后的缓存值不能作为新的证明：基于它做出的保留决策可能会仅凭 API server 已不再携带的标签而维持分组的资格。
type modelDeploymentMemberServing struct {
	// Eligible 表示该成员是否带有资格标签。
	Eligible bool
	// Generation 是资格注解中指定的成员代次；若成员未携带该注解，则为空。
	Generation modelDeploymentMembershipGeneration
	// Ready 表示在实时读取中该成员是否报告为就绪。
	//
	// 它同样是实时读取而非取自调用方列表，原因与标签相同：资格判定的就绪分支读取的是调用方列表，而落后于 API server 的列表无法证明成员已经就绪。实时结果才是本次采集的判定依据，因此由它来决定该分组是否可以提供服务。
	Ready bool
}

// modelDeploymentPassObservation 表示单轮协调对部署各分组的观测结果，也是所有读取该轮结果的消费者共享的载体。
//
// 恢复决策与状态更新必须读取同一份观测结果。第二次采集会得到针对另一个时刻的另一份结果，重新判定资格也会针对相同 Pod 的另一套分组得出另一份结论，从而导致状态发布出与标签写入依据不符的判定结果。
type modelDeploymentPassObservation struct {
	// Serving 是采集到的 Router 结果，包含各分组的事实及新鲜度时间戳。
	Serving ServingAnswer
	// Qualifications 是资格写入操作所依据的整组判定结果。
	Qualifications []modelDeploymentInstanceQualification
	// CollectiveMembers 是当部署拓扑包含非副本形态的逻辑分组时，该分组的当前成员列表，即弹性集合（elastic collective）的全部 worker 集合。
	//
	// 对于其他所有拓扑形态，该字段均为空，此时状态仅发布副本分组。消费方会从部署配置中识别弹性角色，因此这里只携带成员列表而不单独归组。
	//
	// 它是已拉取并经过属主校验的列表，因此填充该字段不会触发二次拉取或额外探测。
	CollectiveMembers []corev1.Pod
}

// modelDeploymentServingUnobserved 构造未执行观测的路径所发布的默认结果。
//
// 它不能使用零值，因为零值的状态为空字符串，根本不是有效的服务状态：发布零值会误导读者认为 operator 掌握了某种确定信息。未声明 Router 的部署会明确表明未配置，而声明了 Router 的部署则会表明本轮未读取它。
func modelDeploymentServingUnobserved(md *workercore.ModelDeployment, reason string) ServingAnswer {
	if md.Spec.Router == nil {
		return ServingAnswer{State: workercore.ModelDeploymentServingStateNotConfigured}
	}

	return ServingAnswer{
		State:  workercore.ModelDeploymentServingStateUnknown,
		Reason: reason,
	}
}

// modelDeploymentAnnotationKeyEndpointEligibleGeneration 标记某成员确立资格时所处的分组成员代次。
//
// 它使用注解而非标签，因为代次是由排序后的成员 UID 集合用斜杠拼接而成的，而标签值不能包含斜杠。缺少该注解表示成员从未获得过资格，其值与分组当前代次不同则表示在确立资格后成员代次发生了变化。
//
// 它与资格标签在同一次 patch 中写入，因此两者不会出现不一致；同时它绑定在单个 Pod 上，会随承载它的 Pod 一同销毁。
const modelDeploymentAnnotationKeyEndpointEligibleGeneration = "modeldeployment." + systemname.LabelPrefix +
	"endpoint-eligible-generation"

// modelDeploymentServingFreshNow 报告在读取该结果的决策执行时刻，已采集的结果是否仍处于新鲜度窗口内。
//
// 该复检仅基于已有的时钟和既定窗口进行比较，绝不触发二次拉取：结果只采集一次，再次读取各个 Router 会成为伴随新预算与新结果的第二次采集。
//
// 之所以需要这一检查，是因为窗口在聚合时只评估一次，而使用该结果的决策可能稍后才发生。采集之后的标签写入是一系列针对每个变更成员的 API 往返请求，耗时没有上限：聚合时仍新鲜的结果，到最后一个成员写入完成时可能已经过期。
func (r *ModelDeploymentReconciler) modelDeploymentServingFreshNow(answer ServingAnswer) bool {
	if answer.CollectedAt.IsZero() {
		return false
	}

	return r.modelDeploymentNow().Sub(answer.CollectedAt) <= modelDeploymentServingFreshness
}

// modelDeploymentPhysicalEndpoint 是并集统计的基本单位：一个 Pod 上声明的一个监听器。Router 为该监听器注册的所有虚拟行都会折叠到这一个键上。
type modelDeploymentPhysicalEndpoint struct {
	podUID   types.UID
	listener int32
}

// aggregateModelDeploymentServing 实现 Feature 2 的状态机。`configured` 表示部署是否声明了 Router；`observations` 是从各个进程采集到的结果；`freshness` 表示采集视图在多长时间内仍可作为有效依据。
func aggregateModelDeploymentServing(
	configured bool, observations []RouterServingObservation, freshness time.Duration,
) ServingAnswer {
	if !configured {
		return ServingAnswer{State: workercore.ModelDeploymentServingStateNotConfigured}
	}
	if len(observations) == 0 {
		return ServingAnswer{
			State:  workercore.ModelDeploymentServingStateUnknown,
			Reason: "no router view was collected",
		}
	}

	// 启动代次（boot generation）是按进程区分的值。两个 Router 实现都将其定义为进程启动时间戳（毫秒），因此两个独立副本预期会报告不同的值；跨不同进程比较同一标量曾导致所有多副本部署永久处于 NotConverged 状态——无论等待多久都无法消除，因为这种差异是正常现象而非瞬态不一致。
	// 因此一致性必须按单个进程分别校验。唯一会使结果失真的不一致是“同一个”进程报告了两个不同的代次，这意味着 Router 在观测期间发生了重启，各视图描述的是它的不同实例。对于未携带身份标识的视图，无法归属到具体进程，因此将它们归为同一组互相校验：若跳过该校验会把重启误判为收敛，而若将每个视图都当作独立进程则无法进行任何比较。
	generationByRouter := map[types.UID]uint64{}
	for _, observation := range observations {
		if observation.Err != nil {
			return ServingAnswer{
				State:  workercore.ModelDeploymentServingStateUnknown,
				Reason: fmt.Sprintf("a router view is unusable: %v", observation.Err),
			}
		}
		if age := time.Since(observation.CollectedAt); age > freshness {
			return ServingAnswer{
				State:  workercore.ModelDeploymentServingStateUnknown,
				Reason: fmt.Sprintf("a router view is stale by %s", age.Round(time.Second)),
			}
		}
		routerUID := observation.View.RouterPodUID
		generation := observation.View.RouterBootGeneration
		if seen, known := generationByRouter[routerUID]; known && seen != generation {
			return ServingAnswer{
				State: workercore.ModelDeploymentServingStateNotConverged,
				Reason: fmt.Sprintf(
					"a router process reports two boot generations (%d and %d): a router rolled during observation",
					seen, generation,
				),
			}
		}
		generationByRouter[routerUID] = generation
	}

	// 对各视图取去重后的并集，并在合并过程中逐端点校验一致性。视图绝不取交集（否则一个滞后的副本就会使结果归零，掩盖并集本应暴露的端点泄漏问题），也绝不直接相加（两个副本看到的同一个端点仍只是一个端点）。
	//
	// 折叠键是物理监听器，而非视图行。单个成员的引擎可能在视图中表现为多行，因为数据并行成员会为每个虚拟 rank 注册一条记录，而它们共享该成员实际提供服务的同一个监听器。按行计数会反映 Router 的注册表大小而非资源池的服务成员数，因此指向同一 Pod 和同一声明端口的行会折叠为一个端点。角色校验是在折叠后的端点上进行的，而非加入折叠键中，这样当两个视图将同一监听器置于不同角色下时，会被识别为冲突，而不会被重复计为两个端点。
	physical := map[modelDeploymentPhysicalEndpoint]ServingEndpoint{}
	for _, observation := range observations {
		for _, endpoint := range observation.Bound {
			key := modelDeploymentPhysicalEndpoint{
				podUID:   endpoint.PodUID,
				listener: endpoint.Listener,
			}
			previous, seen := physical[key]
			if seen {
				if previous.Serving != endpoint.Serving {
					return ServingAnswer{
						State: workercore.ModelDeploymentServingStateNotConverged,
						Reason: fmt.Sprintf(
							"views disagree about whether pod %s listener %d is selected",
							endpoint.PodUID, endpoint.Listener,
						),
					}
				}
				if previous.Role != endpoint.Role {
					return ServingAnswer{
						State: workercore.ModelDeploymentServingStateNotConverged,
						Reason: fmt.Sprintf("views seat pod %s listener %d as both %q and %q",
							endpoint.PodUID, endpoint.Listener, previous.Role, endpoint.Role),
					}
				}
			}
			physical[key] = endpoint
		}
	}

	// 全局计数与按角色计数均在同一次遍历中源自同一个折叠结果，因此它们不可能描述不同的资源池。对并集统计两次相当于对同一问题给出两个答案，一旦某角色的端点发生变化，两者就会出现偏差。
	count := int32(0)
	byRole := make(map[string]int32, len(physical))
	for _, endpoint := range physical {
		if !endpoint.Serving {
			continue
		}
		count++
		byRole[endpoint.Role]++
	}

	return ServingAnswer{
		State:  workercore.ModelDeploymentServingStateConfirmed,
		Value:  &count,
		ByRole: byRole,
	}
}

// modelDeploymentServingFreshness 定义采集视图能够持续作为有效结果依据的时长。它限制了 Router 对资源池的认知在仍被视作有效快照时允许滞后的程度；超过该时长后，结果将变为 Unknown，而不会继续使用已失去证据支撑的数值。
const modelDeploymentServingFreshness = 15 * time.Second

// defaultServingViewFetch 是生产环境下的传输实现：向调用方基于 Router Pod 自身 IP 构造的 URL 发起带短超时的普通 GET 请求——绝不请求 Router 的 Service，因为那样只会采样到单个副本而静默遗漏其余副本。
func defaultServingViewFetch(ctx context.Context, url string) ([]byte, error) {
	// 单次读取有其独立的单次拉取超时上限，并同时受调用方 context 的约束：两者中较早到期的截止时间生效，调用方取消时请求也会随之终止。若没有这一独立上限，仅继承整轮采集预算的请求会在某个无响应的 Router 上耗尽全部预算，而这正是 Router 卡死时的典型表现。
	requestCtx, cancel := modelDeploymentObservationContext(ctx)
	defer cancel()

	request, err := http.NewRequestWithContext(requestCtx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("observer answered %s", response.Status)
	}

	// 响应体在上限限制下读取，而非无限制读到末尾。否则一个始终写不完数据的观测目标会无限期拖住 reconcile，而上述超时上限正是防止单个 Router 导致整轮处理永不返回的唯一防线。
	body := io.LimitReader(response.Body, modelDeploymentObserverMaxBodyBytes+1)
	raw, err := io.ReadAll(body)
	if err != nil {
		return nil, fmt.Errorf("read observer body: %w", err)
	}
	if len(raw) > modelDeploymentObserverMaxBodyBytes {
		return nil, fmt.Errorf("observer served more than the %d byte bound",
			modelDeploymentObserverMaxBodyBytes)
	}

	return raw, nil
}

// observeModelDeploymentServing 执行 reconcile 期间的采集流程：发现部署渲染出的 Router Pods，直接读取每个 Pod 的视图，进行绑定并聚合。过程中的所有失败都作为返回结果的属性体现，而非作为 reconcile error 返回——无法读取的 Router 在协议中对应 Unknown，这正是规范要求该字段表达的语义。
//
// 这是单轮协调中唯一的一次采集。状态更新与恢复门控只读取这份结果，因此两者不会针对不同时刻给出结论。
func (r *ModelDeploymentReconciler) observeModelDeploymentServing(
	ctx context.Context, md *workercore.ModelDeployment,
) ServingAnswer {
	if md.Spec.Router == nil {
		return aggregateModelDeploymentServing(false, nil, 0)
	}
	ctx, cancel := r.modelDeploymentRouterObservationContext(ctx, md)
	defer cancel()

	observations, live, failure := r.collectModelDeploymentRouterObservationsWithMembers(ctx, md)
	if failure.reason != "" {
		answer := ServingAnswer{
			State:  workercore.ModelDeploymentServingStateUnknown,
			Reason: failure.reason,
		}
		if !failure.retainable || len(live) == 0 {
			// 没有最新成员信息，或者失败已经证明有成员发生变动。此时没有可发布的事实，而只有确定事实才能维持分组处于开启状态。
			return answer
		}

		// 分组事实仍会照常发布，因为它们是根据最新成员列表而非 Router 视图计算得出的，未读取到的视图不作为评判分组的依据。这些事实会标记为 Unknown 且不携带已确认计数，其携带的保留状态沿用现有状态：即每个应答成员均持有资格标签，且标签上的注解指明该分组当前完整的成员代次。
		answer.CollectedAt = failure.readAt
		answer.Groups, answer.Members = modelDeploymentServingGroups(md, live, nil, answer)

		return answer
	}

	answer := aggregateModelDeploymentServing(true, observations, modelDeploymentServingFreshness)
	answer.CollectedAt = modelDeploymentServingOldest(observations)
	answer.Groups, answer.Members = modelDeploymentServingGroups(md, live, observations, answer)

	// 对外公布的确认状态会按分组事实进行过滤，而原始证据保持不动。聚合结果保留其自身语义供下线（retirement）路径读取，只被看到一半的分组不能代表整个部署已获确认。
	if answer.State == workercore.ModelDeploymentServingStateConfirmed {
		for _, group := range answer.Groups {
			if group.SnapshotValid {
				continue
			}
			answer.State = workercore.ModelDeploymentServingStateUnknown
			answer.Value = nil
			answer.Reason = group.InvalidReason

			break
		}
	}

	return answer
}

// modelDeploymentServingOldest 返回本次采集中最早一条证据的读取时间。
//
// 结果的新鲜度取决于其最早证据的新鲜度，因此恢复决策会用该时间与新鲜度窗口进行比较。
func modelDeploymentServingOldest(observations []RouterServingObservation) time.Time {
	oldest := time.Time{}
	for _, observation := range observations {
		if observation.CollectedAt.IsZero() {
			continue
		}
		if oldest.IsZero() || observation.CollectedAt.Before(oldest) {
			oldest = observation.CollectedAt
		}
	}

	return oldest
}

// modelDeploymentServingGroups 根据最新成员列表和采集到的视图，计算每个分组的服务事实及每个成员携带的事实。
//
// 所用成员列表是采集时实时读取的结果，成员携带的事实也一并从中获取：它们读自与视图绑定的同一批对象，因此两次读取之间被重新打标的成员不会被误计为当时已携带标签。
func modelDeploymentServingGroups(
	md *workercore.ModelDeployment,
	live []*corev1.Pod,
	observations []RouterServingObservation,
	answer ServingAnswer,
) (map[modelDeploymentGroupKey]modelDeploymentGroupServing, map[types.UID]modelDeploymentMemberServing) {
	members := make(map[types.UID]modelDeploymentMemberServing, len(live))
	for _, pod := range live {
		members[pod.UID] = modelDeploymentMemberServing{
			Eligible: pod.Labels[modelDeploymentLabelKeyEndpointEligible] ==
				modelDeploymentEndpointEligibleValue,
			Generation: modelDeploymentMembershipGeneration(
				pod.Annotations[modelDeploymentAnnotationKeyEndpointEligibleGeneration],
			),
			Ready: podIsReady(pod),
		}
	}

	// 按成员和监听器记录 Router 持有的端点。这里按物理监听器折叠的原因与部署级并集相同：单个成员的引擎会为每个虚拟 rank 注册一行，而它们共享该成员提供服务的同一个监听器。
	held := map[types.UID]map[int32]bool{}
	for _, observation := range observations {
		for _, endpoint := range observation.Bound {
			if held[endpoint.PodUID] == nil {
				held[endpoint.PodUID] = map[int32]bool{}
			}
			held[endpoint.PodUID][endpoint.Listener] = endpoint.Serving
		}
	}

	pods := make([]corev1.Pod, 0, len(live))
	for _, pod := range live {
		pods = append(pods, *pod)
	}

	// 采集自身的判定结果会作为分组事实的门控。
	//
	// 未达到 Confirmed 状态的结果表明采集不完整：可能没有读取到视图、某个视图不可用、某个视图已过期，或两个视图存在冲突。不完整的采集无法证明 Router 实际持有任何端点，而分组事实是基于已绑定端点计算的；若没有这一门控，无响应的 Router 会被误读为未持有任何端点，分组就会仅凭一次从未发生的读取而恢复服务。
	//
	// 保留事实不受此门控限制，因为保留状态由最新成员列表而非视图证明：在结果丢失期间，代次未变的已有资格分组仍会维持现有状态，这正是处理不可用结果时的预期行为。
	unsound := answer.State != workercore.ModelDeploymentServingStateConfirmed
	unsoundReason := answer.Reason

	groups := make(map[modelDeploymentGroupKey]modelDeploymentGroupServing)
	for _, view := range modelDeploymentGroupPods(pods, true) {
		generation := modelDeploymentGenerationOf(view)
		key := modelDeploymentGroupKey{Role: view.Role, Ordinal: view.Ordinal, Generation: generation}
		fact := modelDeploymentGroupFact(md, view, generation, members, held)
		if unsound {
			fact.SnapshotValid = false
			fact.State = workercore.ModelDeploymentServingStateUnknown
			// 已知事实不会被后续的不确定性抹除。采集读取了最新成员列表并发现存在未就绪成员，这一事实无论 Router 视图之后如何变化都成立。采集自身的原因仍保留在原处：即作为整体结果的原因，用于说明采集本身的情况。
			if !fact.MemberNotReady {
				fact.InvalidReason = unsoundReason
			}
		}
		groups[key] = fact
	}

	return groups, members
}

// modelDeploymentGroupFact 计算单个分组在采集视图中的表现。
//
// 预期端点数对应于应答集合，直接从拓扑辅助函数读取而非在此重新推导：一个三成员的普通分组有 1 个预期端点和 3 个成员，如果混淆两者，就会把三成员分组发布为三个端点。
func modelDeploymentGroupFact(
	md *workercore.ModelDeployment,
	view modelDeploymentReplicaView,
	generation modelDeploymentMembershipGeneration,
	members map[types.UID]modelDeploymentMemberServing,
	held map[types.UID]map[int32]bool,
) modelDeploymentGroupServing {
	fact := modelDeploymentGroupServing{Members: int32(len(view.Members))}

	shape := modelDeploymentDeployedReplicaShape(view)
	fact.Expected = int32(shape.Total)
	if shape.State == modelDeploymentReplicaShapeUnreadable {
		fact.State = workercore.ModelDeploymentServingStateUnknown
		fact.InvalidReason = shape.Reason

		return fact
	}

	// 包含未就绪成员的分组不允许提供服务，无论 Router 当前持有多少端点。Router 视图回答的是 Router 是否正在为该分组提供服务；而就绪状态回答的是该分组是否根本被允许提供服务，最新成员列表对此拥有最新发言权。
	//
	// 对于不安全的分组，任何放行路径都不会开启。既不能保留，也不能作为新分组恢复：采集即使读取了完整的应答集合并确认了 Router 对该分组的视图，针对不允许提供服务的分组做出的确认也不能触发恢复。计数也保持未确认，因为只有有效的确认才能携带计数。
	if reason, unsafe := modelDeploymentGroupUnsafe(view, members); unsafe {
		fact.SnapshotValid = false
		fact.State = workercore.ModelDeploymentServingStateUnknown
		fact.InvalidReason = reason
		fact.MemberNotReady = true

		return fact
	}

	answering, reason := modelDeploymentAnsweringMembers(
		modelDeploymentDeployedAnsweringShape(view, md.Spec.Engine.Name), view.Members,
	)
	if reason != "" {
		fact.State = workercore.ModelDeploymentServingStateUnknown
		fact.InvalidReason = reason

		return fact
	}

	expected := make(map[types.UID]bool, len(answering))
	for _, member := range answering {
		expected[member.UID] = true
	}

	// 保留事实：每个应答成员均带有资格标签，且这些成员上的资格注解标明了该分组当前完整的成员代次。它由最新成员列表而非视图证明，因此会在评判视图之前计算：部分视图意味着对半个分组的观测丢失，而保留机制正是让代次未变的已有资格分组得以度过观测丢失的关键。
	fact.Retained = true
	for _, member := range answering {
		carried, known := members[member.UID]
		if !known || !carried.Eligible || carried.Generation != generation {
			fact.Retained = false

			break
		}
	}

	// 对比 Router 为该分组实际持有的端点与预期应持有的端点。
	observed := make(map[types.UID]bool, len(view.Members))
	endpoints := int32(0)
	for _, member := range view.Members {
		listeners, bound := held[member.UID]
		if !bound {
			continue
		}
		observed[member.UID] = true
		for _, serving := range listeners {
			if serving && expected[member.UID] {
				endpoints++
			}
		}
	}

	// 无法判定预期端点的分组，其完整性也无法判定。Router 要么持有该分组的所有预期端点，要么一个都不持有：部分持有属于自相矛盾的状态，只看到一半的分组不能算作已完成观测。
	switch {
	case len(observed) == 0:
		// 一致的零值并非故障。它仅说明 Router 当前未向该分组分发流量，这是关于 Router 状态的事实而非对分组的否定判定，不会阻挡恢复流程。
		fact.SnapshotValid = true
		fact.State = workercore.ModelDeploymentServingStateConfirmed
	case modelDeploymentSameMembers(observed, expected):
		fact.SnapshotValid = true
		fact.State = workercore.ModelDeploymentServingStateConfirmed
		fact.Endpoints = endpoints
	default:
		heldExpected := 0
		for uid := range observed {
			if expected[uid] {
				heldExpected++
			}
		}
		fact.State = workercore.ModelDeploymentServingStateUnknown
		fact.InvalidReason = fmt.Sprintf(
			"the router's view of the group is partial: it holds %d of the group's %d expected "+
				"endpoints, so the group's serving cannot be confirmed",
			heldExpected, len(expected),
		)
	}

	return fact
}

// modelDeploymentGroupUnsafe 报告分组中是否存在未就绪的成员以及原因。
//
// 包含未就绪成员的分组是不安全的。尚未报告就绪的成员不能作为分组的服务提供方，
// 其健康的同级成员也不能使其具备服务能力。
//
// 由最新成员列表决定结果，因为准入资格自身的就绪检查读取的是调用方的列表。
// 落后于 API server 的列表无法判定成员已就绪，因此不能关闭最新读取已判定为不安全的分组；
// 本次收集正是基于最新读取结果定义的。
func modelDeploymentGroupUnsafe(
	view modelDeploymentReplicaView, members map[types.UID]modelDeploymentMemberServing,
) (string, bool) {
	for _, member := range view.Members {
		carried, known := members[member.UID]
		if !known {
			continue
		}
		if !carried.Ready {
			return fmt.Sprintf(
				"the group's member %s is not ready, so the group may not be served",
				member.Name,
			), true
		}
	}

	return "", false
}

// modelDeploymentSameMembers 报告两个成员集合是否包含相同的成员。
//
// 这里比较的是集合本身而非数量，因为对于一个三成员分组，若 Router 仅持有一个
// follower 而非 leader，其成员数量与仅持有 leader 的分组相同，只有集合能将两者区分开。
func modelDeploymentSameMembers(a, b map[types.UID]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for uid := range a {
		if !b[uid] {
			return false
		}
	}

	return true
}

// modelDeploymentCollectionFailure 表示一次收集未能得出结果的原因，以及该失败允许
// operator 得出的结论。
//
// 关键在于失败分类，而非 reason 字符串所能承载的信息。未能证明分组身份发生变化的失败，
// 会让已在当前 generation 通过准入的分组保留现有状态，服务状态回答为 Unknown。
// 而证明有对象发生变动的失败则会关闭分组，因为该分组已不再是此前通过准入的那个分组。
//
// 不可保留的失败即视为关闭。不存在第三种状态，且只有事实才能维持分组处于开放状态：
// 仅当分组事实是基于实际发生的成员列表读取计算得出时，才会在失败路径上发布。
type modelDeploymentCollectionFailure struct {
	reason string
	// retainable 表示该失败未能证明分组的身份发生变化。
	retainable bool
	// readAt 是读取最新成员列表的时间戳。它是失败路径回答所携带的时间点，
	// 因为该成员列表是此类回答所持有的唯一证据。
	readAt time.Time
}

// modelDeploymentGuardError 指明保护检查中的哪一次读取失败，因为起始保护与结束保护
// 对失败的处理方式不同。
//
// 起始保护将本次过程绑定到所查询的对象，因此读取 deployment 自身身份失败会关闭所有分组。
// 结束保护仅作为边界校验：它的失败意味着未能证明任何变动，这与证明有对象发生变动不同。
type modelDeploymentGuardError struct {
	reason string
	// identity 表示无法读取 deployment 自身的身份信息。
	identity bool
	// routers 表示无法列出 Router 进程。
	routers bool
	// members 表示无法列出成员列表。
	members bool
}

func (f modelDeploymentGuardError) Error() string { return f.reason }

// collectModelDeploymentRouterObservationsWithMembers 执行收集并返回其读取的最新成员列表。
//
// 必须一并返回成员列表，因为分组事实是基于它计算的。每个分组的事实都会读取响应集合
// 以及各成员携带的 label 和 annotation；如果改从调用方的 Pod 列表读取，就会读到
// informer 缓存中的 deployment 视图：已落后的缓存值不能作为成员曾经具备资格的新证据。
func (r *ModelDeploymentReconciler) collectModelDeploymentRouterObservationsWithMembers(
	ctx context.Context, md *workercore.ModelDeployment,
) ([]RouterServingObservation, []*corev1.Pod, modelDeploymentCollectionFailure) {
	collectCtx, cancel := r.modelDeploymentRouterObservationContext(ctx, md)
	defer cancel()
	if err := collectCtx.Err(); err != nil {
		return nil, nil, modelDeploymentCollectionFailure{
			reason: fmt.Sprintf("the router observation budget is exhausted: %v", err),
		}
	}

	// 首先读取最新成员列表，且仅读取一次。
	//
	// 它是状态保留所依赖的证据，因此会在任何可能失败的步骤之前读取，起始保护直接复用该结果，
	// 而不再重复列出成员。结束保护仍会重新列出成员，因为其全部目的就是检查成员是否发生变动。
	//
	// 整个过程仍受单一超时预算和一次往返限制：未增加额外读取，反而减少了一次读取。
	live, err := r.listFreshModelDeploymentPods(collectCtx, md)
	if err != nil {
		return nil, nil, modelDeploymentCollectionFailure{
			reason: fmt.Sprintf("the deployment's pods could not be listed uncached: %v", err),
		}
	}
	readAt := r.modelDeploymentNow()

	// 本次收集以实时对象为边界，而非以本轮开始时的快照为边界。在收集视图前后，都会通过
	// 非缓存 reader 读取 deployment 及其成员列表；UID、generation、owner 或成员列表发生
	// 变化，意味着视图所描述的池已不再是本轮所见的对象，基于早期快照确认成员缺失
	// 将导致对当前状态的误判。
	// 每次实时读取都共享本次收集的预算。边界检查属于本轮过程的一部分，若使用调用方自身的
	// context 执行保护读取，其生命周期可能会超出它所约束的收集过程。
	//
	// 第一次边界检查在任何服务发现之前完成。它是本轮收集所依据的基准状态，因此在这之后
	// 出现的新 Router 或离开的成员都属于收集期间的底层变更，而结束边界检查正是为了捕获此类变更。
	before, guardFailure := r.snapshotModelDeploymentObservationGuard(collectCtx, md, live)
	if guardFailure.reason != "" {
		if guardFailure.identity {
			// 本轮过程未能绑定到所查询的对象，因此无法对其中的任何分组得出结论。
			return nil, nil, modelDeploymentCollectionFailure{reason: guardFailure.reason}
		}

		// 无法列出 Router。分组不等同于 Router，且最新成员列表已在手，因此各分组保留原有状态。
		return nil, live, modelDeploymentCollectionFailure{
			reason:     guardFailure.reason,
			retainable: true,
			readAt:     readAt,
		}
	}
	// 第一次边界检查还会与调用方传入的对象进行比对。两次一致的实时快照仅反映这两次读取本身，
	// 若调用方持有的是已被替换的 deployment，其前后快照也会相同，从而把陈旧对象误判为未变更。
	// 调用方的 UID 和 generation 才是本次收集必须围绕的事实依据。
	if !before.matchesCaller(md) {
		return nil, nil, modelDeploymentCollectionFailure{
			reason: fmt.Sprintf(
				"the caller read deployment %s at generation %d but the deployment is now %s at "+
					"generation %d; the views would describe an object this pass was not asked about",
				md.UID, md.Generation, before.deploymentUID, before.generation,
			),
		}
	}

	// 发现阶段执行实时读取。本轮过程将绑定的 Router 进程必须是 API server 当前仍持有的实例，
	// 而非 watch 上次推送的旧实例：否则在缓存更新与本次读取之间发生滚动更新的 Router，
	// 其被读取的地址将无法由本次收集证实。
	routerPods := &corev1.PodList{}
	if err := r.APIReader.List(collectCtx, routerPods,
		ctrlcli.InNamespace(md.Namespace),
		ctrlcli.MatchingLabels{
			modelDeploymentLabelKeyName:     modelDeploymentLabelValueName,
			modelDeploymentLabelKeyInstance: md.Name,
			modelDeploymentRouterLabelKey:   md.Spec.Router.Name,
		},
	); err != nil {
		return nil, live, modelDeploymentCollectionFailure{
			reason:     fmt.Sprintf("the router's pods could not be listed: %v", err),
			retainable: true,
			readAt:     readAt,
		}
	}

	// 绑定过程使用同一份实时对象，而非调用方的列表。视图所绑定的端点会基于本轮重新读取的
	// Pod 进行解析，因此自调用方列出成员后发生过替换、标签变更或原地替换的成员，
	// 不会被算作其所声称的成员。
	fetch := r.servingViewFetch
	if fetch == nil {
		fetch = defaultServingViewFetch
	}

	// observer 运行在所属 profile 已启用的监听器上。llm-d 将其挂载在管理监听器上，
	// 该监听器同时承载 metrics 和管理端点；另外两种 profile 则挂载在对应 Service 所指向的
	// 请求端口上。端口号始终来自 profile 定义，绝不取自任意 Pod 端口，因为 Pod 端口不构成契约。
	port := modelDeploymentRouterObserverPort(md.Spec.Router.Name)
	// 仅从发布分发日志（dispatch journal）的 profile 读取该日志。另外两种 profile 在同一
	// 键下发布的是网关自身的 last_job 结构，本 operator 不对其进行解析。
	journal := md.Spec.Router.Name == workercore.ModelDeploymentRouterLLMD
	// Router 进程背后的属主链在每次收集时仅读取一次，而非每个进程读取一次。
	// 该链仅在本次收集期间有效，下次收集会重新从 API server 读取。
	owners := newRouterOwnerChain(r.APIReader)
	observations := make([]RouterServingObservation, 0, len(routerPods.Items))
	for i := range routerPods.Items {
		routerPod := &routerPods.Items[i]
		// 正在终止中的 Router 进程仍属于运行中的 Router 进程：它仍持有地址且 observer 仍会响应，
		// 若在此处将其忽略，会把关闭过程误判为已收敛的空注册表。只有当进程失去地址时，覆盖才结束。
		if routerPod.Status.PodIP == "" {
			continue
		}
		// 仅当 Pod 的属主链属于该 deployment 时，其视图才与该 deployment 相关：每个 Router
		// 进程都会通过非缓存 reader 进行实时校验，确认其受控于该 deployment 所属的 ReplicaSet。
		// 属主链缺失或无法读取的进程会阻塞本次观测，而非缩小观测范围。
		if err := r.verifyRouterPodOwnership(collectCtx, md, routerPod, owners); err != nil {
			observations = append(observations, RouterServingObservation{
				Err: fmt.Errorf("router process %s is not verified for this deployment: %w",
					routerPod.Name, err),
			})

			continue
		}
		observation, err := collectRouterServingView(collectCtx, func(
			callCtx context.Context, path string,
		) ([]byte, error) {
			return fetch(callCtx, fmt.Sprintf("http://%s:%d%s",
				routerPod.Status.PodIP, port, path))
		}, routerPod.UID)
		if err != nil {
			observations = append(observations, observation)

			continue
		}
		// 在每个视图各自读取完成后单独打上时间戳：若在循环前统一取一个时间戳，后续每个视图
		// 的记录耗时都会叠加上前面所有读取的耗时，一次较慢的首个请求就可能在聚合阶段判定前
		// 把最后一个视图推向新鲜度边界。聚合逻辑中的新鲜度阈值保持不变。
		observation.CollectedAt = r.modelDeploymentNow()
		observation.Bound, observation.RecentDispatch, err = bindRouterObservationView(observation.View, live, journal)
		if err != nil {
			observation.Err = err
		}
		observations = append(observations, observation)
	}

	if err := collectCtx.Err(); err != nil {
		return nil, live, modelDeploymentCollectionFailure{
			reason:     fmt.Sprintf("the router observation budget is exhausted: %v", err),
			retainable: true,
			readAt:     readAt,
		}
	}

	// 与上文 deployment 自身的身份检查一样，属主链也执行边界校验。由于属主对象只读取一次并
	// 由所有进程共享，本轮会在最后重新读取每一个属主对象，若在读取视图期间有属主发生变动，
	// 则拒绝整次收集。否则，共享读取会导致在第一个进程校验后发生的重挂父级变更被后续进程忽略。
	//
	// Router 的变动不等于分组的变动，因此此类失败允许保留状态：用于判定分组的成员列表未动，
	// 服务状态回答为 Unknown。
	if err := owners.verify(collectCtx); err != nil {
		return nil, live, modelDeploymentCollectionFailure{
			reason: fmt.Sprintf("the routers' owning chain changed while the routers were being "+
				"observed; the collected views describe an owner this pass can no longer stand behind: %v", err),
			retainable: true,
			readAt:     readAt,
		}
	}

	after, guardFailure := r.snapshotModelDeploymentObservationGuard(collectCtx, md, nil)
	if guardFailure.reason != "" {
		// 已知的变更优先于后续的不确定性。结束边界检查可能在后续读取失败前就已经读到了足以
		// 证明 deployment 或成员列表发生变动的信息，一次读取失败绝不能抹除同一快照已经证实的变更。
		//
		// 仅比对结束快照实际读取到的事实。在读取 deployment 之前就失败的快照对其一无所知，
		// 在列出成员之前就失败的快照对成员列表也一无所知。
		if before.deploymentMoved(after) {
			return nil, live, modelDeploymentCollectionFailure{
				reason: fmt.Sprintf(
					"the deployment changed while the routers were being observed; the collected " +
						"views describe an earlier snapshot and cannot confirm absence",
				),
				readAt: readAt,
			}
		}
		if before.membersMoved(after) {
			return nil, live, modelDeploymentCollectionFailure{
				reason: fmt.Sprintf(
					"the membership changed while the routers were being observed; the collected " +
						"views describe an earlier snapshot and cannot confirm absence",
				),
				readAt: readAt,
			}
		}

		// 无法读取结束边界快照。它的存在是为了证明有对象发生变动，而此次它未能证明任何变动。
		return nil, live, modelDeploymentCollectionFailure{
			reason:     guardFailure.reason,
			retainable: true,
			readAt:     readAt,
		}
	}
	if before.identityMoved(after) {
		return nil, live, modelDeploymentCollectionFailure{
			reason: fmt.Sprintf(
				"the deployment or its membership changed while the routers were being observed; " +
					"the collected views describe an earlier snapshot and cannot confirm absence",
			),
			readAt: readAt,
		}
	}
	if before.routerFacts != after.routerFacts {
		return nil, live, modelDeploymentCollectionFailure{
			reason: fmt.Sprintf(
				"the routers' processes changed while the routers were being observed; the group's " +
					"members did not move, so the group keeps what it has and the serving answer " +
					"stays unknown",
			),
			retainable: true,
			readAt:     readAt,
		}
	}

	return observations, live, modelDeploymentCollectionFailure{}
}

// listFreshModelDeploymentPods 通过非缓存 reader 读取 deployment 自身的 Pod。
//
// 它刻意保持与缓存列表一致的过滤方式并改用非缓存 reader：缓存所应用的属主过滤规则
// 决定了该 deployment 可以计入哪些 Pod，若在此处重新推导一套规则，就会对
// “哪些 Pod 属于我”产生第二套可能与之偏离的判定标准。
func (r *ModelDeploymentReconciler) listFreshModelDeploymentPods(
	ctx context.Context, md *workercore.ModelDeployment,
) ([]*corev1.Pod, error) {
	pods := new(corev1.PodList)
	if err := r.APIReader.List(ctx, pods,
		ctrlcli.InNamespace(md.Namespace),
		ctrlcli.MatchingLabels{
			modelDeploymentLabelKeyName:     modelDeploymentLabelValueName,
			modelDeploymentLabelKeyInstance: md.Name,
		},
	); err != nil {
		return nil, err
	}

	owned := make([]*corev1.Pod, 0, len(pods.Items))
	for i := range pods.Items {
		if !modelDeploymentOwns(&pods.Items[i], md) {
			continue
		}
		owned = append(owned, &pods.Items[i])
	}

	return owned, nil
}

// modelDeploymentObservationGuard 是本次收集所绑定的非缓存身份边界：包括 deployment 的
// UID、generation 与控制属主，以及其当前拥有的成员 UID。
type modelDeploymentObservationGuard struct {
	deploymentUID string
	generation    int64
	ownerUIDs     string
	memberUIDs    string
	// memberFacts 和 routerFacts 是绑定逻辑所依据的单 Pod 身份事实。将它们作为独立字符串
	// 而非合并进 memberUIDs，是为了在失败时能明确指出两者中哪一个发生了变化，同时让
	// memberUIDs 继续单纯回答成员集合问题。
	memberFacts string
	routerFacts string
	// read 记录了该快照实际读取到了上述哪些事实。
	read modelDeploymentGuardRead
}

// modelDeploymentGuardRead 记录了保护快照中哪些部分已被成功读取。
//
// 中途失败的快照仍掌握已读到的那部分信息。已经读取的事实就是确凿事实，若快照已证明
// deployment 发生变更，绝不能因为随后的成员列表读取失败而丢失该证据。尚未读取到的
// 部分则处于未知状态，而未知值无法参与比对：从未读取的字段不构成变更。
type modelDeploymentGuardRead struct {
	deployment bool
	members    bool
	routers    bool
}

// snapshotModelDeploymentObservationGuard 通过非缓存 reader 读取上述身份边界。
//
// 当调用方已经读取成员列表时直接传入，否则在此处重新列出。起始保护会复用前置提升的
// 成员读取结果，使每次收集在开头只列出一次而非两次；结束保护则会重新列出成员，
// 因为其全部目的就是检查成员是否发生变动。
func (r *ModelDeploymentReconciler) snapshotModelDeploymentObservationGuard(
	ctx context.Context, md *workercore.ModelDeployment, members []*corev1.Pod,
) (modelDeploymentObservationGuard, modelDeploymentGuardError) {
	guard := modelDeploymentObservationGuard{}

	live := new(workercore.ModelDeployment)
	if err := r.APIReader.Get(ctx, ctrlcli.ObjectKeyFromObject(md), live); err != nil {
		return guard, modelDeploymentGuardError{
			reason: fmt.Errorf(
				"the deployment could not be re-read uncached during observation: %w", err,
			).Error(),
			identity: true,
		}
	}
	ownerUIDs := make([]string, 0, len(live.OwnerReferences))
	for _, owner := range live.OwnerReferences {
		ownerUIDs = append(ownerUIDs, string(owner.UID))
	}
	sort.Strings(ownerUIDs)

	// 从此处起 deployment 自身的事实已确定，且在后续读取失败时依然保持已知。
	// 已证明 deployment 发生变更的快照，绝不能因为随后的成员或 Router 列表读取失败而丢失该证据。
	guard.deploymentUID = string(live.UID)
	guard.generation = live.Generation
	guard.ownerUIDs = strings.Join(ownerUIDs, ",")
	guard.read.deployment = true

	owned := make([]corev1.Pod, 0, len(members))
	if members == nil {
		listed := new(corev1.PodList)
		if err := r.APIReader.List(ctx, listed,
			ctrlcli.InNamespace(md.Namespace),
			ctrlcli.MatchingLabels{
				modelDeploymentLabelKeyName:     modelDeploymentLabelValueName,
				modelDeploymentLabelKeyInstance: md.Name,
			},
		); err != nil {
			return guard, modelDeploymentGuardError{
				reason: fmt.Errorf(
					"the membership could not be re-read uncached during observation: %w", err,
				).Error(),
				members: true,
			}
		}
		for i := range listed.Items {
			if modelDeploymentOwns(&listed.Items[i], md) {
				owned = append(owned, listed.Items[i])
			}
		}
	} else {
		for i := range members {
			owned = append(owned, *members[i])
		}
	}
	// 绑定逻辑所依赖的每一项事实都包含在边界校验中，而不仅仅是成员身份。如果一个成员
	// 保留了原有 UID，但其控制属主、角色、成员序号或声明的监听器发生了变化，它实际上已是
	// 顶着相同名字的另一个成员；仅比对 UID 会将其误判为未变化，进而错误地确认其缺失。
	//
	// 上述身份标签只是预过滤条件，因此这里会采用与 listFreshModelDeploymentPods 相同的方式
	// 确认 controller reference。携带这些标签但不属于当前 deployment 的 Pod 不是任何绑定
	// 逻辑可依赖的事实，将其排除在外才能使保护快照精确描述绑定逻辑所读取的集合：
	// 否则此类无关 Pod 的抖动会因一个本不属于当前 deployment 的成员而导致整次收集被拒绝。
	memberUIDs := make([]string, 0, len(owned))
	memberFacts := make([]string, 0, len(owned))
	for i := range owned {
		memberUIDs = append(memberUIDs, string(owned[i].UID))
		memberFacts = append(memberFacts, modelDeploymentMemberFact(&owned[i]))
	}
	sort.Strings(memberUIDs)
	sort.Strings(memberFacts)

	// 从此处起成员列表的事实已确定，且在 Router 列表读取失败时依然保持已知。
	// 收集期间离开的成员属于本次收集已经证实的变更。
	guard.memberUIDs = strings.Join(memberUIDs, ",")
	guard.memberFacts = strings.Join(memberFacts, ";")
	guard.read.members = true

	// 出于同样原因，Router 进程也在同一快照中读取：失去地址或更换属主的进程
	// 已不再是本轮过程此前读取到的进程。
	routers := &corev1.PodList{}
	if err := r.APIReader.List(ctx, routers,
		ctrlcli.InNamespace(md.Namespace),
		ctrlcli.MatchingLabels{
			modelDeploymentLabelKeyName:     modelDeploymentLabelValueName,
			modelDeploymentLabelKeyInstance: md.Name,
			modelDeploymentRouterLabelKey:   md.Spec.Router.Name,
		},
	); err != nil {
		return guard, modelDeploymentGuardError{
			reason: fmt.Errorf(
				"the routers could not be re-read uncached during observation: %w", err,
			).Error(),
			routers: true,
		}
	}
	routerFacts := make([]string, 0, len(routers.Items))
	for i := range routers.Items {
		routerFacts = append(routerFacts, modelDeploymentMemberFact(&routers.Items[i]))
	}
	sort.Strings(routerFacts)

	guard.routerFacts = strings.Join(routerFacts, ";")
	guard.read.routers = true

	return guard, modelDeploymentGuardError{}
}

// identityMoved 报告两次边界检查之间 deployment 自身身份或成员列表是否发生变动。
//
// Router 进程不包含在内。即使收集到的 Router 发生重挂父级或替换而指向另一组进程，
// 分组自身的成员并未变动，且状态保留由最新成员列表而非视图来证明。
func (g modelDeploymentObservationGuard) identityMoved(other modelDeploymentObservationGuard) bool {
	return g.deploymentMoved(other) || g.membersMoved(other)
}

// deploymentMoved 报告两个快照之间 deployment 自身的身份是否发生变动。
//
// 仅比对两个快照都已读取到的事实。在读取 deployment 之前就失败的快照对其一无所知，
// 而未知不代表发生了变更：它意味着没有可确认的变更。
func (g modelDeploymentObservationGuard) deploymentMoved(other modelDeploymentObservationGuard) bool {
	if !g.read.deployment || !other.read.deployment {
		return false
	}

	return g.deploymentUID != other.deploymentUID ||
		g.generation != other.generation ||
		g.ownerUIDs != other.ownerUIDs
}

// membersMoved 报告两个快照之间成员列表是否发生变动。
//
// 出于与 deploymentMoved 相同的原因，仅比对两个快照都已读取到的事实。在列出成员之前
// 就失败的结束快照对成员列表一无所知；而已经读取到成员列表的结束快照，即使随后的
// Router 列表读取失败，依然掌握成员信息。
func (g modelDeploymentObservationGuard) membersMoved(other modelDeploymentObservationGuard) bool {
	if !g.read.members || !other.read.members {
		return false
	}

	return g.memberUIDs != other.memberUIDs || g.memberFacts != other.memberFacts
}

// matchesCaller 报告首次非缓存读取到的对象是否仍与调用方传入的对象一致。
//
// 缓存对象反映的是过去的状态，而本次收集是针对 deployment 的当前状态定义的。
// 调用方持有的副本可能来自对象被替换前启动的轮次；若仅比对前后两次实时快照，
// 它们都会一致反映替换后的新对象从而放行，而真正的分歧存在于调用方副本与这两次读取之间。
func (g modelDeploymentObservationGuard) matchesCaller(md *workercore.ModelDeployment) bool {
	return g.deploymentUID == string(md.UID) && g.generation == md.Generation
}

// modelDeploymentMemberFact 是关于该 Pod 的绑定逻辑所读取的身份事实：包括其自身 UID、
// 作为身份标识的控制属主、声明的 workload 角色与成员序号、声明的监听器、是否正在运行，
// 以及是否正在终止。持有相同事实的两个快照描述的是同一个成员；事实不同则表明成员已发生变动。
//
// 就绪状态也是该事实的一部分，因为不再报告就绪的成员即视为发生了变动：它不再是可为分组
// 提供服务的成员，而此前凭早先就绪状态准入的成员也已不再是当时准入的状态。仅靠 Pod phase
// 无法体现这一点，因为 readiness 探针失败的成员仍会保持 Running。
//
// 资格 label 和 generation annotation 同样属于该事实的一部分，因为收集过程现在会消费它们：
// 保留规则通过读取它们来判定分组是否具备资格且 generation 未变；若事实中遗漏了它们，
// 收集期间发生的 label 或 annotation 变更就会被忽略。上文中的 member index 即序数，
// 出于同样原因也包含在此处。
func modelDeploymentMemberFact(pod *corev1.Pod) string {
	owner := "none"
	if ref := meta.GetControllerOf(pod); ref != nil {
		owner = ref.APIVersion + "/" + ref.Kind + "/" + ref.Name + "/" + string(ref.UID)
	}
	listeners := make([]string, 0, len(pod.Spec.Containers))
	for i := range pod.Spec.Containers {
		for _, declared := range pod.Spec.Containers[i].Ports {
			listeners = append(listeners, strconv.Itoa(int(declared.ContainerPort)))
		}
	}
	sort.Strings(listeners)
	terminating := pod.DeletionTimestamp != nil

	return strings.Join([]string{
		string(pod.UID),
		owner,
		pod.Status.PodIP,
		modelDeploymentPodRole(pod),
		pod.Labels[modelDeploymentMemberIndexLabel],
		strings.Join(listeners, "+"),
		string(pod.Status.Phase),
		strconv.FormatBool(terminating),
		strconv.FormatBool(podIsReady(pod)),
		pod.Labels[modelDeploymentLabelKeyEndpointEligible],
		pod.Annotations[modelDeploymentAnnotationKeyEndpointEligibleGeneration],
	}, "|")
}

// routerOwnerChain 保存单次收集已校验的属主对象，且仅限本次收集使用。
//
// Pod 的控制链在其生命周期内不可变，因此位于同一 ReplicaSet 下的每个 Router 进程
// 校验的都是相同的两个对象。将其改为每次收集读取一次而非每个进程读取一次，
// 可以把随副本数线性增长的开销降为常数级；而且仅在此处这样做才是安全的：verify 会在
// 收集结束前重新读取每一个属主对象，从而保证校验结论所依据的属主链在收集期间确未发生变动。
//
// 这不是进程级生命周期缓存。它由单次收集构建、使用并随之销毁。重挂父级的属主、
// 被替换的属主以及同名重建的对象在下一次收集时都会重新读取，而本次收集期间的同名替换
// 则会被拒绝。
type routerOwnerChain struct {
	reader      ctrlcli.Reader
	replicaSets map[ctrlcli.ObjectKey]*appsv1.ReplicaSet
	deployments map[ctrlcli.ObjectKey]*appsv1.Deployment
}

// newRouterOwnerChain 初始化每次收集的属主链对象记录，通过非缓存 reader 读取，
// 因为本收集过程并不以缓存快照作为依据。
func newRouterOwnerChain(reader ctrlcli.Reader) routerOwnerChain {
	return routerOwnerChain{
		reader:      reader,
		replicaSets: map[ctrlcli.ObjectKey]*appsv1.ReplicaSet{},
		deployments: map[ctrlcli.ObjectKey]*appsv1.Deployment{},
	}
}

// replicaSet 通过非缓存 reader 读取指定 ReplicaSet，每次收集仅读取一次。
// 所有查询同一 ReplicaSet 的进程都会得到本次收集已校验的同一个对象。
func (c routerOwnerChain) replicaSet(
	ctx context.Context, key ctrlcli.ObjectKey,
) (*appsv1.ReplicaSet, error) {
	if verified, seen := c.replicaSets[key]; seen {
		return verified, nil
	}
	replicaSet := new(appsv1.ReplicaSet)
	if err := c.reader.Get(ctx, key, replicaSet); err != nil {
		return nil, err
	}
	c.replicaSets[key] = replicaSet

	return replicaSet, nil
}

// deployment 通过非缓存 reader 读取指定 Deployment，每次收集仅读取一次，规则与 replicaSet 相同。
func (c routerOwnerChain) deployment(
	ctx context.Context, key ctrlcli.ObjectKey,
) (*appsv1.Deployment, error) {
	if verified, seen := c.deployments[key]; seen {
		return verified, nil
	}
	deployment := new(appsv1.Deployment)
	if err := c.reader.Get(ctx, key, deployment); err != nil {
		return nil, err
	}
	c.deployments[key] = deployment

	return deployment, nil
}

// verify 重新读取本次收集用于判定属主链的每一个对象，并报告第一个在收集期间发生变动的
// 对象。在收集运行期间发生替换、重建或重挂父级的 ReplicaSet 或 Deployment，意味着
// 其背后的进程并非基于当前属主链完成校验，若确认其观测结果，就相当于对未经复核的属主作出确认。
//
// 该开销相对于进程数量是常数级的：无论有多少个 Router 进程，只需两次读取共享属主链，
// 再用两次读取证明其未变动；而此前每个进程都需要独立校验属主链并消耗两次属主读取。
func (c routerOwnerChain) verify(ctx context.Context) error {
	for key, verified := range c.replicaSets {
		fresh := new(appsv1.ReplicaSet)
		if err := c.reader.Get(ctx, key, fresh); err != nil {
			return fmt.Errorf("the router process's ReplicaSet %q could not be re-read uncached: %w",
				key.Name, err)
		}
		if fresh.UID != verified.UID {
			return fmt.Errorf("the router process's ReplicaSet %q was replaced while the routers "+
				"were being observed", key.Name)
		}
		// 与下文的 Deployment 一样，同时检查身份与属主：两者会独立变化，若一个 ReplicaSet 被
		// 重挂父级到另一个 ModelDeployment，它会保留原有 UID，而此前所有进程校验所依据的属主链
		// 指向的仍是旧属主。
		if ownerIdentity(fresh) != ownerIdentity(verified) {
			return fmt.Errorf("the router process's ReplicaSet %q changed owner while the routers "+
				"were being observed", key.Name)
		}
	}
	for key, verified := range c.deployments {
		fresh := new(appsv1.Deployment)
		if err := c.reader.Get(ctx, key, fresh); err != nil {
			return fmt.Errorf("the router process's Deployment %q could not be re-read uncached: %w",
				key.Name, err)
		}
		// 需同时校验身份与属主，因为二者会独立变化：同名删除并重建的 Deployment 会保留名称
		// 但失去原 UID，而被重新挂载到另一个 ModelDeployment 下的 Deployment 会保留 UID 但
		// 失去本次采集所验证的属主。
		if fresh.UID != verified.UID {
			return fmt.Errorf("the router process's Deployment %q was replaced while the routers "+
				"were being observed", key.Name)
		}
		if ownerIdentity(fresh) != ownerIdentity(verified) {
			return fmt.Errorf("the router process's Deployment %q changed owner while the routers "+
				"were being observed", key.Name)
		}
	}

	return nil
}

// ownerIdentity 返回对象控制属主的完整身份标识，若无控制器则返回 none。
// 两次读取具有相同的属主身份即表示同一属主。
func ownerIdentity(obj ctrlcli.Object) string {
	ref := meta.GetControllerOf(obj)
	if ref == nil {
		return "none"
	}

	return strings.Join([]string{
		ref.APIVersion, ref.Kind, ref.Name, string(ref.UID),
	}, "/")
}

// verifyRouterPodOwnership 校验单个 Router 进程的实时控制属主链：该 Pod 由仍以
// 对应身份存在的 ReplicaSet 控制，且该 ReplicaSet 由当前部署控制。读取通过无缓存
// reader 进行，因为缓存结果是本次采集所依据的快照。
//
// 每个进程都会读取一次 Pod，而属主链每次采集仅读取一次。这一划分是刻意为之的。
// Pod 是本次采集直接针对的对象，因此必须在此刻实时察觉其是否已被替换；而其背后的
// 属主是一条共享链路，每个进程只需将自身 Pod 声明的引用与该共享链路的身份比对即可，
// 因此一次读取便可响应所有进程的校验。
func (r *ModelDeploymentReconciler) verifyRouterPodOwnership(
	ctx context.Context, md *workercore.ModelDeployment, pod *corev1.Pod, owners routerOwnerChain,
) error {
	// 重新读取 Pod 本身。传入的对象来自列表查询（即一份快照）；此处的校验用于确认属主链
	// 处于实时有效状态，因此不能使用采集开始前获取的 Pod 副本作答。列表查询之后被接管
	// 或替换的 Router 只能在此处被观测到。
	live := new(corev1.Pod)
	if err := r.APIReader.Get(ctx, ctrlcli.ObjectKeyFromObject(pod), live); err != nil {
		return fmt.Errorf("the router process could not be re-read uncached: %w", err)
	}
	if live.UID != pod.UID {
		return fmt.Errorf("the router process changed identity during observation")
	}

	ref := meta.GetControllerOf(live)
	if ref == nil {
		return fmt.Errorf("the router process has no controlling owner")
	}
	if ref.Kind != "ReplicaSet" || ref.APIVersion != appsv1.SchemeGroupVersion.String() {
		return fmt.Errorf("the router process is controlled by %s (%s) rather than a ReplicaSet",
			ref.Kind, ref.APIVersion)
	}
	replicaSet, err := owners.replicaSet(ctx,
		ctrlcli.ObjectKey{Namespace: live.Namespace, Name: ref.Name})
	if err != nil {
		return fmt.Errorf("the router process's ReplicaSet could not be read uncached: %w", err)
	}
	// 身份校验并非读取操作本身。Pod 自身的 owner reference 是该进程对其属主链的声明，
	// 会与本次采集为所有进程验证过的属主进行比对，因此指向本次采集从未读取过的属主链
	// 的 Pod 会在此处被拒绝。
	if replicaSet.UID != ref.UID {
		return fmt.Errorf("the router process's ReplicaSet changed identity during observation")
	}
	// 该属主链经由 ReplicaSet 所扩缩的工作负载 Deployment 向上延伸，并止步于当前部署：
	// 每一跳都需校验 kind、API version 与身份标识。
	deploymentRef := meta.GetControllerOf(replicaSet)
	if deploymentRef == nil {
		return fmt.Errorf("the router process's ReplicaSet has no controlling Deployment")
	}
	if deploymentRef.Kind != "Deployment" ||
		deploymentRef.APIVersion != appsv1.SchemeGroupVersion.String() {
		return fmt.Errorf("the router process's ReplicaSet is controlled by %s (%s) rather than a Deployment",
			deploymentRef.Kind, deploymentRef.APIVersion)
	}
	deployment, err := owners.deployment(ctx,
		ctrlcli.ObjectKey{Namespace: live.Namespace, Name: deploymentRef.Name})
	if err != nil {
		return fmt.Errorf("the router process's Deployment could not be read uncached: %w", err)
	}
	if deployment.UID != deploymentRef.UID {
		return fmt.Errorf("the router process's Deployment changed identity during observation")
	}
	modelOwner := meta.GetControllerOf(deployment)
	if modelOwner == nil {
		return fmt.Errorf("the router process's Deployment has no controlling ModelDeployment")
	}
	if modelOwner.Kind != workercore.SchemeGroupVersionKind("ModelDeployment").Kind ||
		modelOwner.APIVersion != workercore.SchemeGroupVersion.String() || modelOwner.UID != md.UID {
		return fmt.Errorf("the router process's Deployment is owned by %s (%s) rather than this deployment",
			modelOwner.Kind, modelOwner.APIVersion)
	}

	return nil
}

// applyModelDeploymentServing 将观测结果写入每个角色的 serving 字段。
// 空结果表示未观测的默认状态，会原样保留状态构建器生成的状态，从而使状态推导
// 逻辑本身与观测细节解耦。
func applyModelDeploymentServing(
	status *workercore.ModelDeploymentStatus, answer ServingAnswer,
) {
	if answer.State == "" {
		return
	}
	for i := range status.Roles {
		status.Roles[i].Endpoints.Serving = workercore.ModelDeploymentServingStatus{
			State: answer.State,
		}
		// 只有已确认（Confirmed）的结果才携带数值。Unknown、NotConverged 和 NotConfigured
		// 均表示未能确立计数，API 中以缺省值表达这一语义：若向从未测量的状态写入零，
		// 会被误读为测得零值，而整条路径正是为了杜绝这种虚假的零值。
		if answer.State != workercore.ModelDeploymentServingStateConfirmed {
			continue
		}
		// 并集中没有端点的角色对应计数为零，而非整个部署的总数。计数从结果自身的按角色
		// 映射表中读取，映射表中未列出的角色即表示没有端点，因此该字段反映的是该角色
		// 当前正在服务的数量，而非整个部署的数量。每个角色的值均为独立副本，不会出现两个
		// 角色共享同一可寻址计数的情况。
		roleValue := answer.ByRole[status.Roles[i].Name]
		status.Roles[i].Endpoints.Serving.Value = &roleValue
	}
}
