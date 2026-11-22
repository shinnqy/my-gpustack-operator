package worker

import (
	"context"
	"net/http"
	"sync"
	"time"

	core "k8s.io/api/core/v1"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/utils/datax"
	"gpustack.ai/gpustack/pkg/worker/scaling/algorithm"
)

// 单次采样周期的边界约束。
const (
	// maxSamplerPodsPerRole 是单次采样对单个角色最多查询的就绪且可响应引擎 Pod 数量。
	//
	// 它是上限而非目标值：若某个角色的就绪 Pod 数超过此上限，会被报告为覆盖不完整，
	// 因为仅从上限以内 Pod 收集的数据不足以代表整个角色。
	maxSamplerPodsPerRole = 64

	// maxSamplerScrapeConcurrency 是单次采样周期内并发抓取的 Pod 数量上限。
	maxSamplerScrapeConcurrency = 32

	// samplerPodTimeout 限制单个 Pod 抓取的超时时间。
	samplerPodTimeout = 2 * time.Second

	// samplerPassTimeout 限制遍历所有角色 Pod 的整轮采样的超时时间。
	samplerPassTimeout = 8 * time.Second

	// samplerMaxBodyBytes 限制单次抓取允许读取的响应体大小上限。
	samplerMaxBodyBytes = 1 << 20

	// modelDeploymentScalerSamplerDefaultIntervalSeconds 是当 scaler 的 spec 未指定
	// 采样间隔时默认采用的采样间隔（秒）。
	modelDeploymentScalerSamplerDefaultIntervalSeconds = 15
)

// modelDeploymentScalerScrapeFunc 读取单个引擎 Pod 暴露的指标文本。
//
// 它接收 Pod 对象而非直接接收地址，因为允许从 Pod 读取什么取决于 Pod 本身：
// Pod 未对外宣告的地址不应被读取。
type modelDeploymentScalerScrapeFunc func(ctx context.Context, pod *core.Pod) ([]byte, error)

// modelDeploymentScalerSampler 读取 scaler 目标的引擎指标，并将其转换为每个角色的
// 单条观测结果。
//
// 它为每个 scaler 维护独立的采样环形缓冲区，而非复用指标子资源的窗口：scaler
// 需要的是信号在其自身采样间隔内的变化速率，而指标子资源针对不同窗口回答的是
// 另一类问题。
type modelDeploymentScalerSampler struct {
	// HTTPClient 用于请求引擎 Pod 的指标端点。
	//
	// 它是默认抓取函数使用的客户端；将其设为字段既便于部署侧限制传输层行为，也便于
	// 测试无需启动服务端即可返回采样结果。
	HTTPClient *http.Client

	// Scrape 读取单个引擎 Pod 暴露的指标文本。
	//
	// 设置该字段后会替代底层 HTTP 传输，以便测试直接用固定指标文本驱动一轮采样。
	// 它接收 Pod 并返回该 Pod 响应的指标内容。
	Scrape modelDeploymentScalerScrapeFunc

	// Clock 读取当前时间。
	//
	// 它是采样器自身的时钟，当调用方未提供本轮采样的评估时间时使用。
	Clock func() time.Time

	mu sync.Mutex
	// rings 保存采样器为每个 scaler 记录的状态，以 scaler 的 UID 为键。
	rings map[string]*modelDeploymentScalerRing
	// passing 记录当前正在执行采样的目标，以目标的 UID 为键。
	passing map[string]bool
	// lastPass 记录上一次对目标开始采样的时间，以目标的 UID 为键。
	//
	// 它构成了目标采样频率的下限间隔：scaler 每次调谐都会请求观测数据，而一轮采样
	// 会给引擎 Pod 带来抓取负载，因此需要限制频率。
	lastPass map[string]time.Time
}

// modelDeploymentScalerRing 保存采样器为单个 scaler 保留的记忆状态。
type modelDeploymentScalerRing struct {
	// pods 保存每个引擎 Pod 的最近一次采样，以 Pod 的 UID 为键。
	pods map[string]modelDeploymentScalerPodSample
	// roles 保存上一轮采样获取的各角色信息，以角色名称为键。
	roles map[string]modelDeploymentScalerRoleSample
	// windows 保存每个角色的滑动窗口，以角色名称为键。
	windows map[string]*modelDeploymentScalerRoleWindow
	// quantile 是已记录观测数据所对应的延迟分位数。
	//
	// 它是观测数据自身携带的分位数；由于不同分位数下的观测反映的是不同的分布，
	// 当分位数发生变化时，已记录的观测数据会被丢弃。
	quantile float64
	// targetKey 是用于索引该 scaler 目标的键。
	//
	// 它标识的是目标而非 scaler 本身，因为采样轮次按目标计数：指向同一目标的两个
	// scaler 会共享该目标的最小采样间隔限制。
	targetKey string
}

// modelDeploymentScalerRoleWindow 是单个角色的滑动窗口。
//
// 它保存 scaler 在各采样周期之间所需的跨周期状态：稳定期历史和需求历史都是
// 基于角色最近样本的窗口，每次采样按窗口读取并追加一个新样本。将其保留在内存中，
// 可避免处于稳态的 scaler 在每个周期都向 apiserver 写入状态。
type modelDeploymentScalerRoleWindow struct {
	// stabilization 保存该角色的稳定期样本。
	stabilization *datax.RingBuffer[workercore.ModelDeploymentScalerStabilizationSample]
	// predictor 是该角色的预测器状态。
	predictor *algorithm.PredictorState
	// seeded 表示该窗口是否已从 scaler 的 status 中初始化填充。
	seeded bool
}

// modelDeploymentScalerRoleSample 表示采样器在一轮采样后掌握的单个角色状态。
//
// 它携带了观测值本身无法表达的背景信息，包括：信号未达到预期读数的原因、
// 计算速率所依据的窗口时长，以及该角色引擎容器的重启次数。能够报告等待指标原因
// 的 scaler，才能让调用方区分信号缺失与指标损坏。
type modelDeploymentScalerRoleSample struct {
	// Observation 是角色各信号所反映的观测值。
	Observation algorithm.RoleObservation
	// Reasons 说明各信号未能给出预期读数的原因。
	Reasons []string
	// WindowSeconds 是读取该角色各速率指标所使用的最短窗口时长（秒）。
	WindowSeconds float64
	// ContainerRestarts 是该角色所有引擎容器中单容器的最大重启次数。
	ContainerRestarts int32
	// ReadyPods 是在本轮采样中成功响应的就绪引擎 Pod 数量。
	ReadyPods int
}

// Forget 清除采样器中与指定 scaler 相关的全部缓存状态。
//
// 它在 scaler 被删除时调用，因为环形缓冲区以 scaler 的 UID 为键：同名重建的
// scaler 拥有不同的 UID，会使用独立的新缓冲区，旧 scaler 留下的状态只会占用内存。
//
// 目标的最小采样间隔限制会予以保留，因为该限制归属于目标而非 scaler：指向同一
// 目标的第二个 scaler 仍受此采样频率下限的约束。
func (s *modelDeploymentScalerSampler) Forget(scaler *workercore.ModelDeploymentScaler) {
	if scaler == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := string(scaler.UID)
	ring, ok := s.rings[key]
	if !ok {
		return
	}
	targetKey := ring.targetKey
	delete(s.rings, key)
	if targetKey == "" {
		return
	}
	for _, other := range s.rings {
		if other.targetKey == targetKey {
			return
		}
	}
	delete(s.lastPass, targetKey)
}

// modelDeploymentScalerRoleKind 将角色类型转换为算法层使用的角色枚举。
func modelDeploymentScalerRoleKind(kind workercore.ModelDeploymentRoleKind) algorithm.RoleKind {
	switch kind {
	case workercore.ModelDeploymentRoleKindPrefill:
		return algorithm.PrefillRole
	case workercore.ModelDeploymentRoleKindDecode:
		return algorithm.DecodeRole
	case workercore.ModelDeploymentRoleKindEncode:
		return algorithm.EncodeRole
	case workercore.ModelDeploymentRoleKindServer:
		return algorithm.ServerRole
	default:
		return algorithm.RoleKindUnknown
	}
}
