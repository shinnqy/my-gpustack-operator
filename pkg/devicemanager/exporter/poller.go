// Package exporter 会采样此节点上运行的 Instances，并将它们作为仪表盘在 Device Manager 现有的 /metrics 端
// 点上发布。
//
// 采样是一个后台循环，而非抓取时刻的工作，原因有二。抓取操作绝不能执行 I/O 操作——它不得阻塞，也不得因某个
// 源不可访问而导致整个端点失败。此外，它也无法提供最新数据：kubelet 自身的维护任务大约每十秒运行一次，其
// 提供的摘要也已被缓存，因此在抓取时刻读取这些数据，与在相似时间段进行轮询相比，并无优势。
package exporter

import (
	"context"
	"errors"
	"time"

	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	klog "k8s.io/klog/v2"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"

	worker "gpustack.ai/gpustack/api/worker/v1"
	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/deviceplugin"
	"gpustack.ai/gpustack/pkg/kubemetrics"
	"gpustack.ai/gpustack/pkg/system"
	"gpustack.ai/gpustack/pkg/utils/datax"
	"gpustack.ai/gpustack/pkg/utils/osx"
	"gpustack.ai/gpustack/pkg/utils/waitx"
)

// InstanceSample 是指一个 Instance 的利用率以及用于标识它的名称，而 worker.InstanceMetricsSample 并不包含
// 这些信息。
type InstanceSample struct {
	// Namespace、`Name`和`UID`用于标识 Instance，而非其背后的 Pod。`UID`用于防止同名被删除后重新创建
	// 的 Instance 继续延续之前实例的系列。
	Namespace string
	Name      string
	UID       types.UID

	Sample worker.InstanceMetricsSample

	// 加速器是分配给 Instance 的设备，由其作为后端的 Pod 记录这些设备。这里仅保留分配信息。这些数字来
	// 自 Device Manager 自身的监控快照，当抓取数据与两者结合时，这些数据与那个快照一样最新，而非本轮
	// 数据。
	//
	// 对于没有任何容器已启动的 Instance 来说，它是空的：它什么都没有，还不包含任何卡片，因此卡片上的
	// 内容属于其他人。
	Accelerators []workercore.DevicesAllocationGroup

	// Grants 记录了每个分配的加速器上 Pod 所获得的权益，因此 scrape 可以依据 Instance 自身的条款来展
	// 示所有数据。当加速器为空时，该值为 nil。
	//
	// 索引在此处构建，即 Pod 所在的位置，因为逻辑切片的计算限制是从该容器上施加的分配中读取的——而上述
	// Pod 级视图无法说明这一点。
	Grants *kubemetrics.AcceleratorGrants
}

// 快照是成功一轮的衡量标准。
type Snapshot struct {
	// 时间戳是指该轮次被存储的时间。
	Timestamp time.Time
	// UsageMeasured 报告节点 kubelet 是否响应了此轮操作。如果未响应，下方的实例将使用其声明的总量，而
	// 不包含测量数据——该轮操作仍然具有价值，因为加速器资源并非来自 kubelet。
	UsageMeasured bool
	// 导出报告，说明是否该 Device Manager 是发布节点按 Instance 划分数据的主体。有关规则，请参见(*
	// Poller).exporting。
	Exporting bool
	// Instances 中每个 Instance 对应一个条目，代表节点 kubelet 的测量值。如果节点没有运行任何实例，则
	// 为空。
	Instances []InstanceSample
}

// Round 保存采集器最近一轮的结果。测量值与本轮状态一起存储，读取方看到的是同一轮的数据。
type Round struct {
	// Duration 记录本轮采集耗时，即使采集失败也保留。持续变慢可能比第一次报错更早出现。
	Duration time.Duration
	// 快照是该轮次所测量的结果，若失败则为零。失败的轮次不会保留任何内容：将几个时期前的数字当作当前
	// 数据报告，不如不报告。
	Snapshot *Snapshot
}

// Poller 会定期采样该节点的 Instances，仅保留最新的一轮数据，因此抓取操作只占用内存，不消耗其他资源。
type Poller struct {
	reader   ctrlcli.Reader
	nodeName string
	period   time.Duration

	round datax.Snapshot[Round]

	// lastFailure 和 lastRoleFailure 记录了上一轮失败及之前失败的选举情况，因此若发生重复情况，会默默
	// 记录下来。如果统计器源端中断，则会在整个进程运行期间每过一段时间就重复显示一行。只有统计循环会
	// 访问它们，因此不需要同步。
	lastFailure     string
	lastRoleFailure string
}

// New 返回该节点的 Instances 的 poller。
func New(c *Config) (*Poller, error) {
	if c.MonitorPeriod <= 0 {
		// waitx 在周期上进行轮询，因此零一会持续运行而非进行采样。
		return nil, errors.New("monitor period must be positive")
	}

	reader := c.ClientReader
	if reader == nil {
		// pkg/manager 在设备管理器应用自身配置之前，将 Device Manager 的运行时配置应用于系统包中。
		// 通过它读取 Pods 会触发该进程对设备插件进行查找——该进程会监控此节点的每个 Pod 并按节点名
		// 称对其进行索引——而不是像每次节点周期都针对 API 服务器列出列表那样操作。
		reader = system.LoopbackCtrlClient.Get()
	}

	return &Poller{
		reader:   reader,
		nodeName: osx.Getenv("KUBERNETES_NODE_NAME"),
		period:   c.MonitorPeriod,
	}, nil
}

// LastRound 返回最新的已完成轮次，在第一个轮次之前为 nil。调用者不得修改它。
func (p *Poller) LastRound() *Round {
	return p.round.Load()
}

// 开始轮询，直到上下文被取消。失败的轮次不会停止循环，也不会导致进程失败：这些指标并非关键路径，如果
// Device Manager 因 kubelet 短暂无法访问而退出，该节点上的设备插件也会随之丢失。
func (p *Poller) Start(ctx context.Context) error {
	if p.nodeName == "" {
		// DaemonSet 会根据 spec.nodeName 设置此值，因此空值表示部署失败。请报告此问题，不要停止进
		// 程，而是提供 Instance 指标。
		logger.Error(nil, "not polling instance metrics, KUBERNETES_NODE_NAME is not set")
		return nil
	}

	return waitx.RepeatUntilContextCancel(ctx, p.period, true, func(ctx context.Context) error {
		p.poll(ctx)
		return nil
	})
}

var logger = klog.Background().WithName("instance-metrics-exporter")

// 轮询运行一轮：从 informer 中读取该节点的 Instance 容器，询问 kubelet 对其的测量结果，并保存结果。
func (p *Poller) poll(ctx context.Context) {
	started := time.Now()

	snapshot, err := p.measure(ctx)
	if err != nil {
		// 什么都不保留：仅报告几个时期前的数据作为当前数据，不如不报告且附上失败指标。
		p.round.Store(&Round{Duration: time.Since(started)})
		p.logFailure(err)
		return
	}

	// 只要完成一轮所有数据的校验，就可以解除重复日志保护：即使出现 kubelet 读取失败，只要完成一轮仍会
	// 被记录，因为该失败情况已被上报。如果忘记这一点，每次周期都会记录相同的错误。
	if snapshot.UsageMeasured {
		p.lastFailure = ""
	}
	p.round.Store(&Round{Duration: time.Since(started), Snapshot: snapshot})
}

// measure 执行一回合的读取操作。
func (p *Poller) measure(ctx context.Context) (*Snapshot, error) {
	pods, err := p.instancePods(ctx)
	if err != nil {
		return nil, err
	}

	// 失败的选举只会损害 Pod 级家庭的利益，不会带来其他影响：加速器数据并不受此规则限制，因为不同制造
	// 商的`ID`是互不重复的。如果无法完成这一轮选举，那些数据将失去其相关性，因为该选举本身没有发言权。
	exporting, err := p.exporting(ctx)
	if err != nil {
		p.logRoleFailure(err)
		exporting = false
	}

	// 门：一个没有开始运行的 Instance，其容器也不消耗任何资源，因此根据逻辑而非实际测量，其使用量为零。
	// 它位于下方的读取操作之上，而非其旁边——测量一个没有开始运行的 Instance 时，只能将邻居的数值当作
	// 自己的数值——而那些所有 Instance 都未开始运行的节点则完全不执行任何读取操作。
	started, unstarted := splitOnStartedContainer(pods)
	if len(started) == 0 {
		return &Snapshot{
			Timestamp: time.Now(),
			// 没有遗漏任何测量内容：通过推理可知每个 Instance 的使用情况。
			UsageMeasured: true,
			Exporting:     exporting,
			Instances:     unstartedSamples(unstarted),
		}, nil
	}

	// 重新读取，不要从缓存中获取：轮次之间恰好间隔一个周期，而条目在中间轮次被标记，因此在这里经过该
	// 周期会反映前一轮的读取结果——节奏减半，没有数据可以显示出来。
	samples, err := kubemetrics.FetchPodSamples(ctx, p.nodeName, podsOf(started), 0)
	if err != nil {
		// kubelet 是两个来源之一，另一个则未受此影响。节点运行的实例以及每个实例所包含的设备信息
		// 来自 informer，因此那些 kubelet 不参与的加速器系列仍然拥有所需的一切。保留原始数据，携
		// 带已声明的总数，让测量数值在归零的成功指标旁不出现。
		p.logFailure(err)
		return &Snapshot{
			Timestamp: time.Now(),
			Exporting: exporting,
			Instances: append(declaredSamples(started), unstartedSamples(unstarted)...),
		}, nil
	}

	return &Snapshot{
		Timestamp:     time.Now(),
		UsageMeasured: true,
		Exporting:     exporting,
		Instances:     append(instanceSamples(started, samples), unstartedSamples(unstarted)...),
	}, nil
}

// splitOnStartedContainer 会在门控系统的两个面上将此节点的 Instance 容器分区，因此，每次轮次中，所测量的
// 内容以及通过推理得出的内容都将被确定。
func splitOnStartedContainer(pods []instancePod) (started, unstarted []instancePod) {
	for i := range pods {
		if kubemetrics.HasStartedContainer(pods[i].pod) {
			started = append(started, pods[i])
			continue
		}
		unstarted = append(unstarted, pods[i])
	}
	return started, unstarted
}

// logFailure 会记录一次一轮的失败，并安静地重复该行为。
func (p *Poller) logFailure(err error) {
	if reason := err.Error(); reason != p.lastFailure {
		p.lastFailure = reason
		logger.Error(err, "polling instance metrics", "node", p.nodeName)
		return
	}
	logger.V(2).Info("polling instance metrics still failing",
		"node", p.nodeName, "reason", p.lastFailure)
}

// logRoleFailure 会报告一次选举失败，随后会安静地重复此事件。它独立于某轮失败而被追踪，因此不会使另一次的
// 报告被掩盖。
func (p *Poller) logRoleFailure(err error) {
	if reason := err.Error(); reason != p.lastRoleFailure {
		p.lastRoleFailure = reason
		logger.Error(err, "deciding whether to export instance metrics, not exporting them",
			"node", p.nodeName)
		return
	}
	logger.V(2).Info("still cannot decide whether to export instance metrics",
		"node", p.nodeName, "reason", p.lastRoleFailure)
}

// instancePod 将一个 Instance 容器与其背后的 Instance 身份关联起来。
type instancePod struct {
	pod      *core.Pod
	instance InstanceSample
}

// instancePods 返回该节点下的 Instance 工作负载，每个 Instance 最多只能有一个。
func (p *Poller) instancePods(ctx context.Context) ([]instancePod, error) {
	podList := &core.PodList{}
	err := p.reader.List(ctx, podList,
		ctrlcli.MatchingFieldsSelector{
			Selector: fields.OneTermEqualSelector(deviceplugin.IndexingPodsByNodeName, p.nodeName),
		},
	)
	if err != nil {
		return nil, err
	}

	// 一个 Instance 在缓存中可能短暂地同时包含两个 Pod，当其中一个被替换时，两个 Pod 都会携带相同的标
	// 签集——请保留较新的那个，因此一个收集过程绝不会产生重复项。
	byInstance := make(map[types.UID]instancePod, len(podList.Items))
	for i := range podList.Items {
		pod := &podList.Items[i]
		inst, ok := instanceOf(pod)
		if !ok {
			continue
		}
		if kept, dup := byInstance[inst.UID]; dup && !isNewer(pod, kept.pod) {
			continue
		}
		byInstance[inst.UID] = instancePod{pod: pod, instance: inst}
	}

	pods := make([]instancePod, 0, len(byInstance))
	for _, ip := range byInstance {
		pods = append(pods, ip)
	}
	return pods, nil
}

// instanceOf 报告了 Pod 所依赖的 Instance 类型，如果 Pod 不是该类型则返回 false。
//
// 控制器 ownerReference 用于命名 Instance。app.kubernetes.io/part-of 标签会重复其 UID，并据此进行验证，因
// 为正是该标签使得 Instance 指标子资源能够解析到 Pod，这两个层面必须就哪个 Pod 属于哪个 Instance 达成一致。
// 一个终止的 Pod 则不是这样的：它的状态正在走向结束。
func instanceOf(pod *core.Pod) (InstanceSample, bool) {
	if pod.DeletionTimestamp != nil {
		return InstanceSample{}, false
	}

	ref := meta.GetControllerOf(pod)
	if ref == nil || ref.Kind != _InstanceKind ||
		schema.FromAPIVersionAndKind(ref.APIVersion, ref.Kind).Group != workercore.GroupName {
		return InstanceSample{}, false
	}
	if pod.Labels[deviceplugin.InstancePartOfLabelKey] != string(ref.UID) {
		return InstanceSample{}, false
	}

	return InstanceSample{
		Namespace: pod.Namespace,
		Name:      ref.Name,
		UID:       ref.UID,
	}, true
}

// _InstanceKind 是 Instance Pod 所携带的所属类型。
const _InstanceKind = "Instance"

// isNewer 用于指示 a 是在 b 之后创建的，会回退到 pod 名称，因此同一秒内被标记为 two 的 pod 在每次轮询中仍
// 会以相同方式进行解析。
func isNewer(a, b *core.Pod) bool {
	if !a.CreationTimestamp.Equal(&b.CreationTimestamp) {
		return a.CreationTimestamp.After(b.CreationTimestamp.Time)
	}
	return a.Name > b.Name
}

// podsOf 将对应的 pod 指向 kubelet 读取的内容。
func podsOf(pods []instancePod) []*core.Pod {
	out := make([]*core.Pod, 0, len(pods))
	for i := range pods {
		out = append(out, pods[i].pod)
	}
	return out
}

// instanceSamples 将身份信息与测量数据关联起来，删除了 kubelet 未考虑的 Instance，同时也未将其发布，因此
// 没有产生任何使用效果。
func instanceSamples(
	pods []instancePod,
	samples map[types.UID]*worker.InstanceMetricsSample,
) []InstanceSample {
	out := make([]InstanceSample, 0, len(samples))
	for i := range pods {
		sample, ok := samples[pods[i].pod.UID]
		if !ok {
			continue
		}
		out = append(out, instanceSampleOf(&pods[i], sample))
	}
	return out
}

// declaredSamples 是 instanceSamples，用于那一轮 kubelet 读取失败的情况：每个 Instance 都保留下来，仅包含
// 其 Pod 所声明的内容。若在此处移除它们，对于 kubelet 响应但未保留内容的 pod 来说则符合规则——这是另一回事。
// 本次轮次没有任何数据被测量，而加速器相关类别仍需要这些条目所包含的分配资源。
func declaredSamples(pods []instancePod) []InstanceSample {
	out := make([]InstanceSample, 0, len(pods))
	for i := range pods {
		out = append(out, instanceSampleOf(&pods[i], kubemetrics.NewSample(pods[i].pod)))
	}
	return out
}

// unstartedSamples 即为 instanceSamples，针对门关闭时的实例：每个使用情况的声明总数和测量零值，且完全没有
// 加速器——与指标子资源为同一 Instance 所给出的答案逐字段一致。
func unstartedSamples(pods []instancePod) []InstanceSample {
	out := make([]InstanceSample, 0, len(pods))
	for i := range pods {
		inst := pods[i].instance
		// Pod 已准备就绪，因此其容器限制即为总限制。Instance 自身的 spec.resources 则是子资源回退
		// 的界面，该资源无法触及。
		inst.Sample = *kubemetrics.NewUnstartedSample(pods[i].pod, nil)
		out = append(out, inst)
	}
	return out
}

// instanceSampleOf 将某个 Instance 的身份与样本及其 Pod 的分配绑定在一起。
func instanceSampleOf(pod *instancePod, sample *worker.InstanceMetricsSample) InstanceSample {
	inst := pod.instance
	inst.Sample = *sample

	allocations, err := deviceplugin.AllocatedAcceleratorsOf(pod.pod)
	if err != nil {
		// 无法阅读的注释名称不指向任何设备：上述容器级数据不受影响，而关于其可能包含的内容进行描
		// 述则属于猜测。
		logger.V(2).Error(err, "reading a pod's allocated accelerators",
			"pod", pod.pod.Namespace+"/"+pod.pod.Name)
	}
	inst.Accelerators = allocations.Aggregate().Groups

	// 当 Pod 中没有任何加速器时，就处于空状态——这既是该领域的既定特性，也是收集器所理解的“此处没有任
	// 何内容可以说明卡片数值的归属”。如果没有任何分配记录，索引将无响应，而该检查过程也会因此中断，因
	// 此它保护的防护机制将永远无法再次触发。
	if len(inst.Accelerators) > 0 {
		inst.Grants = kubemetrics.NewAcceleratorGrants(pod.pod, allocations)
	}
	return inst
}
