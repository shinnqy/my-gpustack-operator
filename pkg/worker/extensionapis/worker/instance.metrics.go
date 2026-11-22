package worker

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"time"

	core "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/registry/rest"
	klog "k8s.io/klog/v2"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"

	worker "gpustack.ai/gpustack/api/worker/v1"
	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/devicemanager"
	"gpustack.ai/gpustack/pkg/devicemanager/detector"
	"gpustack.ai/gpustack/pkg/deviceplugin"
	"gpustack.ai/gpustack/pkg/extensionapi"
	"gpustack.ai/gpustack/pkg/kubemetrics"
	"gpustack.ai/gpustack/pkg/utils/ctrlclix"
	"gpustack.ai/gpustack/pkg/utils/httpx"
	"gpustack.ai/gpustack/pkg/utils/json"
	"gpustack.ai/gpustack/pkg/worker/kuberess"
)

const (
	// _DeviceManagerSecurePort 是设备管理器 Web 服务的默认安全端口，
	// 在其容器端口未命名时使用；它与 chart 中 deviceManager.securePort 的默认值保持一致。
	_DeviceManagerSecurePort = 32443

	// _DeviceManagerSecurePortName 是设备管理器安全容器端口的名称；
	// 实际端口会从 Pod 规约中解析，以确保 chart 层级的 deviceManager.securePort 覆盖配置依然生效。
	_DeviceManagerSecurePortName = "https"

	// _InstanceMetricsTimeout 限制单次请求中整个指标获取操作的超时时间，
	// 包括 kubelet 读取、metrics API 回退，以及按每个已分配厂商向设备管理器拉取一次数据。
	_InstanceMetricsTimeout = 10 * time.Second

	// _MonitorSnapshotMaxBytes 限制单次读取设备管理器快照的最大字节数，
	// 该上限远高于单节点加速器指标的实际大小。
	_MonitorSnapshotMaxBytes = 4 << 20
)

// metricsLogger 记录尽力获取的加速器指标部分返回为空的原因：
// 若没有该日志，缺失 accelerators 字段将无法与“未分配加速卡”区分。
var metricsLogger = klog.Background().WithName("instance-metrics")

// deviceManagerSecurePortOf 从设备管理器的 Pod 规约中解析安全端口，
// 当端口未命名时回退到 _DeviceManagerSecurePort。
func deviceManagerSecurePortOf(pod *core.Pod) int {
	for i := range pod.Spec.Containers {
		for j := range pod.Spec.Containers[i].Ports {
			if pod.Spec.Containers[i].Ports[j].Name == _DeviceManagerSecurePortName {
				return int(pod.Spec.Containers[i].Ports[j].ContainerPort)
			}
		}
	}
	return _DeviceManagerSecurePort
}

// InstanceMetricsHandler 处理 v1.Instance 对象的 "metrics" 子资源。
//
// InstanceMetricsHandler 返回 v1.Instance 底层 Pod 的当前资源利用率——
// 通过 API-server 节点代理从节点 kubelet 实时读取（CPU/内存支持回退到 metrics.k8s.io）——
// 并合并来自设备管理器最新快照中已分配加速器的指标。
type InstanceMetricsHandler struct {
	extensionapi.GetOperation

	APIReader ctrlcli.Reader

	// MaxAge 是该处理器仍可复用节点 kubelet 读数结果的最大有效时长。由于该读数是节点级别的，
	// 若不设置此值，客户端遍历同一节点上的多个 Instance 时会对每个 Instance 重复读取同一份数据。
	// 零值表示每次请求都重新读取。
	MaxAge time.Duration
}

func newInstanceMetricsHandler(parent rest.Scoper, opts extensionapi.SetupOptions) *InstanceMetricsHandler {
	h := &InstanceMetricsHandler{
		APIReader: opts.Manager.GetAPIReader(),
		MaxAge:    kubemetrics.DefaultMaxAge,
	}

	// 作为存储。
	h.GetOperation = extensionapi.WithSubResourceGet(parent, h)

	return h
}

var (
	_ rest.Storage = (*InstanceMetricsHandler)(nil)
	_ rest.Getter  = (*InstanceMetricsHandler)(nil)
)

func (h *InstanceMetricsHandler) New() runtime.Object {
	return &worker.InstanceMetrics{}
}

func (h *InstanceMetricsHandler) Destroy() {
}

func (h *InstanceMetricsHandler) OnGet(ctx context.Context, key types.NamespacedName, _ ctrlcli.GetOptions) (runtime.Object, error) {
	// 限制整个操作的超时时间，包括下方的对象读取。
	ctx, cancel := context.WithTimeout(ctx, _InstanceMetricsTimeout)
	defer cancel()

	// 获取底层 Instance。
	inst := &workercore.Instance{}
	err := h.APIReader.Get(ctx, key, inst, ctrlclix.WithoutQuorum)
	if err != nil {
		return nil, err
	}
	// 获取当前 Instance 对应的底层 Pod。尚未创建的 Pod 以及仍属于上一个生命周期实例的 Pod，
	// 均视为“当前 Instance 没有 Pod”：绝不能将上一个实例的指标数据用于当前实例。
	pod, err := h.backingPod(ctx, key, inst)
	if err != nil {
		return nil, err
	}

	// 前置门槛：只要 Instance 中没有任何容器启动，就不会消耗资源，因此按逻辑其用量为零。
	// 该判断位于下方所有读取操作（kubelet、metrics API 及各设备管理器）之前，
	// 因为在此状态下采集只会读到相邻实例的数据或返回 503，且客户端不应为处于启动中或已停止的 Instance 编写额外分支逻辑。
	if !kubemetrics.HasStartedContainer(pod) {
		return newInstanceMetrics(inst, kubemetrics.NewUnstartedSample(pod, inst.Spec.Resources)), nil
	}

	// 获取底层 Pod 当前的 CPU/内存/存储用量。返回的 error 中已包含所有失败来源及原因，
	// 因此可直接原样返回给客户端。这是唯一仍返回服务不可用错误的场景：
	// 容器确实已启动，因此用量属于真实测量值，但没有任何数据源能够采集到它。
	sample, err := kubemetrics.FetchPodSample(ctx, inst.Status.NodeName, pod, h.MaxAge)
	if err != nil {
		return nil, kerrors.NewServiceUnavailable(err.Error())
	}

	// 尽力合并已分配加速器的最新指标：
	// 上方的 Pod 用量为主结果，设备管理器读取失败仅会省略加速器指标部分。
	allocations, aerr := deviceplugin.AllocatedAcceleratorsOf(pod)
	if aerr != nil {
		metricsLogger.Error(aerr, "reading the allocated accelerators", "instance", key)
	}
	if allocGroups := allocations.Aggregate().Groups; len(allocGroups) != 0 {
		sample.Accelerators = h.currentAcceleratorMetrics(
			ctx, inst.Status.NodeName, allocGroups, kubemetrics.NewAcceleratorGrants(pod, allocations),
		)
	}

	return newInstanceMetrics(inst, sample), nil
}

// backingPod 返回当前 Instance 对应的底层 Pod；当 Instance 没有对应 Pod 时返回 nil：
// 包括尚未调度到任何节点、尚未生成 Pod，或仅存在上一个生命周期实例的 Pod——后者不应由当前 Instance 测量，
// 也绝不能冒充当前 Instance 的数据返回。
//
// 返回 nil 的 Pod 是一种正常状态而非错误。Instance 自身的声明仍记录了其请求的资源，
// 因此调用方会据此生成响应，避免让每个客户端都单独处理 503 分支。
func (h *InstanceMetricsHandler) backingPod(
	ctx context.Context,
	key types.NamespacedName,
	inst *workercore.Instance,
) (*core.Pod, error) {
	if inst.Status.NodeName == "" {
		return nil, nil
	}

	pod := &core.Pod{}
	err := h.APIReader.Get(ctx, key, pod, ctrlclix.WithoutQuorum)
	if err != nil {
		if kerrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	if pod.Labels[deviceplugin.InstancePartOfLabelKey] != string(inst.UID) {
		return nil, nil
	}
	return pod, nil
}

// newInstanceMetrics 将采样结果包装为对外返回的对象，以 Instance 作为标识，而非以产生数据的底层对象作为标识。
func newInstanceMetrics(
	inst *workercore.Instance,
	sample *worker.InstanceMetricsSample,
) *worker.InstanceMetrics {
	return &worker.InstanceMetrics{
		ObjectMeta: meta.ObjectMeta{
			Namespace: inst.Namespace,
			Name:      inst.Name,
			UID:       inst.UID,
		},
		Sample: *sample,
	}
}

// currentAcceleratorMetrics 读取设备管理器的最新快照，并按已分配的设备 ID 进行过滤。
// 任何失败都会降级为不返回加速器指标部分。
func (h *InstanceMetricsHandler) currentAcceleratorMetrics(
	ctx context.Context,
	nodeName string,
	allocGroups []workercore.DevicesAllocationGroup,
	grants *kubemetrics.AcceleratorGrants,
) []worker.InstanceAcceleratorMetrics {
	pods, err := h.listNodeDeviceManagerPods(ctx, nodeName)
	if err != nil {
		metricsLogger.Error(err, "listing device manager pods", "node", nodeName)
		return nil
	}

	// chart 会为每个厂商部署一个独立的设备管理器 DaemonSet 并传入 --manufacturer 参数，
	// 因此每个快照仅包含对应厂商的加速卡：
	// 应按每个已分配厂商分别读取快照，而不是只挑选单个 Pod。
	var accelerators []worker.InstanceAcceleratorMetrics
	for _, manufacturer := range allocatedManufacturers(allocGroups) {
		pod := selectDeviceManagerPod(pods, manufacturer)
		if pod == nil {
			metricsLogger.V(2).Info("no ready device manager pod",
				"node", nodeName, "manufacturer", manufacturer)
			continue
		}

		url := "https://" + net.JoinHostPort(pod.Status.PodIP, strconv.Itoa(deviceManagerSecurePortOf(pod))) +
			devicemanager.MonitorSnapshotPath
		snapshot, ferr := fetchMonitorSnapshot(ctx, url)
		if ferr != nil {
			metricsLogger.Error(ferr, "reading device manager snapshot",
				"node", nodeName, "manufacturer", manufacturer, "pod", ctrlcli.ObjectKeyFromObject(pod))
			continue
		}
		accelerators = append(accelerators,
			filterAllocatedAcceleratorMetrics(snapshot, allocGroups, grants)...)
	}
	return accelerators
}

// allocatedManufacturers 按分配记录中的原始顺序，返回去重后的厂商列表。
func allocatedManufacturers(allocGroups []workercore.DevicesAllocationGroup) []string {
	seen := make(map[string]struct{}, len(allocGroups))
	manufacturers := make([]string, 0, len(allocGroups))
	for i := range allocGroups {
		manufacturer := allocGroups[i].Manufacturer
		if _, ok := seen[manufacturer]; ok {
			continue
		}
		seen[manufacturer] = struct{}{}
		manufacturers = append(manufacturers, manufacturer)
	}
	return manufacturers
}

// filterAllocatedAcceleratorMetrics 将快照中已分配设备的指标映射到 API 类型。
// 过滤逻辑本身（包括过期时间上限以及厂商和 ID 的匹配）属于生成快照的设备管理器，
// 且与其自身的 exporter 共享，以确保两个入口不会为同一个 Instance 上报不同的加速卡。
// 出于同样原因，每条指标具体内容的解析也由 grants 索引统一负责。
func filterAllocatedAcceleratorMetrics(
	snapshot *devicemanager.MonitorSnapshot,
	allocGroups []workercore.DevicesAllocationGroup,
	grants *kubemetrics.AcceleratorGrants,
) []worker.InstanceAcceleratorMetrics {
	allocated := detector.AllocatedAcceleratorMetricsOf(snapshot, allocGroups)
	if len(allocated) == 0 {
		// 返回 nil 而非空切片，不过在响应中两者并无区别：该字段带有 omitempty 标签，两者都会被省略。
		// 因此响应不会区分“该 Instance 未持有加速卡”与“无法读取设备管理器”——两者最终都不会携带加速器字段。
		return nil
	}

	accelerators := make([]worker.InstanceAcceleratorMetrics, 0, len(allocated))
	for i := range allocated {
		// 一张加速卡可能对应多条记录：持有同一张卡上两个硬件分区的 Instance
		// 会拥有两个授权（grant），各自具有独立的标识和指标数据。
		accelerators = append(accelerators, grants.Resolve(
			snapshot.Slices, allocated[i].Manufacturer, &allocated[i].Metrics,
		)...)
	}
	return accelerators
}

// selectDeviceManagerPod 返回服务指定厂商且处于 Ready、非终止状态的设备管理器 Pod；若不存在则返回 nil。
// 其他厂商的 Pod 绝不能作为替代：其快照中不可能包含所查询的加速卡。
// 若厂商标签未设置（即早于该标签引入前的分配记录），则接受任意设备管理器 Pod。
func selectDeviceManagerPod(pods []core.Pod, manufacturer string) *core.Pod {
	for i := range pods {
		pod := &pods[i]
		if pod.DeletionTimestamp != nil || pod.Status.PodIP == "" || !deviceplugin.IsPodReady(pod) {
			continue
		}
		if manufacturer != "" && pod.Labels[deviceplugin.ManufacturerLabelKey] != manufacturer {
			continue
		}
		return pod
	}
	return nil
}

// listNodeDeviceManagerPods 列出系统 Namespace 中位于指定节点上的设备管理器 Pod。
func (h *InstanceMetricsHandler) listNodeDeviceManagerPods(ctx context.Context, nodeName string) ([]core.Pod, error) {
	podList := &core.PodList{}
	err := h.APIReader.List(ctx, podList,
		ctrlcli.InNamespace(kuberess.SystemNamespaceName),
		ctrlcli.MatchingLabels{deviceplugin.ComponentLabelKey: deviceplugin.DeviceManagerComponent},
		ctrlcli.MatchingFieldsSelector{Selector: fields.OneTermEqualSelector("spec.nodeName", nodeName)},
		ctrlclix.WithoutQuorum,
		ctrlcli.UnsafeDisableDeepCopy,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to list device manager pods on node %s: %w", nodeName, err)
	}
	return podList.Items, nil
}

// _monitorSnapshotClient 是用于读取设备管理器数据的共享客户端：整个进程复用一个 keep-alive transport，
// 避免每次请求都创建并泄漏连接。
var _monitorSnapshotClient = &http.Client{
	Transport: httpx.Transport(httpx.TransportOptions().WithoutProxy().WithTLSClientConfig(
		&tls.Config{InsecureSkipVerify: true}, // nolint: gosec
	)),
}

// fetchMonitorSnapshot 通过 GET 请求获取并解码设备管理器 Pod 的监控快照读数。
// 操作超时由调用方控制。
//
// 设备管理器默认使用自签名证书，因此在同一信任域内跳过证书校验（关于 mTLS 后续方案见设计文档的 Risks 章节）。
// 同时显式禁用代理：项目的 transport 辅助函数会读取代理环境变量，但绝不能因此重定向 Pod 间流量。
func fetchMonitorSnapshot(ctx context.Context, url string) (*devicemanager.MonitorSnapshot, error) {
	client := _monitorSnapshotClient

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create monitor snapshot request: %w", err)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	// 由于跳过了证书校验，对端身份尚未确认：因此同时限制解码与 keep-alive 排空的读取上限，而不假定对端会主动停止发送。
	body := io.LimitReader(resp.Body, _MonitorSnapshotMaxBytes)
	defer func() {
		_, _ = io.Copy(io.Discard, body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status %s", resp.Status)
	}

	snapshot := &devicemanager.MonitorSnapshot{}
	if err = json.NewDecoder(body).Decode(snapshot); err != nil {
		return nil, fmt.Errorf("failed to decode monitor snapshot: %w", err)
	}
	return snapshot, nil
}
