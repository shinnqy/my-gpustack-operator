package detector

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"os"
	"slices"
	"strings"
	"time"

	core "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	klog "k8s.io/klog/v2"
	nfd "sigs.k8s.io/node-feature-discovery/api/nfd/v1alpha1"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/device"
	"gpustack.ai/gpustack/pkg/devicemanager/kuberess"
	"gpustack.ai/gpustack/pkg/devicemanager/procattr"
	"gpustack.ai/gpustack/pkg/kubeclientset"
	"gpustack.ai/gpustack/pkg/kubemeta"
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/system"
	"gpustack.ai/gpustack/pkg/systemname"
	"gpustack.ai/gpustack/pkg/utils/datax"
	"gpustack.ai/gpustack/pkg/utils/mapx"
	"gpustack.ai/gpustack/pkg/utils/osx"
	"gpustack.ai/gpustack/pkg/utils/slicex"
	"gpustack.ai/gpustack/pkg/utils/stringx"
	"gpustack.ai/gpustack/pkg/utils/waitx"
)

var logger = klog.Background().WithName("detector")

// MonitorSnapshot 保存设备管理器最近一次采集的加速器指标。整个快照有独立的采样时间，
// 方便读取方统一判断新鲜度。
type MonitorSnapshot struct {
	// Timestamp 是保存快照的时间，首次采样前为零值。
	Timestamp time.Time `json:"timestamp"`
	// PeriodSeconds 记录采样时实际采用的周期，供读取方按周期判断数据是否过期。
	PeriodSeconds int64 `json:"periodSeconds"`
	// Groups 保存最近一次设备指标，首次采样前为空。
	Groups device.MetricsGroupList `json:"groups"`
	// Slices 保存各 Instance 在整卡指标中所占的份额。没有测量结果时省略。存在但为空代表本轮确实读到了空集合。
	// SchemaVersion 供读取方判断生产端是否支持该结构。
	Slices *MonitorSliceSection `json:"slices,omitempty"`
}

// Detector 依次采集各厂商的硬件清单，并分别保留最近一次成功的结果。
type Detector struct {
	noPCICheck   bool
	detectors    []device.Detector // 装着选中的厂商探测器
	lastDetected map[string]device.DevicesGroupList

	manufacturers           sets.Set[string]
	noFastFailed            bool
	monitorPeriod           time.Duration
	monitorSnapshot         datax.Snapshot[MonitorSnapshot]
	detectedManufacturersCh chan<- sets.Set[string]
	// reportedInterfaces 保存最近一次成功上报时枚举到的网卡，供监控循环比较变化。比较时会忽略首次发现
	// 故障的时间戳。
	//
	// 完整探测和监控回调在同一个 goroutine 中顺序执行，因此这里和 lastDetected 一样无需单独加锁。
	reportedInterfaces []workercore.DeviceInterface
	// reportedInterfacesKnown 表示是否已有一次成功的接口枚举。成功枚举到零个接口也算有基准，首次读取
	// 失败时则应继续重试。
	reportedInterfacesKnown bool
	// podLister 列出本节点的 Pod，procResolver 根据宿主机进程查出所属 Pod 和容器。把这两项依赖保存为
	// 字段，也便于在单元测试中单独控制它们。
	podLister    func(ctx context.Context) ([]core.Pod, error)
	procResolver *procattr.Resolver
}

// New 根据节点平台和请求的厂商构造探测器。在 WSL 环境自动跳过 PCI 预检查。
func New(c *Config) (*Detector, error) {
	// osx.GetRelease() 通过 uname 取得 Linux 内核版本字符串
	isWsl := stringx.ContainSubstrings(osx.GetRelease(), "microsoft", "wsl")
	noPCICheck := c.NoPCICheck || isWsl

	// 求交集。比如用户请求 {nvidia, ascend}，Linux 这份程序包含这两家，结果就是这两家。
	manufacturers := getAllowedManufacturers(c.Manufacturers)

	// [是 detector_linux.go 里的那个方法] 再从表里取出 nvidia.New 和 ascend.New
	creators := getAllowedDetectorCreators(manufacturers)
	createOpts := device.DetectorOptions{
		Logger: logger.V(3),
	}
	detectors := make([]device.Detector, 0, len(creators))
	for i := range creators {
		detectors = append(detectors, creators[i](createOpts))
	}

	d := &Detector{
		noPCICheck:              noPCICheck,
		manufacturers:           manufacturers,
		noFastFailed:            c.NoFastFailed,
		detectors:               detectors,
		monitorPeriod:           c.MonitorPeriod,
		detectedManufacturersCh: c.DetectedManufacturersCh,
	}
	d.podLister = d.listNodePods
	// Device Manager 使用 hostPID。它读取的 /proc 与厂商驱动报告 PID 时看到的是同一批宿主机进程。
	d.procResolver = procattr.New(os.DirFS("/proc"))
	return d, nil
}

// DetectAccelerator 逐厂商执行一次探测，返回合并后的设备组和本轮未能测量的厂商。
// 读取失败时深拷贝上一次成功结果，使下游不会因一次驱动异常而撤销已有库存。首次失败的厂商没有历史数据。
// 成功返回空清单才清掉该厂商的缓存，避免已拔出的卡在后续故障时重新出现。
// 本方法持有探测循环的跨轮状态，同一探测器上的调用应串行。
func (d *Detector) DetectAccelerator(
	_ context.Context,
) (grpListMerged device.DevicesGroupList, unmeasured sets.Set[string]) {
	unmeasured = sets.New[string]()

	for i := range d.detectors {
		manufacturer := d.detectors[i].Name()
		logger := logger.V(2).WithValues("manufacturer", manufacturer)

		grpList, err := d.detectors[i].DetectAccelerator(d.noPCICheck)
		if err != nil {
			logger.Error(err, "detect accelerators")
			unmeasured.Insert(manufacturer)
			if lastGrpList := d.lastDetected[manufacturer]; len(lastGrpList) != 0 {
				logger.Info("reporting the accelerators last detected")
				grpListMerged = append(grpListMerged, cloneDeviceGroups(lastGrpList)...)
			}
			continue
		}
		if len(grpList) == 0 {
			delete(d.lastDetected, manufacturer)
			continue
		}

		if d.lastDetected == nil {
			d.lastDetected = make(map[string]device.DevicesGroupList, len(d.detectors))
		}
		d.lastDetected[manufacturer] = grpList

		grpListMerged = append(grpListMerged, grpList...)
		logger.Info("detected accelerators")
	}

	return grpListMerged, unmeasured
}

// MonitorAccelerator 读取各厂商的实时指标，同时返回本轮读取失败的厂商集合。
//
// 本轮失败的厂商只记录在未测量集合中。沿用旧的指标会给旧数据打上新时间戳。调用者通过失败集合可以区分读不
// 到指标和设备真的消失。
func (d *Detector) MonitorAccelerator(
	_ context.Context,
) (grpListMerged device.MetricsGroupList, unmeasured sets.Set[string]) {
	unmeasured = sets.New[string]()

	for i := range d.detectors {
		manufacturer := d.detectors[i].Name()
		logger := logger.V(2).WithValues("manufacturer", manufacturer)

		grpList, err := d.detectors[i].MonitorAccelerator(d.noPCICheck)
		if err != nil {
			logger.Error(err, "monitor accelerators")
			unmeasured.Insert(manufacturer)
			continue
		}
		if len(grpList) == 0 {
			continue
		}

		grpListMerged = append(grpListMerged, grpList...)
		logger.Info("monitored accelerators")
	}

	return grpListMerged, unmeasured
}

// MonitorSnapshot 返回最近一次成功保存的加速器指标快照，首次采样前返回 nil。
func (d *Detector) MonitorSnapshot() *MonitorSnapshot {
	return d.monitorSnapshot.Load()
}

// _DeviceKey 将厂商、设备 ID 和健康状态组合成可比较的键。
//
// 比较两轮的键可以发现新卡、消失的卡或健康状态变化，据此决定何时重新做完整探测。
type _DeviceKey struct {
	Manufacturer string
	ID           string
	Unhealthy    bool
}

// detectEveryNMonitorRounds 限定两次完整探测之间最多经过多少轮监控。
//
// 监控能直接发现设备集合和网卡状态变化。互联域等属性只有完整探测才能读到，因此也要定期重新探测。按轮数计
// 算后，最长间隔会随 monitorPeriod 调整。
//
// 默认每 15 秒监控一次，40 轮约为 10 分钟。重新接线或升级驱动后会刷新这些低频变化的属性，无需每轮都重新枚
// 举所有端点。
const detectEveryNMonitorRounds = 40

// Start 按配置的周期探测设备、采样指标，直到上下文被取消。
func (d *Detector) Start(ctx context.Context) error {
	// 保护单厂商进程的首次启动
	holdUntilFirstDetected := !d.noFastFailed && d.manufacturers.Len() == 1

	return waitx.RepeatUntilContextCancel(ctx, d.monitorPeriod, true, func(ctx context.Context) error {
		logger.V(2).Info("detecting")

		// -- 外层只调用统一的 DetectAccelerator 方法，到了里面，NVIDIA 和 Ascend 才各自找自己的驱动。
		devicesGrpList, unmeasuredByDetect := d.DetectAccelerator(ctx)

		// 记录本轮看到的厂商，以及每张卡的厂商、ID 和健康状态。监控时用它们判断是否需要重新探测。
		deviceKeys := sets.New[_DeviceKey]()
		manufacturers := sets.New[string]()
		if len(devicesGrpList) != 0 {
			for i := range devicesGrpList {
				manufacturers.Insert(devicesGrpList[i].Manufacturer)
				for j := range devicesGrpList[i].Accelerators {
					// -- 给后续监控保存一个比较基准
					deviceKeys.Insert(_DeviceKey{
						devicesGrpList[i].Manufacturer,
						devicesGrpList[i].Accelerators[j].ID,
						devicesGrpList[i].Accelerators[j].Status.Unhealthy,
					})
				}
			}
		}

		// 只指定一个厂商时，DaemonSet 已根据 PCI 厂商标签选中了这台节点。第一次读不到卡可能是驱动
		// 还在启动。先保留集群里的库存，下轮再读，直到厂商驱动返回设备。
		//
		// 这里的错误只结束本轮，外层循环会继续重试。如果进程在等待中退出，循环会保留最后一次错误。
		// 显式设置 --no-fast-failed 时，首轮空结果也可上报。
		if holdUntilFirstDetected {
			if manufacturers.Len() == 0 {
				err := fmt.Errorf("manufacturer %s is expected but not detected", d.manufacturers.UnsortedList()[0])
				logger.Error(err, "not reporting a node with no accelerators, detecting again")
				return err
			}
			holdUntilFirstDetected = false
		}

		// -- detectedManufacturersCh 发送的是厂商集合，例如 {"nvidia"}。`select` 带 default，通道满时记录日志，探测协程还能继续上报
		// 把本轮发现的厂商集合发给分配器。
		if d.detectedManufacturersCh != nil {
			select {
			case d.detectedManufacturersCh <- manufacturers:
			default:
				logger.Info("skipping publishing detected manufacturers, channel is full")
			}
		}

		// 将本轮设备清单上报到 Kubernetes。
		logger.V(2).Info("reporting")
		// -- 把结果交给 Kubernetes
		if err := d.reportDevices(ctx, devicesGrpList); err != nil {
			logger.Error(err, "reporting")
			deviceKeys.Clear() // 清空比较用的设备集合，让下一次监控触发重新探测。
		}

		// 采样设备指标并更新最新快照。
		//
		// rounds 从本次完整探测后开始计数。离开监控循环会重新探测，下一段监控也会重新计数。
		rounds := 0
		// IMPORTANT: 开始周期性轮询监控，默认每 15 秒监控一次
		_ = waitx.RepeatUntilContextCancel(ctx, d.monitorPeriod, true, func(ctx context.Context) error {
			logger.V(3).Info("monitoring")

			// -- 这个结果里有采样时间、实际周期和整卡指标
			metricsGrpList, unmeasuredByMonitor := d.MonitorAccelerator(ctx)
			if len(metricsGrpList) != 0 {
				// 整卡指标和进程用量分两次读取，时间只相差几毫秒。两者独立计算，共用一次
				// 快照的时间戳。快照只接收按容器汇总的用量，原始进程记录留在节点上。
				// -- 存起来给 HTTP 端点和 Prometheus 用
				d.monitorSnapshot.Store(&MonitorSnapshot{
					Timestamp: time.Now(),
					// 周期以秒保存，遇到不足一秒的尾数向上取整，避免消费方过早判定样
					// 本已经过期。
					PeriodSeconds: int64((d.monitorPeriod + time.Second - 1) / time.Second),
					Groups:        metricsGrpList,
					Slices:        d.collectSlices(ctx, metricsGrpList),
				})
			}

			// 从监控结果中取出每张卡的身份和健康状态。
			curDeviceKeys := sets.New[_DeviceKey]()
			if len(metricsGrpList) != 0 {
				for i := range metricsGrpList {
					for j := range metricsGrpList[i].Accelerators {
						curDeviceKeys.Insert(_DeviceKey{
							metricsGrpList[i].Manufacturer,
							metricsGrpList[i].Accelerators[j].ID,
							metricsGrpList[i].Accelerators[j].Unhealthy,
						})
					}
				}
			}

			// 探测失败的厂商本轮使用了上一次的结果，首次失败时甚至没有旧结果。需要再次探测，
			// 否则一次失败之后，监控看起来可能一直正常。
			if unmeasuredByDetect.Len() != 0 {
				logger.Info("a detect pass could not measure, going to detect again",
					"manufacturers", unmeasuredByDetect.UnsortedList())
				return waitx.ErrStopRetrying
			}

			// 对比完整探测与当前监控看到的设备。设备集合和健康状态一致时继续监控，发生变化时
			// 重新探测。
			//
			// 监控读取失败的厂商暂时退出比较，避免误判为设备消失。厂商成功返回空清单时，之前
			// 存在的卡才算消失。
			if !measuredDeviceKeys(deviceKeys, unmeasuredByMonitor).Equal(curDeviceKeys) {
				logger.Info("changed, going to detect again")
				return waitx.ErrStopRetrying
			}

			// 设备集合里没有网络状态。每轮监控也要检查网卡。这样即使 GPU 没变化，RDMA 链路断
			// 开也能触发上报，撤下失效的 rdma.capable 标签。
			//
			// interfacesChanged 只比较影响 RDMA 可用性的记录。否则每次 Pod 新建 veth 网卡，都
			// 会触发一次完整的厂商驱动探测。
			detected, detectedErr := DetectInterfaces()
			if interfacesChanged(d.reportedInterfaces, d.reportedInterfacesKnown, detected, detectedErr) {
				logger.Info("network interfaces changed, going to detect again")
				return waitx.ErrStopRetrying
			}

			// 监控能看到卡和网卡的变化。节点加入或离开高速互联域时，卡 ID 和网卡都可能保持原
			// 样，fabric.domain 只有通过完整探测才能刷新。
			//
			// 每经过固定轮数再完整探测一次，重新读取互联域、驱动版本和设备型号。按轮数计时让
			// 刷新间隔跟随 monitorPeriod。
			rounds++
			if rounds >= detectEveryNMonitorRounds {
				logger.V(1).Info("detect period elapsed, going to detect again")
				return waitx.ErrStopRetrying
			}
			return nil
		})

		return nil
	})
}

// nodeWideDeviceGroups 汇总整台节点的设备组，包括本轮探测到的组，以及集群中其他厂商已上报的组。
//
// mine 指明本轮负责的厂商。这些厂商的组以最新探测为准，其余厂商的组沿用各自 DaemonSet 的报告。这样不会把
// 本轮确认已移除的卡从旧清单中加回来。
//
// 单元测试验证这段合并逻辑。要检验两个上报者能否最终写出一致的 NodeFeature，需要一台同时装有两家厂商加速
// 器的节点，并让两个 DaemonSet 都读到各自的卡。
//
// 需要追加其他厂商的组时，先复制自己的清单。调用方稍后还要单独上报本厂商的原始探测结果。
func nodeWideDeviceGroups(
	own, stored device.DevicesGroupList, mine sets.Set[string],
) device.DevicesGroupList {
	var others device.DevicesGroupList
	for i := range stored {
		if !mine.Has(stored[i].Manufacturer) {
			others = append(others, stored[i])
		}
	}
	if len(others) == 0 {
		return own
	}
	return append(cloneDeviceGroups(own), others...)
}

// cloneDeviceGroups 深拷贝整组设备，避免使用者修改返回值后污染下一轮还要复用的缓存。
func cloneDeviceGroups(groups device.DevicesGroupList) device.DevicesGroupList {
	out := make(device.DevicesGroupList, len(groups))
	for i := range groups {
		groups[i].DeepCopyInto(&out[i])
	}
	return out
}

type (
	// _DeviceGroupKey 用厂商和组 ID 共同标识一个设备组。
	_DeviceGroupKey struct {
		Manufacturer string
		ID           string
	}
	// _DeviceGroupValue 保存组数据和待删除标志，供对齐时记录此次枚举是否仍包含该组。
	_DeviceGroupValue struct {
		Group  device.DevicesGroup
		Remove bool
	}
)

// measuredDeviceKeys 从之前探测到的设备键中排除本轮监控读取失败的厂商，返回可用于比较的集合。
func measuredDeviceKeys(keys sets.Set[_DeviceKey], unmeasured sets.Set[string]) sets.Set[_DeviceKey] {
	if unmeasured.Len() == 0 {
		return keys
	}

	measured := sets.New[_DeviceKey]()
	for k := range keys {
		if !unmeasured.Has(k.Manufacturer) {
			measured.Insert(k)
		}
	}
	return measured
}

// IMPORTANT: 把结果交给 Kubernetes
func (d *Detector) reportDevices(ctx context.Context, eGroups device.DevicesGroupList) error {
	ndName := osx.Getenv("KUBERNETES_NODE_NAME")
	if ndName == "" {
		return errors.New("environment variable KUBERNETES_NODE_NAME is not set")
	}

	//
	lpCli := system.LoopbackKubeClient.Get()
	nd, err := lpCli.CoreV1().Nodes().Get(ctx, ndName,
		meta.GetOptions{
			ResourceVersion: "0", // 告诉 API Server 直接从内存 watch cache 读即可，减轻 etcd 压力
		})
	if err != nil {
		return err
	}

	// 对象正在删除时，跳过本轮。
	if nd.DeletionTimestamp != nil {
		return errors.New("skip deleted node")
	}

	// 网卡属于整个节点，所有厂商共用。先枚举一次网卡，再用结果构造 NodeFeature 的 RDMA 标签。
	//
	// 读取网卡出错时保留之前发布的清单和标签。首次上报没有旧记录，就先创建不含接口的对象，并用错误日
	// 志说明原因。
	eInterfaces, eInterfacesErr := DetectInterfaces()
	if eInterfacesErr != nil {
		logger.Error(eInterfacesErr, "could not enumerate network interfaces",
			"node", ndName,
			"consequence", "keeping whatever inventory and labels were recorded before this pass")
	}
	// 只有成功枚举时才更新监控的比较基准。比较时会忽略链路首次故障的时间戳。这样一次读取失败不会让下
	// 一次成功读取被误判为网卡变化。
	if eInterfacesErr == nil {
		d.reportedInterfaces = cloneInterfaces(eInterfaces)
		d.reportedInterfacesKnown = true
	}

	// 整轮共用同一时间。首次创建对象和更新旧对象都需要记录首次发现链路故障的时刻，否则第一次漏记的时
	// 间会一直缺失。
	now := meta.Now()
	carryLinkFirstSeen(nil, eInterfaces, now)

	// 距离标签描述整个节点。计算最近距离时，需要考虑节点上所有厂商的加速器。
	//
	// 每个厂商的 DaemonSet 只探测自己的卡，却共同更新节点的设备特征对象。先从共享的 Devices 取出其他
	// 厂商的设备组，才能得到整机一致的最短距离，并避免不同上报者反复覆盖同一标签。
	//
	// 共享清单读取失败时，先用本轮探测到的设备计算。设备子集的最短距离只可能相同或更远，因此不会夸大
	// 接近程度。成功读取后再收敛。
	devsCli := lpCli.WorkerV1alpha1().Devices()
	nodeGroups := eGroups
	aDevs, derr := devsCli.Get(ctx, ndName, meta.GetOptions{ResourceVersion: "0"})
	syncNodeWide := nodeWideReadSynced(derr)
	switch {
	case derr == nil:
		nodeGroups = nodeWideDeviceGroups(eGroups, aDevs.Spec.Groups, d.manufacturers)
	case kerrors.IsNotFound(derr):
		// 首次上报还没有 Devices 对象，日志按正常情况记录。此时也看不到全节点的设备，互联域标签要
		// 等后续读取成功。
		logger.V(3).Info("the node stores no Devices yet, so this is its first pass",
			"node", ndName,
			"consequence", "the distance is reduced over this pass's accelerators only, "+
				"and the fabric labels wait for a pass that can see the whole node")
	default:
		logger.V(1).Info("could not read the node's Devices for the node-wide reductions",
			"node", ndName, "error", derr.Error(),
			"consequence", "the distance is reduced over this pass's accelerators only, "+
				"and the fabric labels are left as previously published")
	}

	// 先发布描述节点特征的 NodeFeature。

	nfCli := lpCli.NfdV1alpha1().NodeFeatures(kuberess.SystemNamespaceName)
	eNf := &nfd.NodeFeature{
		ObjectMeta: meta.ObjectMeta{
			Name:      ndName + "-gpustack-device-manager",
			Namespace: kuberess.SystemNamespaceName,
			Labels: map[string]string{
				nfd.NodeFeatureObjNodeNameLabel: ndName,
				"app.kubernetes.io/part-of":     "gpustack-operator-device-manager",
			},
		},
		Spec: func() nfd.NodeFeatureSpec {
			nfs := nfd.NewNodeFeatureSpec()
			// -- 将本轮探测到的设备和网卡信息转换成要发布的节点标签
			nfs.Labels = nodeFeatureLabels(eGroups, nodeGroups, eInterfaces,
				eInterfacesErr == nil, syncNodeWide)
			return *nfs
		}(),
	}
	kubemeta.ControlOnWithoutBlock(eNf, nd, core.SchemeGroupVersion.WithKind("Node"))
	nfAlignment := nodeFeatureAlignment{
		expected:       eNf,
		node:           nd,
		syncInterfaces: eInterfacesErr == nil,
		syncFabric:     syncNodeWide,
	}

	aNf, err := kubeclientset.Create(ctx, nfCli, eNf,
		kubeclientset.WithUpdateIfExisted(nfAlignment.apply))
	if err != nil {
		return fmt.Errorf("failed to sync NodeFeature object for node %s: %w", ndName, err)
	}

	// 再发布逐卡清单 Devices。

	// 从刚写入的 NodeFeature 取本轮特征，加上 Node 的操作系统和架构，供 Worker 用标签列出相应的
	// Devices。NFD 稍后才把特征合并到 Node。直接从 Node 读取会漏掉新节点的首轮报告。managed 标签由
	// NodeDevicesReconciler 单独同步。
	devsLabels := acceleratableDevicesSelectorLabels(nd, aNf.Spec.Labels)
	eDevs := &workercore.Devices{
		ObjectMeta: meta.ObjectMeta{
			Name:   ndName,
			Labels: devsLabels,
		},
		Spec: workercore.DevicesSpec{
			Groups:     eGroups,
			Interfaces: eInterfaces,
		},
	}
	// Devices 是集群级对象，由同为集群级的 Node 持有。若把有命名空间的 NodeFeature 设为所有者，垃圾收
	// 集器无法解析这个引用，节点被删除后设备清单也会残留。
	kubemeta.ControlOnWithoutBlock(eDevs, nd, core.SchemeGroupVersion.WithKind("Node"))
	devsAlignment := devicesAlignment{
		expected:       eDevs,
		node:           nd,
		labels:         devsLabels,
		manufacturers:  d.manufacturers,
		syncInterfaces: eInterfacesErr == nil,
		now:            now,
	}

	_, err = kubeclientset.Create(ctx, devsCli, eDevs,
		kubeclientset.WithUpdateIfExisted(devsAlignment.apply))
	if err != nil {
		return fmt.Errorf("failed to sync Devices object for node %s: %w", ndName, err)
	}

	return nil
}

// controlOnNodeWithoutBlock 将 Node 设为对象唯一的控制者。先移除旧的、类型不同的控制者引用，再写入 Node
// 的引用，避免同时出现两个控制者。
func controlOnNodeWithoutBlock(obj kubemeta.MetaObject, nd *core.Node) {
	if ctrlRef := kubemeta.GetControllerOfNoCopy(obj); ctrlRef != nil && ctrlRef.UID != nd.UID {
		kubemeta.ControlOff(obj, nil, schema.FromAPIVersionAndKind(ctrlRef.APIVersion, ctrlRef.Kind))
	}
	kubemeta.ControlOnWithoutBlock(obj, nd, core.SchemeGroupVersion.WithKind("Node"))
}

// nodeWideReadSynced 判断本轮是否成功读到节点已有的 Devices 清单，据此决定是否更新整机标签。
//
// Devices 尚未创建时会返回 NotFound，这是首次上报的正常情况。此时其他厂商可能还未写入清单，仅凭本轮设备无
// 法断定整机属于同一个互联域。
//
// 新节点会等到下一次读到 Devices 才发布互联域。最多等待 detectEveryNMonitorRounds 个监控周期。等待期间，
// 调度器不会凭尚未确认的域来放置工作负载。
//
// 成功读取只能看到已经发布的厂商报告。另一家厂商若仍在启动或探测失败，清单暂时还可能不完整，随后各轮会继
// 续收敛。
//
// 单独保留这个判断，便于测试首次上报与读失败各自的处理条件。
func nodeWideReadSynced(err error) bool {
	return err == nil
}

// nodeFeatureLabels 将本轮探测到的设备和网卡信息转换成要发布的节点标签。
//
// 将标签推导独立出来，可以逐一验证每类标签该读取本厂商的设备组，还是整机设备组。
//
// ownGroups 只包含本次负责的厂商。nodeGroups 还包含其他厂商已经上报的组。syncInterfaces 和 syncNodeWide
// 分别表示网卡和整机清单的读取是否成功。
func nodeFeatureLabels(
	ownGroups, nodeGroups device.DevicesGroupList, interfaces []device.Interface,
	syncInterfaces, syncNodeWide bool,
) map[string]string {
	// 每个厂商只发布描述自己设备组的加速器标签。
	labels := nodefeature.ConstructAcceleratableNodeLabels(ownGroups)

	// 互联域和 RDMA 距离描述的是整台节点，因此两者都需要考虑其他厂商已经上报的加速器。
	//
	// 成功读取整机清单后才更新互联域标签。只有局部信息时，既无法证明原来的域失效，也无法证明整台机器
	// 都属于本厂商读到的域。
	if syncNodeWide {
		maps.Copy(labels, nodefeature.ConstructFabricNodeLabels(nodeGroups))
	}

	// 只有成功枚举网卡后才更新 RDMA 标签。暂时无法读取时保留上次的结果，避免无依据地撤下仍可用的节点。
	if syncInterfaces {
		maps.Copy(labels, nodefeature.ConstructRDMANodeLabels(nodeGroups, interfaces))
	}

	return labels
}

// nodeFeatureAlignment 将已有的 NodeFeature 与本轮探测得到的特征对齐。
//
// 将更新规则放到独立方法后，可以直接检查一个已经失效的 RDMA 或互联域标签是否从旧对象中移除。
type nodeFeatureAlignment struct {
	// expected 是按照本轮观察结果构造的目标对象。对象首次创建时直接写入它。
	expected *nfd.NodeFeature
	// node 是这个对象的所有者。
	node *core.Node
	// syncInterfaces 为 false 时，本轮未能枚举网卡，保留已发布的 RDMA 标签。若在这时删除标签，仍有可
	// 用链路的节点可能失去被选中的机会。
	syncInterfaces bool
	// syncFabric 为 false 时，本轮未能读取节点上其他厂商的设备组。保留原有互联域标签，等有完整信息再
	// 更新。
	syncFabric bool
}

func (a nodeFeatureAlignment) apply(aNf *nfd.NodeFeature) (_ *nfd.NodeFeature, skip bool, err error) {
	skip = true
	if aNf.Labels == nil {
		aNf.Labels = make(map[string]string)
	}
	if aNf.Spec.Labels == nil {
		aNf.Spec.Labels = make(map[string]string)
	}
	// 补齐元数据标签，已有标签的值变化时一并更新。
	for k, v := range a.expected.Labels {
		if aNf.Labels[k] != v {
			aNf.Labels[k] = v
			skip = false
		}
	}
	// 把本轮产生的特征写入 spec.labels。
	for k, v := range a.expected.Spec.Labels {
		if aNf.Spec.Labels[k] != v {
			aNf.Spec.Labels[k] = v
			skip = false
		}
	}
	// 在本次负责的标签前缀下，删除成功探测后已经不再上报的键。
	//
	// 仅增加或覆盖标签会留下过时的状态。例如 RDMA 链路已不可用，或节点已经退出互联域，旧标签若一直保
	// 留，选择器仍会选中这台节点。
	//
	// 网卡枚举和整机设备组读取互相独立，分别成功才清理对应前缀。加速器组的标签继续采用原有的追加和覆
	// 盖规则。
	if a.syncInterfaces &&
		removeUnreported(aNf.Spec.Labels, a.expected.Spec.Labels, nodefeature.RDMAFeatureLabelPrefix) {
		skip = false
	}
	if a.syncFabric &&
		removeUnreported(aNf.Spec.Labels, a.expected.Spec.Labels, nodefeature.FabricFeatureLabelPrefix) {
		skip = false
	}
	// 校正指向 Node 的所有者引用。
	if !kubemeta.IsControlledBy(aNf, a.node) {
		controlOnNodeWithoutBlock(aNf, a.node)
		skip = false
	}
	return aNf, skip, err
}

// removeUnreported 删除指定前缀下本轮不再上报的标签，返回是否发生删除。其他前缀的标签留给各自的上报者维护。
func removeUnreported(stored, reported map[string]string, prefix string) (removed bool) {
	for k := range stored {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		if _, ok := reported[k]; !ok {
			delete(stored, k)
			removed = true
		}
	}
	return removed
}

// devicesAlignment 将已有 Devices 清单与本轮结果对齐。只有内容真的变化时才向 API 写入。
//
// 将对齐判断单独放在 apply 方法中，测试可以直接改变网卡清单，而保持 GPU 清单相同，检查这次变化是否触发更
// 新。
type devicesAlignment struct {
	// expected 是按照本轮观察结果构造的目标对象。对象首次创建时直接写入它。
	expected *workercore.Devices
	// node 是集群级 Devices 对象的所有者。NodeFeature 位于命名空间中，不能作为这个对象的控制者。
	node *core.Node
	// labels 是要写入 Devices 的加速器规格选择标签。
	labels map[string]string
	// manufacturers 限定本轮有权更新哪些厂商的设备组。
	manufacturers sets.Set[string]
	// syncInterfaces 为 false 表示本轮读取网卡失败，需要保留旧清单。读取成功且主机确实没有网卡时，才
	// 用空清单更新。
	syncInterfaces bool
	// now 是整轮共用的观察时间。同时首次发现的多条链路故障会得到相同的时间戳。
	now meta.Time
}

func (a devicesAlignment) apply(actual *workercore.Devices) (*workercore.Devices, bool, error) {
	skip := true

	// 更新本轮负责的设备组。
	if !kubemeta.DeepEqual(actual.Spec.Groups, a.expected.Spec.Groups) {
		groups, groupsChanged := alignDeviceGroups(actual.Spec.Groups, a.expected.Spec.Groups, a.manufacturers)
		if groupsChanged {
			skip = false
		}
		actual.Spec.Groups = groups
	}

	// 单独比较网卡清单。即使加速器没有变化，网卡发生变化也需要更新。
	if a.syncInterfaces {
		// 比较前先沿用仍在故障中的链路原有的首次发现时间。否则每轮都会写入一个新时间，让同一个故
		// 障触发无意义的 API 更新。
		carryLinkFirstSeen(actual.Spec.Interfaces, a.expected.Spec.Interfaces, a.now)
		if !kubemeta.DeepEqual(actual.Spec.Interfaces, a.expected.Spec.Interfaces) {
			actual.Spec.Interfaces = a.expected.Spec.Interfaces
			skip = false
		}
	}

	// 更新 Devices 的选择标签，使 Worker 能按标签查到这台节点的设备清单。
	if len(a.labels) > 0 && actual.Labels == nil {
		actual.Labels = make(map[string]string)
	}
	for k, v := range a.labels {
		if actual.Labels[k] != v {
			actual.Labels[k] = v
			skip = false
		}
	}

	// 校正指向 Node 的所有者引用。
	if !kubemeta.IsControlledBy(actual, a.node) {
		controlOnNodeWithoutBlock(actual, a.node)
		skip = false
	}

	return actual, skip, nil
}

// alignDeviceGroups 将已有设备组与本轮实测组对齐。只移除当前负责厂商已经确认消失的组，
// 其余厂商的记录继续保留。返回值指明内容或存储顺序是否变化。
func alignDeviceGroups(
	aGroups, eGroups device.DevicesGroupList, allowedManufacturers sets.Set[string],
) (groups device.DevicesGroupList, changed bool) {
	// 先规范化两边的顺序，避免把随机枚举顺序误判为硬件变化。另存原始列表用于最后比较，
	// 这样旧列表顺序不规范时仍会得到一次修正。
	stored := aGroups
	aGroups = canonicalDeviceGroups(aGroups)
	eGroups = canonicalDeviceGroups(eGroups)

	// 以厂商和组 ID 两个字段共同索引旧组。
	devGrpIndex := make(map[_DeviceGroupKey]*_DeviceGroupValue)
	for i := range aGroups {
		k := _DeviceGroupKey{aGroups[i].Manufacturer, aGroups[i].ID}
		v := &_DeviceGroupValue{
			Group: aGroups[i],
			// 当前探测器不负责的厂商，默认保留旧记录。
			Remove: allowedManufacturers.Has(aGroups[i].Manufacturer),
		}
		devGrpIndex[k] = v
	}
	// 遍历实测组，匹配到旧组便检查更新，否则加入新组。
	groups = make(device.DevicesGroupList, 0, len(eGroups))
	for i := range eGroups {
		k := _DeviceGroupKey{eGroups[i].Manufacturer, eGroups[i].ID}
		if v, ok := devGrpIndex[k]; ok {
			// 本轮仍能看到这个组。保留它，并在内容变化时换成实测值。
			v.Remove = false
			if !kubemeta.DeepEqual(v.Group, eGroups[i]) {
				v.Group = eGroups[i]
				changed = true
			}
			continue
		}
		// 首次看到此组，把它加入结果。
		groups = append(groups, eGroups[i])
		changed = true
	}
	// 遍历旧组，跳过仍标记为删除的组，把保留项写入结果。
	for i := range aGroups {
		k := _DeviceGroupKey{aGroups[i].Manufacturer, aGroups[i].ID}
		v := devGrpIndex[k]
		if v.Remove {
			changed = true
			continue
		}
		groups = append(groups, v.Group)
	}

	// 新组此前先于旧组加入列表，最后统一排序，避免存储顺序依赖每个型号第一次出现的时间。
	groups = canonicalDeviceGroups(groups)

	return groups, changed || !kubemeta.DeepEqual(stored, groups)
}

// canonicalDeviceGroups 先按设备枚举索引排序组内卡，再按厂商、组内最小索引及组 ID 排序各组。
// 每个厂商独立编号，空组排在本厂商末尾。
// 复制组切片和每个加速器切片后再排序，调用方的旧列表保持原样。API 仍按标识关联对象，
// 稳定顺序仅用于持久化结果和判断变化。
func canonicalDeviceGroups(groups device.DevicesGroupList) device.DevicesGroupList {
	byIndex := func(a, b device.Accelerator) int { return cmp.Compare(a.Index, b.Index) }

	out := slices.Clone(groups)
	for i := range out {
		out[i].Accelerators = slices.Clone(out[i].Accelerators)
		slices.SortStableFunc(out[i].Accelerators, byIndex)
	}
	slices.SortStableFunc(out, func(a, b device.DevicesGroup) int {
		if c := cmp.Compare(a.Manufacturer, b.Manufacturer); c != 0 {
			return c
		}
		if c := cmp.Compare(firstAcceleratorIndex(a), firstAcceleratorIndex(b)); c != 0 {
			return c
		}
		return cmp.Compare(a.ID, b.ID)
	})

	return out
}

// firstAcceleratorIndex 返回组内最小枚举索引，空组返回最大索引，使其排在有卡的组之后。
func firstAcceleratorIndex(grp device.DevicesGroup) uint32 {
	if len(grp.Accelerators) == 0 {
		return math.MaxUint32
	}
	return slices.MinFunc(grp.Accelerators, func(a, b device.Accelerator) int {
		return cmp.Compare(a.Index, b.Index)
	}).Index
}

// acceleratableDevicesSelectorLabels 为 Devices 计算操作系统、架构和加速器组的选择标签。这里调用的
// ExtractNodeFlavors 在第 6 章实现，所以第 5 章首先观察 NodeFeature 上的数量和 Devices 里的单卡 ID。
//
// 加速器特征直接取自刚发布的 NodeFeature，避免等待 NFD 把标签同步回 Node。操作系统和架构来自 Node 的稳定
// 标签。managed 和实际的 CPU 分组由 NodeDevicesReconciler 另行同步。数量标签 .count 用于确定节点规格大小。
func acceleratableDevicesSelectorLabels(node *core.Node, publishedFeatureLabels map[string]string) map[string]string {
	src := &core.Node{ObjectMeta: meta.ObjectMeta{Labels: map[string]string{
		core.LabelOSStable:   node.Labels[core.LabelOSStable],
		core.LabelArchStable: node.Labels[core.LabelArchStable],
	}}}
	maps.Copy(src.Labels, publishedFeatureLabels)

	// 合并各加速器规格的节点标签，过滤掉由 Worker 维护的管理标志、CPU 分组，以及只用于规格大小的
	// .count。
	accelerated := slicex.Filter(nodefeature.ExtractNodeFlavors(src), func(nf nodefeature.NodeFlavor) bool {
		return nf.Acceleratable
	})
	return mapx.Merge(slicex.Transform(accelerated, func(nf nodefeature.NodeFlavor) map[string]string {
		return mapx.Filter(nf.NodeLabels, func(k, _ string) bool {
			return k != systemname.ManagedLabelKey &&
				!strings.HasSuffix(k, ".count") &&
				!strings.HasPrefix(k, nodefeature.GeneralFeatureLabelPrefix)
		})
	})...)
}
