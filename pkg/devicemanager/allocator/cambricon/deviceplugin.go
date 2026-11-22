package cambricon

import (
	"context"
	"fmt"
	"strings"

	core "k8s.io/api/core/v1"
	klog "k8s.io/klog/v2"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/device"
	"gpustack.ai/gpustack/pkg/devicemanager/controllers"
	"gpustack.ai/gpustack/pkg/deviceplugin"
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/utils/gox"
	"gpustack.ai/gpustack/pkg/utils/strconvx"
)

const Manufacturer = nodefeature.ManufacturerCambricon

func New(opts device.AllocatorOptions) device.Allocator {
	logger := opts.Logger.WithName(Manufacturer)
	servers := []deviceplugin.Server{
		newServer(logger, workercore.DeviceAllocationModeExclusive),
	}
	if !opts.NoShared {
		servers = append(servers,
			newServer(logger, workercore.DeviceAllocationModeShared),
		)
	}
	if !opts.NoSliced {
		servers = append(servers,
			newServer(logger, workercore.DeviceAllocationModeSliced),
		)
	}
	// 可见性服务器会将其容器共分配至其所属容器所获致的同一物理设备：其分配操作会复用所属容器的预留设
	// 备，响应方会返回与未切片模式相同的简单设备可见性响应（仅涉及设备 cgroup 访问，无切片影响）。
	servers = append(servers,
		newServer(logger, workercore.DeviceAllocationModeVisibility),
	)

	return &aggregated{
		logger:     logger,
		servers:    servers,
		kubeSocket: opts.KubeSocket,
		sliced:     !opts.NoSliced,
	}
}

type aggregated struct {
	logger     klog.Logger
	servers    []deviceplugin.Server
	kubeSocket string
	// 报告是否对 Sliced 服务器进行了注册，从而限制每个制造商的状态化 sMLU 回收循环。
	sliced bool
	// 生命周期拥有以下任务运行的上下文，因此停止此分配器会终止所有任务，而不仅仅是监控服务器的任务。
	lifecycle gox.Lifecycle
}

func (*aggregated) Name() string {
	return Manufacturer
}

func (in *aggregated) Start(ctx context.Context) error {
	in.logger.Info("starting")

	tasks := make([]func(context.Context) error, 0, len(in.servers)+1)
	for i := range in.servers {
		srv := in.servers[i]
		tasks = append(tasks, func(ctx context.Context) error {
			return srv.Start(ctx, in.kubeSocket)
		})
	}
	// 分片资源池没有 Release 回调，因此 sMLU 实例由基于级别的循环回收，该循环接收了协调器的广播实时节
	// 点集以及同步计时器。
	if in.sliced {
		tasks = append(tasks, func(ctx context.Context) error {
			reconciler := controllers.Get[*deviceplugin.DevicesReconciler]()
			r := newReclaimer(newSMLUDriver(), deviceplugin.OperatorPodsDir, in.logger.WithName("reclaim"))
			deviceplugin.RunReclaimLoop(ctx, reconciler, Manufacturer,
				workercore.DeviceAllocationModeSliced, r.reconcile)
			return nil
		})
	}

	return in.lifecycle.Start(ctx, tasks...)
}

// 停止所有任务，启动后不会返回，直到它们完成。服务器不会被手动关闭：取消其所服务的上下文就会使其退役，之
// 后手动关闭则会将完成退役报告为从未启动的服务器。
func (in *aggregated) Stop() {
	in.logger.Info("stopping")

	in.lifecycle.Stop()
}

type server struct {
	deviceplugin.ResourceServer

	// smlu 是 cnDev 缝线，即被切割的响应器驱动器。非切割模式则为空。
	smlu smluDriver
}

func newServer(logger klog.Logger, mode workercore.DeviceAllocationMode) deviceplugin.Server {
	logger = logger.WithName(strings.ToLower(mode.String()))

	s := &server{
		ResourceServer: deviceplugin.ResourceServer{
			Logger:         logger,
			Manufacturer:   Manufacturer,
			AllocationMode: mode,
			Reconciler:     controllers.Get[*deviceplugin.DevicesReconciler](),
		},
	}
	if mode == workercore.DeviceAllocationModeSliced {
		s.smlu = newSMLUDriver()
	}
	s.Responder = s

	return s
}

func (s *server) GetContainerAllocateResponse(
	_ context.Context,
	pod *core.Pod,
	ctr *core.Container,
	devs *workercore.Devices,
	allocated map[deviceplugin.Resource]int32,
) (*deviceplugin.ContainerAllocateResponse, error) {
	// 分片容器拥有真正的 sMLU 逻辑分片隔离机制（即一个 cnDev 实例及其设备节点）。独占/共享/可见性设置
	// 则保留了普通的设备可见性响应机制。
	if s.AllocationMode == workercore.DeviceAllocationModeSliced {
		return s.getSlicedContainerAllocateResponse(pod, ctr, devs, allocated)
	}

	// 分配的加速器按照容器编号的顺序排列。
	accelerators := deviceplugin.AllocatedAccelerators(devs, allocated)

	// 在任何节点被探测之前就进行了渲染，因此没有 cnDev 索引的加速器会首先失败分配。环境是指运行
	// cambricon-container-runtime 密钥的部署环境，它携带与注入节点相同的索引，因此容器无法将某一张卡
	// 上的节点交给另一张卡，也通过其环境无法实现这种操作。
	visible, err := visibleDevices(accelerators)
	if err != nil {
		return nil, err
	}

	ctrResp := &deviceplugin.ContainerAllocateResponse{
		Envs: map[string]string{
			"CAMBRICON_VISIBLE_DEVICES": visible,
		},
	}
	for i := range accelerators {
		if err = appendCardDevices(ctrResp, accelerators[i].Accel); err != nil {
			return nil, err
		}
	}
	appendNodeDevices(ctrResp)

	return ctrResp, nil
}

// 设备节点路径来自 Cambricon 设备插件 v2.2.0，提交信息为 cc0f7735f208，位于
// device-plugin/pkg/mlu/server.go 中的 PrepareResponse 函数，路径常量定义在
// device-plugin/pkg/mlu/const.go 中。较旧或较新的 Neuware 可能会使用不同的命名方式。
const (
	cardDevicePathFormat     = "/dev/cambricon_dev%d"
	cardIPCMDevicePathFormat = "/dev/cambricon_ipcm%d"
)

var (
	// optionalCardDevicePathFormats 只是某些驱动程序构建版本中暴露的每张卡片节点格式。每个格式都包含
	// 卡片的驱动程序索引，因此每个格式中都会有一个 %d。切片路径也注入了 IPC 节点，来自相同的格式，因
	// 此一个名称无法在一处正确设置，而在另一处则出错。
	optionalCardDevicePathFormats = []string{
		"/dev/cambr-msgq:%d",
		"/dev/cambr-rpc:%d",
		"/dev/cmsg_ctrl%d",
		"/dev/commu%d",
		cardIPCMDevicePathFormat,
	}
	// nodeDevicePaths 属于主机，而非任何单个卡设备：cnDev 通过控制节点进行枚举，而 GPUDirect RDMA 则
	// 需要 GDR 节点。没有索引对其进行命名，但无论包含多少卡设备，响应都会将其包含进去。
	nodeDevicePaths = []string{
		"/dev/cambricon_ctl",
		"/dev/cambricon_gdr",
	}
)

// cardDevicePath 返回作为加速器的节点，其名称由驱动程序索引决定。
func cardDevicePath(index uint32) string {
	return fmt.Sprintf(cardDevicePathFormat, index)
}

// cardIPCMDevicePath 返回一张卡片的 IPC 节点，以其驱动程序索引命名。无论是整张卡片数据还是切片响应都会包
// 含该节点，其中整张卡片数据属于可选节点之一。
func cardIPCMDevicePath(index uint32) string {
	return fmt.Sprintf(cardIPCMDevicePathFormat, index)
}

// optionalCardDevicePaths 返回一张卡片的可选节点，以其驱动器索引命名。
func optionalCardDevicePaths(index uint32) []string {
	paths := make([]string, 0, len(optionalCardDevicePathFormats))
	for _, format := range optionalCardDevicePathFormats {
		paths = append(paths, fmt.Sprintf(format, index))
	}
	return paths
}

// driverIndex 返回了驱动程序所识别的加速器编号，对于 Cambricon 而言，即 cnDev 设备索引，该索引被探测器记
// 录在 PhysicalIndexes 中。
//
// 正是那个数字，而非运营商自身的逻辑索引，决定了加速器字符设备的命名、厂商自己的运行时路径的解析方式，以
// 及运营商手动修复 sMLU 模式时必须处理的内容。这两种情况仅在主机上检测到所有卡片时才会一致，逻辑索引会统
// 计已检测到的卡片数量，因此当某张卡片无法通过探针检测时，所有后续的逻辑索引都会低于其 cnDev 索引值。
//
// 没有 cnDev 索引的记录是格式错误，而非损坏，因此会被拒绝——就像 Ascend 响应器拒绝缺少 dcmi 地址的卡片一样
// ——而不是猜测一个能指向其他卡片的索引。
func driverIndex(accel *workercore.Accelerator) (uint32, error) {
	if len(accel.PhysicalIndexes) == 0 {
		return 0, fmt.Errorf("accelerator %q carries no cnDev device index", accel.ID)
	}
	return accel.PhysicalIndexes[0], nil
}

// visibleDevices 渲染了 CAMBRICON_VISIBLE_DEVICES 的值：每个分配的加速器驱动程序索引，按容器编号的顺序用
// 逗号连接。
func visibleDevices(accels []deviceplugin.AllocatedAccelerator) (string, error) {
	indexes := make([]string, 0, len(accels))
	for i := range accels {
		index, err := driverIndex(accels[i].Accel)
		if err != nil {
			return "", err
		}
		indexes = append(indexes, strconvx.FormatUint(index, 10))
	}
	return strings.Join(indexes, ","), nil
}

// appendCardDevices 会追加一个已分配卡片的设备节点。
//
// 需要使用卡片自身的节点：如果主机没有暴露任何节点，那么该卡片根本无法被交付给容器使用，因此分配失败，只
// 能将其命名为错误，而无法启动没有加速器且无法说明原因的容器。可选的节点则会在主机已存在的位置被注入，这
// 是每个设备构建器都进行的存在性检查。
func appendCardDevices(resp *deviceplugin.ContainerAllocateResponse, accel *workercore.Accelerator) error {
	index, err := driverIndex(accel)
	if err != nil {
		return err
	}

	ownPath := cardDevicePath(index)
	pDev := deviceplugin.NewRWDevice(ownPath)
	if pDev == nil {
		return fmt.Errorf("accelerator %q of card %q has no device node %q",
			accel.ID, accel.Topology.PciBusID, ownPath)
	}
	resp.Devices = append(resp.Devices, pDev)

	for _, path := range optionalCardDevicePaths(index) {
		if pDev = deviceplugin.NewRWDevice(path); pDev != nil {
			resp.Devices = append(resp.Devices, pDev)
		}
	}

	return nil
}

// appendNodeDevices 会追加节点级控制设备，无论分配了多少卡片，响应中都会包含这些设备。
func appendNodeDevices(resp *deviceplugin.ContainerAllocateResponse) {
	for _, path := range nodeDevicePaths {
		if pDev := deviceplugin.NewRWDevice(path); pDev != nil {
			resp.Devices = append(resp.Devices, pDev)
		}
	}
}

// getSlicedContainerAllocateResponse 用于为切片容器实现 sMLU 逻辑切片注入机制：它通过驱动程序接口预留一个
// cnDev sMLU 实例（即包含计算配额和 VRAM 限制的规格），并将关联信息和规格标记写入 Pod 工作目录中，同时注
// 入实例的设备节点以及加速器控制节点。对于 --use-runtime 部署方式，会设置 VIRTUAL_DEVICES 环境变量作为替
// 代方案（因为 sMLU/mim 不支持 CDI）。
//
// 一个 sMLU 请求相当于 1 个 Pod/1 个容器/1 个加速器，因此拒绝多加速器分片分配方案（即 Ascend 单加速器模式），
// 而非仅默默地分片分配一个。
func (s *server) getSlicedContainerAllocateResponse(
	pod *core.Pod,
	ctr *core.Container,
	devs *workercore.Devices,
	allocated map[deviceplugin.Resource]int32,
) (*deviceplugin.ContainerAllocateResponse, error) {
	var (
		group *workercore.DevicesGroup
		accel *workercore.Accelerator
		count int
	)
	for i := range devs.Spec.Groups {
		g := &devs.Spec.Groups[i]
		for j := range g.Accelerators {
			a := &g.Accelerators[j]
			if _, existed := allocated[deviceplugin.Resource{Group: g.ID, Device: a.ID}]; !existed {
				continue
			}
			count++
			group, accel = g, a
		}
	}
	if count == 0 {
		return nil, fmt.Errorf("no allocated accelerator found for sliced container %q", ctr.Name)
	}
	if count > 1 {
		return nil, fmt.Errorf("sliced container %q allocated %d cards, but Cambricon sMLU slicing is single-card", ctr.Name, count)
	}

	// 在请求内容之前，卡片的身份信息与 Ascend 响应方相同，因此格式错误的记录和格式错误的请求总是先报
	// 告记录信息。
	slot, err := driverIndex(accel)
	if err != nil {
		return nil, err
	}
	card := accel.Topology.PciBusID

	// Compute 和 VRAM 是独立的维度。两者都直接来自共享的辅助组件（百分比直接作为 sMLU mluQuota 使用，
	// 无需进行 CU 转换）。
	coresPct := deviceplugin.SlicedCoresPercent(ctr,
		nodefeature.GetAcceleratableSlicedCoresPercentageResourceName(Manufacturer))
	memMib, err := deviceplugin.SlicedMemoryMib(ctr,
		nodefeature.GetAcceleratableSlicedMemoryPercentageResourceName(Manufacturer),
		nodefeature.GetAcceleratableSlicedMemoryMibResourceName(Manufacturer),
		int64(group.Memory))
	if err != nil {
		return nil, fmt.Errorf("derive sliced memory limit: %w", err)
	}

	inst, err := reserveInstance(s.smlu, string(pod.UID), ctr.Name, card, int(slot), coresPct, memMib, s.Logger)
	if err != nil {
		return nil, fmt.Errorf("reserve cambricon smlu instance: %w", err)
	}

	ctrResp := &deviceplugin.ContainerAllocateResponse{}
	if pDev := deviceplugin.NewRWDevice(cardDevicePath(slot)); pDev != nil {
		ctrResp.Devices = append(ctrResp.Devices, pDev)
	}
	if pDev := deviceplugin.NewRWDevice(cardIPCMDevicePath(slot)); pDev != nil {
		ctrResp.Devices = append(ctrResp.Devices, pDev)
	}
	if inst.devNode != "" {
		if pDev := deviceplugin.NewRWDevice(inst.devNode); pDev != nil {
			ctrResp.Devices = append(ctrResp.Devices, pDev)
		}
	}
	// 一个切片需要节点级控制设备，其原因与整个卡片所需相同。厂商自有的 sMLU 分支也提供了这些设备。同
	// 一个辅助组件同时服务于两条路径，因此两者在“节点级”的定义上不会产生分歧。
	appendNodeDevices(ctrResp)
	// VIRTUAL_DEVICES 环境变量是 --use-runtime 回退选项：它指定了 sMLU 实例的设备节点，该节点适用于按
	// 环境变量而非注入节点来映射设备的运行时环境。仅当读取结果填充了设备节点时才设置该值——如果值为空，
	// 则会导致运行环境的配置错误（上述节点挂载方式也采用相同方式保护）。
	if inst.devNode != "" {
		ctrResp.Envs = map[string]string{"VIRTUAL_DEVICES": inst.devNode}
	}
	return ctrResp, nil
}
