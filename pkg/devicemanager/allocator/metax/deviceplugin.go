package metax

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
)

const Manufacturer = nodefeature.ManufacturerMetaX

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
	// 报告是否对 Sliced 服务器进行了注册，从而限制每个制造商的状态化 SGPU 回收循环。
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
	// 分片资源池没有 Release 回调，因此 sgpu 子设备由基于级别的循环机制回收，该机制包含协调器的广播实
	// 时 Pod 集合以及同步计时器。
	if in.sliced {
		tasks = append(tasks, func(ctx context.Context) error {
			reconciler := controllers.Get[*deviceplugin.DevicesReconciler]()
			r := newReclaimer(newSysfsSGPUManager(), deviceplugin.OperatorPodsDir, in.logger.WithName("reclaim"))
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

	// sgpu 是 sysfs 中用于划分的响应器驱动器的部分。非划分模式则为空。
	sgpu sgpuManager
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
		s.sgpu = newSysfsSGPUManager()
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
	// 分片容器具备真正的 sgpu 逻辑分片隔离机制（即子设备与 METAX_SGPUS）。独占/共享/可见性设置则保留
	// 了普通设备可见性响应机制。
	if s.AllocationMode == workercore.DeviceAllocationModeSliced {
		return s.getSlicedContainerAllocateResponse(pod, ctr, devs, allocated)
	}

	ctrResp := &deviceplugin.ContainerAllocateResponse{}

	appendNodeDevices(ctrResp, s.Logger)

	for _, allocatedAccel := range deviceplugin.AllocatedAccelerators(devs, allocated) {
		appendCardDevices(ctrResp, allocatedAccel.Accel, s.Logger)
	}

	return ctrResp, nil
}

// 从该制造商自己的容器文档中读取的设备节点路径。它们是变量而非常量，因此测试可以将其指向临时树结构，而非
// 主机自身的节点——这是 AMD、`Hygon` 和 THead 响应器出于相同原因所采用的机制。
var (
	// nodeDevicePaths 属于主机，而非任何单个加速器，因此响应会将其包含进来，无论该主机包含多少个加速
	// 器。切片路径仅会取其中的第一个：通过控制节点及其自身的渲染节点可以访问一个 sgpu 子设备，而厂商
	// 的切片分支也仅提供如此多的资源。
	nodeDevicePaths = []string{controlDevicePath, "/dev/mxnd", "/dev/mxgd"}
	// controlDevicePath 是切片路径所指向的单一节点级设备：通过它以及该子设备的自身生成节点，可以访问
	// 到 sgpu 子设备，除此之外没有别的路径。
	controlDevicePath = "/dev/mxcd"
	// cardDevicePathFormat 和 renderDevicePathFormat 用于指代加速器自身的两个 DRM 节点。每个节点都采
	// 用某种格式来承载其 DRM 次要参数，因此两者都包含%d。整个卡片和分割路径的渲染均使用这两种格式，因
	// 此一个名称无法在一处正确设置，而在另一处又可能出错。
	cardDevicePathFormat   = "/dev/dri/card%d"
	renderDevicePathFormat = "/dev/dri/renderD%d"
)

// cardDevicePath 返回一个加速器中的 DRM 卡节点，其名称由 DRM 次要版本号确定。
func cardDevicePath(minor uint32) string {
	return fmt.Sprintf(cardDevicePathFormat, minor)
}

// renderDevicePath 返回了某个加速器中的 DRM 渲染节点，其名称由 DRM 次要版本号确定。
func renderDevicePath(minor uint32) string {
	return fmt.Sprintf(renderDevicePathFormat, minor)
}

// appendNodeDevices 会追加节点级控制设备，无论授予了多少加速器，响应中都会包含这些设备。
func appendNodeDevices(resp *deviceplugin.ContainerAllocateResponse, logger klog.Logger) {
	for _, path := range nodeDevicePaths {
		appendNodeDevice(resp, logger, path)
	}
}

// appendNodeDevice 会添加一个节点级设备，或者说明为何该响应不包含该设备。
func appendNodeDevice(resp *deviceplugin.ContainerAllocateResponse, logger klog.Logger, path string) {
	pDev := deviceplugin.NewRWDevice(path)
	if pDev == nil {
		logger.Info("the host exposes no control device node, so the container will not receive it",
			"path", path)

		return
	}
	resp.Devices = append(resp.Devices, pDev)
}

// appendCardDevices 会追加一个已分配的加速器自身的 DRM 节点，这些节点由主机持有。
//
// 每个记录的索引都受到其自身长度的约束。探测器从该制造商的 sysfs DRM 目录中读取这些索引，该目录会提供数字、
// 仅 DRM card<N> 编号，或者根本没有任何内容——因此，索引机制无法处理缺失的情况。对缺失的索引进行索引操作会
// 导致系统崩溃，且没有任何拦截器能够在此处理程序中恢复这种崩溃状态：对于每一个制造商，在无法读取 DRM 目录
// 的加速器上，负责节点上所有分配工作的进程也会随之崩溃。
//
// 此处配置的设备属于尽力获取范畴，其中 AMD 响应器的关闭机制为故障关闭：无法命名的节点，或者主机没有的节点，
// 都会被忽略，但分配仍然能够成功。这个包一直都是这样运行的，目前没有硬件可以确认没有这些节点时，容器还能
// 做什么，因此这种状态被保留了下来——每一个被忽略的情况都会被大声指出。当容器使用的加速器数量少于预期时，
// 日志就会对此进行解释，而不是操作员从现象中推断出来。
func appendCardDevices(
	resp *deviceplugin.ContainerAllocateResponse,
	accel *workercore.Accelerator,
	logger klog.Logger,
) {
	if len(accel.PhysicalIndexes) == 0 {
		logger.Info("an allocated card records no drm index, so none of its device nodes can be named "+
			"and the container will receive none of them",
			"card", accel.ID)

		return
	}
	appendCardDevice(resp, accel, logger, cardDevicePath(accel.PhysicalIndexes[0]))

	if len(accel.PhysicalIndexes) < 2 {
		logger.Info("an allocated card records no drm render index, so its render node cannot be named "+
			"and the container will not receive it",
			"card", accel.ID)

		return
	}
	appendCardDevice(resp, accel, logger, renderDevicePath(accel.PhysicalIndexes[1]))
}

// appendCardDevice 会添加已分配的加速器中的一个节点，或者说明为何该响应无法包含该节点。
func appendCardDevice(
	resp *deviceplugin.ContainerAllocateResponse,
	accel *workercore.Accelerator,
	logger klog.Logger,
	path string,
) {
	pDev := deviceplugin.NewRWDevice(path)
	if pDev == nil {
		logger.Info("the host has no device node an allocated card names, so the container will not "+
			"receive it",
			"card", accel.ID, "path", path)

		return
	}
	resp.Devices = append(resp.Devices, pDev)
}

// getSlicedContainerAllocateResponse 用于生成 sliced 容器的 sgpu 逻辑切片注入机制：它通过 sysfs seam 为每
// 个加速器 sgpu 子设备分配资源（固定共享计算配额 + VRAM 限制），并将关联信息及插槽标记写入 Pod 工作目录中，
// 随后返回 METAX_SGPUS 以及控制文件（/dev/mxcd）和每个加速器渲染相关设备节点（/dev/dri/renderD*）。全加速
// 器切片采用原生路径（无 sgpu 子设备，无 METAX_SGPUS），但仍会记录占用标记。
//
// MetaX sgpu 切片功能将单个加速器划分为多个部分，而每个容器对应的标记则仅记录一个加速器。因此，多加速器切
// 片分配方案会被拒绝（即 Ascend 单加速器模式），而不是只是默默地只切片一个加速器。
func (s *server) getSlicedContainerAllocateResponse(
	pod *core.Pod,
	ctr *core.Container,
	devs *workercore.Devices,
	allocated map[deviceplugin.Resource]int32,
) (*deviceplugin.ContainerAllocateResponse, error) {
	accels := deviceplugin.AllocatedAccelerators(devs, allocated)
	if count := len(accels); count == 0 {
		return nil, fmt.Errorf("no allocated accelerator found for sliced container %q", ctr.Name)
	} else if count > 1 {
		return nil, fmt.Errorf("sliced container %q allocated %d cards, but MetaX sgpu slicing is single-card", ctr.Name, count)
	}
	group, accel := accels[0].Group, accels[0].Accel

	// Compute 和 VRAM 是独立的维度（没有统一的比例）。两者都直接来自共享的辅助项（百分比直接使用，无
	// 需进行 CU 转换）。
	coresPct := deviceplugin.SlicedCoresPercent(ctr,
		nodefeature.GetAcceleratableSlicedCoresPercentageResourceName(Manufacturer))
	memMib, err := deviceplugin.SlicedMemoryMib(ctr,
		nodefeature.GetAcceleratableSlicedMemoryPercentageResourceName(Manufacturer),
		nodefeature.GetAcceleratableSlicedMemoryMibResourceName(Manufacturer),
		int64(group.Memory))
	if err != nil {
		return nil, fmt.Errorf("derive sliced memory limit: %w", err)
	}

	bdf := accel.Topology.PciBusID
	wholeCard := coresPct >= 100 && memMib >= int64(group.Memory)
	res, err := reserveSlice(s.sgpu, string(pod.UID), ctr.Name, bdf, coresPct, memMib, wholeCard)
	if err != nil {
		return nil, fmt.Errorf("reserve metax sgpu slice: %w", err)
	}

	ctrResp := &deviceplugin.ContainerAllocateResponse{}
	// 控制节点与这个子设备的自身生成节点，都源自相同的辅助组件，因此整个卡片路径中使用的节点名称无法
	// 不同。一个 sgpu 切片所包含的元素不超过这些数量。
	appendNodeDevice(ctrResp, s.Logger, controlDevicePath)
	if len(accel.PhysicalIndexes) < 2 {
		s.Logger.Info("an allocated card records no drm render index, so its render node cannot be named "+
			"and the sliced container will not receive it",
			"card", accel.ID)
	} else {
		appendCardDevice(ctrResp, accel, s.Logger, renderDevicePath(accel.PhysicalIndexes[1]))
	}
	// 部分切片会注入 METAX_SGPUS。完整的加速器则采用原生路径（无环境变量）。
	if !res.wholeCard {
		ctrResp.Envs = map[string]string{"METAX_SGPUS": res.envValue}
	}
	return ctrResp, nil
}
