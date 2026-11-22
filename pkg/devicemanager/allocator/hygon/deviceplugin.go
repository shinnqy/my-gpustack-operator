package hygon

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	core "k8s.io/api/core/v1"
	klog "k8s.io/klog/v2"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/device"
	"gpustack.ai/gpustack/pkg/devicemanager/controllers"
	"gpustack.ai/gpustack/pkg/deviceplugin"
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/utils/gox"
	"gpustack.ai/gpustack/pkg/utils/osx"
)

const Manufacturer = nodefeature.ManufacturerHygon

func New(opts device.AllocatorOptions) device.Allocator {
	logger := opts.Logger.WithName(Manufacturer)

	// 硬件分区服务器提供“<base>.partitioned”服务——该厂商在其 API 和工具中将其自己的分区功能称为 MIG，
	// 因此也使用了这个名称。没有分区功能的厂商根本没有这样的资源名称，因此不会注册服务器，也不会有宣
	// 传空名称的服务器。
	partitioned := !opts.NoPartitioned &&
		nodefeature.GetAcceleratableResourceName(Manufacturer, workercore.DeviceAllocationModePartitioned) != ""
	// 分区驱动程序在构建过程中会加载厂商库，因此只需构建一次——仅在处理分区服务的场景中进行——并由处理
	// 这些分区的两台服务器共享：分区服务器会创建一个实例，可见性服务器会在再次命名之前确认其仍然处于
	// 正常运行状态。不处理分区的节点则不会加载任何内容。
	var mig migDriver
	if partitioned {
		mig = newMigDriver()
	}

	servers := []deviceplugin.Server{
		newServer(logger, workercore.DeviceAllocationModeExclusive, nil),
	}
	if !opts.NoShared {
		servers = append(servers,
			newServer(logger, workercore.DeviceAllocationModeShared, nil),
		)
	}
	if !opts.NoSliced {
		servers = append(servers,
			newServer(logger, workercore.DeviceAllocationModeSliced, nil),
		)
	}
	if partitioned {
		servers = append(servers,
			newServer(logger, workercore.DeviceAllocationModePartitioned, mig),
		)
	}
	// 可见性服务器会将其容器共分配至其所属容器所获致的同一物理设备：其分配操作会复用所属容器的预留设
	// 备，响应方会返回与未切片模式相同的简单设备可见性响应（仅涉及设备 cgroup 访问，无切片影响）。
	servers = append(servers,
		newServer(logger, workercore.DeviceAllocationModeVisibility, mig),
	)

	return &aggregated{
		logger:      logger,
		servers:     servers,
		kubeSocket:  opts.KubeSocket,
		partitioned: partitioned,
	}
}

type aggregated struct {
	logger     klog.Logger
	servers    []deviceplugin.Server
	kubeSocket string
	// 分区报告会判断分区服务器是否已注册，从而控制分区回收循环：该循环的存在是为了释放该服务器创建的
	// 实例，此节点上其他任何东西都无法释放它们——设备插件协议没有释放回调，且该厂商在任何实例仍然存在
	// 时无法退出多`实例`模式。
	partitioned bool
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
	if in.partitioned {
		tasks = append(tasks, func(ctx context.Context) error {
			r := newMigReclaimer(newMigDriver(), deviceplugin.OperatorPodsDir, in.logger.WithName("reclaim"))
			deviceplugin.RunReclaimLoop(ctx, controllers.Get[*deviceplugin.DevicesReconciler](),
				Manufacturer, workercore.DeviceAllocationModePartitioned, r.reconcile)
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
	// mig 是分区驱动程序，仅设置在指向分区的两台服务器上：即被分区的服务器和用于标识所有者分区的可见
	// 性服务器。在其他地方它的值为空，当存在该值时，分区入口点会拒绝操作而非继续执行。
	mig migDriver
}

func newServer(
	logger klog.Logger, mode workercore.DeviceAllocationMode, mig migDriver,
) deviceplugin.Server {
	logger = logger.WithName(strings.ToLower(mode.String()))

	s := &server{
		mig: mig,
		ResourceServer: deviceplugin.ResourceServer{
			Logger:         logger,
			Manufacturer:   Manufacturer,
			AllocationMode: mode,
			Reconciler:     controllers.Get[*deviceplugin.DevicesReconciler](),
		},
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
	// 分片容器具有每个加速器对应的 vdev.conf + CU 掩码隔离机制。独占/共享可见性设置则确保了整个加速器
	// 层面的直通功能得以保持。
	if s.AllocationMode == workercore.DeviceAllocationModeSliced {
		return s.getSlicedContainerAllocateResponse(pod, ctr, devs, allocated)
	}

	// 拒绝那些最终无果的资助，就像下面的路径那样。该响应仍然包含节点级控制设备以及运行时挂载机制，因
	// 此没有这些的话，容器就无法使用：没有加速器，也没有任何错误可提及。
	accelerators := deviceplugin.AllocatedAccelerators(devs, allocated)
	if len(accelerators) == 0 {
		return nil, fmt.Errorf("no allocated accelerator found for container %q", ctr.Name)
	}

	ctrResp := &deviceplugin.ContainerAllocateResponse{}

	appendNodeDevices(ctrResp, s.Logger)

	// 安装驱动程序、库和工具。
	if pMount := deviceplugin.NewROMount(hostHyhalDir); pMount != nil {
		ctrResp.Mounts = append(ctrResp.Mounts, pMount)
	}

	for i := range accelerators {
		appendCardDevices(ctrResp, accelerators[i].Accel, s.Logger)
	}

	return ctrResp, nil
}

// 设备节点路径从该制造商自己的容器文档中读取，该文档会注入两个控制节点以及每个加速器对应的 DRM 对。它们是
// 变量而非常量，因此测试可以将其指向临时树结构，而非主机自身的节点——这是 AMD 和 THead 响应器采用的方式，
// 也是该包能够声明设备集合的授予内容而非仅列出其未命名的路径的原因。
var (
	// nodeDevicePaths 属于主机，而非任何单个加速器：它们是每个分配的加速器共享的计算路径，因此无论包
	// 含多少内容，响应都会将其携带。
	nodeDevicePaths = []string{"/dev/kfd", "/dev/mkfd"}
	// cardDevicePathFormat 和 renderDevicePathFormat 用于指代加速器自身的两个 DRM 节点。每个节点都采
	// 用某种格式来承载其 DRM 次要参数，因此两者都包含%d。整个卡片和分割路径的渲染均使用这两种格式，因
	// 此一个名称无法在一处正确设置，而在另一处又可能出错。
	cardDevicePathFormat   = "/dev/dri/card%d"
	renderDevicePathFormat = "/dev/dri/renderD%d"
)

// nodeDevicePermissions 是该制造商官方文档赋予控制节点的权限：与加速器自身节点所拥有的"rw"权限不同，这里
// 使用的是"rwm"权限。
const nodeDevicePermissions = "rwm"

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
	pDev := deviceplugin.NewDevice(path, nodeDevicePermissions)
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

// 容器内部路径以及主机 DTK/hyhal 运行时目录中所需的 vdev.conf 文件部分。
const (
	ctrVdevDir    = "/etc/vdev/docker"
	ctrHygonDrvr  = "/opt/hygondriver"
	hostHygonPath = "/opt/dtk" // HYGONPATH 默认设置
	hostHyhalDir  = "/opt/hyhal"
)

// getSlicedContainerAllocateResponse 用于实现分片容器的 Hygon 逻辑分片注入机制：每个分配的加速器对应一个
// vdev.conf 文件，其中包含由核心数生成的 CU 位掩码以及每个加速器级别的 VRAM 限制设置，这些文件被发布到
// Pod 工作目录中，并挂载于 /etc/vdev/docker 路径下，此外还包含 DTK/hyhal 运行时目录以及各个加速器级别的设
// 备节点。主机上的 DTK/hyhal 用户空间运行时环境会读取 vdev.conf 文件以强制执行分片机制。整个加速器分片仍
// 然会写入完整的 vdev.conf 占用标记（allocateVdev 不会跳过写入操作），因此磁盘上的扫描器永远不会遗漏已分
// 配的加速器。
func (s *server) getSlicedContainerAllocateResponse(
	pod *core.Pod,
	ctr *core.Container,
	devs *workercore.Devices,
	allocated map[deviceplugin.Resource]int32,
) (*deviceplugin.ContainerAllocateResponse, error) {
	// 分配的 `DCU` 按照容器编号的顺序排列，即 vdev0.conf、vdev1.conf……的顺序。
	accels := deviceplugin.AllocatedAccelerators(devs, allocated)
	if len(accels) == 0 {
		return nil, fmt.Errorf("no allocated accelerator found for sliced container %q", ctr.Name)
	}

	coresPct := deviceplugin.SlicedCoresPercent(ctr,
		nodefeature.GetAcceleratableSlicedCoresPercentageResourceName(Manufacturer))
	memPctRes := nodefeature.GetAcceleratableSlicedMemoryPercentageResourceName(Manufacturer)
	memMibRes := nodefeature.GetAcceleratableSlicedMemoryMibResourceName(Manufacturer)

	ctrResp := &deviceplugin.ContainerAllocateResponse{}

	// 控制设备节点（由每个分配的加速器共享的计算路径），由整个卡片路径使用的同一辅助组件提供，因此这
	// 两组无法分离。
	appendNodeDevices(ctrResp, s.Logger)

	// DTK/hyhal 用户空间运行时目录（HYGONPATH -> /opt/hygondriver, /opt/hyhal ro）。
	if osx.Exists(hostHygonPath) {
		ctrResp.Mounts = append(ctrResp.Mounts,
			&deviceplugin.Mount{ContainerPath: ctrHygonDrvr, HostPath: hostHygonPath, ReadOnly: true})
	}
	if pMount := deviceplugin.NewROMount(hostHyhalDir); pMount != nil {
		ctrResp.Mounts = append(ctrResp.Mounts, pMount)
	}

	// /etc/vdev/docker/下的每个 Pod vdev 目录（用于存储 vdev<i>.conf 文件）以只读方式挂载：DTK/hyhal
	// 运行时仅能读取该目录，而操作员可在主机端写入内容，因此容器无需拥有写入权限。这些配置文件也是权
	// 威的插槽账本 allocateVdev 的扫描结果，因此读写挂载方式允许共享租户对另一个分区的 CU/vdev/pipe
	// 会计数据进行篡改。allocateVdev 在写入第一个配置文件时，会创建主机目录。
	vdevHostDir := filepath.Join(deviceplugin.PodWorkDir(string(pod.UID), ctr.Name), "etc/vdev/docker")
	ctrResp.Mounts = append(ctrResp.Mounts,
		&deviceplugin.Mount{ContainerPath: ctrVdevDir, HostPath: vdevHostDir, ReadOnly: true})

	// 每个分配的加速器对应一个 vdev.conf 文件，每个文件具有独立的时隙分配。整个加速器切片（核心数百分
	// 比 >= 100 且 memMiB 大于等于加速器 VRAM）将被解析为全掩码/全内存配置。循环索引 i 为容器级别的
	// device_id（DTK device_id 的语义属于硬件验证项）。
	//
	// 这种配置能够持久保留其分配状态：它会在主机上指定一个文件，而 allocateVdev 仅在该路径下的文件已
	// 指定相同的加速器时才会重新使用槽位。准入机制会将请求分配到恰好一个加速器上，因此路径始终为
	// vdev0.conf，它所指定的加速器无法移动。谁要打开那道门，就必须保留现有的路径到加速器的分配关系，
	// 而不是从这里的订单中重新派生出新的路径：重试操作会重新编号，发现某个 conf 文件持有当前要分配的
	// 加速器，就会将其视为被占用，从而拒绝完整的分配份额，或是在第一个份额之外再授予第二个配额。
	//
	// 在多加速器分配过程中出现部分失败，之前的加速器配置会保留在磁盘上：这是基于层级设计的有意设计——
	// 一个幂等的 kubelet 重试操作会重新使用这些配置，且当 Pod 消失后 podDirGC 会回收这些配置，因此不
	// 会尝试回滚。
	for i := range accels {
		group, accel := accels[i].Group, accels[i].Accel
		memMib, err := deviceplugin.SlicedMemoryMib(ctr, memPctRes, memMibRes, int64(group.Memory))
		if err != nil {
			return nil, fmt.Errorf("derive sliced memory limit: %w", err)
		}
		selfPath := filepath.Join(vdevHostDir, fmt.Sprintf("vdev%d.conf", i))
		if _, err := allocateVdev(deviceplugin.OperatorPodsDir, selfPath, accel.Topology.PciBusID, group.Cores, coresPct, memMib, i); err != nil {
			return nil, fmt.Errorf("allocate vdev for card %s: %w", accel.Topology.PciBusID, err)
		}

		// 每个加速器 DRM 设备节点都来自同一辅助组件，用于整个卡片路径。
		appendCardDevices(ctrResp, accel, s.Logger)
	}

	return ctrResp, nil
}
