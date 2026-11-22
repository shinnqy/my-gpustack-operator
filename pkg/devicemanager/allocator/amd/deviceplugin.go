package amd

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

const Manufacturer = nodefeature.ManufacturerAMD

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
	// 可见性服务器会将容器共分配至其所属容器被授予的同一物理设备上：其分配操作会复用所属设备的预留资
	// 源，响应方也会像非切片模式那样返回相同的简单响应——即所属的设备节点，且不存在切片带来的任何影响。
	servers = append(servers,
		newServer(logger, workercore.DeviceAllocationModeVisibility),
	)

	return &aggregated{
		logger:     logger,
		servers:    servers,
		kubeSocket: opts.KubeSocket,
	}
}

type aggregated struct {
	logger     klog.Logger
	servers    []deviceplugin.Server
	kubeSocket string
	// 生命周期拥有以下任务运行的上下文，因此停止此分配器会终止所有任务，而不仅仅是监控服务器的任务。
	lifecycle gox.Lifecycle
}

func (*aggregated) Name() string {
	return Manufacturer
}

func (in *aggregated) Start(ctx context.Context) error {
	in.logger.Info("starting")

	tasks := make([]func(context.Context) error, 0, len(in.servers))
	for i := range in.servers {
		srv := in.servers[i]
		tasks = append(tasks, func(ctx context.Context) error {
			return srv.Start(ctx, in.kubeSocket)
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
	s.Responder = s

	return s
}

// 从 AMD 设备插件中读取的 Device-node 路径，相关功能位于 rocm/k8s-device-plugin 的
// internal/pkg/plugin/plugin.go 文件中。
//
// 它们是变量而非常量，因此测试可以将其指向临时树节点，而非主机自身的节点——这也是 THead 分区响应器所采用的
// 机制。这使得该包能够断言设备是否设置了 CARRIES 授权，而不仅仅是拒绝授权：这里的每条路径都是必需的，因此
// 在没有 ROCm 驱动程序的机器上，任何成功的响应都将无法访问，进而导致切片计算掩码和内存限制情况也无法解决。
//
// 它们每一个都是必需的，与其他供应商提供的可选单卡节点不同。共享设备规格构造函数对于不存在的路径会返回
// nil。而仅包含非 null 值的响应器则会将缺失的节点转换为成功的分配结果，同时会返回一个无法到达其所依赖加速
// 器的容器，且没有任何地方报告任何问题。
var (
	// nodeDevicePaths 属于主机，而非任何单个加速器： /dev/kfd 是计算驱动程序的单一入口点，因此无论包
	// 含多少个加速器，响应都会通过它传递。
	nodeDevicePaths = []string{"/dev/kfd"}
	// cardDevicePathFormat 和 renderDevicePathFormat 用于指代加速器自身的两个 DRM 节点。每个节点都采
	// 用某种格式来承载其 DRM 次要参数，因此两者都包含 `%d`。
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

// driverMinors 返回了驱动程序所识别的加速器对应的两个数字，对于 AMD 来说，分别是 PhysicalIndexes 中记录的
// DRM 卡和生成相关的次要参数。
//
// 要么两个数字，要么没有。检测器从 amdgpu 驱动程序自己的 sysfs 树中读取这些数据，从而得到该对值。或者只有
// 卡号，甚至什么都没有——因此该机制无法假设会有这样的对值。对值的索引缺失会使负责节点上所有制造商的分配工
// 作的单一进程陷入恐慌状态。如果缺少该对值，则属于格式错误，而非性能下降，因此会被拒绝，而不是被用来通过
// 猜测的 Minor 值来指代另一个加速器。
func driverMinors(accel *workercore.Accelerator) (card, render uint32, err error) {
	if len(accel.PhysicalIndexes) < 2 {
		return 0, 0, fmt.Errorf("card %s records no drm card and render node, so the device nodes it "+
			"needs cannot be named", accel.ID)
	}

	return accel.PhysicalIndexes[0], accel.PhysicalIndexes[1], nil
}

// appendDeviceNodes 会追加满足这些加速器授权所需的设备集合：首先添加节点级别的计算节点，然后按授权顺序添
// 加每个加速器自身的 DRM 对。
//
// 这是 AMD 设备插件注入的内容，它使得 ROCm 容器能够仅使用驱动程序运行——无需容器运行时去解释环境变量，加速
// 器就能正常工作。
func appendDeviceNodes(
	resp *deviceplugin.ContainerAllocateResponse,
	accels []deviceplugin.AllocatedAccelerator,
) error {
	if err := appendNodeDevices(resp); err != nil {
		return err
	}

	// 哪个加速器宣称了每个 DRM 节点，因此可能出现双方都持有异议的情况。
	claimed := make(map[string]*workercore.Accelerator, 2*len(accels))
	for i := range accels {
		if err := appendCardDevices(resp, accels[i].Accel, claimed); err != nil {
			return err
		}
	}

	return nil
}

// appendNodeDevices 会追加节点级别的设备节点，无论授予了多少加速器，响应中都会包含这些节点。
func appendNodeDevices(resp *deviceplugin.ContainerAllocateResponse) error {
	for _, path := range nodeDevicePaths {
		pDev := deviceplugin.NewRWDevice(path)
		if pDev == nil {
			return fmt.Errorf("the host has no device node %q, so no accelerator can be granted", path)
		}
		resp.Devices = append(resp.Devices, pDev)
	}

	return nil
}

// appendCardDevices 会追加已授予的加速器自身的设备节点，并将这些节点记录在“claimed”列表中，以便后续尝试命
// 名相同节点的加速器被拒绝。
//
// 两个加速器在命名一个节点时，所记录的子节点并未指明是加速器，因此任何授权都无法被信任以访问其负责处理的
// 硬件。测量的硬件记录每个总线地址对应一张卡和一个渲染节点，这没有任何可依赖之处。一种不同的枚举方式会被
// 拒绝，而不是被接受，因为该设置可能属于其相邻设备。
//
// 拒绝处理，而非静默的重复操作，因为从当前来看，这两种读取结果无法区分：一个将总线地址与错误的加速器错误
// 关联的探测器，看起来就像真正在两个逻辑加速器之间共享一个节点的部件，只有第一个是安全的可使用的。消息中
// 提到了两个总线地址以及要读取的 sysfs 目录，因此访问它的人就能知道各自属于哪个。
func appendCardDevices(
	resp *deviceplugin.ContainerAllocateResponse,
	accel *workercore.Accelerator,
	claimed map[string]*workercore.Accelerator,
) error {
	card, render, err := driverMinors(accel)
	if err != nil {
		return err
	}

	for _, path := range []string{cardDevicePath(card), renderDevicePath(render)} {
		if owner, ok := claimed[path]; ok {
			return fmt.Errorf("cards %s and %s both record drm node %q, so neither can be granted; "+
				"compare their /sys/module/amdgpu/drivers/pci:amdgpu/%s/drm and "+
				"/sys/module/amdgpu/drivers/pci:amdgpu/%s/drm listings",
				owner.ID, accel.ID, path, owner.Topology.PciBusID, accel.Topology.PciBusID)
		}
		claimed[path] = accel

		pDev := deviceplugin.NewRWDevice(path)
		if pDev == nil {
			return fmt.Errorf("card %s has no device node %q on the host", accel.ID, path)
		}
		resp.Devices = append(resp.Devices, pDev)
	}

	return nil
}

// runtimeVisibleDevicesNone 就是 AMD_VISIBLE_DEVICES 当前所包含的内容，因为响应器自己注入了设备节点：这是
// 一条明确的指令，要求 amd-container-runtime 不要添加任何内容。
//
// 这两个通道无法协调，它们会合并——已测量。当运行时环境已安装，变量被命名为加速器时，注入的节点便无法实现
// 协调，容器会同时达到两个状态：它被授予的状态以及变量所指向的状态，且没有任何错误。如今这个包中没有任何
// 东西能解决这种分歧，因为序列索引和 DRM 索引是同一个检测记录的两个字段，但变量对抵消风险毫无作用——无论它
// 指向什么，都已在上方被注入。使用 "none" 会移除第二个授予通道，而不是让该通道保持激活状态，希望两者永远
// 不会产生差异。
//
// 明确说明而非省略：当变量不存在时，运行时不会注入任何内容，但这是其默认行为，而默认行为并不构成契约。而“
// none”则属于。
const runtimeVisibleDevicesNone = "none"

func (s *server) GetContainerAllocateResponse(
	_ context.Context,
	_ *core.Pod,
	ctr *core.Container,
	devs *workercore.Devices,
	allocated map[deviceplugin.Resource]int32,
) (*deviceplugin.ContainerAllocateResponse, error) {
	accels := deviceplugin.AllocatedAccelerators(devs, allocated)
	// 拒绝那种最终无果的资助申请，就像被切开的路径一样。下面列出的设备仍会包含节点级计算节点，因此没
	// 有加速器且没有错误提示，容器无法使用：没有加速器，也没有任何错误。
	if len(accels) == 0 {
		return nil, fmt.Errorf("no allocated accelerator found for container %q", ctr.Name)
	}
	// 拒绝一个没有身份的加速器，原因与切片路径相同：ID 是 ROCr 与代理匹配的依据，而空条目不会将容器限
	// 定在所授予的加速器上。明显拒绝这一要求才是安全的选择——否则就会有一个容器在无人授予的硬件上默默
	// 运行。
	for i := range accels {
		if accels[i].Accel.ID == "" {
			return nil, fmt.Errorf("card at index %d reports no unique id, so it cannot be granted",
				accels[i].Accel.Index)
		}
	}
	ctrResp := &deviceplugin.ContainerAllocateResponse{
		Envs: map[string]string{
			"AMD_VISIBLE_DEVICES": runtimeVisibleDevicesNone,
		},
	}
	// 注入加速器自身的设备节点，因此授权不依赖于容器运行时是否已安装，以便进行任何解释。
	if err := appendDeviceNodes(ctrResp, accels); err != nil {
		return nil, err
	}

	return ctrResp, nil
}
