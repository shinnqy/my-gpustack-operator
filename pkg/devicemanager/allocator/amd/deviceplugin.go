package amd

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
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

// AMD ROCm 拦截库在容器内使用的挂载路径。
//
// ctrVrocmLibPath 应与 pack/gpustack-operator/rootfs/etc/gpustack/lib/amd/ld.so.preload 中的路径一
// 致。这个文件覆盖容器的预加载配置，动态加载器按其中的路径查库。观察工具与拦截库安装在同一目录。
const (
	ctrLdPreloadPath  = "/etc/ld.so.preload"
	ctrVrocmLibPath   = "/usr/local/vrocm/libvrocm.so"
	ctrVrocmMonPath   = "/usr/local/vrocm/rocm-monitor"
	ctrVrocmCheckPath = "/usr/local/vrocm/rocm-cumask-check"
	ctrLedgerDir      = "/var/run/vrocm"
	ctrLedgerPath     = ctrLedgerDir + "/ledger"
)

// readTopologyFn 在 Linux 读取实际 HSA 拓扑，单元测试可以独立验证其上层的整数分配算法。
var readTopologyFn = readTopology

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

// rocrVisibleDevices 将探测器记录的 GPU-<serial> UUID 写成 ROCR_VISIBLE_DEVICES，ROCr 用这些 ID 匹
// 配 agent。
//
// ROCr 先应用可见列表，然后为设备编号。HSA_CU_MASK 的 GPU_list 和 VROCM_DEVICE_MEMORY_LIMIT_<i> 都
// 使用这一顺序。三个值必须来自同一份有序列表，否则掩码和显存额度可能作用到另一张卡。
func rocrVisibleDevices(accels []deviceplugin.AllocatedAccelerator) string {
	ids := make([]string, 0, len(accels))
	for i := range accels {
		ids = append(ids, accels[i].Accel.ID)
	}
	return strings.Join(ids, ",")
}

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

// PlaceLogicalSliced 根据每卡的算力预算，在节点已有分配旁为容器放置 CU 窗口。
//
// 该方法在设备插件的节点级分配锁内执行。HSA 拓扑按卡缓存，窗口计算只做整数运算。释放锁之前先把选好
// 的窗口记入预留，以便下一个并发分配看到它。
func (s *server) PlaceLogicalSliced(
	_ context.Context,
	_ *core.Pod,
	ctr *core.Container,
	devs *workercore.Devices,
	allocated map[deviceplugin.Resource]int32,
	occupied deviceplugin.Placements,
) (deviceplugin.Placements, error) {
	accels := deviceplugin.AllocatedAccelerators(devs, allocated)
	if len(accels) == 0 {
		return nil, fmt.Errorf("no allocated accelerator found for sliced container %q", ctr.Name)
	}

	coresRes := nodefeature.GetAcceleratableSlicedCoresPercentageResourceName(Manufacturer)
	coresPct := deviceplugin.SlicedCoresPercent(ctr, coresRes)

	placed := make(deviceplugin.Placements, len(accels))
	for i := range accels {
		accel := accels[i].Accel
		if accel.ID == "" {
			// 设备可见性依赖这张卡的唯一 ID。缺少 ID 时拒绝分配，避免容器意外看见其它卡。
			return nil, fmt.Errorf("card at index %d reports no unique id, so it cannot be sliced", accel.Index)
		}
		res := deviceplugin.Resource{Group: accels[i].Group.ID, Device: accel.ID}
		topo, err := readTopologyFn(accel.Topology.PciBusID, accel.ID)
		if err != nil {
			return nil, fmt.Errorf("read topology of card %s: %w", accel.ID, err)
		}
		length, err := WindowCUs(topo, coresPct)
		if err != nil {
			return nil, fmt.Errorf("derive the compute window of card %s: %w", accel.ID, err)
		}
		// 请求与硬件步长不对齐时，向下取整并记录实际分配比例。Kueue 按申请值计费，而较多着色器引擎可能让实
		// 际比例低几个百分点。
		if delivered := length * 100 / topo.CU; delivered != coresPct {
			s.Logger.Info("compute request aligned down to the card's allocation atom",
				"card", accel.ID, "requested", coresPct, "delivered", delivered,
				"atomCUs", topo.Quantum(), "cardCUs", topo.CU)
		}
		placed[res] = []workercore.AcceleratorPlacement{
			PackWindow(topo, length, occupied[res]),
		}
	}
	return placed, nil
}

// GetLogicalSlicedResponse 将显存配额、ROCr 初始化时读取的 CU 掩码，以及预加载库和跨进程账本目录写
// 入分配响应。
//
// 使用锁内已经记入预留的窗口，容器的掩码必须和节点账本里的位置一致。
func (s *server) GetLogicalSlicedResponse(
	_ context.Context,
	pod *core.Pod,
	ctr *core.Container,
	devs *workercore.Devices,
	allocated map[deviceplugin.Resource]int32,
	placements deviceplugin.Placements,
) (*deviceplugin.ContainerAllocateResponse, error) {
	accels := deviceplugin.AllocatedAccelerators(devs, allocated)
	if len(accels) == 0 {
		return nil, fmt.Errorf("no allocated accelerator found for sliced container %q", ctr.Name)
	}

	// 软切分容器和整卡容器使用相同的设备节点，切片的额外约束来自显存配额和计算窗口。
	ctrResp := &deviceplugin.ContainerAllocateResponse{}
	if err := appendDeviceNodes(ctrResp, accels); err != nil {
		return nil, err
	}

	// 账本按容器建立。它以 ROCR_VISIBLE_DEVICES 中的位置索引卡，不同容器各自的 0 号设备可能属于不同物
	// 理卡。目录放在 Pod 工作目录下，回收 Pod 时一起删除。拦截库自行创建账本文件，目录允许不同 UID 的
	// 工作负载写入。
	ledgerDir := filepath.Join(deviceplugin.PodWorkDir(string(pod.UID), ctr.Name), "run/vrocm")
	if err := osx.MkdirAll(ledgerDir, 0o777); err != nil {
		return nil, fmt.Errorf("create %q: %w", ledgerDir, err)
	}

	memPctRes := nodefeature.GetAcceleratableSlicedMemoryPercentageResourceName(Manufacturer)
	memMibRes := nodefeature.GetAcceleratableSlicedMemoryMibResourceName(Manufacturer)

	envs := map[string]string{
		// ROCr 根据 ROCR_VISIBLE_DEVICES 筛选并排列可见设备。实测无法解析其中一个设备 ID 时，它会静默返回
		// 零个 GPU agent。这里从注入设备节点时使用的同一份列表生成环境变量。
		"AMD_VISIBLE_DEVICES":  runtimeVisibleDevicesNone,
		"ROCR_VISIBLE_DEVICES": rocrVisibleDevices(accels),
		"VROCM_LEDGER_PATH":    ctrLedgerPath,
	}

	masks := make([]string, 0, len(accels))
	for i := range accels {
		accel := accels[i].Accel

		// ROCm 拦截库读取不带单位后缀的 MiB 整数。NVIDIA 的 HAMi-core 则使用带 m 后缀的字符串。
		memMib, err := deviceplugin.SlicedMemoryMib(ctr, memPctRes, memMibRes, int64(accels[i].Group.Memory))
		if err != nil {
			return nil, fmt.Errorf("derive sliced memory limit: %w", err)
		}
		envs["VROCM_DEVICE_MEMORY_LIMIT_"+strconv.Itoa(i)] = strconv.FormatInt(memMib, 10)

		res := deviceplugin.Resource{Group: accels[i].Group.ID, Device: accel.ID}
		window, ok := placements[res]
		if !ok || len(window) == 0 {
			return nil, fmt.Errorf("no compute window was placed for card %s", accel.ID)
		}
		// GPU_list 取这张卡在可见列表中的十进制下标 i。写入物理序号会选错卡，填入 UUID 则会让 ROCr 静默丢
		// 弃整个掩码。
		masks = append(masks, Mask(i, window[0]))
	}
	// 每张卡生成一个 GPU_list:CU_list 片段，ROCr 要求用分号连接各片段。
	envs["HSA_CU_MASK"] = strings.Join(masks, ";")

	// 日志级别 1 会记录拒绝和错误。显式注入该值可将日志行为固定在本次分配中，容器自行声明
	// LIBVROCM_LOG_LEVEL 时保留其值。
	if !deviceplugin.ContainerEnvDeclared(ctr, "LIBVROCM_LOG_LEVEL") {
		envs["LIBVROCM_LOG_LEVEL"] = "1"
	}

	libDir := filepath.Join(deviceplugin.OperatorLibDir, "amd")
	ctrResp.Envs = envs
	// IMPORTANT: 挂载 so 文件等
	ctrResp.Mounts = []*deviceplugin.Mount{
		{ContainerPath: ctrLdPreloadPath, HostPath: filepath.Join(libDir, "ld.so.preload"), ReadOnly: true},
		{ContainerPath: ctrVrocmLibPath, HostPath: filepath.Join(libDir, "libvrocm.so"), ReadOnly: true},
		{ContainerPath: ctrVrocmMonPath, HostPath: filepath.Join(libDir, "rocm-monitor"), ReadOnly: true},
		{ContainerPath: ctrVrocmCheckPath, HostPath: filepath.Join(libDir, "rocm-cumask-check"), ReadOnly: true},
		{ContainerPath: ctrLedgerDir, HostPath: ledgerDir, ReadOnly: false},
	}

	return ctrResp, nil
}
