package ascend

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	core "k8s.io/api/core/v1"
	klog "k8s.io/klog/v2"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/device"
	"gpustack.ai/gpustack/pkg/devicemanager/controllers"
	productascend "gpustack.ai/gpustack/pkg/devicemanager/product/ascend"
	"gpustack.ai/gpustack/pkg/deviceplugin"
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/utils/gox"
	"gpustack.ai/gpustack/pkg/utils/osx"
	"gpustack.ai/gpustack/pkg/utils/strconvx"
	"gpustack.ai/gpustack/pkg/utils/stringx"
)

const Manufacturer = nodefeature.ManufacturerAscend

func New(opts device.AllocatorOptions) device.Allocator {
	logger := opts.Logger.WithName(Manufacturer)
	// 任何能够在一个加速器上放置第二个容器的模式，都会产生相同的容器共享问题，因此这些功能会被构建一
	// 次并由它们共享。Exclusive 会返回 nil：它拥有整个加速器，不得修改该标志。
	share := newShareDriver(logger)
	// A5 产品是一种节点级事实，每种模式都会注入类似的拓扑文件，因此所有模式共享一个解析器，节点只需读
	// 取一次，而非每个服务器各读一次。
	product := productascend.NewResolver(newProductDriver(logger))

	// IMPORTANT: 分配窗口1 Exclusive（整卡独占）
	servers := []deviceplugin.Server{
		newServer(logger, workercore.DeviceAllocationModeExclusive, nil, product),
	}
	if !opts.NoShared {
		// IMPORTANT: 分配窗口2 Shared（多容器共享整卡）
		servers = append(servers,
			newServer(logger, workercore.DeviceAllocationModeShared, share, product),
		)
	}
	if !opts.NoSliced {
		// IMPORTANT: 分配窗口3 Sliced（软件逻辑切分显存/算力）
		servers = append(servers,
			newServer(logger, workercore.DeviceAllocationModeSliced, share, product),
		)
	}
	// IMPORTANT: 分配窗口4 Partitioned（MIG 硬件切分）
	// 可见性服务器会将容器与其所属容器被授予的相同物理 NPU 进行共分配。在非切片模式下，响应方仅会发出
	// 带有精确分配索引的 ASCEND_VISIBLE_DEVICES —— Ascend 没有 `all` 通配符 —— 这正是设备-cgroup 授权
	// 所需的（没有 vcann-rt 逻辑切片的影响）。
	servers = append(servers,
		newServer(logger, workercore.DeviceAllocationModeVisibility, share, product),
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

	// share 是 dcmi 容器共享接缝，响应器为所分配的加速器开启此接缝。仅对于独占资源而言为 nil，因为独
	// 占资源拥有整个加速器，不得触及该标志。
	share shareDriver

	// 产品就是该节点所代表的 A5 形状，这也就是互联网络拓扑文件中的名称。每种模式都包含它，因此与
	// share 不同，它永远不会为 null。
	product *productascend.Resolver
}

func newServer(
	logger klog.Logger,
	mode workercore.DeviceAllocationMode,
	share shareDriver,
	product *productascend.Resolver,
) deviceplugin.Server {
	logger = logger.WithName(strings.ToLower(mode.String()))

	s := &server{
		ResourceServer: deviceplugin.ResourceServer{
			Logger:         logger,
			Manufacturer:   Manufacturer,
			AllocationMode: mode,
			Reconciler:     controllers.Get[*deviceplugin.DevicesReconciler](),
		},
		share:   share,
		product: product,
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
	// 分配的加速器按照容器编号的顺序排列。
	accelerators := deviceplugin.AllocatedAccelerators(devs, allocated)

	resp, err := s.getContainerAllocateResponse(pod, ctr, accelerators)
	if err != nil {
		return nil, err
	}
	// 适用于该模式产生的任何响应，因为拓扑文件是节点的属性，而非注入操作的属性：A5 上的切片容器会读取
	// 与完整加速器相同的互联网络。
	s.setHcclTopoFilePath(resp, accelerators)

	return resp, nil
}

// setHcclTopoFilePath 指定了文件 HCCL 从其中读取该节点互联网络拓扑的路径，该路径位于一个代次中。
//
// 无法读取的拓扑提示会被记录下来并保留，而不会导致分配失败。这是厂商自己的行为——其设备插件在相同的计算路
// 径中出现每个错误时都会提前返回——这是合理的选择：没有该提示的容器在启动时会因自身诊断问题而失败，而此处
// 拒绝分配的方案则会导致那些从未接触网络结构的工作负载被中断。
func (s *server) setHcclTopoFilePath(resp *deviceplugin.ContainerAllocateResponse, accels []deviceplugin.AllocatedAccelerator) {
	i := slices.IndexFunc(accels, func(a deviceplugin.AllocatedAccelerator) bool {
		return a.Group.Family == family950
	})
	if i < 0 {
		return
	}
	// Ascend 检测器将 dcmi 自身的地址信息记录在 PhysicalIndexes 中，格式为{物理 ID，卡片 ID，卡片中的
	// 设备 ID}。
	accel := accels[i].Accel
	if len(accel.PhysicalIndexes) < 3 {
		s.Logger.Info("skipping the hccl topology file, the accelerator carries no dcmi card/device index",
			"accelerator", accel.ID)
		return
	}
	cardID, deviceID := int32(accel.PhysicalIndexes[1]), int32(accel.PhysicalIndexes[2])

	product, err := s.product.Resolve(cardID, deviceID)
	if err != nil {
		s.Logger.Error(err, "skipping the hccl topology file", "accelerator", accel.ID)
		return
	}
	// 厂商未提供文件的产品属于一种回应，而非失败，因此会被记录：空形状意味着读者没有可查找的内容，而
	// 数字则代表驾驶员实际所说的内容。
	path := hcclTopoFilePaths[product.Type]
	if path == "" {
		s.Logger.Info("skipping the hccl topology file, this product has none",
			"accelerator", accel.ID, "productType", product.Type, "productCode", product.Code)
		return
	}

	if resp.Envs == nil {
		resp.Envs = make(map[string]string, 1)
	}
	resp.Envs[hcclTopoFilePathEnv] = path
}

func (s *server) getContainerAllocateResponse(
	pod *core.Pod,
	ctr *core.Container,
	accelerators []deviceplugin.AllocatedAccelerator,
) (*deviceplugin.ContainerAllocateResponse, error) {
	// 分片容器具备真正的逻辑分片隔离功能（vcann-rt 预加载 + 配额限制）。其他模式则返回如下的普通设备
	// 可见性响应，中间仅包含容器共享的预处理步骤。
	if s.AllocationMode == workercore.DeviceAllocationModeSliced {
		return s.getSlicedContainerAllocateResponse(pod, ctr, accelerators)
	}

	// 在以下预检查操作之前已渲染完成，因此没有 dcmi 地址的加速器在任何内容到达主机之前就无法完成分配。
	visible, err := visibleDevices(accelerators)
	if err != nil {
		return nil, err
	}

	// Shared 和 visibility 机制会在加速器上创建第二个容器，因此它们需要与 preflight slice 相同的功能。
	// 与 slice 不同，它们可以包含多个加速器，因此也需要循环处理。Exclusive 机制是特意设计的：一个容器
	// 拥有整个加速器，因此无需进行权限验证，也没有理由取消驱动程序自身的单容器保护机制。
	//
	// 那个保护机制正是预检查所启用的功能，在运行 V1 DCMI API 的 910B2 上进行了测量：当模式关闭时，打
	// 开同一设备的第二个容器会失败。这并非普遍适用的规则。提供 V2 API 的驱动程序在其 API 中没有任何此
	// 类标志声明，预检查允许这些分配通过，而不是因为该代次没有对应的入口点而拒绝它们——参见
	// errShareNotDeclared。目前尚无法确认该代次是否通过其他方式实施同样的单容器保护机制。
	switch s.AllocationMode {
	case workercore.DeviceAllocationModeShared, workercore.DeviceAllocationModeVisibility:
		for i := range accelerators {
			if err := s.ensureShareEnabled(accelerators[i].Accel); err != nil {
				return nil, err
			}
		}
	}

	// 将委托给容器运行时进行设备注入，使用驱动程序索引作为 ASCEND_VISIBLE_DEVICES 值，目前不支持
	// Atlas200I。
	//
	// ASCEND_RUNTIME_OPTIONS 被故意省略了，尽管厂商自带的插件总是将其写入 visibility env 中。它只有两
	// 个值：NODRV 和 VIRTUAL。每次读取时都会对查找结果进行 strings.Contains 检查，对于不存在的键会返
	// 回空字符串——因此省略该参数并设置厂商为非 vNPU 分配所设定的空字符串，其输入结果是一致的。不过这
	// 两个值在这里都不适用：NODRV 会抑制依赖该注入的驱动挂载行为，而 VIRTUAL 则选择厂商的 vNPU 分割方
	// 式，这种分配器并不使用该方式——它只是通过 vcann-rt 进行分割，而 A5 根本不支持这种方式。
	ctrResp := &deviceplugin.ContainerAllocateResponse{
		Envs: map[string]string{
			"ASCEND_VISIBLE_DEVICES": visible,
		},
	}
	return ctrResp, nil
}

// family950 属于 A5 家族，该代次负责推理服务 V2 API 的推理工作，也是唯一一个其可见性环境编号不同的实例。
// 它是共享包中的常量，由于该文件几乎在每一行都引用它，因此在此处进行了别名处理。
const family950 = productascend.Family

// physicalIndex 返回探测器在加速器 PhysicalIndexes 中记录的 dcmi 物理标识，该标识就是 vcann-rt 配置中规定
// 的密钥数量，作为 pack/ 下的已打包 dsmi 补丁，它来自硬件记录。
//
// 就是那个数字，而非操作者的自身逻辑索引。这两个数字仅在主机上检测到所有加速器时才会一致，逻辑索引则统计
// 已检测到的数量，因此当某个加速器无法通过探针测试时，所有后续的逻辑索引都会低于其物理编号。
//
// 没有 dcmi 寻址的记录会被判定为格式错误，而非性能下降，因此会被拒绝，不会根据用于指代其他加速器猜测的数
// 字进行分配。
func physicalIndex(accel *workercore.Accelerator) (uint32, error) {
	if len(accel.PhysicalIndexes) == 0 {
		return 0, fmt.Errorf("accelerator %q carries no dcmi physical index", accel.ID)
	}
	return accel.PhysicalIndexes[0], nil
}

// visibleIndex 返回了厂商运行时解析 ASCEND_VISIBLE_DEVICES 中某个条目时所得到的编号。
//
// dcmi 的两个数字中，哪一个与代次有关，且它们是不同的数字：
//
//   - 除了 A5 之外，都是物理 ID。ascend-docker-runtime 将每个可见设备视为物理 ID，并在命名为 /dev/davinci
//     节点之前将其转换为逻辑 ID。
//   - 在 A5 上，这是 dcmi 设备（逻辑）ID，探测器将其记录在 slot 1 中——该代次没有卡片级别，因此 cardId ==
//     devId == logicId。厂商自带的设备插件会在输出环境数据之前将其转换，而运行时的两种注入路径（传统规范
//     路径和 CDI 路径）随后会严格按照该值构建 /dev/davinci<N>，不会进行任何自身转换。将物理 ID 发送过去后，
//     无论驱动程序如何编号，都会将之指向另一个加速器。
//
// 仅包含其物理 ID 的 A5 记录被拒绝，原因与完全未包含地址信息的记录相同：这里没有运行时可以解析的数字，且
// 物理 ID 也不是某个名称下的一个。
func visibleIndex(group *workercore.DevicesGroup, accel *workercore.Accelerator) (uint32, error) {
	if group.Family != family950 {
		return physicalIndex(accel)
	}
	if len(accel.PhysicalIndexes) < 2 {
		return 0, fmt.Errorf("accelerator %q carries no dcmi device index", accel.ID)
	}
	return accel.PhysicalIndexes[1], nil
}

// visibleDevices 渲染了 ASCEND_VISIBLE_DEVICES 的值：即厂商运行时解析的每个分配的加速器数量，以容器编号的
// 顺序用逗号连接。
func visibleDevices(accels []deviceplugin.AllocatedAccelerator) (string, error) {
	indexes := make([]string, 0, len(accels))
	for i := range accels {
		index, err := visibleIndex(accels[i].Group, accels[i].Accel)
		if err != nil {
			return "", err
		}
		indexes = append(indexes, strconvx.FormatUint(index, 10))
	}
	return strings.Join(indexes, ","), nil
}

// vcann-rt 在容器内读取的固定路径。
const (
	ctrLdPreloadPath = "/etc/ld.so.preload"
	ctrVruntimePath  = "/opt/enpu/vcann-rt/lib/libvruntime.so"
	ctrMonitorPath   = "/opt/enpu/vcann-rt/tools/enpu-monitor"
	ctrConfigPath    = "/etc/enpu/vcann-rt/npu_info.config"
	ctrDevShmPath    = "/dev/shm"

	// vcann-rt 的调度策略取值为 1（固定份额）、2（弹性，默认）或 3（尽力而为）。
	vcannSchedulingPolicy = 2
)

// getSlicedContainerAllocateResponse 将每卡显存和 AI Core 预算写入容器自己的 npu_info.config，挂载 vcann-rt 并设置 ASCEND_VISIBLE_DEVICES。vcann-rt 的一份配置只对应一张物理 NPU。
func (s *server) getSlicedContainerAllocateResponse(
	pod *core.Pod,
	ctr *core.Container,
	accels []deviceplugin.AllocatedAccelerator,
) (*deviceplugin.ContainerAllocateResponse, error) {
	if len(accels) == 0 {
		return nil, fmt.Errorf("no allocated accelerator found for sliced container %q", ctr.Name)
	}
	// 一份 npu_info.config 只描述一张物理 NPU。分配多张时拒绝请求，避免只限制第一张却把其余 NPU 一并暴
	// 露给容器。
	if len(accels) > 1 {
		return nil, fmt.Errorf("sliced container %q allocated %d accelerators, but vcann-rt logical slicing is single-NPU", ctr.Name, len(accels))
	}

	// 下面为唯一分配到的 NPU 构造配置。
	//
	// 容器运行时解析可见设备号，vcann-rt 配额文件使用物理编号。A5 上两个编号可能不同，见 visibleIndex。
	// 950 上以物理编号写此配置尚待真机确认，厂商未提供该代 vcann-rt 供核对。
	group, accel := accels[0].Group, accels[0].Accel
	npuID, err := physicalIndex(accel)
	if err != nil {
		return nil, err
	}
	visible, err := visibleIndex(group, accel)
	if err != nil {
		return nil, err
	}

	// AI Core 算力百分比与显存 MiB 分别计算。
	coresPct := deviceplugin.SlicedCoresPercent(ctr,
		nodefeature.GetAcceleratableSlicedCoresPercentageResourceName(Manufacturer))
	memMib, err := deviceplugin.SlicedMemoryMib(ctr,
		nodefeature.GetAcceleratableSlicedMemoryPercentageResourceName(Manufacturer),
		nodefeature.GetAcceleratableSlicedMemoryMibResourceName(Manufacturer),
		int64(group.Memory))
	if err != nil {
		return nil, fmt.Errorf("derive sliced memory limit: %w", err)
	}

	// 接下来会修改宿主驱动的共享开关，先完成请求和配额校验。
	if err = s.ensureShareEnabled(accel); err != nil {
		return nil, err
	}

	podWorkDir := deviceplugin.PodWorkDir(string(pod.UID), ctr.Name)
	if err = osx.MkdirAll(podWorkDir, 0o777); err != nil {
		return nil, fmt.Errorf("create pod work dir %q: %w", podWorkDir, err)
	}

	configHostPath := filepath.Join(podWorkDir, "etc/enpu/vcann-rt/npu_info.config")
	vnpuID := lowestFreeVNPUID(deviceplugin.OperatorPodsDir, npuID, configHostPath)
	cfg := renderNPUInfoConfig(npuID, vnpuID, coresPct, memMib, accel.ID)
	if err = osx.WriteFile(configHostPath, stringx.ToBytes(&cfg), 0o644); err != nil {
		return nil, fmt.Errorf("write npu_info.config %q: %w", configHostPath, err)
	}

	cannDir := ascendCANNDir(group.RuntimeVersion, group.Family)
	libDir := filepath.Join(deviceplugin.OperatorLibDir, "ascend")
	mounts := []*deviceplugin.Mount{
		{ContainerPath: ctrLdPreloadPath, HostPath: filepath.Join(libDir, "ld.so.preload"), ReadOnly: true},
		{ContainerPath: ctrVruntimePath, HostPath: filepath.Join(libDir, cannDir, "lib/libvruntime.so"), ReadOnly: true},
		{ContainerPath: ctrMonitorPath, HostPath: filepath.Join(libDir, cannDir, "tools/enpu-monitor"), ReadOnly: true},
		{ContainerPath: ctrConfigPath, HostPath: configHostPath, ReadOnly: true},
		{ContainerPath: ctrDevShmPath, HostPath: ctrDevShmPath, ReadOnly: false},
	}

	// 可见性环境变量只写入这张 NPU 的运行时编号。
	envs := map[string]string{
		"ASCEND_VISIBLE_DEVICES": strconvx.FormatUint(visible, 10),
	}

	// vcann-rt 的 ENPU_LOG_LEVEL 默认为 3，拦截调用会输出详细信息。仅在容器没有显式声明时设为 1，保留
	// 调试入口。
	if !deviceplugin.ContainerEnvDeclared(ctr, "ENPU_LOG_LEVEL") {
		envs["ENPU_LOG_LEVEL"] = "1"
	}

	// 默认让 npu-smi info 显示切片额度。项目的 vcann-rt 补丁通过 ENPU_DSMI_HOOK 启用 dsmi_get_hbm_info
	// 拦截。容器显式声明的值优先，包括设为关闭。ContainerEnvDeclared 看不到 envFrom 的值，若要退出此默
	// 认行为，须在容器 Env 中声明。
	if !deviceplugin.ContainerEnvDeclared(ctr, "ENPU_DSMI_HOOK") {
		envs["ENPU_DSMI_HOOK"] = "1"
	}

	return &deviceplugin.ContainerAllocateResponse{
		Envs:   envs,
		Mounts: mounts,
	}, nil
}

// lowestFreeVNPUID 扫描同一物理 NPU 上其他容器的配置，选取最小的空闲虚拟 NPU 编号。重启或重试时，
// 如果当前容器已有属于该物理卡的有效配置，就沿用原编号。
func lowestFreeVNPUID(podsDir string, npuId uint32, selfConfigPath string) int {
	if phy, virt, ok := parseNPUInfoConfig(selfConfigPath); ok && phy == int(npuId) {
		return virt
	}

	used := make(map[int]bool)
	matches, _ := filepath.Glob(filepath.Join(podsDir, "*", "*", "etc/enpu/vcann-rt/npu_info.config"))
	for _, f := range matches {
		if f == selfConfigPath {
			continue
		}
		if phy, virt, ok := parseNPUInfoConfig(f); ok && phy == int(npuId) {
			used[virt] = true
		}
	}
	for i := 0; ; i++ {
		if !used[i] {
			return i
		}
	}
}

// renderNPUInfoConfig 生成 vcann-rt 读取的 npu_info.config。
//
// physical-npu-id 是驱动的物理设备编号。探测漏掉某张卡时，Operator 的逻辑序号可能已和它错开。
// aicore-quota 取 .sliced.cores-percentage，是计算预算百分比。
// memory-quota 是从 .sliced.memory-percentage 或 .sliced.memory-mib 推导的每卡显存 MiB。
// shm-id 将设备 ID 中的空格换成连字符，生成 vcann-rt 需要的 VDie-ID 格式。
// scheduling-policy 固定为弹性策略 2。
func renderNPUInfoConfig(npuId uint32, vnpuId, aicoreQuota int, memoryQuotaMib int64, acceleratorID string) string {
	shmID := strings.ReplaceAll(acceleratorID, " ", "-")
	var b strings.Builder
	_, _ = fmt.Fprintf(&b, "physical-npu-id=%d\n", npuId)
	_, _ = fmt.Fprintf(&b, "virtual-npu-id=%d\n", vnpuId)
	_, _ = fmt.Fprintf(&b, "aicore-quota=%d\n", aicoreQuota)
	_, _ = fmt.Fprintf(&b, "memory-quota=%d\n", memoryQuotaMib)
	_, _ = fmt.Fprintf(&b, "shm-id=%s\n", shmID)
	_, _ = fmt.Fprintf(&b, "scheduling-policy=%d\n", vcannSchedulingPolicy)
	return b.String()
}

// parseNPUInfoConfig 从 vcann-rt 配置中读取 physical-npu-id 和 virtual-npu-id。
func parseNPUInfoConfig(path string) (npuId, vnpuId int, ok bool) {
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, false
	}
	defer func() { _ = f.Close() }()

	phy, vnpu := -1, -1
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		key, val, found := strings.Cut(scanner.Text(), "=")
		if !found {
			continue
		}
		n, err := strconv.Atoi(strings.TrimSpace(val))
		if err != nil {
			continue
		}
		switch strings.TrimSpace(key) {
		case "physical-npu-id":
			phy = n
		case "virtual-npu-id":
			vnpu = n
		}
	}
	if phy < 0 || vnpu < 0 {
		return 0, 0, false
	}
	return phy, vnpu, true
}

// ascendCANNDir 按 CANN 主版本和小写型号族选择 vcann-rt 的库目录，例如 cann-8-910b。未知版本的默认
// 主版本为 8。
func ascendCANNDir(runtimeVersion, family string) string {
	return "cann-" + device.RuntimeMajor(runtimeVersion, "8") + "-" + strings.ToLower(family)
}
