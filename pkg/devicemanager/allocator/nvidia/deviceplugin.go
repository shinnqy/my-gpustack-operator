package nvidia

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

const Manufacturer = nodefeature.ManufacturerNVIDIA

// injectionConfig 是该制造商用于共享设备注入解析器的术语：变量 nvidia-container-runtime 从某个列表中读取
// 设备信息，而 nvidia-ctk 则负责发布所有加速器。MIG 分区拥有其特有的类型，因此该分区家族从不使用 CDI 通道。
var injectionConfig = deviceplugin.InjectionConfig{
	Manufacturer:      Manufacturer,
	CDIKind:           "nvidia.com/gpu",
	VisibleDevicesEnv: "NVIDIA_VISIBLE_DEVICES",
}

// defaultInjectionResolver 是未使用此组件的响应器所使用的组件。使用单个实例而非每次调用一个实例，因此其“
// 选择了哪个通道”的日志仍会记录一次。
var defaultInjectionResolver = deviceplugin.DefaultInjectionResolver(injectionConfig)

func New(opts device.AllocatorOptions) device.Allocator {
	logger := opts.Logger.WithName(Manufacturer)

	// 硬件分区服务器提供“<base>.partitioned”服务——由“NVIDIA”以自身名义下的“MIG”提供。没有分区类型的制
	// 造商根本没有这样的资源名称，因此不会注册服务器，也不会有宣传空名称的服务器。
	partitioned := !opts.NoPartitioned &&
		nodefeature.GetAcceleratableResourceName(Manufacturer, workercore.DeviceAllocationModePartitioned) != ""
	// MIG 驱动程序在构建时会进行 NVML 初始化，因此它只构建一次——仅在处理分区服务的场景中——并由处理分
	// 区的两台服务器共享：分区服务器会实例化一个实例，可见性服务器则会在再次命名之前确认该实例仍然可
	// 用。不处理分区的节点不会进行任何初始化。
	var mig migDriver
	if partitioned {
		mig = newMigDriver()
	}

	// 节点的一种解决方案：它缓存从主机读取的数据，并记录一次所确定的通道，而非每个响应者都记录一次。
	// 如果配置错误会被报告，节点会保持默认设置，因为拒绝启动分配器会导致加速器随之失效。
	injection, err := deviceplugin.NewInjectionResolver(injectionConfig)
	if err != nil {
		logger.Error(err, "keeping the default device-injection strategy")
		injection = deviceplugin.DefaultInjectionResolver(injectionConfig)
	}

	// IMPORTANT: 分配窗口1 Exclusive（整卡独占）
	servers := []deviceplugin.Server{
		newServer(logger, workercore.DeviceAllocationModeExclusive, nil, injection),
	}
	if !opts.NoShared {
		// IMPORTANT: 分配窗口2 Shared（多容器共享整卡）
		servers = append(servers,
			newServer(logger, workercore.DeviceAllocationModeShared, nil, injection),
		)
	}
	if !opts.NoSliced {
		// IMPORTANT: 分配窗口3 Sliced（软件逻辑切分显存/算力）
		servers = append(servers,
			newServer(logger, workercore.DeviceAllocationModeSliced, nil, injection),
		)
	}
	if partitioned {
		// 分区的响应者不会查询解析器：一个 MIG 实例在分配时被实体化，且没有预生成的 CDI 规范可以
		// 为其命名。
		// IMPORTANT: 分配窗口4 Partitioned（MIG 硬件切分）
		servers = append(servers,
			newServer(logger, workercore.DeviceAllocationModePartitioned, mig, nil),
		)
	}
	// IMPORTANT: 分配窗口5 Visibility（同 Pod 里其他辅助容器共享看到主容器拿到的同一张卡）
	// 可见性服务器会将其容器与所属容器所获得的同一物理 GPU 进行共享分配。在非切片模式下，响应器仅会输
	// 出 NVIDIA_VISIBLE_DEVICES 信息，这正是设备-cgroup 授权所需要的（没有 HAMi 逻辑切片带来的干扰）。
	// 在基于分区管理的加速器上，env 必须指定所属分区的名称，而不是父加速器的名称，这正是共享 MIG 驱动
	// 程序所针对的场景。基于分区的可见性响应由 MIG 路径处理，该路径从不查询解析器。解析器在此用于普通
	// 场景，即共享分配的容器能够像其所属容器那样完全看到整个加速器。
	servers = append(servers,
		newServer(logger, workercore.DeviceAllocationModeVisibility, mig, injection),
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
	// 分区报告了分区服务器是否已注册，从而限制了按供应商的 MIG 回收流程：该流程的存在是为了释放该服务
	// 器创建的实例。
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
	// 设备插件资源池没有 Release 回调，因此 MIG GPU/compute 实例由基于级别的循环机制回收，该机制包含
	// 协调器的广播实时节点集以及同步计时器。
	if in.partitioned {
		tasks = append(tasks, func(ctx context.Context) error {
			reconciler := controllers.Get[*deviceplugin.DevicesReconciler]()
			r := newReclaimer(newMigDriver(), deviceplugin.OperatorPodsDir, in.logger.WithName("reclaim"),
				liveClaimsFrom(ctx, reconciler))
			deviceplugin.RunReclaimLoop(ctx, reconciler, Manufacturer,
				workercore.DeviceAllocationModePartitioned, r.reconcile)
			return nil
		})
	}

	return in.lifecycle.Start(ctx, tasks...)
}

// liveClaimsFrom 将协调器基于注解定义的实时物理切片占用情况，转换为回收器按加速器 UUID 的放置视图（资源的
// 加速器字段即为 NVIDIA 的 GPU UUID）。它是归属自检的源：回收操作不会销毁仍在运行的 Pod，其实例仍会被认领。
func liveClaimsFrom(ctx context.Context, reconciler *deviceplugin.DevicesReconciler) func() (map[string][]migPlacement, error) {
	return func() (map[string][]migPlacement, error) {
		occupied, err := reconciler.LivePhysicalOccupied(ctx)
		if err != nil {
			return nil, err
		}
		claims := make(map[string][]migPlacement, len(occupied))
		for res, placements := range occupied {
			for i := range placements {
				claims[res.Device] = append(claims[res.Device],
					migPlacement{Start: placements[i].Start, Length: placements[i].Length})
			}
		}
		return claims, nil
	}
}

// 停止所有任务，启动后不会返回，直到它们完成。服务器不会被手动关闭：取消其所服务的上下文就会使其退役，之
// 后手动关闭则会将完成退役报告为从未启动的服务器。
func (in *aggregated) Stop() {
	in.logger.Info("stopping")

	in.lifecycle.Stop()
}

type server struct {
	deviceplugin.ResourceServer

	// mig 是 NVML MIG 接缝：分区的响应器会根据“<base>.partitioned.mig`-<规格>”请求来驱动它，而可见性
	// 响应器则读取它以证明一个共分配分区仍然有效。对于其他模式，该值为空。对于完全不提供分区服务的节
	// 点，该值也为空。
	mig migDriver

	// 注入机制决定哪个通道会将授予的加速器传递给容器。一个解析器负责处理节点上每个响应者。如果为
	// `nil`，则采用默认策略。因此，未设置该解析器的响应者会遵循该包在策略存在之前的行为。分区化及分区
	// 支持可见性响应器则完全不参考该机制——具体原因可参考 mig.go 文件，了解为何分区无法通过 CDI 进行传
	// 递。
	injection *deviceplugin.InjectionResolver
}

// resolver 返回该响应者注入解析器，如果未构建解析器则使用默认解析器。
func (s *server) resolver() *deviceplugin.InjectionResolver {
	if s.injection == nil {
		return defaultInjectionResolver
	}

	return s.injection
}

func newServer(
	logger klog.Logger,
	mode workercore.DeviceAllocationMode,
	mig migDriver,
	injection *deviceplugin.InjectionResolver,
) deviceplugin.Server {
	logger = logger.WithName(strings.ToLower(mode.String()))

	s := &server{
		ResourceServer: deviceplugin.ResourceServer{
			Logger:         logger,
			Manufacturer:   Manufacturer,
			AllocationMode: mode,
			Reconciler:     controllers.Get[*deviceplugin.DevicesReconciler](),
		},
		mig:       mig,
		injection: injection,
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
	// 按容器编号顺序分配的 GPUs，对于该厂商而言是按 PCI 总线 ID 从低到高排列的：NVML 以这种方式列出可
	// 见的卡，而 CUDA 则遵循下方注入的 PCI_BUS_ID 顺序。每个位置上的 CUDA_DEVICE_MEMORY_LIMIT_<i> 就
	// 是依据此规则进行读取的，因此以任何其他顺序输出会限制某张卡的性能不超过另一张卡的预算。
	accelerators := deviceplugin.AllocatedAccelerators(devs, allocated)
	ids := deviceplugin.AllocatedAcceleratorIDs(accelerators)

	// 分片容器具备真正的逻辑分片隔离机制（HAMi-核心预加载 + 配额）。独占/共享模式则保留普通设备可见性
	// 响应机制。
	if s.AllocationMode == workercore.DeviceAllocationModeSliced {
		return s.getSlicedContainerAllocateResponse(pod, ctr, ids, accelerators)
	}

	// 拒绝授予最终无用的授权，如上面的分割路径所示。下方的两个通道都不会报告此情况：环境变量将包含空
	// 值，注释则会显示空设备列表，因此响应结果为“成功”，容器无法使用该授权。
	if len(accelerators) == 0 {
		return nil, fmt.Errorf("no allocated accelerator found for container %q", ctr.Name)
	}

	// 以该节点所支持的任何渠道来命名授予的加速器：要么是容器运行时环境变量，要么是容器推理引擎自行解
	// 析的 CDI 请求。只能选择其中一个，不能同时拥有两者。
	ctrResp := &deviceplugin.ContainerAllocateResponse{}
	if err := s.resolver().Apply(s.Logger, ctrResp, pod, ids); err != nil {
		return nil, err
	}
	return ctrResp, nil
}

// HAMi-core 在容器内读取的固定路径。
const (
	ctrLdPreloadPath   = "/etc/ld.so.preload"
	ctrVgpuLibPath     = "/usr/local/vgpu/libvgpu.so"
	ctrVgpuLockPath    = "/tmp/vgpulock"
	ctrVgpuCacheDir    = "/tmp/vgpu"
	ctrVgpuSharedCache = "/tmp/vgpu/cudevshr.cache"
	ctrDevShmPath      = "/dev/shm"
)

// 宿主机锁目录会挂入不同容器，供 HAMi-core 跨进程协调。用变量保存路径，以便单元测试改用临时目录。
var hostVgpuLockPath = "/tmp/vgpulock"

// getSlicedContainerAllocateResponse 将每卡显存 MiB 限额与 SM 百分比分别交给 HAMi-core，挂载预加载库和共享缓存，并交由节点的注入器设置设备可见性。
func (s *server) getSlicedContainerAllocateResponse(
	pod *core.Pod,
	ctr *core.Container,
	ids []string,
	accels []deviceplugin.AllocatedAccelerator,
) (*deviceplugin.ContainerAllocateResponse, error) {
	if len(accels) == 0 {
		return nil, fmt.Errorf("no allocated accelerator found for sliced container %q", ctr.Name)
	}

	// 建立每个容器独立的工作目录，并准备各进程共用的锁目录。
	podWorkDir := deviceplugin.PodWorkDir(string(pod.UID), ctr.Name)
	// 每个容器独立的工作目录（放在 Pod UID + 容器名下），用来存这个容器的共享内存缓存文件 cudevshr.cache，防止容器间数据串台，且 Pod 删除时方便统一回收。
	vgpuCacheDir := filepath.Join(podWorkDir, "tmp/vgpu")
	for _, dir := range []string{hostVgpuLockPath, podWorkDir, vgpuCacheDir} {
		if err := osx.MkdirAll(dir, 0o777); err != nil {
			return nil, fmt.Errorf("create %q: %w", dir, err)
		}
	}

	// 算力取 .sliced.cores-percentage。每张卡的显存上限取 .sliced.memory-percentage 或
	// .sliced.memory-mib，单位是 MiB。HAMi-core 还需要共享缓存路径。
	coresRes := nodefeature.GetAcceleratableSlicedCoresPercentageResourceName(Manufacturer)
	memPctRes := nodefeature.GetAcceleratableSlicedMemoryPercentageResourceName(Manufacturer)
	memMibRes := nodefeature.GetAcceleratableSlicedMemoryMibResourceName(Manufacturer)
	envs := map[string]string{
		"CUDA_DEVICE_SM_LIMIT":            strconv.Itoa(deviceplugin.SlicedCoresPercent(ctr, coresRes)),
		"CUDA_DEVICE_MEMORY_SHARED_CACHE": ctrVgpuSharedCache,
	}
	for i := range accels {
		limit, err := deviceplugin.SlicedMemoryMib(ctr, memPctRes, memMibRes, int64(accels[i].Group.Memory))
		if err != nil {
			return nil, fmt.Errorf("derive sliced memory limit: %w", err)
		}
		envs["CUDA_DEVICE_MEMORY_LIMIT_"+strconv.Itoa(i)] = strconv.FormatInt(limit, 10) + "m"
	}

	// 显存上限按位置编号。HAMi-core 根据 NVML 枚举顺序写入各卡的上限，读取时却使用 CUDA 上下文的设备序
	// 号。CUDA_DEVICE_ORDER=PCI_BUS_ID 让这两个序号一致。默认的性能排序在混合型号节点上可能对调两张卡，
	// 按 NVML 序号设置 CUDA_VISIBLE_DEVICES 的工作负载也依赖这个约定。保留容器显式设置的顺序。目前准入
	// 只允许单卡切片，可见卡在两种顺序下都是 0 号。如果将来允许多卡切片，需要在准入时拒绝不兼容的顺序。
	// 否则 FASTEST_FIRST 可能把一张共享卡的显存额度给另一张卡，挤占同卡其他容器的显存。kubelet 最后追
	// 加容器自己的环境变量，仅靠设备插件注入无法覆盖它。
	if !deviceplugin.ContainerEnvDeclared(ctr, "CUDA_DEVICE_ORDER") {
		envs["CUDA_DEVICE_ORDER"] = "PCI_BUS_ID"
	}

	// HAMi-core 默认日志级别为 1，每次拦截调用都可能输出初始化或清理消息。容器未声明 LIBCUDA_LOG_LEVEL
	// 时设为 0，显式设置则留给工作负载调试。
	if !deviceplugin.ContainerEnvDeclared(ctr, "LIBCUDA_LOG_LEVEL") {
		envs["LIBCUDA_LOG_LEVEL"] = "0"
	}

	// 容器只挂载一份 libvgpu.so，所以分到的所有 GPU 必须使用同一 CUDA 主版本。混用主版本时返回错误。
	cudaDir := nvidiaCUDADir(accels[0].Group.RuntimeVersion)
	for i := range accels {
		if d := nvidiaCUDADir(accels[i].Group.RuntimeVersion); d != cudaDir {
			return nil, fmt.Errorf("sliced container %q spans CUDA majors %s and %s; a single libvgpu cannot serve both", ctr.Name, cudaDir, d)
		}
	}
	libDir := filepath.Join(deviceplugin.OperatorLibDir, "nvidia")
	mounts := []*deviceplugin.Mount{
		{ContainerPath: ctrVgpuLockPath, HostPath: hostVgpuLockPath, ReadOnly: false},
		{ContainerPath: ctrLdPreloadPath, HostPath: filepath.Join(libDir, "ld.so.preload"), ReadOnly: true},
		{ContainerPath: ctrVgpuLibPath, HostPath: filepath.Join(libDir, cudaDir, "libvgpu.so"), ReadOnly: true},
		{ContainerPath: ctrVgpuCacheDir, HostPath: vgpuCacheDir, ReadOnly: false},
		{ContainerPath: ctrDevShmPath, HostPath: ctrDevShmPath, ReadOnly: false},
	}

	ctrResp := &deviceplugin.ContainerAllocateResponse{
		Envs:   envs,
		Mounts: mounts,
	}
	// HAMi-core 通过预加载和环境变量取得配额。设备可见性由本节点的注入方式另行设置。
	if err := s.resolver().Apply(s.Logger, ctrResp, pod, ids); err != nil {
		return nil, err
	}

	return ctrResp, nil
}

// nvidiaCUDADir 根据 CUDA 主版本选择 HAMi-core 库目录。无法识别版本时使用 cuda-12。
func nvidiaCUDADir(runtimeVersion string) string {
	return "cuda-" + device.RuntimeMajor(runtimeVersion, "12")
}
