package iluvatar

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

const Manufacturer = nodefeature.ManufacturerIluvatar

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
	// 分片服务器提供 "<base>.sliced"：来自一个加速器的一部分资源，该资源由该操作器的图像阶段中的
	// HAMi-core 预加载对子在容器内部强制执行，而非由驱动程序执行。corex 的 ix-container-runtime 仍会
	// 注入加速器，因此该分片不需要分区驱动程序。
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

func (s *server) GetContainerAllocateResponse(
	_ context.Context,
	pod *core.Pod,
	ctr *core.Container,
	devs *workercore.Devices,
	allocated map[deviceplugin.Resource]int32,
) (*deviceplugin.ContainerAllocateResponse, error) {
	// 分配的加速器按照容器编号的顺序排列，以及它们的 `UUID`——即 IX_VISIBLE_DEVICES 值。这里的切片请求
	// 是单个加速器，因此顺序对内存限制没有额外影响。无论采用哪种方式，都是同一个集合。
	accelerators := deviceplugin.AllocatedAccelerators(devs, allocated)
	ids := deviceplugin.AllocatedAcceleratorIDs(accelerators)

	// 分片容器具备真正的逻辑分片隔离机制（HAMi-核心预加载 + 配额）。独占/共享/可见性设置则保留了普通
	// 设备可见性响应机制。
	if s.AllocationMode == workercore.DeviceAllocationModeSliced {
		return s.getSlicedContainerAllocateResponse(pod, ctr, ids, accelerators)
	}

	// 将设备注入任务委托给容器运行时处理，将 GPU UUID 作为 IX_VISIBLE_DEVICES 值使用。
	ctrResp := &deviceplugin.ContainerAllocateResponse{
		Envs: map[string]string{
			"IX_VISIBLE_DEVICES": strings.Join(ids, ","),
		},
	}
	return ctrResp, nil
}

// 容器内部路径是 HAMi-核心逻辑切片运行时所期望的。libvgpu.so 位于 iluvatar ld.so.preload 资源文件指定的位
// 置，因此这些文件和常量属于同一规范。
const (
	ctrLdPreloadPath   = "/etc/ld.so.preload"
	ctrVgpuLibPath     = "/usr/local/vgpu/libvgpu.so"
	ctrVgpuLockPath    = "/tmp/vgpulock"
	ctrVgpuCacheDir    = "/tmp/vgpu"
	ctrVgpuSharedCache = "/tmp/vgpu/cudevshr.cache"
	ctrDevShmPath      = "/dev/shm"
)

// hostVgpuLockPath 是主机 /tmp 挂载路径下，HAMi-core 用于跨进程锁定的主机目录。它是一个变量（而非常量），
// 因此测试中可以将其重定向到真实的 /tmp 之外。
var hostVgpuLockPath = "/tmp/vgpulock"

// getSlicedContainerAllocateResponse 实现了用于切片容器的 HAMi-核心逻辑切片注入机制：包括来自容器 "
// .sliced.cores-percentage" 的计算（SM）限制，以及来自其 ".sliced.memory-percentage" 和 "
// .sliced.memory-mib" 的每加速器 VRAM 限制（独立维度，无统一比例），此外还包含预加载 libvgpu.so 并提供共
// 享锁/缓存的挂载项。加速器通过 IX_VISIBLE_DEVICES 保持可见性（corex 的 ix-container-runtime 会注入该机制）。
// 通过 /etc/ld.so.preload 预加载的 HAMi-核心在运行时强制执行这些限制。corex 采用单一 CUDA 兼容的驱动级别，
// 因此一个分步骤操作的 HAMi-核心 libvgpu 即可服务于所有加速器——无需像 NVIDIA 分支那样进行每加速器 CUDA 级
// 别的协调。
func (s *server) getSlicedContainerAllocateResponse(
	pod *core.Pod,
	ctr *core.Container,
	ids []string,
	accels []deviceplugin.AllocatedAccelerator,
) (*deviceplugin.ContainerAllocateResponse, error) {
	if len(accels) == 0 {
		return nil, fmt.Errorf("no allocated accelerator found for sliced container %q", ctr.Name)
	}

	// 每个容器工作目录 + 共享的跨进程锁定。
	podWorkDir := deviceplugin.PodWorkDir(string(pod.UID), ctr.Name)
	vgpuCacheDir := filepath.Join(podWorkDir, "tmp/vgpu")
	for _, dir := range []string{hostVgpuLockPath, podWorkDir, vgpuCacheDir} {
		if err := osx.MkdirAll(dir, 0o777); err != nil {
			return nil, fmt.Errorf("create %q: %w", dir, err)
		}
	}

	// Envs：从 .sliced.cores-百分比中获取 SM（计算）百分比，以及从 .sliced.memory-百分比 /
	// .sliced.memory-mib（独立维度）中获取每个加速器 VRAM 限制（MiB），再加上共享缓存。准入机制将逻辑
	// 切片绑定到单个加速器上，因此循环仅运行一次并输出 CUDA_DEVICE_MEMORY_LIMIT_0。无论如何都会进行多
	// 次写入，与 NVIDIA 分支的设定一致。
	coresRes := nodefeature.GetAcceleratableSlicedCoresPercentageResourceName(Manufacturer)
	memPctRes := nodefeature.GetAcceleratableSlicedMemoryPercentageResourceName(Manufacturer)
	memMibRes := nodefeature.GetAcceleratableSlicedMemoryMibResourceName(Manufacturer)
	envs := map[string]string{
		"IX_VISIBLE_DEVICES":              strings.Join(ids, ","),
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

	// 默认情况下，HAMi-核心的每次调用拦截日志记录为静默模式：其 LIBCUDA_LOG_LEVEL 默认值为 1，会在每
	// 次拦截的调用中打印 [HAMI-core Msg ...] 行信息。设置 LIBCUDA_LOG_LEVEL 的容器会保留该值——即调试
	// 通道——因此只有在工作负载未声明使用静默模式时，才需要注入默认的静默设置。
	if !deviceplugin.ContainerEnvDeclared(ctr, "LIBCUDA_LOG_LEVEL") {
		envs["LIBCUDA_LOG_LEVEL"] = "0"
	}

	libDir := filepath.Join(deviceplugin.OperatorLibDir, Manufacturer)
	mounts := []*deviceplugin.Mount{
		{ContainerPath: ctrVgpuLockPath, HostPath: hostVgpuLockPath, ReadOnly: false},
		{ContainerPath: ctrLdPreloadPath, HostPath: filepath.Join(libDir, "ld.so.preload"), ReadOnly: true},
		{ContainerPath: ctrVgpuLibPath, HostPath: filepath.Join(libDir, "libvgpu.so"), ReadOnly: true},
		{ContainerPath: ctrVgpuCacheDir, HostPath: vgpuCacheDir, ReadOnly: false},
		{ContainerPath: ctrDevShmPath, HostPath: ctrDevShmPath, ReadOnly: false},
	}

	return &deviceplugin.ContainerAllocateResponse{
		Envs:   envs,
		Mounts: mounts,
	}, nil
}
