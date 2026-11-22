package thead

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

const Manufacturer = nodefeature.ManufacturerTHead

func New(opts device.AllocatorOptions) device.Allocator {
	logger := opts.Logger.WithName(Manufacturer)

	// 硬件分区服务器提供“<base>.partitioned”服务——厂商将其自己的分区功能称为 MIG，因此也使用了这个名
	// 称。没有分区功能的制造商根本没有这样的资源名称，因此不会注册服务器，也不会有宣传空名称的服务器。
	partitioned := !opts.NoPartitioned &&
		nodefeature.GetAcceleratableResourceName(Manufacturer, workercore.DeviceAllocationModePartitioned) != ""
	// 分区驱动程序在构建过程中会初始化一个厂商管理库，因此它只会被构建一次——仅在处理分区服务的场景中—
	// —并由处理这些分区的两台服务器共享：分区服务器会实例化一个实例，可见性服务器则会在再次命名之前确
	// 认其仍然处于正常运行状态。不处理分区的节点不会进行任何初始化操作。
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
	// 该逻辑切片服务器提供 "<base>.sliced"：作为某个加速器的一部分份额，该限制由该操作符图像所构建的
	// 预加载对机制在容器内部实施，而非由驱动程序实现。它不需要分区驱动程序——逻辑切片永远不会触及分区
	// 表面。
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
	// 可见性服务器会将容器共同分配至其所属容器已获授权的同一物理设备：其“分配”操作会复用所有者的预留
	// 设备，响应方也会返回与未切片模式相同的简单设备可见性响应（仅支持设备-cgroup 访问，无切片伪影）。
	// 在基于分区管理的加速器上，该响应必须指向所有者的分区，而非父加速器，而这正是共享驱动程序所负责
	// 的内容。
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
	// 分区报告了分区服务器是否已注册，从而限制了按供应商分区的实例回收循环：该循环的存在是为了释放该
	// 服务器创建的实例。
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
	// 设备插件资源池没有释放回调，因此分区服务器创建的 GPU/compute 实例会被一个基于级别的循环机制回收，
	// 该机制由协调器的广播实时节点集以及同步计时器组成。该循环机制会使用其自身的驱动实例，复制蓝图配
	// 置，而非共享服务器的配置。
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

// liveClaimsFrom 将协调器基于注解得到的实时物理切片占用情况，转换为回收器针对每个加速器的放置视图（
// Resource Accelerator 字段即为该厂商的加速器 UUID）。它是归属权自我校验的来源：回收过程不会销毁正在运行
// 的 Pod，它仍然拥有归属权。
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

	// mig 是厂商分区接缝：分区响应器为满足“<base>.partitioned.mig`-<profile>”请求而使用它，可见性响应
	// 器读取它以证明共分配分区仍然有效。对于其他模式，该值为零。对于完全不提供分区服务的节点，该值也
	// 为零。
	mig migDriver
}

func newServer(logger klog.Logger, mode workercore.DeviceAllocationMode, mig migDriver) deviceplugin.Server {
	logger = logger.WithName(strings.ToLower(mode.String()))

	s := &server{
		ResourceServer: deviceplugin.ResourceServer{
			Logger:         logger,
			Manufacturer:   Manufacturer,
			AllocationMode: mode,
			Reconciler:     controllers.Get[*deviceplugin.DevicesReconciler](),
		},
		mig: mig,
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
	ctrResp := &deviceplugin.ContainerAllocateResponse{}

	// mount 控制设备。每个容器需要这些设备一次，而非每个加速器都需要。这里每个节点都必须配备：该厂商
	// 没有容器运行时间隔机制，因此注入的节点就相当于整个容器的访问范围。如果缺少其中某个设备，就会启
	// 动一个根本无法指向其加速器的容器。这些设备的解析是通过分区路径所使用的相同失败关闭辅助机制来完
	// 成的，而不是通过共享设备规格辅助机制——后者对于不存在的路径会返回 nil，并将缺失的节点转化为一个
	// 无声地不完整的成功分配。
	for _, p := range sharedControlNodePaths() {
		pDev, err := requireDeviceNode(p)
		if err != nil {
			return nil, err
		}
		ctrResp.Devices = append(ctrResp.Devices, pDev)
	}

	// 按切片路径索引的每加速器内存数值顺序，运行所指定的设备，并使用分配的加速器。
	accelerators := deviceplugin.AllocatedAccelerators(devs, allocated)
	for i := range accelerators {
		// 厂商根据加速器的卡片序号——即加速器索引——来命名其设备节点。通过分区路径所使用的同一保护
		// 机制，可以证明该序号确实指向了加速器，而不是假设出来的。它所命名的节点必须携带探测器为
		// 该加速器记录的小数值。无法被证明的加速器将被拒绝，而不是被赋予一个缺乏自身节点的设备集
		// 合，或者携带邻近加速器的节点。
		_, cardNode, err := requireCardNode(devs, accelerators[i].Accel.ID)
		if err != nil {
			return nil, err
		}
		ctrResp.Devices = append(ctrResp.Devices, cardNode)
	}

	// 分片容器会包含相同的设备集，以及 shim 的预加载和配额设置。其他模式的响应则仅包含设备集，因为该
	// 厂商没有用于解释其他内容的容器运行时钩子。
	if s.AllocationMode == workercore.DeviceAllocationModeSliced {
		return s.getSlicedContainerAllocateResponse(pod, ctr, ctrResp.Devices, accelerators)
	}

	return ctrResp, nil
}

// 容器内部路径，PPU 切片层所期望的路径。这两个共享对象位于 ld.so.preload 资源所指定的位置，因此文件和这些
// 常量属于同一规范。
const (
	ctrLdPreloadPath = "/etc/ld.so.preload"
	ctrDlsymHookPath = "/usr/local/vppu/hgml_dlsym_hook.so"
	ctrQuotaLibPath  = "/usr/local/vppu/hggc_quota.so"
	ctrMonitorPath   = "/usr/local/vppu/ppu-monitor"
	ctrLedgerDir     = "/var/run/vppu"
	ctrLedgerPath    = ctrLedgerDir + "/ledger"
)

// getSlicedContainerAllocateResponse 用于实现切片容器的逻辑切片注入功能：配额定义了 shim 在加载时的读取量、
// 预加载的挂载点，以及其跨进程使用区域所在的可写目录。devices 是容器已验证的设备集合，切片会保持不变——厂
// 商没有容器运行时钩子，因此节点即代表容器的全部访问权限，无论该容器是否拥有加速器的一部分或全部。
func (s *server) getSlicedContainerAllocateResponse(
	pod *core.Pod,
	ctr *core.Container,
	devices []*deviceplugin.DeviceSpec,
	accels []deviceplugin.AllocatedAccelerator,
) (*deviceplugin.ContainerAllocateResponse, error) {
	if len(accels) == 0 {
		return nil, fmt.Errorf("no allocated accelerator found for sliced container %q", ctr.Name)
	}

	// 使用区域按容器划分，而非按节点划分，与 NVIDIA 分支中的主机 /dev/shm 不同：该区域由容器本地化的
	// 加速器索引来标识，因此共享位置会导致两个容器的索引 0 占用一个槽位。该位置位于 Pod 工作目录下，
	// 因此现有的每个 Pod 回收机制会随 Pod 一同被移除。shim 会自行创建该区域文件。这仅是其目录，因为工
	// 作负载的用户并非我们所能预测，因此该目录为全局可写。
	ledgerDir := filepath.Join(deviceplugin.PodWorkDir(string(pod.UID), ctr.Name), "run/vppu")
	if err := osx.MkdirAll(ledgerDir, 0o777); err != nil {
		return nil, fmt.Errorf("create %q: %w", ledgerDir, err)
	}

	// 变量 `shape` 属于 HAMi-核心，这是有意为之的：每个加速器对应的内存限制由循环位置索引，以及一个未
	// 索引的计算限制，该限制被 shim 视为所有没有自身数值的加速器的上限。准入机制将逻辑切片绑定到单个
	// 加速器上，因此今天索引为 0。无论如何，循环都被写入为多个，与 NVIDIA 分支的情况一致。
	//
	// 没有从 HAMi-核心模块复制的内容，是指那个缺失的计算指标。它默认会占用整个加速器上的计算资源，导
	// 致该加速器在此处无法使用。当没有进行任何请求时，SlicedCoresPercent 的值会返回 100——因此即使为
	// 100，该指标也会被输出，因为忽略它就会与“无计算配额”状态在下游系统中无法区分。
	coresRes := nodefeature.GetAcceleratableSlicedCoresPercentageResourceName(Manufacturer)
	memPctRes := nodefeature.GetAcceleratableSlicedMemoryPercentageResourceName(Manufacturer)
	memMibRes := nodefeature.GetAcceleratableSlicedMemoryMibResourceName(Manufacturer)
	envs := map[string]string{
		"HGGC_DEVICE_SM_LIMIT": strconv.Itoa(deviceplugin.SlicedCoresPercent(ctr, coresRes)),
		"HGGC_LEDGER_PATH":     ctrLedgerPath,
	}
	for i := range accels {
		// MiB 这个单位没有后缀，与 NVIDIA 分支的“…m”不同：HAMi-核心会解析后缀，而 shim 则解析纯
		// MiB 整数。
		memMib, err := deviceplugin.SlicedMemoryMib(ctr, memPctRes, memMibRes, int64(accels[i].Group.Memory))
		if err != nil {
			return nil, fmt.Errorf("derive sliced memory limit: %w", err)
		}
		envs["HGGC_DEVICE_MEMORY_LIMIT_"+strconv.Itoa(i)] = strconv.FormatInt(memMib, 10)
	}

	// 明确切片所运行的详细程度，而不是继承它：级别 1 报告拒绝和错误，这是当前该插件的默认设置。将其命
	// 名为特定名称，使该级别成为分配行为的属性，而非库默认设置，这样后续插件更新时就不会将其覆盖。设
	// 置 LIBHGGC_LOG_LEVEL 的容器本身会保留其值——这就是调试的出口通道。
	if !deviceplugin.ContainerEnvDeclared(ctr, "LIBHGGC_LOG_LEVEL") {
		envs["LIBHGGC_LOG_LEVEL"] = "1"
	}

	libDir := filepath.Join(deviceplugin.OperatorLibDir, "thead")
	return &deviceplugin.ContainerAllocateResponse{
		Devices: devices,
		Envs:    envs,
		Mounts: []*deviceplugin.Mount{
			{ContainerPath: ctrLdPreloadPath, HostPath: filepath.Join(libDir, "ld.so.preload"), ReadOnly: true},
			{ContainerPath: ctrDlsymHookPath, HostPath: filepath.Join(libDir, "hgml_dlsym_hook.so"), ReadOnly: true},
			{ContainerPath: ctrQuotaLibPath, HostPath: filepath.Join(libDir, "hggc_quota.so"), ReadOnly: true},
			{ContainerPath: ctrMonitorPath, HostPath: filepath.Join(libDir, "ppu-monitor"), ReadOnly: true},
			{ContainerPath: ctrLedgerDir, HostPath: ledgerDir, ReadOnly: false},
		},
	}, nil
}
