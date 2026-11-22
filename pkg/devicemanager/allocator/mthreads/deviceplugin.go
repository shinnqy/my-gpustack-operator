package mthreads

import (
	"context"
	"fmt"
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
)

const Manufacturer = nodefeature.ManufacturerMThreads

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
	// 已分配的设备按照容器编号的顺序排列，其`ID`为 MTHREADS_VISIBLE_DEVICES 值。
	accelerators := deviceplugin.AllocatedAccelerators(devs, allocated)
	ids := deviceplugin.AllocatedAcceleratorIDs(accelerators)

	// 分片容器具有真正的每片 QoS 隔离机制（一个严格的 VRAM 限制 + 相对计算权重）。独占/共享/可见性设
	// 置则确保普通设备可见性响应机制保持不变。
	if s.AllocationMode == workercore.DeviceAllocationModeSliced {
		return s.getSlicedContainerAllocateResponse(pod, ctr, ids, accelerators)
	}

	// 将设备注入任务委托给容器运行时处理，使用设备`ID`作为 MTHREADS_VISIBLE_DEVICES 值。
	ctrResp := &deviceplugin.ContainerAllocateResponse{
		Envs: map[string]string{
			"MTHREADS_VISIBLE_DEVICES": strings.Join(ids, ","),
		},
	}
	return ctrResp, nil
}

// getSlicedContainerAllocateResponse 用于为切片容器实现 MThreads QoS 逻辑切片机制：
// MTHREADS_QOS_MEMORY_LIMIT 是基于容器中的 ".sliced.memory-percentage" 和 `".sliced.memory-mib“ 生成的、
// 针对每个加速器固定的 VRAM 字节限制。而 MTHREADS_QOS_COMPUTING_POWER_WEIGHT 则是从 "
// .sliced.cores-percentage" 中得到的相对计算权重（属于尽力获取范畴，并非固定限制）。加速器状态可通过
// MTHREADS_VISIBLE_DEVICES 查看。主机 sGPU kmod 与 MThreads 容器运行时会在运行时强制执行 QoS 规则，因此不
// 会进行任何文件、挂载或设备节点的处理。
func (s *server) getSlicedContainerAllocateResponse(
	_ *core.Pod,
	ctr *core.Container,
	ids []string,
	accels []deviceplugin.AllocatedAccelerator,
) (*deviceplugin.ContainerAllocateResponse, error) {
	if len(accels) == 0 {
		return nil, fmt.Errorf("no allocated accelerator found for sliced container %q", ctr.Name)
	}

	coresRes := nodefeature.GetAcceleratableSlicedCoresPercentageResourceName(Manufacturer)
	memPctRes := nodefeature.GetAcceleratableSlicedMemoryPercentageResourceName(Manufacturer)
	memMibRes := nodefeature.GetAcceleratableSlicedMemoryMibResourceName(Manufacturer)
	// 分片分配采用单加速器模式，DevicesGroup 中的每个加速器共享相同的 VRAM（内存属于群体属性），因此
	// 分配到的加速器的群体 VRAM 即为上限值。
	memMib, err := deviceplugin.SlicedMemoryMib(ctr, memPctRes, memMibRes, int64(accels[0].Group.Memory))
	if err != nil {
		return nil, fmt.Errorf("derive sliced memory limit: %w", err)
	}

	return &deviceplugin.ContainerAllocateResponse{
		Envs: map[string]string{
			"MTHREADS_VISIBLE_DEVICES":            strings.Join(ids, ","),
			"MTHREADS_QOS_MEMORY_LIMIT":           strconv.FormatInt(memMib*1024*1024, 10),
			"MTHREADS_QOS_COMPUTING_POWER_WEIGHT": strconv.Itoa(deviceplugin.SlicedCoresPercent(ctr, coresRes)),
		},
	}, nil
}
