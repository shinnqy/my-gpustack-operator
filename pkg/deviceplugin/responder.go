package deviceplugin

import (
	"context"

	core "k8s.io/api/core/v1"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
)

type (
	// ContainerAllocateResponder 负责处理 kubelet 对容器的分配响应。每个厂商分配器都实现这一功能，并
	// 直接处理专属和共享两类请求。以下两个可选接口会根据请求类型进行分配——硬件分区使用
	// PhysicalSlicedResponder，逻辑切片使用 LogicalSlicedResponder——因此未实现这两种接口的响应器仍可
	// 以保持不变地处理这些请求。
	//
	// 在工作负载分配中，这被称为分配后处理。一旦分配被永久记录，拒绝该分配将由服务器进行补偿：注释条
	// 目和预留资源将被归还。这种补偿仅针对框架本身所设定的规则，这要求每个实现都满足这一要求：一旦在
	// 响应之外产生了任何实体——例如子设备、磁盘上的所有权标记——则不得返回错误。Cambricon 和 MetaX 都实
	// 现了这种机制，且目前都符合这一要求：每次错误返回都会发生在创建操作之前，之后则不会出现错误。若
	// 某个实现违反此规则，会导致硬件保持运行，而服务器则放弃其账本声明，厂商的回收机制也会维持这种状
	// 态——它们保留了一个仍列在 Pod 列表中的实体，以及一个刚刚出现错误的 Pod。这样的响应者需要服务器能
	// 够调用的回滚机制，就像 PhysicalSlicedResponder 所具备的那样，而不是一种猜测式的补偿机制。
	//
	// 该保证仅限于工作负载分配。在 `Allocate` 方法中，虽然调用了此方法，但并未进行任何补丁处理或预留
	// 操作，因此没有可补偿的内容，也没有任何任务在运行。
	ContainerAllocateResponder interface {
		// GetContainerAllocateResponse 根据给定的 Pod、设备和已分配的资源，构造 kubelet 交给容器
		// 的分配结果。
		GetContainerAllocateResponse(
			context.Context,
			*core.Pod,
			*core.Container,
			*workercore.Devices,
			map[Resource]int32,
		) (*ContainerAllocateResponse, error)
	}
)

// Placements 将加速器资源——即其组 ID 加上自身 ID——与该资源上所分配的运行实例对应。它同时包含两个账本，其
// 计量单位也是账本所统计的单位：硬件分区的内存切片索引（NVIDIA: MIG 内存切片），以及逻辑切片的厂商自身计
// 算单元（AMD: CU-mask 位索引，与 HSA_CU_MASK 中的定义一致）。
//
// 由 Resource 进行标识，而非仅由 UUID 标识，因为放置账本的每个生产者与消费者都会按组处理 Devices.Spec 或
// 分配状态，并且掌握着该组信息：决策排名的分区候选者、每个加速器的 AllocatedProfiles/RemainingProfiles，
// 账本折叠写入与匹配状态加速器中的内容，以及服务器发布的预留项，所有这些都以这种方式进行标识。
type Placements map[Resource][]workercore.AcceleratorPlacement
