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
	// PhysicalSlicedResponder 是可选的厂商能力，用于创建硬件分区（如 NVIDIA MIG），
	// 并在同一 Pod 的其他容器请求访问时识别原分区。两项操作放在同一接口，保证能创建分区的实现也能准确指出它；
	// 缺少该接口时不能接受分区请求。
	PhysicalSlicedResponder interface {
		// ActuatePhysicalSliced 为申请 <base>.partitioned.<kind>-<profile> 的容器创建或复用分区。
		// 服务先预留选中的卡，再由厂商在单卡锁内落实；实际规格和位置在写 Pod 注解前记入分配状态。
		ActuatePhysicalSliced(
			context.Context,
			*core.Pod,
			*core.Container,
			*workercore.Devices,
			map[Resource]int32,
			string,
		) (*PhysicalSlicedAllocation, error)

		// GetPhysicalSlicedVisibilityResponse 为边车构造只开放主容器已有分区的响应。它使用容器、
		// 所选卡和归属容器名，从节点上的持久记录确认仍存活的分区。身份不清时返回错误，避免退回空响应或暴露父卡。
		// 各厂商可在完整响应中使用自己的环境变量或设备节点。
		GetPhysicalSlicedVisibilityResponse(
			context.Context,
			*core.Pod,
			*core.Container,
			*workercore.Devices,
			map[Resource]int32,
			string,
		) (*ContainerAllocateResponse, error)
	}
	// LogicalSlicedResponder 适用于切片需要选具体位置的厂商，例如 AMD 会用 CU 掩码划定一段计算单元。
	// 重叠的窗口会使两个容器争用同一段单元。不实现此接口时无需读取或登记逻辑位置。
	//
	// PlaceLogicalSliced 在节点锁内根据现有快照纯计算，结果在解锁前进入预留。
	// GetLogicalSlicedResponse 在锁外用同一结果设置环境、创建目录等，无需重复选址。
	// 逻辑掩码没有在卡上创建硬件对象，所以没有硬件回滚步骤。
	LogicalSlicedResponder interface {
		// PlaceLogicalSliced 根据现有分配的占用，为本容器在每张已分配卡上选择区间。允许返回空映射，
		// 表示此容器无需登记具体位置。
		PlaceLogicalSliced(
			context.Context,
			*core.Pod,
			*core.Container,
			*workercore.Devices,
			map[Resource]int32,
			Placements,
		) (Placements, error)

		// GetLogicalSlicedResponse 按已经发布的位置生成容器响应，
		// 逻辑切片模式用它替代通常的 GetContainerAllocateResponse。
		GetLogicalSlicedResponse(
			context.Context,
			*core.Pod,
			*core.Container,
			*workercore.Devices,
			map[Resource]int32,
			Placements,
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

// PhysicalSlicedAllocation 保存硬件分区操作的结果，包括写回 Pod 注解的各卡实际位置，
// 以及令容器只看到所分区设备的厂商响应。
type PhysicalSlicedAllocation struct {
	// Profile 是实际创建的硬件分区规格，例如 1g.10gb。
	Profile string
	// Placements 记录各张卡上实际占用的显存区间。服务将其写入 Pod 注解，供控制器重建账本。
	Placements Placements
	// IDs 将每张卡映射到驱动分配的分区 ID，例如 MIG UUID。执行器创建或复用实例时读取它，并写入注解，
	// 后续可直接定位分区。
	IDs map[Resource]string
	// Response 携带分区专用的可见设备信息，由服务直接返回；其中没有逻辑切片的挂载等产物。
	Response *ContainerAllocateResponse
	// 写注解失败时，Rollback 撤销本次新建的硬件分区。完全复用已有分区时无需销毁任何东西。
	Rollback func()
}
