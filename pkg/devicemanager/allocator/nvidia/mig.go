package nvidia

import (
	"errors"
)

// migPlacement 是加速器上的一段内存切片区间 [开始位置, 开始位置+长度)，这是插槽选取和标记所依赖的与平台无
// 关的放置几何结构，因此纯核心模块可以在 Darwin 上进行测试。NVML 放置类型则保留在 _linux 接口中。
type migPlacement struct {
	Start  int32
	Length int32
}

// migInstance 是加速器上存在一个实时运行或新创建的 MIG 分区：其 GPU 实例和计算实例的 ID、计算切片数量（该
// 数量与放置长度共同决定了规格几何结构）、内存切片位置，以及作为 NVIDIA_VISIBLE_DEVICES 注入的 MIG 设备
// UUID。
type migInstance struct {
	GiID          uint32
	CiID          uint32
	ComputeSlices int32
	Placement     migPlacement
	UUID          string
}

// migCardState 是加速器的实时 MIG 状态，用于某个规格进行读取：该规格中合法的空加速器位置，以及加速器上所
// 有活跃的 GPU 实例（涵盖所有规格）。核心逻辑会从“可能位置”中排除已占用的活跃实例，以选择空闲位置，并遍历
// “活跃实例”以寻找可重新使用的未绑定实例。
type migCardState struct {
	Possible []migPlacement
	Live     []migInstance
}

// errInstanceInUse 表示被 NVML_ERROR_IN_USE 拒绝的驱动程序正在被销毁——某个残留进程仍持有该实例。回收循环
// 将其视为有边界的、可重试的部分失败情况（不会清除去抖机制），并在边界处显示给操作员可见的日志。_linux
// seam 会对其进行包装，因此与平台无关的回收核心可以通过 errors.Is 进行测试。
var errInstanceInUse = errors.New("mig instance in use")

// errNoAddressableDevice 表示某个分区没有 MIG 设备句柄可以寻址：要么是没有计算实例的 GPU 实例，要么是驱动
// 程序的 MIG 设备无法访问的容器。以这种方式读取加速器中的每个分区。这是回收循环允许销毁所涉及的那个进程查
// 询失败——那里没有任何可询问的内容，与那些被询问但未得到响应的驱动程序形成对比。_linux 的 seam 会处理这个
// 问题，因此与平台无关的回收核心可以通过 errors.Is 进行测试。
var errNoAddressableDevice = errors.New("no mig device addresses the instance")

// migLiveInstance 是用于回收的全局枚举的实时 GPU 实例：其加速器 UUID 加上该实例。它使得无标记的 GPU 实例
// 能够在任何加速器上被孤儿 GC 发现（包括完全没有标记的加速器），而无需针对每个加速器提供规格说明。
//
// Card 字段名称已冻结：它仅在 mig_driver_linux.go 中作为带键字面量构建，而非 Linux 构建版本无法编译该文件，
// 因此错过重命名操作不会在本地产生问题。
type migLiveInstance struct {
	Card string
	Inst migInstance
}

// migDriver 是 _linux.go/_other.go 构建标签背后的 NVML 执行器接口：真正的实现负责在 Linux 上进行绑定/nvml
// 处理。由于存在 Darwin stub 错误，因此纯化的标记/槽位选择核心通过假驱动进行表测试。每个操作都通过 GPU
// UUID（即 NVIDIA 的运算符加速器 ID）来识别加速器。CardState/CreateInstance 函数会获取规格文件的几何信息，
// 因为仅通过计算切片数量无法确定相同计算环境的 REV 规格文件 ID（1g.5gb 与 1g.10gb）——该接口根据名称与规格
// 文件匹配。
type migDriver interface {
	// CardState 会读取给定规格下加速器的实时 MIG 状态（包括其合法的空加速器配置以及加速器上的所有活跃
	// GPU 实例）。
	CardState(cardUUID, profile string, computeSlices, memorySlices int32) (migCardState, error)
	// CreateInstance 会根据规格在插槽中实例化 GPU 实例及其完整的 GI 计算实例，并返回所创建的分区信息
	// （ID、放置位置、MIG UUID）。
	CreateInstance(cardUUID, profile string, computeSlices, memorySlices int32, slot migPlacement) (migInstance, error)
	// DestroyInstance 会销毁分区的计算实例以及其 GPU 实例。当仍有进程阻塞销毁过程时，它会返回包含
	// errInstanceInUse 的错误信息。
	DestroyInstance(cardUUID string, inst migInstance) error
	// ListInstances 会枚举所有具备 MIG 能力的加速器中的活跃 GPU 实例，每个实例都带有其加速器 UUID，因
	// 此 reclaim 的孤儿 GC 能够在被释放的加速器上找到没有标记的 GI。MIG GI 没有操作器标签，因此这是发
	// 现未被追踪的 GI 的唯一方法。
	ListInstances() ([]migLiveInstance, error)
	// InstanceProcesses 统计了在某一分区上运行的计算进程，它会读取每个 MIG 设备句柄对应的地址——这些句
	// 柄包含该分区的进程列表，同时包含加速器针对整个卡端的查询响应。当无法查询到分区时，它会报错而非
	// 返回零值，调用者绝不能将此情况理解为“没有人使用它”。当没有设备可供查询时，会抛出
	// errNoAddressableDevice 错误。
	InstanceProcesses(cardUUID string, inst migInstance) (int, error)
	// CardInstances 列出了某个加速器的实时 GPU 实例，供那些已知正在选择哪个加速器的人使用。其存在是为
	// 了避免回收循环中的验证重读过程需要遍历节点：该读取操作是在持有加速器的锁的情况下进行的，而全节
	// 点的遍历则涉及数百次 NVML 调用，而每次调用都会同时阻塞加速器的所有分配操作。其错误提示与
	// ListInstances 相同。
	CardInstances(cardUUID string) ([]migInstance, error)
}
