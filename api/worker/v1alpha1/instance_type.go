package v1alpha1

import (
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// InstanceType 是 worker.gpustack.ai 的 schema 定义。
//
// 在底层，一个 InstanceType 对应一个 Kueue ClusterQueue，
// 且 InstanceType 的名称与对应 ClusterQueue 的名称相同。
//
// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:crd-gen:resource:scope="Cluster",categories=["gpustack"],shortName=["instype"],subResources=["status"]
type InstanceType struct {
	meta.TypeMeta   `json:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Spec   InstanceTypeSpec   `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`
	Status InstanceTypeStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

var _ runtime.Object = (*InstanceType)(nil)

// InstanceTypeSpec 定义 InstanceType 的期望规格。其字段顺序与 protobuf 编号
// 将管理员可编辑字段（DisplayName、Description、Inactive）排在前面，随后是
// 不可变的标识和硬件输入字段。
type InstanceTypeSpec struct {
	// DisplayName 是 InstanceType 的可读名称标签。它由管理员编辑，对于派生生成的
	// InstanceType，会在派生时写入。
	//
	// +k8s:validation:maxLength=64
	DisplayName string `json:"displayName,omitempty" protobuf:"bytes,1,opt,name=displayName"`

	// Description 是管理员为 InstanceType 添加的自由格式注释。
	//
	// +k8s:validation:maxLength=1024
	Description string `json:"description,omitempty" protobuf:"bytes,2,opt,name=description"`

	// Inactive 用于将 InstanceType 下线停用。为 true 时，InstanceTypeReconciler
	// 会挂起底层的 ClusterQueue（阻止新的准入，但不驱逐正在运行的 Workload）；
	// 将其清除可重新激活队列。通过任何方式停止的队列都会反向反映为 Inactive=true。
	Inactive bool `json:"inactive,omitempty" protobuf:"varint,3,opt,name=inactive"`

	// AcceleratorGroup 是 InstanceType 的加速器组（可加速节点键），例如
	// "nvidia-a10g"。它用于选择该类型调度到的加速器池；当 Acceleratable 为 true 时，
	// 验证 Webhook 会要求必须填写该字段。
	AcceleratorGroup string `json:"acceleratorGroup,omitempty" protobuf:"bytes,4,opt,name=acceleratorGroup"`

	// GeneralGroup 是 InstanceType 的通用（CPU）组（通用节点键），例如
	// "amd-epyc-7763"，或字面量 "generic" 表示不区分 CPU 厂商的资源池。
	// 变更 Webhook 会将空值默认设为 "generic"；仅当启用
	// instance-type-aware-cpu-manufacturer 时，它才作为调度区分条件参与调度。
	GeneralGroup string `json:"generalGroup,omitempty" protobuf:"bytes,5,opt,name=generalGroup"`

	// Acceleratable 指示该 InstanceType 是否支持加速。
	Acceleratable bool `json:"acceleratable" protobuf:"varint,6,name=acceleratable"`

	// OS 是 InstanceType 的操作系统，例如 "linux"、"windows"。
	//
	// 它是管理员可写的必填输入字段，由验证 Webhook 强制校验。
	OS string `json:"os" protobuf:"bytes,7,opt,name=os"`

	// Arch 是 InstanceType 的架构，例如 "amd64"、"arm64"。
	//
	// 它是管理员可写的必填输入字段，由验证 Webhook 强制校验。
	Arch string `json:"arch" protobuf:"bytes,8,opt,name=arch"`

	// UnitResources 描述 InstanceType 的单位资源规格。
	//
	// 它是管理员可写的必填输入字段，由验证 Webhook 强制校验，且创建后不可变。
	// 派生生成的 InstanceType 会在创建时一次性写入该值：带加速器的类型取自按产品
	// 划分的预设表（表未识别的产品回退为 4 CPU / 16Gi），纯 CPU 类型则设为 1 CPU / 2Gi。
	UnitResources InstanceTypeUnitResources `json:"unitResources" protobuf:"bytes,9,opt,name=unitResources"`

	// LocalStorage 是 InstanceType 的临时本地存储，例如 "100Gi"。
	//
	// 它是管理员可写的必填输入字段，需携带区分大小写的 "Gi" 后缀（由验证 Webhook
	// 强制校验），且创建后不可变；派生生成的 InstanceType 统一固定为 100Gi，各预设
	// 均不改变此值。
	LocalStorage string `json:"localStorage" protobuf:"bytes,10,opt,name=localStorage"`
}

// InstanceTypeCPU 描述 CPU 信息。
type InstanceTypeCPU struct {
	// PhysicalCores 是 CPU 的物理核心数，例如 "4"、"8"。
	PhysicalCores string `json:"physicalCores,omitempty" protobuf:"bytes,1,opt,name=physicalCores"`

	// ThreadsPerPhysicalCore 是 CPU 每个物理核心的线程数，例如 "2"、"4"。
	ThreadsPerPhysicalCore string `json:"threadsPerPhysicalCore,omitempty" protobuf:"bytes,2,opt,name=threadsPerPhysicalCore"`

	// LogicalCores 是 CPU 的逻辑核心数，例如 "8"、"16"。
	LogicalCores string `json:"logicalCores,omitempty" protobuf:"bytes,3,opt,name=logicalCores"`

	// Stepping 是 CPU 的步进版本（stepping），例如 "0"、"1"。
	Stepping string `json:"stepping,omitempty" protobuf:"bytes,4,opt,name=stepping"`

	// ClockSpeed 是 CPU 的时钟频率（单位为 Hz），例如 "2000"
	ClockSpeed string `json:"clockSpeed,omitempty" protobuf:"bytes,5,opt,name=clockSpeed"`

	// MaxClockSpeed 是 CPU 的最大时钟频率（单位为 Hz），例如 "3000"
	MaxClockSpeed string `json:"maxClockSpeed,omitempty" protobuf:"bytes,6,opt,name=maxClockSpeed"`

	// CacheLine 是 CPU 的缓存行大小（单位为字节），例如 "64"、"128"。
	CacheLine string `json:"cacheLine,omitempty" protobuf:"bytes,7,opt,name=cacheLine"`

	// Cache 描述 CPU 的缓存信息。
	Cache InstanceTypeCPUCache `json:"cache,omitempty" protobuf:"bytes,8,opt,name=cache"`
}

// InstanceTypeCPUCache 描述 CPU 的缓存信息。
type InstanceTypeCPUCache struct {
	// L1I 是 CPU 的 L1 指令缓存大小（单位为字节）。
	L1I string `json:"l1i,omitempty" protobuf:"bytes,1,opt,name=l1i"`

	// L1D 是 CPU 的 L1 数据缓存大小（单位为字节）。
	L1D string `json:"l1d,omitempty" protobuf:"bytes,2,opt,name=l1d"`

	// L2 是 CPU 的 L2 缓存大小（单位为字节）。
	L2 string `json:"l2,omitempty" protobuf:"bytes,3,opt,name=l2"`

	// L3 是 CPU 的 L3 缓存大小（单位为字节）。
	L3 string `json:"l3,omitempty" protobuf:"bytes,4,opt,name=l3"`
}

// InstanceTypeAcceleratorCPU 描述加速器的 CPU 信息。
type InstanceTypeAcceleratorCPU struct {
	// Manufacturer 是 CPU 厂商名称，例如 "amd"、"intel"。
	Manufacturer string `json:"manufacturer,omitempty" protobuf:"bytes,1,opt,name=manufacturer"`

	// Product 是 CPU 产品名称。
	Product string `json:"product,omitempty" protobuf:"bytes,2,opt,name=product"`

	// Family 是 CPU 的系列（family）。
	Family string `json:"family,omitempty" protobuf:"bytes,3,opt,name=family"`

	// Detail 内联 CPU 的详细信息。
	InstanceTypeCPU `json:",inline" protobuf:"bytes,4,opt,name=cpu"`
}

// InstanceTypeDetail 是 InstanceType 的观测硬件描述符，由 reconciler 根据匹配的
// ResourceFlavor 注解（notes）和资源池的 Devices 台账计算得出。它位于 status 侧：
// 由于其中嵌入了包含切片字段的 AcceleratorSlicedDetail，该结构体不可比较（not comparable），
// 因此绝不能出现在可比较、可作为 map 键的 InstanceTypeSpec 中。
type InstanceTypeDetail struct {
	// Manufacturer 是 InstanceType 厂商名称。
	Manufacturer string `json:"manufacturer,omitempty" protobuf:"bytes,1,opt,name=manufacturer"`

	// Product 是 InstanceType 产品名称。
	Product string `json:"product,omitempty" protobuf:"bytes,2,opt,name=product"`

	// Family 是 InstanceType 的系列（family）。
	Family string `json:"family,omitempty" protobuf:"bytes,3,opt,name=family"`

	// CPU 描述 InstanceType 的 CPU 信息。
	InstanceTypeCPU `json:",inline" protobuf:"bytes,4,opt,name=cpu"`

	// Accelerator 描述 InstanceType 的加速器信息。
	InstanceTypeAcceleratorDetail `json:",inline" protobuf:"bytes,5,opt,name=accelerator"`
}

// AcceleratorReady 报告带加速器的 InstanceType 是否已计算出观测硬件 Detail。
// reconciler 会从匹配的 ResourceFlavor 注解中填充 Manufacturer，而带加速器的资源池
// flavor 总是带有该注解，因此 Manufacturer 非空即表示 Detail 已填充；空 Detail 表示
// 尚未同步的状态，读取方必须将其视为未就绪。
//
// reconciler 的 computeDetail 会在一次 Devices 读取中同步填充 Manufacturer 和加速器的
// SlicedDetail，因此 Manufacturer 就绪即意味着切分详情同样是最新值：依赖 Status.Detail
// 判断可切分性的读取方可以信任这种原子性，对于可切分型号绝不会出现 Manufacturer 已填充
// 而 SlicedDetail 仍为旧值或空值的情况。
func (in InstanceTypeDetail) AcceleratorReady() bool {
	return in.Manufacturer != ""
}

// InstanceTypeAcceleratorDetail 描述 InstanceType 的观测加速器信息。
// 它携带在资源池维度聚合的 SlicedDetail（即观测到的切分能力），从而让 status 侧的 detail
// 能够容纳包含切片字段、不可置于可比较 Spec 中的 AcceleratorSlicedDetail。
type InstanceTypeAcceleratorDetail struct {
	// Memory 是加速器的显存大小，例如 "65535Mi"。
	Memory string `json:"memory,omitempty" protobuf:"bytes,1,opt,name=memory"`

	// Cores 是加速器的核心数，例如 "128"、"256"。
	Cores string `json:"cores,omitempty" protobuf:"bytes,2,opt,name=cores"`

	// ComputeCapability 是加速器的计算能力（compute capability），例如 "8.0"、"7.0"。
	ComputeCapability string `json:"computeCapability,omitempty" protobuf:"bytes,3,opt,name=computeCapability"`

	// RuntimeVersion 是整个资源池中观测到的加速器运行时版本，由探测器规范化为
	// "major.minor" 格式，例如 CUDA 为 "12.9"，CANN 为 "8.2"。
	//
	// 它是支撑该加速器组的所有节点上的最小值，而非任意代表值。一个 InstanceType 覆盖所有此类节点，
	// 驱动滚动升级期间各节点的版本可能会不一致；基于旧运行时构建的容器可以在新驱动上运行，反之则不行，
	// 因此现有最低版本才是所有节点都能运行其镜像的版本。这一点至关重要，因为 Workload 的镜像在
	// 准入阶段选定目标节点之前就已经固定。
	//
	// 空值表示未观测到任何版本（例如没有已同步的 flavor，或资源池本身不含加速器组），
	// 它与所有节点上报同一版本的情况有本质区别，消费方绝不能将其当作默认值。
	//
	// 该字段始终等于 RuntimeVersions 的第一个元素，两者由同一个已排序列表赋值，
	// 因此在构造上保证不会出现不一致。
	RuntimeVersion string `json:"runtimeVersion,omitempty" protobuf:"bytes,6,opt,name=runtimeVersion"`

	// RuntimeVersions 是资源池各节点上报的所有不重复运行时版本列表，按升序排列。
	//
	// 该字段用于让消费方区分资源池内版本是否一致，这是单个值无法表达的信息。驱动滚动升级期间
	// 节点版本会暂时不一致，基于最低版本构建的 Workload 需要能够说明其跳过了哪些版本。
	//
	// 消费方通过 len() > 1 判断是否存在版本不一致，并将尾部元素视为被跳过的版本。这只需要一次
	// 长度比较，而无需对聚合结果再做一次拷贝，也正是发布该字段而非在下游从 Devices 台账重新计算的原因。
	//
	// +listType=atomic
	RuntimeVersions []string `json:"runtimeVersions,omitempty" protobuf:"bytes,7,rep,name=runtimeVersions"`

	// SlicedDetail 是该加速器组在资源池维度的聚合切分能力。
	SlicedDetail AcceleratorSlicedDetail `json:"slicedDetail,omitempty" protobuf:"bytes,4,opt,name=slicedDetail"`

	// CPU 描述加速器的 CPU 信息。
	CPU InstanceTypeAcceleratorCPU `json:"cpu,omitempty" protobuf:"bytes,5,opt,name=cpu"`
}

// IsLogicallySliceable 报告资源池是否能够提供逻辑（软件）切分，即其聚合切分详情中的
// 逻辑切分数量大于零。逻辑切分通过 InstanceResources.AcceleratorSlicedMemoryPercentage /
// AcceleratorSlicedCoresPercentage 字段请求。
//
// 该方法刻意不排除支持物理分区的资源池，IsPhysicallySliceable 也不排除支持逻辑切分的资源池。
// 这两种能力在单个加速器维度上是互斥的（因此单卡维度的 device.IsLogicallySliceable 会包含
// !IsPartitioned 判断），但一个资源池会聚合两种类型的加速器，混合节点也会同时宣告这两种能力。
// 如果在此处将其中一个判断条件并入另一个，会导致混合资源池无法提供逻辑切分。
func (in InstanceTypeAcceleratorDetail) IsLogicallySliceable() bool {
	return in.SlicedDetail.Logical.Count > 0
}

// IsPhysicallySliceable 报告资源池是否能够提供硬件分区，即其聚合切分详情中的物理切分数量
// 大于零。分区通过名称在 InstanceResources.AcceleratorPartitionedProfile 中请求，并根据
// Physical 规格（profile）清单进行校验；本谓词用于回答前置问题：资源池是否根本具备该能力。
// 关于为何两者在资源池级别相互独立，参见 IsLogicallySliceable 的说明。
func (in InstanceTypeAcceleratorDetail) IsPhysicallySliceable() bool {
	return in.SlicedDetail.Physical.Count > 0
}

// InstanceTypeStatus 描述 InstanceType 的观测状态。
type InstanceTypeStatus struct {
	// Detail 是 InstanceType 的观测硬件描述符，由 reconciler 根据匹配的
	// ResourceFlavor 注解和资源池的 Devices 台账计算得出。
	Detail InstanceTypeDetail `json:"detail,omitempty" protobuf:"bytes,1,opt,name=detail"`

	// Entrance 是位于命名空间内、作为此 InstanceType 底层 ClusterQueue 入口的 LocalQueue 名称，
	// 即 Workload 为获得准入而设置的 "kueue.x-k8s.io/queue-name" 标签值。该名称由
	// InstanceType 名称派生而来（参见 nodefeature.FormatLocalQueueName）。
	Entrance string `json:"entrance,omitempty" protobuf:"bytes,2,opt,name=entrance"`

	// Phase 是各 condition 状态的汇总。
	Phase string `json:"phase" protobuf:"bytes,3,name=phase"`

	// PhaseMessage 是当前阶段的说明信息。
	PhaseMessage string `json:"phaseMessage,omitempty" protobuf:"bytes,4,opt,name=phaseMessage"`

	// 下方的资源视图由 worker gateway 的 pkg/workergateway/service 中的 AggregatedInstanceType
	// 逐字段镜像（而非嵌入），且没有代码生成器维护该镜像关系。在此处新增视图时代码仍能编译通过，
	// 但 gateway 会在所有集群范围聚合中丢弃该视图，导致整个集群在该新维度上显示为无容量。
	// 请在同一次变更中将其接入该处；当两者不一致时，该包中的反射测试会失败。

	// Accelerator 是可独占分配视图：完全空闲的整卡加速器数量，例如 "1"、"4"。
	Accelerator InstanceTypeResource `json:"accelerator" protobuf:"bytes,5,name=accelerator"`

	// AcceleratorShared 是可共享视图：跨空闲卡和已共享卡累加的单卡归属份额
	// （每张加速器最多允许 SharedResourceMaxSize 个持有者）。
	AcceleratorShared InstanceTypeResource `json:"acceleratorShared" protobuf:"bytes,6,name=acceleratorShared"`

	// AcceleratorSliced 是可切分视图：跨空闲卡和已切分卡累加的单卡显存百分比单位
	// （每张加速器共 100 个单位）。
	AcceleratorSliced InstanceTypeResource `json:"acceleratorSliced" protobuf:"bytes,7,name=acceleratorSliced"`

	// AcceleratorPartitioned 是可硬件分区视图：资源池中处于分区模式的加速器仍可承载的
	// 分区实例数，在这些加速器上累加。它与上述三个视图互不相交——处于分区模式的加速器
	// 无法响应其他类型的资源请求——因此不含分区加速器的资源池此处报告为零。
	AcceleratorPartitioned InstanceTypePartitionedResource `json:"acceleratorPartitioned" protobuf:"bytes,9,name=acceleratorPartitioned"`

	// CPU 是 InstanceType 的 CPU 资源，例如 "4"、"8"。
	CPU InstanceTypeResource `json:"cpu" protobuf:"bytes,8,name=cpu"`
}

// InstanceTypeResource 描述 InstanceType 的资源情况。
type InstanceTypeResource struct {
	// OnceMaxRequest 是单次可请求该资源的最大值。
	//
	// 这是一个软限制，请求该值仍可能导致调度失败。
	OnceMaxRequest resource.Quantity `json:"onceMaxRequest,omitempty" protobuf:"bytes,1,opt,name=onceMaxRequest"`

	// Remaining 是该资源当前剩余的可请求量。
	Remaining resource.Quantity `json:"remaining,omitempty" protobuf:"bytes,2,opt,name=remaining"`

	// Capacity 是该资源的总容量。
	Capacity resource.Quantity `json:"capacity,omitempty" protobuf:"bytes,3,opt,name=capacity"`
}

// InstanceTypePartitionedResource 描述 InstanceType 的可硬件分区资源：包含所有资源通用的
// 标量视图，以及资源池按规格（profile）统计的台账。
//
// 按规格划分的列表用于回答"当前还能获取哪些分区规格"。其他两种替代字段都无法回答该问题：
// 标量 Remaining 表示单张加速器上各规格中的最优值而非总量（同一张加速器上的不同规格会竞争
// 相同的物理切分，直接相加会重复计算同一硬件）；而 Detail.SlicedDetail 是静态能力目录，
// 按设计不会随着实例的切分和释放而动态变化。
type InstanceTypePartitionedResource struct {
	InstanceTypeResource `json:",inline" protobuf:"bytes,1,opt,name=resource"`

	// AllocatedProfiles 按规格名称列出资源池中各分区加速器当前已占用的每种规格实例数
	// （在这些加速器上累加）。未持有任何实例的规格不会出现在列表中，而非记为零——
	// 这与 RemainingProfiles 不同，在后者中零值具有明确语义。
	//
	// worker gateway 会从每个候选资源池读取该字段并按规格名称累加到对外提供的集群级聚合结果中，
	// 因此更改其零值处理或出现规则也会影响该聚合结果。其读取方是本状态的消费方（UI 与 GPUStack 应用），
	// 用于同时展示资源池正在运行的实例以及还能容纳的容量，而它们无法自行推导出该数据：
	// 仅凭 RemainingProfiles 无法区分某个零值究竟是"已被占用"还是"被同级规格挤占"。
	//
	// +listType=map
	// +listMapKey=name
	AllocatedProfiles []AcceleratorProfileCount `json:"allocatedProfiles,omitempty" protobuf:"bytes,2,rep,name=allocatedProfiles"` // nolint: lll

	// RemainingProfiles 按规格名称列出资源池中各分区加速器还能继续承载的每种规格实例数
	// （在这些加速器上累加）。
	//
	// 资源池提供的每一种规格都会保留一条记录，即使数量为零也是如此，这样当某规格的空间被同级规格
	// 实例占满时，其读数为 0 而非从列表中消失——读取方借此可区分"支持该规格但已满"与"根本不支持"，
	// 同时整块写入状态时键集合也不会在每次切分和释放时频繁变动。
	//
	// +listType=map
	// +listMapKey=name
	RemainingProfiles []AcceleratorProfileCount `json:"remainingProfiles,omitempty" protobuf:"bytes,3,rep,name=remainingProfiles"` // nolint: lll
}

// InstanceTypeUnitResources 描述 InstanceType 的单位资源。
type InstanceTypeUnitResources struct {
	// CPU 是 InstanceType 的单位 CPU 资源（核数）。
	CPU string `json:"cpu,omitempty" protobuf:"bytes,1,opt,name=cpu"`

	// RAM 是 InstanceType 的单位内存资源（Mi）。
	RAM string `json:"ram,omitempty" protobuf:"bytes,3,opt,name=ram"`
}

// InstanceTypeList 保存 InstanceType 列表。
//
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type InstanceTypeList struct {
	meta.TypeMeta `json:",inline"`
	meta.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Items []InstanceType `json:"items" protobuf:"bytes,2,rep,name=items"`
}

var _ runtime.Object = (*InstanceTypeList)(nil)
