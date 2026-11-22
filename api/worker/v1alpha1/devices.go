package v1alpha1

import (
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// Devices 是 worker.gpustack.ai 的 schema。
//
// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:crd-gen:resource:scope="Cluster",subResources=["status"]
type Devices struct {
	meta.TypeMeta   `json:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Spec   DevicesSpec   `json:"spec" protobuf:"bytes,2,name=spec"`
	Status DevicesStatus `json:"status,omitempty" protobuf:"bytes,3,name=status"`
}

var _ runtime.Object = (*Devices)(nil)

// DevicesSpec 定义 Devices 的期望状态。
type DevicesSpec struct {
	// Groups 是 worker 上的设备组列表。
	//
	// +listType=map
	// +listMapKey=id
	// +listMapKey=manufacturer
	Groups []DevicesGroup `json:"groups" yaml:"groups" protobuf:"bytes,1,name=groups"`

	// Interfaces 是 worker 上的网络接口列表。
	//
	// 它挂在 worker 上而不是设备组上，因为网络接口属于整台机器，而非某个厂商的加速器：
	// 将两者关联是读取方的职责，应通过比对双方携带的总线坐标来完成，绝不能在此存储
	// 交叉引用。
	//
	// 该字段缺失涵盖两种情况且不加区分：一种是 worker 没有任何网络接口（真实硬件不会处于
	// 此状态），另一种是枚举从未成功过。单次失败会保留之前记录的数据，而非将其替换为空列表，
	// 因此只要 worker 至少成功画像过一次，这里保留的就是上一次的清单。只有首次枚举就失败
	// 才会使该字段缺失，且该失败会以 Error 级别记录在 device manager 日志中，而不在 API 中建模——
	// 能在下次扫描自行恢复的状态不值得暴露到 API 层。读取方不得将缺失视为“此 worker 没有接口”。
	//
	// 所有内核网络接口都会被记录，包括临时虚拟接口——并且对于这些接口，列表不保证实时最新。
	// 仅限于不携带 RDMA 设备且无链路判定结果的虚拟接口的变更，本身不会触发重新读取：
	// 每次 Pod 启停都会增删一个 veth，若将其视为硬件变更，会导致每个 Pod 事件都按厂商数量
	// 重写一次该集群作用域对象。因此这类接口的加入或离开只会在其他变更上报时顺带发布，
	// 而非在发生时立即更新。任何带有 RDMA 记录的接口均不受此限制，始终保持最新。
	// 需要实时最新 veth 列表的消费方必须读取节点本身，而不是读取此字段。
	//
	// +listType=map
	// +listMapKey=name
	Interfaces []DeviceInterface `json:"interfaces,omitempty" yaml:"interfaces,omitempty" protobuf:"bytes,2,opt,name=interfaces"`
}

// DevicesStatus 定义 Devices 的观测状态。
type DevicesStatus struct {
	// Groups 是 worker 上的设备组列表。
	//
	// +listType=map
	// +listMapKey=id
	// +listMapKey=manufacturer
	Groups []DevicesAllocationGroup `json:"groups,omitempty" yaml:"groups,omitempty" protobuf:"bytes,1,opt,name=groups"`
}

type (
	// DevicesGroup 是具有相同元数据的一组设备。
	DevicesGroup struct {
		// ID 是此设备组的全局唯一标识符。
		ID string `json:"id" yaml:"id" protobuf:"bytes,1,opt,name=id"`

		// Manufacturer 是设备制造商的名称。
		Manufacturer string `json:"manufacturer" yaml:"manufacturer" protobuf:"bytes,2,name=manufacturer"`

		// Name 是设备产品的名称。
		Name string `json:"name" yaml:"name" protobuf:"bytes,3,name=name"`

		// Memory 是设备的显存大小（单位为 MiB）。
		Memory uint64 `json:"memory" yaml:"memory" protobuf:"bytes,4,name=memory"`

		// Cores 是设备的核心数量。
		Cores uint32 `json:"cores,omitempty" yaml:"cores,omitempty" protobuf:"bytes,5,opt,name=cores"`

		// DriverVersion 是设备所使用的驱动版本。
		DriverVersion string `json:"driverVersion,omitempty" yaml:"driverVersion,omitempty" protobuf:"bytes,6,opt,name=driverVersion"`

		// RuntimeVersion 是设备所使用的运行时版本。
		RuntimeVersion string `json:"runtimeVersion,omitempty" yaml:"runtimeVersion,omitempty" protobuf:"bytes,7,opt,name=runtimeVersion"`

		// ComputeCapability 是设备的计算能力。
		ComputeCapability string `json:"computeCapability,omitempty" yaml:"computeCapability,omitempty" protobuf:"bytes,8,opt,name=computeCapability"`

		// Family 是设备的所属系列。
		Family string `json:"family,omitempty" yaml:"family,omitempty" protobuf:"bytes,9,opt,name=family"`

		// Accelerators 是此组中的加速器设备列表。
		//
		// +listType=map
		// +listMapKey=id
		Accelerators []Accelerator `json:"accelerators,omitempty" yaml:"accelerators,omitempty" protobuf:"bytes,10,opt,name=accelerators"`

		// AcceleratorSlicedDetail 是该设备组的切分能力，由组内各加速器的切分状态汇总得到。
		AcceleratorSlicedDetail AcceleratorSlicedDetail `json:"acceleratorSlicedDetail,omitempty" yaml:"acceleratorSlicedDetail,omitempty" protobuf:"bytes,11,opt,name=acceleratorSlicedDetail"` // nolint: lll
	}

	// DeviceTopology 描述设备的拓扑信息。
	DeviceTopology struct {
		// PciBusID 是设备的 PCI 总线 ID。
		PciBusID string `json:"pciBusId" yaml:"pciBusId" protobuf:"bytes,1,name=pciBusId"`

		// PciRootID 是位于设备上方最外层 PCI 桥的地址；如果上方没有桥，则为设备自身的地址。
		//
		// 尽管字段名称如此，但它并非根复合体（root complex）的标识符。两个设备共享该值仅表明它们
		// 通过同一个桥子树到达上游，并不意味着它们位于同一个交换机之后（这一更严格的亲和关系由
		// 下方的 PciSwitches 表示）。对于直接连接到根复合体的设备，该值为设备自身地址，此时
		// 值相等仅相当于身份相同校验，而不表示位于同一根复合体下。
		PciRootID string `json:"pciRootId" yaml:"pciRootId" protobuf:"bytes,2,name=pciRootId"`

		// PciClass 是设备的 PCI 类别。
		PciClass string `json:"pciClass" yaml:"pciClass" protobuf:"bytes,3,name=pciClass"`

		// NumaAffinity 是设备所属的 NUMA 节点。为空表示未知（UNKNOWN），绝不会规范化为
		// 节点 0，否则会报告实际未读取到的亲和性。内核中的 -1（表示无亲和性）哨兵值以及
		// 无法解析的读数在此均表现为空值，因此本字段与下方的网卡字段遵循相同的约定。
		NumaAffinity string `json:"numaAffinity" yaml:"numaAffinity" protobuf:"bytes,4,name=numaAffinity"`

		// CpuAffinity 是与设备亲近的 CPU 核心。
		CpuAffinity string `json:"cpuAffinity" yaml:"cpuAffinity" protobuf:"bytes,5,name=cpuAffinity"`

		// RoCE 是设备的 RoCE（基于融合以太网的 RDMA）网络信息。
		RoCE *DeviceEthernet `json:"roce,omitempty" yaml:"roce,omitempty" protobuf:"bytes,6,opt,name=roce"`

		// PciSwitches 是设备上游的 PCI 桥/交换机路径，按由内到外的顺序排列。
		//
		// 两个共享完整路径的设备位于同一交换机之后，这种亲和关系比仅共享最外层桥更为严格。
		// 上方的 PciRootID 表示的是最外层桥而非交换机：将该字段相等当作交换机级别的亲和性
		// 会虚报未实际测得的亲近关系，因此这两个字段绝不能视为相同的断言。
		//
		// 对于完全没有 PCI 路径的设备，该字段缺省（nil），绝不保留空切片作为占位标记。构造时
		// 即已排好序，因此硬件未变更时连续两次读取的结果在字节上完全一致。
		//
		// +listType=atomic
		PciSwitches []string `json:"pciSwitches,omitempty" yaml:"pciSwitches,omitempty" protobuf:"bytes,7,rep,name=pciSwitches"`

		// Fabric 是该设备所属的纵向扩展（scale-up）互联域。若设备代际本身不具备此类互联
		// 结构，或驱动从未对这些读取操作返回过结果，则该字段缺省。
		//
		// 该字段存在并不代表写入此对象时所有子字段均在当次扫描中被成功读取。当驱动暂时拒绝
		// 读取时，先前已读到的域信息会继续保留，否则就会错误撤回节点实际上并未离开的域——
		// 因此这里的记录保存了各字段最后一次获得的响应值，仅当驱动返回新结果时才会替换。
		Fabric *DeviceFabric `json:"fabric,omitempty" yaml:"fabric,omitempty" protobuf:"bytes,8,opt,name=fabric"`
	}

	// DeviceFabric 描述设备所属的纵向扩展（scale-up）互联域，即通过厂商自有互联直接互相
	// 寻址的加速器集合；在部分硬件代际上该域可跨越多台机器。
	//
	// 该信息记录在每个设备上，而从不作为设备组或 worker 级别的字段，因为互联域会横跨这两者：
	// NVIDIA 按 GPU 上报 clique，单台机器可能包含多个；AMD 的 hive id 同样按 GPU 读取。
	// 因此“同型号、同机器、不同域”是可能出现的状态，使用更粗粒度的字段会抹平这一差异。
	// 互联域本身通过共享相同 Kind 和 ID 的设备推导得出，这与 DevicesSpec.Interfaces 将网卡
	// 关联到加速器的规则一致：仅发布可比较的坐标，绝不存储交叉引用。在每个设备上记录更高
	// 层级的事实也与上方的 RoCE 一致（RoCE 属于板卡而非单个加速器）。
	//
	// 目前由 Ascend 和 NVIDIA 检测器填充此结构。下方各字段注释均注明了由哪家厂商发布，以便
	// 将空值理解为“该代际无此属性”而非字段缺失：厂商驱动不上报的信息，检测器无法记录。
	//
	// 并非所有互联都适用此结构。Cambricon 的 MLU-Link 完全不发布域标识，仅提供每条链路的
	// 对端信息，它描述的是边列表，因此不在此处记录。
	DeviceFabric struct {
		// Kind 标识互联类型，也是使 ID 具有可解释性的前提：Ascend UB 超节点 ID、NVIDIA NVLink
		// 集群 UUID 与 AMD XGMI hive ID 之间不存在共享命名空间，因此仅当 Kind 和 ID 同时匹配时，
		// 两个设备才处于同一个互联域中。
		//
		// 取值为 `ub`、`nvlink` 或 `xgmi` 之一。
		Kind string `json:"kind" yaml:"kind" protobuf:"bytes,1,name=kind"`

		// ID 标识互联域，并且可在不同 worker 之间进行比较——这正是发布该字段的意义所在，
		// 因为跨机器的互联域无法仅凭单台机器的记录识别出来。
		//
		// Ascend 上报超节点 ID，NVIDIA 上报 fabric cluster UUID，AMD 上报 XGMI hive ID。
		//
		// 未识别到互联域时该字段为空——在 Ascend 上，这也包括驱动已响应超节点查询的设备。
		// 驱动会对所有形态返回响应（因为形态正是通过该响应确定的），并在不属于任何超节点的机器上
		// 将坐标标记为无效。因此，针对 `pod-1d` 和 `pod-2d` 会发布该 ID；对于上报的 ID 和规模
		// 非无效标记的 `server-8p`、`card-1p` 或 `card-4p`（即厂商自身 rank table 跨机使用的
		// “超服务器”形态）也会发布；而对 `server-16p` 或 `server-32p` 则绝不发布。发布无效占位
		// 标记会导致两台无关机器被归入同一个域，而该字段会在跨 worker 间进行比较。
		//
		// NVIDIA 则是在未就绪时不提供整条 Fabric 记录而非仅留空此字段：在加速器完成向
		// fabric manager 的注册之前，cluster UUID 不具备标识意义，且该厂商也没有其他可附带
		// 发布的字段。
		ID string `json:"id" yaml:"id" protobuf:"bytes,2,name=id"`

		// Type 是互联域的形态，以可读名称而非厂商编号表示——包括 `pod-1d`、`pod-2d`、
		// `server-8p`、`server-16p`、`server-32p`、`card-1p`、`card-4p`。
		//
		// 仅适用于 Ascend。组装通信计划的消费方通过读取该字段区分超节点、普通服务器与独立板卡，
		// 进而决定通信计划自身的结构形态。
		Type string `json:"type,omitempty" yaml:"type,omitempty" protobuf:"bytes,3,opt,name=type"`

		// CliqueID 表示当互联域被划分为多个子集时，内部能够真正互相寻址的设备子集。
		//
		// 仅适用于 NVIDIA，且在该代际上是关键约束而非装饰字段：两个共享 ID 但 CliqueID
		// 不同的 GPU 虽然处于同一个 fabric 中，彼此仍无法通信，因此仅比较 ID 的消费方
		// 会把任务共置到无法互相通信的设备上。
		CliqueID string `json:"cliqueId,omitempty" yaml:"cliqueId,omitempty" protobuf:"bytes,4,opt,name=cliqueId"`

		// MemberCount 是互联域上报的成员总数。为 0 表示厂商未上报该值，绝不表示域内没有成员。
		//
		// 仅适用于 Ascend，取自超节点自身的规模字段。它无需枚举全部成员即可说明域的规模，
		// 这正是调度器在尚未观测到域内所有 worker 时所需的信息。发布规则与 ID 相同，此外
		// 若驱动将规模本身标记为无效则不予发布：域可能已被识别，但其规模尚未确定。
		//
		// 字段未命名为 Size：每个 protobuf 消息都会生成 Size() 方法，同名字段会与之冲突。
		MemberCount uint32 `json:"memberCount,omitempty" yaml:"memberCount,omitempty" protobuf:"bytes,5,opt,name=memberCount"` // nolint: lll

		// NodeIndex 是当前 worker 在互联域内的编号（按域对机器的编号方式），并非 Kubernetes Node
		// 名称，也不可与之比较。
		//
		// 仅适用于 Ascend，取自超节点的 server id。发布规则与 ID 相同，此外若驱动将该坐标
		// 标记为无效则不予发布：机器可能已处于某个域中，但尚未获知自身所在位置。
		NodeIndex string `json:"nodeIndex,omitempty" yaml:"nodeIndex,omitempty" protobuf:"bytes,6,opt,name=nodeIndex"`

		// RackID 是当前 worker 所在的机架编号（按域对机架的编号方式）。
		//
		// 仅适用于 Ascend，取自超节点的 chassis id。发布与省略规则与 NodeIndex 相同。
		RackID string `json:"rackId,omitempty" yaml:"rackId,omitempty" protobuf:"bytes,7,opt,name=rackId"`

		// Endpoints 是该设备在 fabric 上的自身地址列表，采用厂商自有编码。
		//
		// 仅适用于 Ascend，其中每一项均为 32 位小写十六进制字符表示的 UB 端点标识符。
		// 这些字节故意以未解析的原始形式发布：功能实体（function entity）、die、端口以及端点是否
		// 承载设备间流量均作为位字段编码在其中，由消费方按需自行解析，避免本 API 绑定可能仅由
		// 厂商变更的位布局。
		//
		// 构造时即已排好序，因此硬件未变更时连续两次读取的结果在字节上完全一致。
		//
		// +listType=atomic
		Endpoints []string `json:"endpoints,omitempty" yaml:"endpoints,omitempty" protobuf:"bytes,8,rep,name=endpoints"`
	}

	// DeviceEthernet 描述设备的以太网信息。
	DeviceEthernet struct {
		// IP 是设备以太网接口的 IP 地址。
		IP string `json:"ip" yaml:"ip" protobuf:"bytes,1,name=ip"`

		// SubnetMask 是设备以太网接口的子网掩码。
		SubnetMask string `json:"subnetMask" yaml:"subnetMask" protobuf:"bytes,2,name=subnetMask"`

		// Gateway 是设备以太网接口的网关地址。
		Gateway string `json:"gateway" yaml:"gateway" protobuf:"bytes,3,name=gateway"`
	}

	// DeviceInterface 描述 worker 上的一个网络接口。
	//
	// 枚举采用接口优先（interface-first）策略：以网络接口作为主标识，将其 PCI 设备解析为
	// 附属属性，这与先遍历 PCI 总线再反向关联的做法相反。通过非 PCI 互联接入的接口在以 PCI
	// 为根的遍历中是不可见的，而这正是本记录不能遗漏的场景——因此此处所有 PCI 字段均为可选，
	// 它们同时缺省代表一种特定类型的接口，而非记录缺失。
	DeviceInterface struct {
		// Name 是内核网络接口名称，同时作为该接口的唯一标识。
		Name string `json:"name" yaml:"name" protobuf:"bytes,1,name=name"`

		// Bus 指明发现该接口的总线/互联类型，从而使没有 PCI 坐标的接口能够明确上报自身类型，
		// 而不会被误判为 PCI 查找失败。
		Bus string `json:"bus,omitempty" yaml:"bus,omitempty" protobuf:"bytes,2,opt,name=bus"`

		// PciBusID 是接口的 PCI 总线 ID。对于非 PCI 接口该字段缺省。
		PciBusID string `json:"pciBusId,omitempty" yaml:"pciBusId,omitempty" protobuf:"bytes,3,opt,name=pciBusId"`

		// PciRootID 是位于接口上方最外层 PCI 桥的地址；如果上方没有桥，则为接口自身的地址。
		// 尽管字段名称如此，但它并非根复合体（root complex），将其当作根复合体会虚报未实际测得的
		// 亲近关系。对于非 PCI 接口该字段缺省。该值与加速器侧使用同一套遍历方式获得，因此两者可直接比较。
		PciRootID string `json:"pciRootId,omitempty" yaml:"pciRootId,omitempty" protobuf:"bytes,4,opt,name=pciRootId"`

		// PciSwitches 是上游的 PCI 桥/交换机路径，按由内到外的顺序排列——与 DeviceTopology 中的
		// 坐标一致，因此无需转换层即可将加速器与网络接口直接对比。对于非 PCI 接口该字段缺省。
		//
		// +listType=atomic
		PciSwitches []string `json:"pciSwitches,omitempty" yaml:"pciSwitches,omitempty" protobuf:"bytes,5,rep,name=pciSwitches"`

		// PciVendor 和 PciDevice 为原始十六进制 PCI ID，有意不解析为型号名称：解析名称需要
		// 读取精简镜像中可能不存在的宿主机数据文件，而在一个 worker 上能解析、在另一个上无法解析的
		// 名称反而不如始终可用的十六进制 ID 可靠。
		PciVendor string `json:"pciVendor,omitempty" yaml:"pciVendor,omitempty" protobuf:"bytes,6,opt,name=pciVendor"`

		// PciDevice 是原始十六进制 PCI 设备 ID。参见 PciVendor。
		PciDevice string `json:"pciDevice,omitempty" yaml:"pciDevice,omitempty" protobuf:"bytes,7,opt,name=pciDevice"`

		// NumaAffinity 是接口所属的 NUMA 节点。为空表示未知（UNKNOWN），绝不会规范化为节点 0，
		// 否则会报告实际未读取到的亲和性。
		NumaAffinity string `json:"numaAffinity,omitempty" yaml:"numaAffinity,omitempty" protobuf:"bytes,8,opt,name=numaAffinity"`

		// CpuAffinity 是与接口亲近的 CPU 核心。为空表示未知。
		CpuAffinity string `json:"cpuAffinity,omitempty" yaml:"cpuAffinity,omitempty" protobuf:"bytes,9,opt,name=cpuAffinity"`

		// MTU 是链路 MTU。为 0 表示未能读取到该值，而非 MTU 等于 0——正常运行的接口绝不会
		// 上报 0，因此用 0 表示缺省不存在歧义。
		MTU int32 `json:"mtu,omitempty" yaml:"mtu,omitempty" protobuf:"varint,10,opt,name=mtu"`

		// Up 报告接口的运行状态（是否处于启用状态）。
		Up bool `json:"up,omitempty" yaml:"up,omitempty" protobuf:"varint,11,opt,name=up"`

		// Virtual 标记背后没有物理设备的虚拟接口（如 loopback、bridge、veth）。此类接口会被
		// 记录并加以标记，而绝不会丢弃：如果一个 worker 上只有网桥接口，必须呈现为“存在一个
		// 虚拟接口”，而不是“没有接口”。
		Virtual bool `json:"virtual,omitempty" yaml:"virtual,omitempty" protobuf:"varint,12,opt,name=virtual"`

		// RDMA 报告该接口是否绑定了 RDMA 设备。它不反映链路是否真正可用——链路可用性由下方的
		// Link 表示，在真实硬件上这两者可能并不一致。
		RDMA bool `json:"rdma,omitempty" yaml:"rdma,omitempty" protobuf:"varint,13,opt,name=rdma"`

		// RDMADevice 是所绑定的 RDMA 设备名称。当 RDMA 为 false 时为空。
		RDMADevice string `json:"rdmaDevice,omitempty" yaml:"rdmaDevice,omitempty" protobuf:"bytes,14,opt,name=rdmaDevice"`

		// SRIOV 报告该接口是否为 SR-IOV 物理功能（PF）。这是一个独立于 VirtualFunctions 长度
		// 的事实：“配置了 0 个 VF 的 PF”与“根本不是 PF”是两种不同的状态，若仅通过空 VF 列表
		// 来推断后者会将这两种状态混为一谈。
		SRIOV bool `json:"sriov,omitempty" yaml:"sriov,omitempty" protobuf:"varint,15,opt,name=sriov"`

		// VirtualFunctions 是该物理功能（PF）下的虚拟功能（VF）列表，采用嵌套形式存储而非
		// 作为平级条目列出：拥有 8 个 VF 的 PF 表现为 1 个顶层条目及其嵌套的 8 个子条目，
		// 绝不是 9 个顶层条目。按总线 ID 排序，因此连续两次读取的结果在字节上完全一致。
		//
		// +listType=atomic
		VirtualFunctions []DeviceInterfaceVirtualFunction `json:"virtualFunctions,omitempty" yaml:"virtualFunctions,omitempty" protobuf:"bytes,16,rep,name=virtualFunctions"` // nolint: lll

		// Link 是验证该接口 RDMA 链路是否可用的检查结果。如果没有可供验证的 RDMA 链路，则为 nil。
		Link *DeviceInterfaceLink `json:"link,omitempty" yaml:"link,omitempty" protobuf:"bytes,17,opt,name=link"`
	}

	// DeviceInterfaceVirtualFunction 描述物理功能（PF）下的一个 SR-IOV 虚拟功能（VF）。
	//
	// 它被定义为独立类型而非嵌套的 DeviceInterface，因为 SR-IOV 严格只有一层嵌套——VF
	// 本身不能再被划分为 VF——若使用自引用结构就会多出一个永远无法填充的层级。
	//
	// VF 与父级共享的是上游桥路径——即 `pciRootId`（最外层桥，而非根复合体）和
	// `pciSwitches`——因此这些字段直接从父级读取而不再重复存储。VF 是具有独立地址的
	// PCI 功能，上述共享仅说明两者之上的路径相同，别无他意。每个 VF 可能不同的属性则在此
	// 单独记录：各自的地址、各自的 NUMA 节点与 CPU 列表，以及各自的 RDMA 设备和链路检查结果。
	DeviceInterfaceVirtualFunction struct {
		// Name 是虚拟功能的内核接口名称。
		Name string `json:"name" yaml:"name" protobuf:"bytes,1,name=name"`

		// PciBusID 是虚拟功能的 PCI 总线 ID，用于将其与同级设备及父设备区分开来。
		PciBusID string `json:"pciBusId,omitempty" yaml:"pciBusId,omitempty" protobuf:"bytes,2,opt,name=pciBusId"`

		// NumaAffinity 是虚拟功能所连接的 NUMA 节点。为空表示未知。
		NumaAffinity string `json:"numaAffinity,omitempty" yaml:"numaAffinity,omitempty" protobuf:"bytes,3,opt,name=numaAffinity"`

		// CpuAffinity 是靠近该虚拟功能的 CPU 核心。为空表示未知。
		CpuAffinity string `json:"cpuAffinity,omitempty" yaml:"cpuAffinity,omitempty" protobuf:"bytes,4,opt,name=cpuAffinity"`

		// MTU 是链路 MTU。为 0 表示未读取。参见 DeviceInterface.MTU。
		MTU int32 `json:"mtu,omitempty" yaml:"mtu,omitempty" protobuf:"varint,5,opt,name=mtu"`

		// Up 报告虚拟功能的运行状态。
		Up bool `json:"up,omitempty" yaml:"up,omitempty" protobuf:"varint,6,opt,name=up"`

		// RDMA 报告是否有 RDMA 设备绑定到此虚拟功能。
		RDMA bool `json:"rdma,omitempty" yaml:"rdma,omitempty" protobuf:"varint,7,opt,name=rdma"`

		// RDMADevice 是绑定的 RDMA 设备名称。当 RDMA 为 false 时为空。
		RDMADevice string `json:"rdmaDevice,omitempty" yaml:"rdmaDevice,omitempty" protobuf:"bytes,8,opt,name=rdmaDevice"`

		// Link 是验证该虚拟功能 RDMA 链路的结果。当没有需要验证的 RDMA 链路时为 nil。
		Link *DeviceInterfaceLink `json:"link,omitempty" yaml:"link,omitempty" protobuf:"bytes,9,opt,name=link"`
	}

	// DeviceInterfaceLink 是验证支持 RDMA 的接口链路是否实际可用的结果。
	//
	// 通过将结果设为显式状态而非布尔值，将状态上报与调度限制解耦：只有 Failed 状态
	// 会阻止发布节点的 RDMA 标签，因为“无法检查此接口”绝不应被发布为“此 worker 没有 RDMA”。
	DeviceInterfaceLink struct {
		// State 是验证结果。
		// +k8s:validation:enum=["OK","Unverified","Failed"]
		State DeviceInterfaceLinkState `json:"state" yaml:"state" protobuf:"bytes,1,name=state,casttype=DeviceInterfaceLinkState"`

		// Reason 原样携带检查器的原始输出文本，包括其读取到的属性值。若状态非 OK
		// 却没有 Reason，运维人员将无法仅凭该记录回答“为什么？”。
		Reason string `json:"reason,omitempty" yaml:"reason,omitempty" protobuf:"bytes,2,opt,name=reason"`

		// FirstSeenTime 是首次观测到持续 Failed 状态的时间。只要故障持续存在，该值
		// 在多轮检查间保持不变，并在状态变为其他任何值时立即清除。若每轮检查都刷新该值，
		// 将无法回答“该故障已持续多久？”，而这正是该字段存在的意义。
		//
		// 对于其他两种状态（包括 Unverified）均为 nil：未得出结论的状态不存在需要计时
		// 起点的中断故障，因此该字段并非表示“当前的非 OK 状态”，而是专指失败状态。
		FirstSeenTime *meta.Time `json:"firstSeenTime,omitempty" yaml:"firstSeenTime,omitempty" protobuf:"bytes,3,opt,name=firstSeenTime"`
	}

	// DevicesAllocationGroup 描述已分配的设备组。
	DevicesAllocationGroup struct {
		// ID 是该设备组的全局唯一标识符。
		ID string `json:"id" yaml:"id" protobuf:"bytes,1,opt,name=id"`

		// Manufacturer 是设备制造商的名称。
		Manufacturer string `json:"manufacturer" yaml:"manufacturer" protobuf:"bytes,2,name=manufacturer"`

		// Accelerators 是该设备组中已分配的加速器设备列表。
		//
		// +listType=map
		// +listMapKey=id
		Accelerators []AcceleratorAllocation `json:"accelerators,omitempty" yaml:"accelerators,omitempty" protobuf:"bytes,3,opt,name=accelerators"`
	}
)

// DeviceInterfaceLinkState 是接口 RDMA 链路验证的结果。
//
// 之所以设计三种状态，是因为“链路正常”与“无法探测”是具有不同后果的截然不同的事实，
// 将它们压缩为一个布尔值必然迫使其中之一失真。
// +enum
type DeviceInterfaceLinkState string

const (
	// DeviceInterfaceLinkStateOK 表示链路已检查并通过验证。
	DeviceInterfaceLinkStateOK DeviceInterfaceLinkState = "OK"
	// DeviceInterfaceLinkStateUnverified 表示检查未得出结论：所需属性不可读、无法列出
	// 端口目录，或存在 RDMA 目录树但完全无法读取。此处并不存在按制造商区分而缺失检查器
	// 的情况——检查直接读取 RDMA 子系统自身的端口属性，不依赖任何分发逻辑。
	//
	// 该状态会上报，且不会阻止发布节点的 RDMA 标签：无法探测的链路绝不能静默地将其
	// worker 排除在调度之外。
	DeviceInterfaceLinkStateUnverified DeviceInterfaceLinkState = "Unverified"
	// DeviceInterfaceLinkStateFailed 表示检查已执行且链路不可用。这是唯一会阻止发布
	// 节点 RDMA 标签的状态，且绝不会因文件缺失或不可读而触发——不可读的属性对应
	// Unverified 状态。
	DeviceInterfaceLinkStateFailed DeviceInterfaceLinkState = "Failed"
)

// DeviceAllocationMode 描述加速器设备的分配模式。
// +enum
type DeviceAllocationMode uint32

const (
	// DeviceAllocationModeNone 表示设备的分配模式未知。
	DeviceAllocationModeNone DeviceAllocationMode = iota
	// DeviceAllocationModeExclusive 表示设备独占分配给单个消费者。
	DeviceAllocationModeExclusive
	// DeviceAllocationModeShared 表示设备分配给多个消费者，且资源在它们之间共享。
	DeviceAllocationModeShared
	// DeviceAllocationModeSliced 表示设备分配给多个消费者，且资源在它们之间切分隔离。
	DeviceAllocationModeSliced
	// DeviceAllocationModePartitioned 表示设备被切分为硬件分区（例如 NVIDIA MIG
	// GPU 实例），每个分区分配给一个消费者。与 Sliced 不同，该模式下隔离由硬件强制实施，
	// 加速器必须先进入分区模式，且之后不能再服务于其他分配模式。
	DeviceAllocationModePartitioned
	// DeviceAllocationModeVisibility 是一种仅限内部使用的模式：它授予容器对同一 Pod 内
	// 其他容器所分配物理设备的 device-cgroup 可见性，而不进行真实的设备选择或任何资源
	// 计量。该模式永不在 InstanceType 上通告，也永不写入 Devices 状态。
	DeviceAllocationModeVisibility
)

func (in DeviceAllocationMode) String() string {
	switch in {
	case DeviceAllocationModeExclusive:
		return "Exclusive"
	case DeviceAllocationModeShared:
		return "Shared"
	case DeviceAllocationModeSliced:
		return "Sliced"
	case DeviceAllocationModePartitioned:
		return "Partitioned"
	case DeviceAllocationModeVisibility:
		return "Visibility"
	default:
		return "None"
	}
}

type (
	// Accelerator 描述加速器设备的信息。
	Accelerator struct {
		// ID 是该设备的全局唯一标识符。
		ID string `json:"id" yaml:"id" protobuf:"bytes,1,opt,name=id"`

		// Index 是设备的逻辑编号，从 0 开始。
		Index uint32 `json:"index" yaml:"index" protobuf:"varint,2,name=index"`

		// PhysicalIndexes 是由制造商构建的设备字符设备索引列表。
		PhysicalIndexes []uint32 `json:"physicalIndexes" yaml:"physicalIndexes" protobuf:"varint,3,rep,name=physicalIndexes"`

		// Topology 是设备的拓扑信息。
		Topology DeviceTopology `json:"topology" yaml:"topology" protobuf:"bytes,4,name=topology"`

		// Status 是设备的当前状态。
		Status AcceleratorStatus `json:"status" yaml:"status" protobuf:"bytes,5,name=status"`
	}

	// AcceleratorPlacement 表示单次分配在加速器上占用的连续区间 [Start, Start+Length)。
	// 它作为两类放置台账的通用记录载体，其计数单位取决于承载它的字段：
	//
	//   - 显存切片（memory slices），用于物理分区台账——即硬件 GPU 分区（例如
	//     NVIDIA MIG GPU 实例）所占用的区间；能力的空加速器合法槽位缓存以及通过
	//     注解传递的已占用槽位均以此为单位表示。
	//   - 制造商自身的计算单元，用于逻辑切片台账——在 AMD 上即与 HSA_CU_MASK 中
	//     完全一致的 CU 掩码位索引。逻辑切片在加速器上占用的是一个具体位置，而非仅占
	//     一份配额：被分配到相同区间的两个切片会共享这些计算单元，而非共享整个加速器。
	AcceleratorPlacement struct {
		// Start 是该区间覆盖的起始单元（从 0 开始）。
		Start int32 `json:"start" yaml:"start" protobuf:"varint,1,opt,name=start"`

		// Length 是该区间跨越的单元数量；区间范围为 [Start, Start+Length)。
		// 命名为 Length 而非 Size，是为了避免与 protobuf 为该消息生成的 Size()
		// 方法冲突。
		Length int32 `json:"length" yaml:"length" protobuf:"varint,2,opt,name=length"`
	}

	// AcceleratorPhysicalSlicedProfile 描述设备型号的一种硬件分区规格（profile），
	// 例如 NVIDIA MIG 规格（如 "1g.5gb"）。计算与显存切片数表达了各个维度上的请求
	// 粒度，比单一标量步长更丰富一个维度。
	AcceleratorPhysicalSlicedProfile struct {
		// Name 是规格标识符，例如 "1g.5gb"。它既是显示名称，也是物理切片请求
		// 未来的资源键后缀。
		Name string `json:"name" yaml:"name" protobuf:"bytes,1,opt,name=name"`

		// MemoryMib 是该规格单个实例的显存大小，单位为 MiB。
		MemoryMib int64 `json:"memoryMib" yaml:"memoryMib" protobuf:"varint,2,opt,name=memoryMib"`

		// ComputeSlices 是单个实例占用的计算切片数——即计算维度上的请求粒度
		// （在当前硬件上为 1..7）。
		ComputeSlices int32 `json:"computeSlices" yaml:"computeSlices" protobuf:"varint,3,opt,name=computeSlices"` // nolint: lll

		// MemorySlices 是单个实例占用的显存切片数——即显存维度上的请求粒度
		// （在当前硬件上为 1..8）。
		MemorySlices int32 `json:"memorySlices" yaml:"memorySlices" protobuf:"varint,4,opt,name=memorySlices"` // nolint: lll

		// Count 是单个加速器上该规格实例的最大数量。
		Count int32 `json:"count" yaml:"count" protobuf:"varint,5,opt,name=count"`

		// Placements 是该规格在空加速器上的完整合法放置集合（以显存切片为单位的
		// start:size），在设备探测时一次性枚举。调谐器（reconciler）从该缓存集合中
		// 减去根据 Pod 注解重建的已占用区间，即可推导出加速器的 RemainingProfiles，
		// 从而避免每次调谐都执行设备查询。缓存完整的空加速器集合可确保无论制造商的
		// 可用放置查询自身是否感知占用状态，减法推导都能保持正确。对于无物理切片规格的
		// 加速器，该字段为空。
		//
		// +listType=atomic
		Placements []AcceleratorPlacement `json:"placements,omitempty" yaml:"placements,omitempty" protobuf:"bytes,6,rep,name=placements"` // nolint: lll
	}

	// AcceleratorLogicalSliced 描述加速器的逻辑（软件）切片能力。
	AcceleratorLogicalSliced struct {
		// CoresPercentageOvercommit 报告每个切片是否可声明使用高达 100% 的设备算力
		// （时间片共享 / 加权共享）；为 false 表示算力是划分隔离的。
		CoresPercentageOvercommit bool `json:"coresPercentageOvercommit,omitempty" yaml:"coresPercentageOvercommit,omitempty" protobuf:"varint,1,opt,name=coresPercentageOvercommit"` // nolint: lll

		// Count 是该加速器可承载的最大逻辑切片数量。当前已启用 MIG 模式的加速器
		// 该值始终为 0，从而将其排除在逻辑容量键之外；处于待启用（pending-enable）状态的
		// 加速器尚未分区，仍会上报其逻辑切片数量。
		Count int32 `json:"count,omitempty" yaml:"count,omitempty" protobuf:"varint,2,opt,name=count"`
	}

	// AcceleratorPhysicalSliced 描述加速器的物理（硬件）切片能力。
	AcceleratorPhysicalSliced struct {
		// Profiles 在加速器不支持或未启用硬件切片时为空。
		//
		// +listType=map
		// +listMapKey=name
		Profiles []AcceleratorPhysicalSlicedProfile `json:"profiles,omitempty" yaml:"profiles,omitempty" protobuf:"bytes,1,rep,name=profiles"` // nolint: lll

		// Count 是加速器的物理切片上限——即 Profiles 中的最大 Count 值（例如
		// A100 上来自 7 个 1g.5gb 的 7）。它决定了处于分区状态的加速器在 DevicePlugin 中
		// 裸 ".partitioned" 令牌池的大小，后者正是服务于它的资源族；已分区的加速器
		// 不提供逻辑切片，因此完全退出 ".sliced" 资源池。当 Profiles 为空时该值为 0。
		Count int32 `json:"count,omitempty" yaml:"count,omitempty" protobuf:"varint,2,opt,name=count"`
	}

	// AcceleratorStatus 描述加速器设备的观测状态。
	AcceleratorStatus struct {
		// Unhealthy 指示设备是否不健康。
		Unhealthy bool `json:"unhealthy" yaml:"unhealthy" protobuf:"varint,1,name=unhealthy"`

		// LogicalSliced 是加速器的逻辑（软件）切片能力。
		LogicalSliced AcceleratorLogicalSliced `json:"logicalSliced,omitempty" yaml:"logicalSliced,omitempty" protobuf:"bytes,2,opt,name=logicalSliced"` // nolint: lll

		// PhysicalSliced 是加速器的物理（硬件）切片能力。
		PhysicalSliced AcceleratorPhysicalSliced `json:"physicalSliced,omitempty" yaml:"physicalSliced,omitempty" protobuf:"bytes,3,opt,name=physicalSliced"` // nolint: lll
	}

	// AcceleratorSlicedLogicalDetail 聚合了设备组的逻辑切片能力。单卡级别的
	// LogicalSliced 供加速器级决策读取；而该设备组视图供外部查询读取，以了解节点
	// 是否接受逻辑切片请求（Count > 0）以及是否允许算力超卖。
	AcceleratorSlicedLogicalDetail struct {
		// CoresPercentageOvercommit 是按型号定义的属性（同一组内一致），取自任意支持逻辑切分的
		// 加速器；当没有加速器支持逻辑切分时为 false 且无意义。
		CoresPercentageOvercommit bool `json:"coresPercentageOvercommit,omitempty" yaml:"coresPercentageOvercommit,omitempty" protobuf:"varint,1,opt,name=coresPercentageOvercommit"` // nolint: lll

		// Count 是组内各加速器 LogicalSliced.Count 的总和。
		Count int32 `json:"count,omitempty" yaml:"count,omitempty" protobuf:"varint,2,opt,name=count"`
	}

	// AcceleratorSlicedPhysicalDetailProfile 汇总组内加速器上的单个 profile。
	AcceleratorSlicedPhysicalDetailProfile struct {
		// Name 是 profile 标识符，例如 "1g.5gb"。
		Name string `json:"name" yaml:"name" protobuf:"bytes,1,opt,name=name"`

		// Count 是组内各加速器上该 profile 名称对应的 Count 总和。
		Count int32 `json:"count,omitempty" yaml:"count,omitempty" protobuf:"varint,2,opt,name=count"`

		// MemoryMib 是该 profile 单个实例的显存大小，单位为 MiB。同一组内相同 profile 名称的
		// 该值一致，因此直接透传（不累加）。它是 Pod webhook 针对 MIG 请求将其折算进
		// ".sliced.units"（MemoryMibToUnits）的显存基准输入，这也是为什么可从 InstanceType
		// Detail 访问到的聚合视图（不同于单加速器粒度的 Devices）必须携带该字段的原因。
		// 在 schema 中为可选字段（真实 profile 始终携带非零值）；Pod webhook 会将尚未填充
		// 的 detail 视为可重试的未就绪状态，而非依赖 schema 必填校验。
		MemoryMib int64 `json:"memoryMib,omitempty" yaml:"memoryMib,omitempty" protobuf:"varint,3,opt,name=memoryMib"`
	}

	// AcceleratorSlicedPhysicalDetail 汇总该组的物理切分能力。
	AcceleratorSlicedPhysicalDetail struct {
		// Profiles 是该组的物理 profile 列表，按名称累加统计。
		//
		// +listType=map
		// +listMapKey=name
		Profiles []AcceleratorSlicedPhysicalDetailProfile `json:"profiles,omitempty" yaml:"profiles,omitempty" protobuf:"bytes,1,rep,name=profiles"` // nolint: lll

		// Count 是组内各加速器 PhysicalSliced.Count 的总和。
		Count int32 `json:"count,omitempty" yaml:"count,omitempty" protobuf:"varint,2,opt,name=count"`
	}

	// AcceleratorSlicedDetail 是组级别的切分能力视图，由组内各加速器的切分状态聚合而成。
	AcceleratorSlicedDetail struct {
		// Logical 是聚合后的逻辑（软件）切分能力。
		Logical AcceleratorSlicedLogicalDetail `json:"logical,omitempty" yaml:"logical,omitempty" protobuf:"bytes,1,opt,name=logical"`

		// Physical 是聚合后的物理（硬件）切分能力。
		Physical AcceleratorSlicedPhysicalDetail `json:"physical,omitempty" yaml:"physical,omitempty" protobuf:"bytes,2,opt,name=physical"`
	}

	// AcceleratorProfileCount 将物理切分 profile 名称与实例数量配对——具体含义取决于
	// 承载它的字段，可表示已分配（已绑定）或剩余（仍可创建）。该类型仅用于状态展示
	// （绝不作为 map 键），从而使 profile 账本与能力清单
	// （AcceleratorSlicedPhysicalDetailProfile）保持各自独立演进。
	AcceleratorProfileCount struct {
		// Name 是 profile 标识符，例如 "1g.10gb"。
		Name string `json:"name" yaml:"name" protobuf:"bytes,1,opt,name=name"`

		// Count 是该 profile 的实例数量。
		Count int32 `json:"count,omitempty" yaml:"count,omitempty" protobuf:"varint,2,opt,name=count"`
	}

	// AcceleratorAllocation 描述已分配的加速器设备。
	AcceleratorAllocation struct {
		// ID 是该设备的全局唯一标识符。
		ID string `json:"id" yaml:"id" protobuf:"bytes,1,opt,name=id"`

		// Index 是设备的逻辑编号，从 0 开始。
		Index uint32 `json:"index" yaml:"index" protobuf:"varint,2,name=index"`

		// Mode 是设备的分配模式。
		Mode DeviceAllocationMode `json:"mode" yaml:"mode" protobuf:"varint,3,name=mode,casttype=DeviceAllocationMode"`

		// Allocated 是设备已分配的单元数。
		Allocated int32 `json:"allocated,omitempty" yaml:"allocated,omitempty" protobuf:"varint,4,opt,name=allocated"`

		// Remaining 是设备剩余可分配的单元数。
		Remaining int32 `json:"remaining,omitempty" yaml:"remaining,omitempty" protobuf:"varint,5,opt,name=remaining"`

		// AllocatedSlices 表示设备当前承载的逻辑切片数量：每个持有其切片的容器计为一个。
		// 无论 Remaining 为多少，支持逻辑切分的设备最多只能承载 LogicalSliced.Count 个切片，
		// 因此只有当两者均有余量时才能容纳新切片。在该字段引入前写入的账本会读取为零值，
		// 按无切片处理。
		AllocatedSlices int32 `json:"allocatedSlices,omitempty" yaml:"allocatedSlices,omitempty" protobuf:"varint,12,opt,name=allocatedSlices"` // nolint: lll

		// AllocatedProfiles 和 RemainingProfiles 是供 AdmissionCheck 读取的单加速器物理切分
		// 账本——即调谐器根据下方单 Pod 的 AllocatedPhysicalProfile/AllocatedPhysicalPlacements
		// 传输字段（对该加速器上每个 Pod 已占用的槽位取并集）计算出的聚合输出。对于没有
		// 物理切分 profile 的加速器，两者均为空（省略），以确保其序列化结果与引入这些字段前
		// 完全一致。
		//
		// AllocatedProfiles 按 profile 名称列出当前在该加速器上已创建并绑定的实例数量
		// （即各 Pod 已记录的放置数量）。
		//
		// +listType=map
		// +listMapKey=name
		AllocatedProfiles []AcceleratorProfileCount `json:"allocatedProfiles,omitempty" yaml:"allocatedProfiles,omitempty" protobuf:"bytes,6,rep,name=allocatedProfiles"` // nolint: lll

		// RemainingProfiles 按 profile 名称列出在给定加速器已占用放置槽位的情况下，每种
		// profile 还能创建的实例数量——即 AdmissionCheck 用来做准入判定的放置感知剩余容量
		// （标量 Remaining 的按 profile 对应版本）。
		//
		// +listType=map
		// +listMapKey=name
		RemainingProfiles []AcceleratorProfileCount `json:"remainingProfiles,omitempty" yaml:"remainingProfiles,omitempty" protobuf:"bytes,7,rep,name=remainingProfiles"` // nolint: lll

		// AllocatedPhysicalProfile 和 AllocatedPhysicalPlacements 是供调谐器消费以构建上述
		// 账本的单 Pod 注解传输字段，而非状态输出。device-plugin 在 Allocate 时会在 Pod
		// 自身的分配注解中记录该 Pod 在此加速器上持有的单个物理分区（例如 NVIDIA MIG 实例）：
		// 包含其 profile 名称及占用的显存切片区间。每个 Pod 在每张加速器上仅持有一种
		// profile 的一个实例。
		//
		// 该传输组中的所有字段在聚合后的 Devices.Status 中都会被省略——但它们并非不可见：
		// Instance.status.allocations 会原样上报每个 Pod 自身的记录，运维人员可在此查看
		// 其 Instance 持有的具体分区。
		AllocatedPhysicalProfile string `json:"allocatedPhysicalProfile,omitempty" yaml:"allocatedPhysicalProfile,omitempty" protobuf:"bytes,8,opt,name=allocatedPhysicalProfile"` // nolint: lll

		// AllocatedPhysicalPlacements 是 Pod 的分区在此加速器上占用的显存切片区间。
		// 调谐器会对节点上所有 Pod 的这些区间取并集，以推导出 RemainingProfiles。
		//
		// +listType=atomic
		AllocatedPhysicalPlacements []AcceleratorPlacement `json:"allocatedPhysicalPlacements,omitempty" yaml:"allocatedPhysicalPlacements,omitempty" protobuf:"bytes,9,rep,name=allocatedPhysicalPlacements"` // nolint: lll

		// AllocatedPhysicalID 是分区自身的标识符，即其在创建时由驱动赋予的名称（如 NVIDIA MIG
		// UUID、平头哥 PPU MIG UUID）。它同属上述注解传输字段，在聚合后的 Devices.Status 中
		// 为空（省略）。
		//
		// 记录该字段是因为分配器在分配时已持有它，否则读取方需要耗费额外代价反查：若无此
		// 字段，定位一个分区就需要将记录的 profile 名称转换为驱动 profile ID、枚举该 profile
		// 下的所有实例并按放置位置匹配——每个监控周期每张卡都要执行数十次驱动调用，只为找回
		// 在 Allocate 时原本就已掌握的信息。因此，它也是寻址分区的唯一方式：未携带该字段的
		// 分配记录会直接按缺失处理，而不会尝试推导。
		//
		// 它并非代次标记（generation marker）。这些 UUID 基于名称生成，由父设备与实例自身
		// 标识推导而来，因此在同一放置位置销毁分区并重建另一个分区会产生相同的标识符。
		// 应将记录的 ID 视为快速查找分区的手段，绝不能将其当作“找到的分区就是当初授予的
		// 那个分区”的证明。
		AllocatedPhysicalID string `json:"allocatedPhysicalID,omitempty" yaml:"allocatedPhysicalID,omitempty" protobuf:"bytes,11,opt,name=allocatedPhysicalID"` // nolint: lll

		// AllocatedLogicalPlacements 是逻辑切片账本的单 Pod 注解传输字段：表示 Pod 的逻辑切片
		// 在此加速器上持有的计算几何布局，以厂商自身的计算单元表示（在 AMD 上即与
		// HSA_CU_MASK 中完全一致的 CU 掩码位索引）。device-plugin 在 Allocate 时记录该字段，
		// 以便后续放置决策读取节点上现有切片已占用的位置。在聚合后的 Devices.Status 中以及
		// 逻辑切片无位置概念的厂商设备上均为空（省略）。
		//
		// +listType=atomic
		AllocatedLogicalPlacements []AcceleratorPlacement `json:"allocatedLogicalPlacements,omitempty" yaml:"allocatedLogicalPlacements,omitempty" protobuf:"bytes,10,rep,name=allocatedLogicalPlacements"` // nolint: lll
	}
)

// DevicesList 包含 Devices 的列表。
//
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type DevicesList struct {
	meta.TypeMeta `json:",inline"`
	meta.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,name=metadata"`

	Items []Devices `json:"items" protobuf:"bytes,2,rep,name=items"`
}

var _ runtime.Object = (*DevicesList)(nil)
