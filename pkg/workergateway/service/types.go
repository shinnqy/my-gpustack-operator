package service

import (
	"k8s.io/apimachinery/pkg/api/resource"

	worker "gpustack.ai/gpustack/api/worker/v1"
	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
)

// 跨集群聚合的 InstanceTypeFlavor。
type (
	// AggregatedInstanceTypeFlavor 表示多个集群中相同硬件池的聚合视图。
	AggregatedInstanceTypeFlavor struct {
		Name string `json:"name"`

		Spec AggregatedInstanceTypeFlavorSpec `json:"spec"`
	}

	// AggregatedInstanceTypeFlavorSpec 保存硬件池规格及其来源。
	AggregatedInstanceTypeFlavorSpec struct {
		worker.InstanceTypeFlavorSpec `json:",inline"`

		// Clusters 列出提供该硬件池的集群，例如 cluster-a 和 cluster-b。
		Clusters []string `json:"clusters"`
	}

	// AggregatedInstanceTypeFlavorList 是聚合硬件池的列表。
	AggregatedInstanceTypeFlavorList struct {
		Items []AggregatedInstanceTypeFlavor `json:"items"`
	}
)

// 跨集群聚合的 InstanceType。
type (
	// AggregatedInstanceType 表示各集群硬件规格的聚合结果。
	AggregatedInstanceType struct {
		Name string `json:"name"`

		Spec   AggregatedInstanceTypeSpec   `json:"spec"`
		Status AggregatedInstanceTypeStatus `json:"status"`
	}

	// AggregatedInstanceTypeSpec 沿用 Worker 的 InstanceTypeSpec 表达需求。
	AggregatedInstanceTypeSpec = workercore.InstanceTypeSpec

	// AggregatedInstanceTypeResource 使用 Worker 的资源数量结构。
	AggregatedInstanceTypeResource = workercore.InstanceTypeResource

	// AggregatedInstanceTypePartitionedResource 表达硬件分区的标量视图和候选成员上报的逐 profile 账本。
	AggregatedInstanceTypePartitionedResource = workercore.InstanceTypePartitionedResource

	// AggregatedInstanceTypeStatus 保存汇总后的可用资源、档位以及观测到的硬件描述。
	AggregatedInstanceTypeStatus struct {
		// Detail 保存这些候选共有的硬件身份，如厂商、型号、架构系列及单卡显存和算力。身份来自已协调的候选。
		// Detail.SlicedDetail 单独汇总所有档位的切片能力，物理 profile 按名累加。
		Detail workercore.InstanceTypeDetail `json:"detail,omitempty"`

		// OnceMaxRequest 是按主维度胜出的一个档位所能提供的完整单次资源组合。加速器规格优先看整卡，纯 CPU 规格看 CPU，
		// 其他维度跟随同一个档位。
		OnceMaxRequest AggregatedInstanceTypeOverviewResource `json:"onceMaxRequest"`

		// Remaining 是所有档位的逐维可申请余量之和。一次分配能拿到的组合见 OnceMaxRequest。
		Remaining AggregatedInstanceTypeOverviewResource `json:"remaining"`

		// Tiers 按候选的原始整卡 OnceMaxRequest 分档。纯 CPU 规格的整卡维度均为零，因此其候选同处一档，
		// 仍可按各自的 CPU 数量比较。
		Tiers []AggregatedInstanceTypeOnceMaxRequestTier `json:"tiers"`
	}

	// AggregatedInstanceTypeOverviewResource 描述整卡、共享、逻辑切片、硬件分区和 CPU 等维度的汇总。
	AggregatedInstanceTypeOverviewResource struct {
		// Accelerator 是可以按整卡方式申请的加速器数量，例如 1 或 4。
		Accelerator resource.Quantity `json:"accelerator"`

		// AcceleratorShared 是可申请的加速器共享资源数量。
		AcceleratorShared resource.Quantity `json:"acceleratorShared"`

		// AcceleratorSliced 是可申请的逻辑切片资源数量。
		AcceleratorSliced resource.Quantity `json:"acceleratorSliced"`

		// AcceleratorPartitioned 按硬件分区 profile 表示可用数。单次最大请求使用胜出候选的各项数量，每项最多为一。
		// 总余量则按名称累加所有 Active 候选的可用数量并排序。
		//
		// 同卡的不同 profile 可能争用同一批物理切片，直接相加不能表示整卡容量。已支持但没有空位的 profile 保留零值，
		// 未上报逐项账本的成员不贡献此维度。硬切分卡使用独立的分配资源。
		AcceleratorPartitioned []workercore.AcceleratorProfileCount `json:"acceleratorPartitioned,omitempty"`

		// CPU 是这一概览对应的 CPU 数量。
		CPU resource.Quantity `json:"cpu"`
	}

	// AggregatedInstanceTypeOnceMaxRequestTier 是单次申请额度相同的一组候选成员。
	AggregatedInstanceTypeOnceMaxRequestTier struct {
		// OnceMaxRequest 是该档中按主维度胜出的候选能够提供的完整组合。加速器或 CPU 决定胜出者，各维度均来自同一成员。
		OnceMaxRequest AggregatedInstanceTypeOverviewResource `json:"onceMaxRequest"`

		// Remaining 汇总该档所有 Active 候选在各维度的可用余量。整个档位的合计可能大于单次分配能力。
		Remaining AggregatedInstanceTypeOverviewResource `json:"remaining"`

		// Candidates 列出该档的成员。它们有相同的整卡单次最大请求值，但共享、逻辑切片和 CPU 等维度可以不同。
		Candidates []AggregatedInstanceTypeOnceMaxRequestCandidate `json:"candidates"`

		// AcceleratorSlicedDetail 汇总本档每个候选的切片能力，逻辑和物理数量累加，各物理 profile 再按名称累加。
		AcceleratorSlicedDetail workercore.AcceleratorSlicedDetail `json:"acceleratorSlicedDetail,omitempty"`
	}

	// AggregatedInstanceTypeOnceMaxRequestCandidate 保存一个集群中的实际 InstanceType，
	// 作为该档的候选。
	AggregatedInstanceTypeOnceMaxRequestCandidate struct {
		// Cluster 是该候选所在的集群。
		Cluster string `json:"cluster"`

		// Name 是集群中原有的 InstanceType 名称。
		Name string `json:"name"`

		// Phase 保存原规格的状态，例如 Active、Inactive 或 Draining。只有 Active 候选贡献可调度资源。
		Phase string `json:"phase"`

		// Accelerator 是该候选可按整卡方式交付的资源。
		Accelerator AggregatedInstanceTypeResource `json:"accelerator"`

		// AcceleratorShared 是该候选的共享加速器资源。
		AcceleratorShared AggregatedInstanceTypeResource `json:"acceleratorShared"`

		// AcceleratorSliced 是该候选的逻辑切片资源。
		AcceleratorSliced AggregatedInstanceTypeResource `json:"acceleratorSliced"`

		// AcceleratorPartitioned 保存候选的硬件分区资源及逐 profile 可用账本，上层按 profile 名汇总。
		AcceleratorPartitioned AggregatedInstanceTypePartitionedResource `json:"acceleratorPartitioned"`

		// CPU 是候选上报的 CPU 资源，包含其单次可申请的额度。
		CPU AggregatedInstanceTypeResource `json:"cpu"`

		// AcceleratorSlicedDetail 保存该候选从 Status.Detail 读取的切片能力，作为档位和聚合项的汇总输入。
		AcceleratorSlicedDetail workercore.AcceleratorSlicedDetail `json:"acceleratorSlicedDetail,omitempty"`
	}

	// AggregatedInstanceTypeList 表示跨集群归并后的规格列表。
	AggregatedInstanceTypeList struct {
		Items []AggregatedInstanceType `json:"items,omitempty"`
	}
)

// 带来源集群的硬件池。
type (
	// ClusterInstanceTypeFlavor 是单个集群中的 InstanceTypeFlavor，附带集群身份。
	ClusterInstanceTypeFlavor struct {
		worker.InstanceTypeFlavor

		Cluster string `json:"cluster"`
	}

	// ClusterInstanceTypeFlavorList 是带集群来源的硬件池列表。
	ClusterInstanceTypeFlavorList struct {
		Items []ClusterInstanceTypeFlavor `json:"items"`
	}
)

// 带来源集群的实例规格。
type (
	// ClusterInstanceType 是一个集群中的实例规格。
	ClusterInstanceType struct {
		worker.InstanceType

		Cluster string `json:"cluster"`
	}

	// ClusterInstanceTypeList 是带集群来源的实例规格列表。
	ClusterInstanceTypeList struct {
		Items []ClusterInstanceType `json:"items"`
	}
)

// 带来源集群的实例。
type (
	// ClusterInstance 是一个集群中的 Instance，保留其原始 Kubernetes 身份。
	ClusterInstance struct {
		worker.Instance

		Cluster string `json:"cluster"`
	}

	// ClusterInstanceList 是带集群来源的实例列表。
	ClusterInstanceList struct {
		Items []ClusterInstance `json:"items"`
	}
)

// 带来源集群的持久卷类型。
type (
	// ClusterInstancePersistentVolumeType 是指定集群中的持久卷类型。
	ClusterInstancePersistentVolumeType struct {
		worker.InstancePersistentVolumeType

		Cluster string `json:"cluster"`
	}

	// ClusterInstancePersistentVolumeTypeList 是带集群来源的持久卷类型列表。
	ClusterInstancePersistentVolumeTypeList struct {
		Items []ClusterInstancePersistentVolumeType `json:"items"`
	}
)

// 带来源集群的持久卷。
type (
	// ClusterInstancePersistentVolume 是指定集群与命名空间里的持久卷。
	ClusterInstancePersistentVolume struct {
		worker.InstancePersistentVolume

		Cluster string `json:"cluster"`
	}

	// ClusterInstancePersistentVolumeList 是带集群来源的持久卷列表。
	ClusterInstancePersistentVolumeList struct {
		Items []ClusterInstancePersistentVolume `json:"items"`
	}
)

// 带来源集群的镜像拉取凭据资源。
type (
	// ClusterInstanceImagePullSecret 是指定集群中的镜像拉取凭据资源。
	ClusterInstanceImagePullSecret struct {
		worker.InstanceImagePullSecret

		Cluster string `json:"cluster"`
	}
	// ClusterInstanceImagePullSecretList 是带集群来源的镜像拉取凭据资源列表。
	ClusterInstanceImagePullSecretList struct {
		Items []ClusterInstanceImagePullSecret `json:"items"`
	}
)

// 带来源集群的 SSH 公钥。
type (
	// ClusterInstanceSSHPublicKey 是一个集群中的 SSH 公钥资源。
	ClusterInstanceSSHPublicKey struct {
		worker.InstanceSSHPublicKey

		Cluster string `json:"cluster"`
	}

	// ClusterInstanceSSHPublicKeyList 是带集群来源的 SSH 公钥列表。
	ClusterInstanceSSHPublicKeyList struct {
		Items []ClusterInstanceSSHPublicKey `json:"items"`
	}
)
