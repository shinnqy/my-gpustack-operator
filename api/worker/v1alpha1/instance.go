package v1alpha1

import (
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// Instance 是 worker.gpustack.ai 的结构定义。
//
// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:crd-gen:resource:scope="Namespaced",subResources=["status"]
type Instance struct {
	meta.TypeMeta   `json:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Spec   InstanceSpec   `json:"spec" protobuf:"bytes,2,opt,name=spec"`
	Status InstanceStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

var _ runtime.Object = (*Instance)(nil)

// InstanceSpec 定义 Instance 的期望状态。
type InstanceSpec struct {
	// DisplayName 是 Instance 的显示名称。
	//
	// +k8s:validation:maxLength=64
	DisplayName string `json:"displayName,omitempty" protobuf:"bytes,1,opt,name=displayName"`

	// Description 是 Instance 的描述信息。
	//
	// +k8s:validation:maxLength=1024
	Description string `json:"description,omitempty" protobuf:"bytes,2,opt,name=description"`

	// Type 是用于供给对应资源的 InstanceType 名称。
	//
	// 创建后不可变。
	//
	// +required
	Type string `json:"type" protobuf:"bytes,3,name=type"`

	// InstanceTemplate 是用于运行 Instance 的模板。
	//
	// 创建后不可变。
	InstanceTemplate `json:",inline" protobuf:"bytes,4,name=instanceTemplate"`

	// Volume 是要挂载到 Instance 中的卷。
	//
	// 创建后不可变。
	//
	// +required
	Volume InstanceVolume `json:"volume,omitempty" protobuf:"bytes,5,opt,name=volume"`

	// SSHPublicKey 是对 InstanceSSHPublicKey 的引用，其中包含用于访问 Instance 的 SSH 公钥。
	//
	// 创建后不可变。
	SSHPublicKey *core.LocalObjectReference `json:"sshPublicKey,omitempty" protobuf:"bytes,6,opt,name=sshPublicKey"`

	// Stop 指示创建 Instance 后是否将其停止。
	Stop bool `json:"stop,omitempty" protobuf:"varint,7,opt,name=stop"`

	// NodeName 将 Instance 固定到指定的 Kubernetes Node，该 Node 在创建 Instance 时必须已存在。
	// 它会被渲染为底层 Pod 在 kubernetes.io/hostname 上的 nodeSelector，而非直接设置为 Pod
	// 自身的 nodeName，因此调度器和 Kueue 准入仍会介入放置决策。除 Node 是否存在外不做
	// 其他校验，因此若该 Node 无法满足所引用的 InstanceType，Pod 会处于 Pending 状态而非被
	// 拒绝。为空表示不固定节点。
	//
	// 除非 Instance 处于停止状态，否则不可变。
	//
	// +k8s:validation:maxLength=253
	NodeName string `json:"nodeName,omitempty" protobuf:"bytes,8,opt,name=nodeName"`
}

// InstanceTemplate 定义用于运行 Instance 的模板。
type InstanceTemplate struct {
	// Image 是要运行的容器镜像。
	//
	// +required
	Image string `json:"image" protobuf:"bytes,1,name=image"`

	// ImagePullPolicy 是要使用的镜像拉取策略。
	//
	// +default="IfNotPresent"
	// +k8s:validation:enum=["Always","IfNotPresent","Never"]
	ImagePullPolicy core.PullPolicy `json:"imagePullPolicy,omitempty" protobuf:"bytes,2,name=imagePullPolicy"`

	// Command 是在 Instance 中运行的命令，
	// 将覆盖容器镜像默认的 CMD 命令。
	//
	// +listType=atomic
	Command []string `json:"command,omitempty" protobuf:"bytes,3,rep,name=command"`

	// Privileged 指示容器是否应以特权模式运行。
	//
	// +k8s:validation:default=false
	Privileged bool `json:"privileged,omitempty" protobuf:"bytes,4,opt,name=privileged"`

	// Ports 是从 Instance 暴露的端口列表。
	//
	// +patchMergeKey=port
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=port
	// +listMapKey=protocol
	Ports []InstancePort `json:"ports,omitempty" patchStrategy:"merge" patchMergeKey:"port" protobuf:"bytes,5,rep,name=ports"`

	// Env 是要在 Instance 中设置的环境变量列表。
	//
	// +patchMergeKey=name
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=name
	Env []InstanceEnvVar `json:"env,omitempty" patchStrategy:"merge" patchMergeKey:"name" protobuf:"bytes,6,rep,name=env"`

	// Resources 是 Instance 的资源需求。
	Resources *InstanceResources `json:"resources,omitempty" protobuf:"bytes,7,opt,name=resources"`

	// VolumeMount 是在 Instance 中挂载卷的路径。
	//
	// +default="/workspace"
	// +k8s:validation:pattern="^(/[^/]+)+$"
	// +k8s:validation:maxLength=1024
	VolumeMount string `json:"volumeMount,omitempty" protobuf:"bytes,8,opt,name=volumeMount"`

	// ImagePullSecret 是对 InstanceImagePullSecret 的引用，其中包含拉取容器镜像所需的凭据。
	ImagePullSecret *core.LocalObjectReference `json:"imagePullSecret,omitempty" protobuf:"bytes,9,opt,name=imagePullSecret"`

	// AdditionalVolumes 是除工作区外要在 Instance 中挂载的卷列表，每个卷挂载在各自独立
	// 的路径下。它们仅挂载到 Instance 的主容器中，SSH 服务端也会看到这些挂载。
	//
	// 除非 Instance 处于停止状态，否则不可变。
	//
	// +listType=atomic
	AdditionalVolumes []InstanceAdditionalVolume `json:"additionalVolumes,omitempty" protobuf:"bytes,10,rep,name=additionalVolumes"` // nolint: lll
}

// InstanceAdditionalVolume 定义除工作区外要在 Instance 中挂载的单个卷。
// 必须且只能指定一个卷源。
type InstanceAdditionalVolume struct {
	// MountPath 是容器内用于挂载卷的绝对路径。它不能与其他条目的路径重复，也不能与
	// 工作区的 VolumeMount 重复。
	//
	// +required
	// +k8s:validation:pattern="^(/[^/]+)+$"
	// +k8s:validation:maxLength=1024
	MountPath string `json:"mountPath" protobuf:"bytes,1,name=mountPath"`

	// ReadOnly 以只读方式挂载卷。
	ReadOnly bool `json:"readOnly,omitempty" protobuf:"varint,2,opt,name=readOnly"`

	// SubPath 挂载卷内的相对路径而非其根目录。
	// 它不能是绝对路径，也不能包含 ".." 路径段。
	//
	// +k8s:validation:maxLength=1024
	SubPath string `json:"subPath,omitempty" protobuf:"bytes,3,opt,name=subPath"`

	// Persistent 是同一 Namespace 下要挂载的 InstancePersistentVolume 引用。
	Persistent *core.LocalObjectReference `json:"persistent,omitempty" protobuf:"bytes,4,opt,name=persistent"`

	// ConfigMap 是同一 Namespace 下要挂载的 ConfigMap 引用。
	ConfigMap *core.LocalObjectReference `json:"configMap,omitempty" protobuf:"bytes,5,opt,name=configMap"`

	// Secret 是同一 Namespace 下要挂载的 Secret 引用。
	Secret *core.LocalObjectReference `json:"secret,omitempty" protobuf:"bytes,6,opt,name=secret"`

	// HostPath 是 Kubernetes Node 上要挂载的路径。由于跨越了节点边界，使用该路径需要
	// 开启 instance-host-path-volume-allowed Setting —— 无论是在创建时，还是在后续
	// 新增或扩大此类挂载的变更中。Instance 已持有的挂载不会被重新校验，因此关闭该
	// Setting 不会使其受阻。
	HostPath *core.HostPathVolumeSource `json:"hostPath,omitempty" protobuf:"bytes,7,opt,name=hostPath"`

	// Model 挂载同一 Namespace 下某个 ModelArtifact 的权重。无论 ReadOnly 如何设置，
	// 该卷始终以只读方式挂载，且不接受 SubPath：ModelArtifact 自身的路径即为子路径。
	// 当前版本仅支持位于 PersistentVolumeClaim 上的制品；位于模型仓库（model hub）上的
	// 制品需要节点分发能力，目前尚未实现。
	Model *InstanceModelVolumeSource `json:"model,omitempty" protobuf:"bytes,8,opt,name=model"`
}

// InstanceModelVolumeSource 引用 Instance 卷所挂载的 ModelArtifact。
type InstanceModelVolumeSource struct {
	// ArtifactRef 指定 Instance 所在 Namespace 中的 ModelArtifact 名称。
	//
	// +required
	ArtifactRef core.LocalObjectReference `json:"artifactRef" protobuf:"bytes,1,name=artifactRef"`
}

// InstancePort 定义 Instance 对外暴露的端口。
type InstancePort struct {
	// Port 是 Instance 对外暴露的端口号。
	Port int32 `json:"port" protobuf:"varint,1,name=port"`

	// Protocol 是该端口使用的协议。
	//
	// +k8s:validation:enum=["TCP","UDP","SCTP"]
	// +k8s:validation:default="TCP"
	Protocol core.Protocol `json:"protocol,omitempty" protobuf:"bytes,2,opt,name=protocol"`

	// Name 是端口的名称。
	//
	// +k8s:validation:maxLength=16
	Name string `json:"name,omitempty" protobuf:"bytes,3,opt,name=name"`
}

// InstanceEnvVar 定义在 Instance 中设置的环境变量。
type InstanceEnvVar struct {
	// Name 是环境变量的名称，
	// 同一个 Instance 中的每个名称必须唯一。
	Name string `json:"name" protobuf:"bytes,1,name=name"`

	// Value 是环境变量的值。
	Value string `json:"value" protobuf:"bytes,2,name=value"`
}

// InstanceResources 定义 Instance 的资源需求。
type InstanceResources struct {
	// CPU 是 Instance 的 CPU 资源需求，例如 "4"、"8"。
	//
	// +required
	CPU resource.Quantity `json:"cpu" protobuf:"bytes,1,name=cpu"`

	// RAM 是 Instance 的内存资源需求，例如 "40G"、"16G"。
	//
	// +required
	RAM resource.Quantity `json:"ram" protobuf:"bytes,2,name=ram"`

	// LocalStorage 是 Instance 的本地存储资源需求，例如 "100G"、"500G"。
	//
	// +required
	LocalStorage resource.Quantity `json:"localStorage" protobuf:"bytes,3,name=localStorage"`

	// Accelerator 是 Instance 的加速器资源需求，例如 "1"、"2"。
	Accelerator *resource.Quantity `json:"accelerator,omitempty" protobuf:"bytes,4,opt,name=accelerator"`

	// AcceleratorSlicedMemoryPercentage 是在切分型（sliced）InstanceType 上为每个
	// 加速器请求的显存预算，取值范围为 [0,100] 的百分比。0 表示禁用切分（该请求
	// 将成为独占整张加速器的请求）。Pod Webhook 会将其折算为标准化的 .sliced.units；
	// 非切分请求会忽略该字段。
	//
	// +optional
	AcceleratorSlicedMemoryPercentage int32 `json:"acceleratorSlicedMemoryPercentage,omitempty" protobuf:"varint,5,opt,name=acceleratorSlicedMemoryPercentage"` // nolint: lll

	// AcceleratorSlicedCoresPercentage 是在切分型（sliced）InstanceType 上为每个
	// 加速器请求的计算单元（SM）预算，取值范围为 [0,100] 的百分比。它与
	// AcceleratorSlicedMemoryPercentage 相互独立；当仅设置其中之一时，Webhook 会
	// 将其值复制到另一个字段。非切分请求会忽略该字段。
	//
	// +optional
	AcceleratorSlicedCoresPercentage int32 `json:"acceleratorSlicedCoresPercentage,omitempty" protobuf:"varint,6,opt,name=acceleratorSlicedCoresPercentage"` // nolint: lll

	// AcceleratorPartitionedProfile 是在提供硬件分区的 InstanceType 上请求的硬件分区
	// 规格（profile），例如 "3g.40gb"。非空值表示请求一个该规格的硬件分区，它与上述
	// 两个切分百分比字段互斥：硬件分区与软件切分不能同时应用于同一个加速器。
	// 不提供分区的 InstanceType 会忽略该字段。
	//
	// +optional
	AcceleratorPartitionedProfile string `json:"acceleratorPartitionedProfile,omitempty" protobuf:"bytes,7,opt,name=acceleratorPartitionedProfile"` // nolint: lll
}

// InstanceVolume 定义在 Instance 中挂载的卷，
// 可以是临时卷（ephemeral）或持久卷（persistent）。
type InstanceVolume struct {
	// Ephemeral 是要在 Instance 中挂载的临时卷。
	Ephemeral *InstanceEphemeralVolume `json:"ephemeral,omitempty" protobuf:"bytes,1,opt,name=ephemeral"`

	// Persistent 是要在 Instance 中挂载的 InstancePersistentVolume 引用。
	Persistent *core.LocalObjectReference `json:"persistent,omitempty" protobuf:"bytes,2,opt,name=persistent"`
}

// InstanceEphemeralVolume 定义在 Instance 中挂载的临时卷。
type InstanceEphemeralVolume struct {
	// Capacity 是该卷的大小上限。
	//
	// +required
	Capacity resource.Quantity `json:"capacity" protobuf:"bytes,1,name=capacity"`
}

// InstanceStatus 定义 Instance 的观测状态。
type InstanceStatus struct {
	// Phase 是 Instance 当前所处的阶段。
	Phase string `json:"phase,omitempty" protobuf:"bytes,1,name=phase"`

	// PhaseMessage 是描述 Instance 当前阶段的消息。
	PhaseMessage string `json:"phaseMessage,omitempty" protobuf:"bytes,2,opt,name=phaseMessage"`

	// NodeName 是运行该 Instance 的 Kubernetes Node 名称。
	NodeName string `json:"nodeName,omitempty" protobuf:"bytes,3,opt,name=nodeName"`

	// AccessAddresses 保存分配给该 Instance 的可访问地址。
	AccessAddresses []string `json:"accessAddresses,omitempty" protobuf:"bytes,4,rep,name=accessAddresses"`

	// HostIPs 保存分配给宿主机的 IP 地址。
	HostIPs []core.HostIP `json:"hostIPs,omitempty" protobuf:"bytes,5,rep,name=hostIPs"`

	// PodIPs 保存分配给该 Instance 关联的 Kubernetes Pod 的 IP 地址。
	PodIPs []core.PodIP `json:"podIPs,omitempty" protobuf:"bytes,6,rep,name=podIPs"`

	// Ports 是从该 Instance 暴露的端口列表。
	Ports []InstanceServicePort `json:"ports,omitempty" protobuf:"bytes,7,rep,name=ports"`

	// Allocations 是分配给该 Instance 的设备列表。
	Allocations []DevicesAllocationGroup `json:"allocations,omitempty" protobuf:"bytes,8,rep,name=allocations"`
}

// InstanceServicePort 定义从容器暴露的端口以及在节点上暴露的端口。
type InstanceServicePort struct {
	InstancePort `json:",inline" protobuf:"bytes,1,name=instancePort"`

	// NodePort 是在 Kubernetes Node 上暴露的端口号。
	NodePort int32 `json:"nodePort,omitempty" protobuf:"varint,2,opt,name=nodePort"`
}

// InstanceList 保存 Instance 的列表。
//
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type InstanceList struct {
	meta.TypeMeta `json:",inline"`
	meta.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Items []Instance `json:"items" protobuf:"bytes,2,rep,name=items"`
}

var _ runtime.Object = (*InstanceList)(nil)
