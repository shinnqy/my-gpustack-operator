package v1alpha1

import (
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	gpustack "gpustack.ai/gpustack/api/v1"
)

const (
	// ModelDeploymentScalerBindingAnnotationKey 指定用于承载扩缩器与其所驱动
	// ModelDeployment 之间绑定关系的注解键。
	//
	// 该负载包含扩缩器名称及其写入时所依据的任期。带有此注解的 ModelDeployment 仅属于
	// 该扩缩器，因此第二个扩缩器会拒绝与其绑定，worker 协调器也会拒绝代表其他任何方写入
	// 副本数。
	ModelDeploymentScalerBindingAnnotationKey = "modeldeployment.gpustack.ai/scaler-binding"

	// ModelDeploymentScalerDecisionAnnotationKey 指定用于承载扩缩器最近一次已提交
	// 决策的注解键。
	//
	// 该负载包含决策标识、写入时所依据的任期以及其提交的副本向量。规划器在重启后会读取
	// 该注解，以便根据目标实际承载的内容重建状态，而不是重新提交其仅在内存中记录的向量。
	ModelDeploymentScalerDecisionAnnotationKey = "modeldeployment.gpustack.ai/scaler-decision"

	// ModelDeploymentScalerFinalizerKey 指定扩缩器在绑定到目标期间于自身保留的
	// finalizer 键。
	//
	// 该 finalizer 使被删除的扩缩器能够在扩缩器对象消失前移除其绑定注解，从而确保目标
	// 保持服务状态并重新变为可绑定状态。
	ModelDeploymentScalerFinalizerKey = "modeldeployment.gpustack.ai/scaler-binding"

	// ModelDeploymentScalerModelIDAnnotationKey 在部署 spec 未明确指定时，于目标
	// ModelDeployment 上声明确切的目录模型标识。
	ModelDeploymentScalerModelIDAnnotationKey = "worker.gpustack.ai/model-id"

	// ModelDeploymentScalerHardwareIDAnnotationKey 在部署 spec 未明确指定时，于目标
	// ModelDeployment 上声明确切的目录硬件标识。
	ModelDeploymentScalerHardwareIDAnnotationKey = "worker.gpustack.ai/hardware-id"

	// ModelDeploymentScalerPrecisionAnnotationKey 在部署 spec 未明确指定时，于目标
	// ModelDeployment 上声明确切的目录精度或量化标识。
	ModelDeploymentScalerPrecisionAnnotationKey = "worker.gpustack.ai/precision"

	// ModelDeploymentScalerPrefillHardwareIDAnnotationKey 覆盖目标 ModelDeployment 上
	// Prefill 角色的目录硬件标识声明。
	ModelDeploymentScalerPrefillHardwareIDAnnotationKey = "worker.gpustack.ai/prefill-hardware-id"

	// ModelDeploymentScalerDecodeHardwareIDAnnotationKey 覆盖目标 ModelDeployment 上
	// Decode 角色的目录硬件标识声明。
	ModelDeploymentScalerDecodeHardwareIDAnnotationKey = "worker.gpustack.ai/decode-hardware-id"
)

// 扩缩器上报的状态条件。
//
// Bound 表示扩缩器是否拥有其目标，Active 表示其是否被允许更改目标，MetricsReady
// 表示其是否具备规划所需的指标，CapacityPriorsReady 表示配置的目录先验是否已解析，
// Blocked 表示是什么阻止了它。
const (
	// ModelDeploymentScalerConditionTypeBound 表示扩缩器是否持有对其目标的绑定。
	ModelDeploymentScalerConditionTypeBound = "Bound"

	// ModelDeploymentScalerConditionTypeActive 表示扩缩器是否根据观测结果执行操作。
	ModelDeploymentScalerConditionTypeActive = "Active"

	// ModelDeploymentScalerConditionTypeMetricsReady 表示扩缩器是否具备其算法所需的信号。
	ModelDeploymentScalerConditionTypeMetricsReady = "MetricsReady"

	// ModelDeploymentScalerConditionTypeCapacityPriorsReady 表示扩缩器是否已解析其算法
	// 所需的容量先验。
	ModelDeploymentScalerConditionTypeCapacityPriorsReady = "CapacityPriorsReady"

	// ModelDeploymentScalerConditionTypeBlocked 表示是否有因素阻止了扩缩器。
	ModelDeploymentScalerConditionTypeBlocked = "Blocked"
)

// 扩缩器状态条件携带的原因。
//
// 这些原因描述的是当前处境而非底层机制，因此读者无需阅读判定该状态的代码即可了解
// 扩缩器的真实情况。
const (
	// ModelDeploymentScalerReasonTargetNotFound 表示目标不存在。
	ModelDeploymentScalerReasonTargetNotFound = "TargetNotFound"

	// ModelDeploymentScalerReasonAlreadyBound 表示另一个扩缩器已拥有该目标。
	ModelDeploymentScalerReasonAlreadyBound = "AlreadyBound"

	// ModelDeploymentScalerReasonTargetReplaced 表示目标已被删除并重新创建，因此
	// 当前绑定指向一个已不存在的对象。
	ModelDeploymentScalerReasonTargetReplaced = "TargetReplaced"

	// ModelDeploymentScalerReasonInvalidBinding 表示无法读取目标的绑定注解，因此
	// 任何扩缩器均不得声明拥有该目标。
	ModelDeploymentScalerReasonInvalidBinding = "InvalidBinding"

	// ModelDeploymentScalerReasonBoundsConflict 表示各角色的边界相互冲突。
	ModelDeploymentScalerReasonBoundsConflict = "BoundsConflict"

	// ModelDeploymentScalerReasonRatioConflict 表示比例配置之间或比例与边界之间存在冲突。
	ModelDeploymentScalerReasonRatioConflict = "RatioConflict"

	// ModelDeploymentScalerReasonPacingBounded 表示步长上限或待处理保护机制将向量
	// 保持在当前值。
	ModelDeploymentScalerReasonPacingBounded = "PacingBounded"

	// ModelDeploymentScalerReasonWaitingForMetrics 表示扩缩器尚未读取到其算法所需的
	// 信号。
	ModelDeploymentScalerReasonWaitingForMetrics = "WaitingForMetrics"

	// ModelDeploymentScalerReasonPredictorWarmingUp 表示预测器尚未收集到足够的数据点
	// 来进行预测。
	ModelDeploymentScalerReasonPredictorWarmingUp = "PredictorWarmingUp"

	// ModelDeploymentScalerReasonSuspended 表示扩缩器已暂停。
	ModelDeploymentScalerReasonSuspended = "Suspended"

	// ModelDeploymentScalerReasonShadowMode 表示扩缩器仅进行观测。
	ModelDeploymentScalerReasonShadowMode = "ShadowMode"

	// ModelDeploymentScalerReasonResourceExhausted 表示集群无法为该角色提供其请求的
	// 副本数。
	ModelDeploymentScalerReasonResourceExhausted = "ResourceExhausted"

	// ModelDeploymentScalerReasonScaleDownAborted 表示扩缩器请求的缩容下线失败，
	// 向量已恢复。
	ModelDeploymentScalerReasonScaleDownAborted = "ScaleDownAborted"

	// ModelDeploymentScalerReasonRebalanceNotApplicable 表示各角色之间无法互相转移
	// 资源。
	ModelDeploymentScalerReasonRebalanceNotApplicable = "RebalanceNotApplicable"

	// ModelDeploymentScalerReasonRebalanceBlocked 表示当前无法执行再均衡步骤。
	ModelDeploymentScalerReasonRebalanceBlocked = "RebalanceBlocked"

	// ModelDeploymentScalerReasonCapacityPriorUnresolved 表示无法从目录中解析所需的
	// 容量先验。
	ModelDeploymentScalerReasonCapacityPriorUnresolved = "CapacityPriorUnresolved"

	// ModelDeploymentScalerReasonInsufficientLoadCoverage 表示匹配到的画像未包含足够的
	// 负载数据点或边界证据以确定容量。
	ModelDeploymentScalerReasonInsufficientLoadCoverage = "InsufficientLoadCoverage"

	// ModelDeploymentScalerReasonIncompatibleMetric 表示匹配到的画像未度量该角色所需的
	// 吞吐量分子、延迟分位数或缓存约定。
	ModelDeploymentScalerReasonIncompatibleMetric = "IncompatibleMetric"
)

const (
	// ModelDeploymentScalerScaleDownOutcomeCommitted 表示缩容正在下线流程中推进。
	ModelDeploymentScalerScaleDownOutcomeCommitted = "Committed"

	// ModelDeploymentScalerScaleDownOutcomeAborted 表示下线失败，向量已恢复。
	ModelDeploymentScalerScaleDownOutcomeAborted = "Aborted"
)

// ModelDeploymentScalerBinding 是绑定注解的负载结构。
//
// 它完整承载了单写者契约：目标通过它指定拥有自己的扩缩器以及该所有者写入时使用的任期，
// 从而使后续扩缩器要么显式接管，要么主动退出。
type ModelDeploymentScalerBinding struct {
	// ScalerName 是拥有该目标的扩缩器名称。
	ScalerName string `json:"scalerName" protobuf:"bytes,1,opt,name=scalerName"`

	// ScalerUID 是拥有该目标的扩缩器 UID。
	//
	// 被删除后以相同名称重新创建的扩缩器会携带不同的 UID，因此目标能够区分这两者。
	ScalerUID string `json:"scalerUID" protobuf:"bytes,2,opt,name=scalerUID"`

	// Term 是所有者写入时所依据的绑定任期。
	Term int64 `json:"term" protobuf:"varint,3,opt,name=term"`
}

// ModelDeploymentScalerDecision 是决策注解的负载结构。
//
// 它记录了扩缩器最近一次提交的内容，从而使丢失内存状态的规划器能够从目标重建状态，
// 避免重复提交同一个向量。
type ModelDeploymentScalerDecision struct {
	// DecisionID 标识产生该向量的决策。
	DecisionID string `json:"decisionID" protobuf:"bytes,1,opt,name=decisionID"`

	// Term 是写入该决策时所依据的绑定任期。
	Term int64 `json:"term" protobuf:"varint,2,opt,name=term"`

	// Vector 是该决策提交的各角色副本数。
	Vector map[string]int32 `json:"vector" protobuf:"bytes,3,rep,name=vector"`
}

// ModelDeploymentScaler 是针对单个 ModelDeployment 的指标驱动副本规划器模式定义。
//
// 在 Shadow 模式下，它仅观测并将建议记录在 status 中，不修改目标。在 Active 模式下，
// 其协调器会在单写者准入栅栏下将规划好的副本向量和决策注解提交到目标上，随后
// ModelDeployment 协调器会将服务副本收敛到该数量。
//
// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:crd-gen:resource:scope="Namespaced",categories=["gpustack"],shortName=["mds"],subResources=["status"]
// +k8s:crd-gen:printcolumn:name="Target",type="string",jsonPath=".spec.targetRef.name"
// +k8s:crd-gen:printcolumn:name="Mode",type="string",jsonPath=".spec.mode"
// +k8s:crd-gen:printcolumn:name="Algorithm",type="string",jsonPath=".spec.algorithm.type"
// +k8s:crd-gen:printcolumn:name="Bound",type="string",jsonPath=".status.conditions[?(@.type=='Bound')].status"
// +k8s:crd-gen:printcolumn:name="Active",type="string",jsonPath=".status.conditions[?(@.type=='Active')].status"
// +k8s:crd-gen:printcolumn:name="Blocked",type="string",jsonPath=".status.conditions[?(@.type=='Blocked')].status"
type ModelDeploymentScaler struct {
	meta.TypeMeta   `json:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Spec   ModelDeploymentScalerSpec   `json:"spec" protobuf:"bytes,2,name=spec"`
	Status ModelDeploymentScalerStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

var _ runtime.Object = (*ModelDeploymentScaler)(nil)

// ModelDeploymentScalerList 包含 ModelDeploymentScaler 列表。
//
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type ModelDeploymentScalerList struct {
	meta.TypeMeta `json:",inline"`
	meta.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Items []ModelDeploymentScaler `json:"items" protobuf:"bytes,2,rep,name=items"`
}

var _ runtime.Object = (*ModelDeploymentScalerList)(nil)

// ModelDeploymentScalerSpec 定义 ModelDeploymentScaler 的期望状态。
type ModelDeploymentScalerSpec struct {
	// TargetRef 指定此扩缩器为其进行规划的 ModelDeployment。
	//
	// 目标位于当前 Namespace 中。没有目标的扩缩器无法进行规划，因此该字段为必填项，且
	// 名称不匹配任何对象时会使扩缩器保持未绑定状态。
	//
	// +required
	TargetRef ModelDeploymentScalerTargetReference `json:"targetRef" protobuf:"bytes,1,name=targetRef"`

	// Mode 决定扩缩器是仅进行观测还是同时执行操作。
	//
	// Shadow 模式仅观测并发布其本应执行的操作，不做任何更改。Active 模式则会发布决策，
	// 并由 worker 协调器应用已提交的向量。Shadow 为默认值，因此刚创建的扩缩器在所有者
	// 明确要求之前绝不会改动目标。
	//
	// +optional
	// +k8s:validation:enum=["Shadow","Active"]
	// +k8s:validation:default="Shadow"
	Mode ModelDeploymentScalerMode `json:"mode,omitempty" protobuf:"bytes,2,opt,name=mode"`

	// Suspend 在不删除扩缩器的情况下停止其运行。
	//
	// 处于暂停状态的扩缩器会释放其持有的绑定并停止规划，但不会被删除，并在其 Bound 和
	// Active 条件上持续上报 Suspended，直到恢复运行。
	//
	// +optional
	Suspend bool `json:"suspend,omitempty" protobuf:"varint,3,opt,name=suspend"`

	// Sampling 调整扩缩器检查目标的频率，以及等待已提交扩容获得准入的时长。
	//
	// +optional
	Sampling ModelDeploymentScalerSamplingSpec `json:"sampling,omitempty" protobuf:"bytes,4,opt,name=sampling"`

	// Roles 是此扩缩器为其规划的目标角色列表，包含每个角色所应保持的边界范围。
	//
	// 扩缩器规划的每一个目标角色都需要配置边界，而目标中不存在的角色会被拒绝，从而确保
	// 规划器绝不会针对不存在的角色进行推理。
	//
	// +required
	// +k8s:validation:minItems=1
	// +listType=map
	// +listMapKey=name
	Roles []ModelDeploymentScalerRoleBounds `json:"roles" protobuf:"bytes,5,rep,name=roles"`

	// Ratios 是已提交向量必须维持的角色间比例关系。
	//
	// 比例将分子角色的副本数限制在分母角色副本数的一定范围内，防止耦合的 Prefill 与 Decode
	// 角色对彼此偏离。比例引用缩放器所规划的角色，引用未知角色的比例将被拒绝。
	//
	// +optional
	// +listType=map
	// +listMapKey=numeratorRole
	// +listMapKey=denominatorRole
	Ratios []ModelDeploymentScalerRoleRatio `json:"ratios,omitempty" protobuf:"bytes,6,rep,name=ratios"`

	// Algorithm 指定缩放器如何将观测结果转换为副本数。
	//
	// 此处指定的算法将被实际执行，其他算法的参数则会被忽略，因此每个部署只需声明一种算法及其
	// 对应的一组参数。
	//
	// +required
	Algorithm ModelDeploymentScalerAlgorithmSpec `json:"algorithm" protobuf:"bytes,7,name=algorithm"`

	// Behavior 调节缩放器调整角色副本数的速率，以及在失败后的等待时长。
	//
	// +optional
	Behavior ModelDeploymentScalerBehaviorSpec `json:"behavior,omitempty" protobuf:"bytes,8,opt,name=behavior"`

	// Rebalance 调节当集群资源耗尽导致某个角色无法扩容、而另一个角色可释放资源时缩放器的处理方式。
	//
	// +optional
	Rebalance ModelDeploymentScalerRebalanceSpec `json:"rebalance,omitempty" protobuf:"bytes,9,opt,name=rebalance"`
}

// ModelDeploymentScalerTargetReference 指定缩放器的目标对象。
type ModelDeploymentScalerTargetReference struct {
	// Name 是当前 Namespace 中目标 ModelDeployment 的名称。
	//
	// +required
	// +k8s:validation:minLength=1
	// +k8s:validation:maxLength=253
	Name string `json:"name" protobuf:"bytes,1,name=name"`
}

// ModelDeploymentScalerMode 决定缩放器是否根据观测结果执行操作。
type ModelDeploymentScalerMode string

const (
	// ModelDeploymentScalerModeShadow 仅观测并发布结果，而不修改目标对象。
	ModelDeploymentScalerModeShadow ModelDeploymentScalerMode = "Shadow"

	// ModelDeploymentScalerModeActive 负责观测、发布结果，并由 worker 协调器应用已提交向量。
	ModelDeploymentScalerModeActive ModelDeploymentScalerMode = "Active"
)

// ModelDeploymentScalerSamplingSpec 调节观测间隔和挂起超时时间。
type ModelDeploymentScalerSamplingSpec struct {
	// IntervalSeconds 是缩放器检查目标对象的频率。
	//
	// 它既是缩放器响应速度的下限，也是写入频率的下限；间隔过短会增加采集负载，间隔过长则会降低
	// 响应速度。
	//
	// +optional
	// +k8s:validation:minimum=5
	// +k8s:validation:maximum=300
	IntervalSeconds int32 `json:"intervalSeconds,omitempty" protobuf:"varint,1,opt,name=intervalSeconds"`

	// PendingTimeoutSeconds 是已提交的扩容操作在未被准入前可维持的最长时间，超时后将被撤回。
	//
	// 撤回操作会恢复至扩容开始前的向量，从而在超时后撤销变更，避免角色一直等待无法就绪的资源。
	//
	// +optional
	// +k8s:validation:minimum=60
	// +k8s:validation:maximum=1200
	PendingTimeoutSeconds int32 `json:"pendingTimeoutSeconds,omitempty" protobuf:"varint,2,opt,name=pendingTimeoutSeconds"`
}

// ModelDeploymentScalerRoleBounds 定义单个角色应保持的副本数边界。
type ModelDeploymentScalerRoleBounds struct {
	// Name 是目标对象上的角色名称。
	//
	// +required
	// +k8s:validation:minLength=1
	// +k8s:validation:maxLength=253
	Name string `json:"name" protobuf:"bytes,1,name=name"`

	// MinReplicas 是该角色允许拥有的最少副本数。
	//
	// +optional
	// +k8s:validation:minimum=1
	// +k8s:validation:maximum=1024
	MinReplicas int32 `json:"minReplicas,omitempty" protobuf:"varint,2,opt,name=minReplicas"`

	// MaxReplicas 是该角色允许拥有的最多副本数。
	//
	// +required
	// +k8s:validation:minimum=1
	// +k8s:validation:maximum=1024
	MaxReplicas int32 `json:"maxReplicas" protobuf:"varint,3,opt,name=maxReplicas"`
}

// ModelDeploymentScalerRoleRatio 定义两个角色之间维持的比例关系。
type ModelDeploymentScalerRoleRatio struct {
	// NumeratorRole 是副本数作为分子的角色。
	//
	// +required
	// +k8s:validation:minLength=1
	// +k8s:validation:maxLength=253
	NumeratorRole string `json:"numeratorRole" protobuf:"bytes,1,name=numeratorRole"`

	// DenominatorRole 是副本数作为分母的角色。
	//
	// +required
	// +k8s:validation:minLength=1
	// +k8s:validation:maxLength=253
	DenominatorRole string `json:"denominatorRole" protobuf:"bytes,2,name=denominatorRole"`

	// Target 是规划投影所瞄准的目标比例。
	//
	// 规划器会在所有其他规则的边界内尽量贴近该比例；在没有其他约束决定结果时，耦合角色对也会稳定
	// 在该比例上。
	//
	// +optional
	Target *resource.Quantity `json:"target,omitempty" protobuf:"bytes,3,opt,name=target"`

	// Min 是分子角色与分母角色之间允许的最小比例。
	//
	// +optional
	Min *resource.Quantity `json:"min,omitempty" protobuf:"bytes,4,opt,name=min"`

	// Max 是分子角色与分母角色之间允许的最大比例。
	//
	// +optional
	Max *resource.Quantity `json:"max,omitempty" protobuf:"bytes,5,opt,name=max"`
}

// ModelDeploymentScalerAlgorithmSpec 指定扩缩容算法并保存其参数。
type ModelDeploymentScalerAlgorithmSpec struct {
	// Type 是实际执行的算法类型。
	//
	// +required
	// +k8s:validation:enum=["Proportional","TokenThroughput","Predictive"]
	Type ModelDeploymentScalerAlgorithmType `json:"type" protobuf:"bytes,1,name=type"`

	// Proportional 保存 Proportional 算法的参数。
	//
	// 当 Type 为 Proportional 时必填，其他情况下将被忽略。
	//
	// +optional
	Proportional *ModelDeploymentScalerProportionalSpec `json:"proportional,omitempty" protobuf:"bytes,2,opt,name=proportional"`

	// TokenThroughput 保存 TokenThroughput 算法的参数。
	//
	// 当 Type 为 TokenThroughput 时必填，其他情况下将被忽略。
	//
	// +optional
	TokenThroughput *ModelDeploymentScalerTokenThroughputSpec `json:"tokenThroughput,omitempty" protobuf:"bytes,3,opt,name=tokenThroughput"`

	// Predictive 保存 Predictive 算法的参数。
	//
	// 当 Type 为 Predictive 时必填，其他情况下将被忽略。
	//
	// +optional
	Predictive *ModelDeploymentScalerPredictiveSpec `json:"predictive,omitempty" protobuf:"bytes,4,opt,name=predictive"`

	// CapacityPrior 允许为 TokenThroughput 和 Predictive 算法从实测容量目录中解析缺失的
	// token 容量。
	//
	// 省略该对象表示所有容量均由人工显式指定，缩放器仅使用这些值。配置该对象则允许从其指定的离线
	// 目录中解析缩放器未声明的容量，且缩放器已显式声明的容量绝不会被目录值覆盖。当所有服务容量均已
	// 声明时，完全不会读取目录。
	//
	// +optional
	CapacityPrior *ModelDeploymentScalerCapacityPriorSpec `json:"capacityPrior,omitempty" protobuf:"bytes,5,opt,name=capacityPrior"` // nolint: lll
}

// ModelDeploymentScalerAlgorithmType 表示缩放器运行的算法类型。
type ModelDeploymentScalerAlgorithmType string

const (
	// ModelDeploymentScalerAlgorithmTypeProportional 根据单个副本当前承载的工作量进行扩缩容。
	ModelDeploymentScalerAlgorithmTypeProportional ModelDeploymentScalerAlgorithmType = "Proportional"

	// ModelDeploymentScalerAlgorithmTypeTokenThroughput 根据角色每秒需要处理的 token 数进行扩缩容。
	ModelDeploymentScalerAlgorithmTypeTokenThroughput ModelDeploymentScalerAlgorithmType = "TokenThroughput"

	// ModelDeploymentScalerAlgorithmTypePredictive 预测负载需求并基于预测结果进行扩缩容。
	ModelDeploymentScalerAlgorithmTypePredictive ModelDeploymentScalerAlgorithmType = "Predictive"
)

// ModelDeploymentScalerProportionalSpec 保存 Proportional 算法的参数。
type ModelDeploymentScalerProportionalSpec struct {
	// Metric 是该算法用于扩缩容的指标信号。
	//
	// +required
	// +k8s:validation:enum=["QueueDepth","RunningRequests","WaitingRequests","PromptTokensPerSecond","GenerationTokensPerSecond","TotalTokensPerSecond"]
	Metric ModelDeploymentScalerProportionalMetric `json:"metric" protobuf:"bytes,1,name=metric"`

	// RoleTargets 指定各角色单个副本预期承载的指标目标值。
	//
	// 缩放器所规划的每个角色都需要一个目标值，因为算法会将观测值除以该目标值来计算所需副本数。
	//
	// +optional
	// +listType=map
	// +listMapKey=name
	RoleTargets []ModelDeploymentScalerRoleTarget `json:"roleTargets,omitempty" protobuf:"bytes,2,rep,name=roleTargets"`

	// Tolerance 是围绕目标值的容差比例，观测值落入该范围内时不触发扩缩容。
	//
	// 观测负载已处于容差范围内的角色会保持当前副本数，从而使稳定的部署不会因指标波动而频繁调整。
	//
	// +optional
	Tolerance *resource.Quantity `json:"tolerance,omitempty" protobuf:"bytes,3,opt,name=tolerance"`
}

// ModelDeploymentScalerProportionalMetric 表示 Proportional 算法用于扩缩容的指标信号。
type ModelDeploymentScalerProportionalMetric string

const (
	// ModelDeploymentScalerProportionalMetricQueueDepth 统计角色正在运行及等待处理的请求数。
	ModelDeploymentScalerProportionalMetricQueueDepth ModelDeploymentScalerProportionalMetric = "QueueDepth"

	// ModelDeploymentScalerProportionalMetricRunningRequests 统计角色正在运行的请求数。
	ModelDeploymentScalerProportionalMetricRunningRequests ModelDeploymentScalerProportionalMetric = "RunningRequests"

	// ModelDeploymentScalerProportionalMetricWaitingRequests 统计角色正在等待处理的请求数。
	ModelDeploymentScalerProportionalMetricWaitingRequests ModelDeploymentScalerProportionalMetric = "WaitingRequests"

	// ModelDeploymentScalerProportionalMetricPromptTokensPerSecond 统计角色每秒处理的 prompt
	// token 数。
	ModelDeploymentScalerProportionalMetricPromptTokensPerSecond ModelDeploymentScalerProportionalMetric = "PromptTokensPerSecond"

	// ModelDeploymentScalerProportionalMetricGenerationTokensPerSecond 统计角色每秒生成的 token 数。
	ModelDeploymentScalerProportionalMetricGenerationTokensPerSecond ModelDeploymentScalerProportionalMetric = "GenerationTokensPerSecond"

	// ModelDeploymentScalerProportionalMetricTotalTokensPerSecond 统计角色每秒处理的总 token 数。
	ModelDeploymentScalerProportionalMetricTotalTokensPerSecond ModelDeploymentScalerProportionalMetric = "TotalTokensPerSecond"
)

// ModelDeploymentScalerRoleTarget 定义单个角色副本预期承载的指标目标值。
type ModelDeploymentScalerRoleTarget struct {
	// Name 是该目标值所属的角色名称。
	//
	// +required
	// +k8s:validation:minLength=1
	// +k8s:validation:maxLength=253
	Name string `json:"name" protobuf:"bytes,1,name=name"`

	// Target 是该角色单个副本预期承载的指标目标值。
	//
	// 它使用 Quantity 类型表示，因此速率写为每秒数量，计数写为普通数值。
	//
	// +required
	Target resource.Quantity `json:"target" protobuf:"bytes,2,name=target"`
}

// ModelDeploymentScalerTokenThroughputSpec 保存 TokenThroughput 算法的参数。
type ModelDeploymentScalerTokenThroughputSpec struct {
	// PrefillTargetTokensPerSecond 是单个 Prefill 副本可持续处理的 prompt token 速率。
	//
	// 算法会将需求除以该容量，因此副本数取决于单个副本预期每秒能处理的 prompt token 数。
	//
	// +optional
	PrefillTargetTokensPerSecond *resource.Quantity `json:"prefillTargetTokensPerSecond,omitempty" protobuf:"bytes,1,opt,name=prefillTargetTokensPerSecond"` // nolint: lll

	// DecodeTargetTokensPerSecond 是单个 Decode 副本可持续处理的生成 token 速率。
	//
	// +optional
	DecodeTargetTokensPerSecond *resource.Quantity `json:"decodeTargetTokensPerSecond,omitempty" protobuf:"bytes,2,opt,name=decodeTargetTokensPerSecond"`

	// ServerTargetTokensPerSecond 是单个 Server 副本可持续处理的总 token 速率（涵盖 prompt token
	// 与生成 token）。
	//
	// +optional
	ServerTargetTokensPerSecond *resource.Quantity `json:"serverTargetTokensPerSecond,omitempty" protobuf:"bytes,3,opt,name=serverTargetTokensPerSecond"`

	// StableWindowSeconds 是计算稳定期推荐副本数所用的时间窗口。
	//
	// 稳定窗口足够长，能够平滑突发流量，使短暂的峰值不会触发副本数变动。
	//
	// +optional
	// +k8s:validation:minimum=30
	// +k8s:validation:maximum=600
	StableWindowSeconds int32 `json:"stableWindowSeconds,omitempty" protobuf:"varint,4,opt,name=stableWindowSeconds"`

	// PanicWindowSeconds 是计算紧急期推荐副本数所用的时间窗口。
	//
	// 紧急窗口较短，能够对稳定窗口会平滑掉的突发流量做出快速响应。
	//
	// +optional
	// +k8s:validation:minimum=10
	// +k8s:validation:maximum=300
	PanicWindowSeconds int32 `json:"panicWindowSeconds,omitempty" protobuf:"varint,5,opt,name=panicWindowSeconds"`

	// PanicThreshold 是触发紧急模式的相对就绪副本数增长率阈值。
	//
	// 进入紧急模式后会保持副本数处于高位并暂停缩容，直到负载回落稳定。
	//
	// +optional
	PanicThreshold *resource.Quantity `json:"panicThreshold,omitempty" protobuf:"bytes,6,opt,name=panicThreshold"`

	// TTFTTargetMilliseconds 是该角色预期保持的首 token 延迟（TTFT）上限。
	//
	// 无论 token 计数指标如何，只要观测到角色的首 token 延迟高于该目标值，即表明需要增加副本。
	//
	// +optional
	TTFTTargetMilliseconds *resource.Quantity `json:"ttftTargetMilliseconds,omitempty" protobuf:"bytes,7,opt,name=ttftTargetMilliseconds"`

	// ITLTargetMilliseconds 是该角色预期保持的 token 间延迟（ITL）上限。
	//
	// +optional
	ITLTargetMilliseconds *resource.Quantity `json:"itlTargetMilliseconds,omitempty" protobuf:"bytes,8,opt,name=itlTargetMilliseconds"`

	// LatencyQuantile 是约束该角色延迟分布的分位数。
	//
	// 它是在线直方图的采样分位数，也是解析容量先验所依据的分位数。未设置时默认为第 95 百分位数。
	//
	// 其取值为区间 (0, 1] 内的数值，支持任意百分位。
	// 该值按 Quantity 解析的精度读取，因此如果写入的小数位数超过该精度，实际保留的是舍入后的值而非原始写入值。
	//
	// +optional
	LatencyQuantile *resource.Quantity `json:"latencyQuantile,omitempty" protobuf:"bytes,9,opt,name=latencyQuantile"`
}

// ModelDeploymentScalerPredictiveSpec 保存 Predictive 算法的参数。
//
// 它同时包含 TokenThroughput 参数，因为在尚未收集到足够数据点时，预测器会回退到
// token 吞吐量算法的结果。
type ModelDeploymentScalerPredictiveSpec struct {
	// TokenThroughput 保存预测器回退时使用的参数。
	//
	// +optional
	TokenThroughput ModelDeploymentScalerTokenThroughputSpec `json:"tokenThroughput,omitempty" protobuf:"bytes,1,opt,name=tokenThroughput"`

	// HorizonSeconds 指定向前预测需求的时间跨度（单位：秒）。
	//
	// +optional
	// +k8s:validation:minimum=30
	// +k8s:validation:maximum=600
	HorizonSeconds int32 `json:"horizonSeconds,omitempty" protobuf:"varint,2,opt,name=horizonSeconds"`

	// PrefillTokenCapacityPerReplica 是单个 Prefill 副本可持续处理的提示词 token 速率。
	//
	// +optional
	PrefillTokenCapacityPerReplica *resource.Quantity `json:"prefillTokenCapacityPerReplica,omitempty" protobuf:"bytes,3,opt,name=prefillTokenCapacityPerReplica"` // nolint: lll

	// DecodeTokenCapacityPerReplica 是单个 Decode 副本可持续处理的生成 token 速率。
	//
	// +optional
	DecodeTokenCapacityPerReplica *resource.Quantity `json:"decodeTokenCapacityPerReplica,omitempty" protobuf:"bytes,4,opt,name=decodeTokenCapacityPerReplica"` // nolint: lll

	// ServerTokenCapacityPerReplica 是单个 Server 副本可持续处理的 token 速率。
	//
	// +optional
	ServerTokenCapacityPerReplica *resource.Quantity `json:"serverTokenCapacityPerReplica,omitempty" protobuf:"bytes,5,opt,name=serverTokenCapacityPerReplica"` // nolint: lll

	// QLevel 表示预测的水平分量跟随每次新观测值的程度。
	//
	// +optional
	QLevel *resource.Quantity `json:"qLevel,omitempty" protobuf:"bytes,6,opt,name=qLevel"`

	// QTrend 表示预测的趋势分量跟随每次新观测值的程度。
	//
	// +optional
	QTrend *resource.Quantity `json:"qTrend,omitempty" protobuf:"bytes,7,opt,name=qTrend"`

	// MeasurementNoise 表示预测对每次新观测值的信任程度。
	//
	// +optional
	MeasurementNoise *resource.Quantity `json:"measurementNoise,omitempty" protobuf:"bytes,8,opt,name=measurementNoise"`

	// MinPoints 是预测器开始预测前所需的观测点数量。
	//
	// 在达到该数量之前，算法会报告处于预热状态，并返回 token 吞吐量的计算结果。
	//
	// +optional
	// +k8s:validation:minimum=1
	// +k8s:validation:maximum=64
	MinPoints int32 `json:"minPoints,omitempty" protobuf:"varint,9,opt,name=minPoints"`
}

// ModelDeploymentScalerCapacityPriorSpec 允许为 TokenThroughput 和 Predictive 从离线目录中
// 解析缺失的 token 容量。
//
// 该字段本身的存在即表示启用解析请求，因为它不包含关闭解析的开关。解析过程仅填充
// 扩缩器未显式指定的容量字段，且仅从离线目录获取，不依赖外部来源。
type ModelDeploymentScalerCapacityPriorSpec struct {
	// CatalogRef 指定当前 Namespace 中一个可选的 ConfigMap 及其 key，作为唯一的目录来源。
	//
	// 省略时使用内置目录。
	//
	// +optional
	CatalogRef *ModelDeploymentScalerCatalogReference `json:"catalogRef,omitempty" protobuf:"bytes,2,opt,name=catalogRef"`

	// Headroom 是应用于从目录推导的初始容量的裕度系数，取值范围为 (0, 1]。
	//
	// 当存在 capacityPrior 时默认为 0.75，且不适用于显式指定的容量字段。
	//
	// +optional
	Headroom *resource.Quantity `json:"headroom,omitempty" protobuf:"bytes,4,opt,name=headroom"`
}

// ModelDeploymentScalerCatalogReference 用于选择位于扩缩器所在 Namespace 的 ConfigMap 中的目录 key。
type ModelDeploymentScalerCatalogReference struct {
	// Name 是当前 Namespace 中 ConfigMap 的名称。
	//
	// +required
	// +k8s:validation:minLength=1
	// +k8s:validation:maxLength=253
	Name string `json:"name" protobuf:"bytes,1,name=name"`

	// Key 是 ConfigMap 中保存目录文档的数据键。
	//
	// 该文档采用 YAML 或 JSON 格式，读取器支持通过 priors.yaml 和 priors.json
	// 两种键名读取目录。
	//
	// 省略时默认为 priors.yaml。
	//
	// +optional
	// +k8s:validation:minLength=1
	// +k8s:validation:maxLength=253
	// +k8s:validation:default="priors.yaml"
	Key string `json:"key,omitempty" protobuf:"bytes,2,opt,name=key"`
}

// ModelDeploymentScalerBehaviorSpec 用于调整角色扩缩容的速度以及失败后扩缩器的等待时间。
type ModelDeploymentScalerBehaviorSpec struct {
	// ScaleUp 用于调整角色扩容的速度。
	//
	// +optional
	ScaleUp ModelDeploymentScalerScaleUpRules `json:"scaleUp,omitempty" protobuf:"bytes,1,opt,name=scaleUp"`

	// ScaleDown 用于调整角色缩容的速度以及副本下线被中止后的处理行为。
	//
	// +optional
	ScaleDown ModelDeploymentScalerScaleDownRules `json:"scaleDown,omitempty" protobuf:"bytes,2,opt,name=scaleDown"`
}

// ModelDeploymentScalerScaleUpRules 用于调整角色扩容的速度。
type ModelDeploymentScalerScaleUpRules struct {
	// StabilizationWindowSeconds 是建议在生效前保持稳定的时长（单位：秒）。
	//
	// +optional
	// +k8s:validation:minimum=0
	// +k8s:validation:maximum=600
	StabilizationWindowSeconds int32 `json:"stabilizationWindowSeconds,omitempty" protobuf:"varint,1,opt,name=stabilizationWindowSeconds"`

	// MaxStepReplicas 是单轮操作中最多可为角色增加的副本数。
	//
	// 设为 0 表示不限制副本数上限，此时仅百分比上限生效。
	//
	// +optional
	// +k8s:validation:minimum=0
	MaxStepReplicas int32 `json:"maxStepReplicas,omitempty" protobuf:"varint,2,opt,name=maxStepReplicas"`

	// MaxStepPercent 是单轮操作中最多可为角色增加的当前副本数百分比。
	//
	// 设为 0 表示不限制百分比上限，此时仅副本数上限生效。
	//
	// +optional
	// +k8s:validation:minimum=0
	// +k8s:validation:maximum=1000
	MaxStepPercent int32 `json:"maxStepPercent,omitempty" protobuf:"varint,3,opt,name=maxStepPercent"`

	// StepPolicy 是以单一单位表示的步长上限。
	//
	// 它取代了上述两个上限字段，使部署只需声明一次上限而非两次。
	//
	// +optional
	StepPolicy *ModelDeploymentScalerStepPolicy `json:"stepPolicy,omitempty" protobuf:"bytes,4,opt,name=stepPolicy"`
}

// ModelDeploymentScalerScaleDownRules 用于调整角色缩容的速度。
type ModelDeploymentScalerScaleDownRules struct {
	// StabilizationWindowSeconds 是记录历史建议的时间窗口（单位：秒）。
	//
	// 该窗口内的最大建议值将作为允许缩容的下限，从而避免刚被要求扩容的角色立即缩容。
	//
	// +optional
	// +k8s:validation:minimum=0
	// +k8s:validation:maximum=1800
	StabilizationWindowSeconds int32 `json:"stabilizationWindowSeconds,omitempty" protobuf:"varint,1,opt,name=stabilizationWindowSeconds"`

	// CooldownAfterAbortSeconds 是在副本下线被中止后角色需要维持的冷却时长（单位：秒）。
	//
	// +optional
	// +k8s:validation:minimum=0
	// +k8s:validation:maximum=3600
	CooldownAfterAbortSeconds int32 `json:"cooldownAfterAbortSeconds,omitempty" protobuf:"varint,2,opt,name=cooldownAfterAbortSeconds"`

	// DecodePostScaleUpGraceSeconds 是 Decode 角色在扩容后需要维持的保护时长（单位：秒）。
	//
	// 刚扩容的 Decode 角色在此时间窗口内不会再次缩容，因为一对副本的频繁震荡
	// 带来的开销大于其节省的资源。
	//
	// +optional
	// +k8s:validation:minimum=0
	// +k8s:validation:maximum=3600
	DecodePostScaleUpGraceSeconds int32 `json:"decodePostScaleUpGraceSeconds,omitempty" protobuf:"varint,3,opt,name=decodePostScaleUpGraceSeconds"`

	// StepPolicy 是以单一单位表示的步长上限。
	//
	// 它取代了每轮每个角色最多移除一个副本的默认行为。
	//
	// +optional
	StepPolicy *ModelDeploymentScalerStepPolicy `json:"stepPolicy,omitempty" protobuf:"bytes,4,opt,name=stepPolicy"`
}

// ModelDeploymentScalerStepPolicyType 是步长上限所使用的计量单位。
type ModelDeploymentScalerStepPolicyType string

const (
	// ModelDeploymentScalerStepPolicyTypePods 按副本数计量。
	ModelDeploymentScalerStepPolicyTypePods ModelDeploymentScalerStepPolicyType = "Pods"

	// ModelDeploymentScalerStepPolicyTypePercent 按当前副本数的百分比计量。
	ModelDeploymentScalerStepPolicyTypePercent ModelDeploymentScalerStepPolicyType = "Percent"
)

// ModelDeploymentScalerStepPolicy 限制单个角色在单轮操作中可调整的幅度。
type ModelDeploymentScalerStepPolicy struct {
	// Type 是 Value 的计量单位。
	//
	// +required
	// +k8s:validation:enum=["Pods","Percent"]
	Type ModelDeploymentScalerStepPolicyType `json:"type" protobuf:"bytes,1,name=type"`

	// Value 是以 Type 所指定单位表示的步长上限。
	//
	// +required
	// +k8s:validation:minimum=1
	Value int32 `json:"value" protobuf:"varint,2,opt,name=value"`
}

// ModelDeploymentScalerRebalanceSpec 用于调整当某个角色无法继续扩容时扩缩器的处理策略。
type ModelDeploymentScalerRebalanceSpec struct {
	// Policy 决定其他角色是否可以向资源耗尽的角色归还资源。
	//
	// +optional
	// +k8s:validation:enum=["Opposing","Priority","Disabled"]
	Policy ModelDeploymentScalerRebalancePolicy `json:"policy,omitempty" protobuf:"bytes,1,opt,name=policy"`

	// Priority 按顺序列出角色，从允许让出资源的角色到不允许让出资源的角色。
	//
	// 该字段仅在 Priority 策略下生效，排在顺序末尾的角色会最先被要求缩容。
	// 未在列表中指定的角色保留其自身扩缩建议所处的位置。
	//
	// +optional
	// +listType=atomic
	Priority []string `json:"priority,omitempty" protobuf:"bytes,2,rep,name=priority"`
}

// ModelDeploymentScalerRebalancePolicy 决定其他角色是否可以归还资源。
type ModelDeploymentScalerRebalancePolicy string

const (
	// ModelDeploymentScalerRebalancePolicyOpposing 允许自身建议为缩容的角色将副本
	// 让渡给资源耗尽的角色。
	ModelDeploymentScalerRebalancePolicyOpposing ModelDeploymentScalerRebalancePolicy = "Opposing"

	// ModelDeploymentScalerRebalancePolicyPriority 允许角色即使在自身建议为保持不变时，
	// 也按照 priority 指定的顺序将副本让渡给资源耗尽的角色。
	ModelDeploymentScalerRebalancePolicyPriority ModelDeploymentScalerRebalancePolicy = "Priority"

	// ModelDeploymentScalerRebalancePolicyDisabled 禁用再均衡。
	ModelDeploymentScalerRebalancePolicyDisabled ModelDeploymentScalerRebalancePolicy = "Disabled"
)

// ModelDeploymentScalerStatus 是 ModelDeploymentScaler 的观测状态。
type ModelDeploymentScalerStatus struct {
	// ObservedGeneration 是计算当前状态所依据的资源代数（generation）。
	//
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty" protobuf:"varint,1,opt,name=observedGeneration"`

	// Binding 是当前扩缩器所绑定的目标对象。
	//
	// +optional
	Binding *ModelDeploymentScalerBindingStatus `json:"binding,omitempty" protobuf:"bytes,2,opt,name=binding"`

	// Roles 记录扩缩器观测到的状态以及为每个角色给出的扩缩建议。
	//
	// +optional
	// +listType=map
	// +listMapKey=name
	Roles []ModelDeploymentScalerRoleStatus `json:"roles,omitempty" protobuf:"bytes,3,rep,name=roles"`

	// LastCommit 是扩缩器最近一次提交的决策。
	//
	// +optional
	LastCommit *ModelDeploymentScalerCommitStatus `json:"lastCommit,omitempty" protobuf:"bytes,4,opt,name=lastCommit"`

	// PendingCohort 是扩缩器已提交并正在等待完成的一组扩容操作。
	//
	// +optional
	PendingCohort *ModelDeploymentScalerPendingCohortStatus `json:"pendingCohort,omitempty" protobuf:"bytes,5,opt,name=pendingCohort"`

	// Rebalance 记录一个角色将副本让渡给另一个角色的事务状态。
	//
	// +optional
	Rebalance *ModelDeploymentScalerRebalanceStatus `json:"rebalance,omitempty" protobuf:"bytes,6,opt,name=rebalance"`

	// LastScaleDown 是扩缩器最近一次提交的缩容操作的结果。
	//
	// +optional
	LastScaleDown *ModelDeploymentScalerScaleDownStatus `json:"lastScaleDown,omitempty" protobuf:"bytes,7,opt,name=lastScaleDown"`

	// Predictors 是各角色预测器的当前状态。
	//
	// +optional
	// +listType=map
	// +listMapKey=name
	Predictors []ModelDeploymentScalerPredictorStatus `json:"predictors,omitempty" protobuf:"bytes,8,rep,name=predictors"`

	// Conditions 保存扩缩器的状态条件集合。
	//
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []gpustack.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,9,rep,name=conditions"`

	// ResolvedCapacityPriors 记录各角色已解析的容量先验基准以及各字段的有效容量。
	//
	// +optional
	// +listType=map
	// +listMapKey=name
	ResolvedCapacityPriors []ModelDeploymentScalerResolvedCapacityPriorStatus `json:"resolvedCapacityPriors,omitempty" protobuf:"bytes,10,rep,name=resolvedCapacityPriors"` // nolint: lll

	// LatencyQuantile 是约束扩缩器延迟分布的分位数。
	//
	// 它是扩缩器所请求的分位数，所有角色的观测延迟均按该分位数采样。
	// 仅当扩缩器未指定该值时才为空，此时表示默认采用第 95 百分位数。
	//
	// 它是一个稳定值，仅在配置变更时改变，而不会随采样器读取新样本而变化。
	//
	// +optional
	// +k8s:validation:maxLength=64
	LatencyQuantile string `json:"latencyQuantile,omitempty" protobuf:"bytes,11,opt,name=latencyQuantile"`
}

// ModelDeploymentScalerBindingStatus 表示扩缩器持有的绑定关系。
type ModelDeploymentScalerBindingStatus struct {
	// TargetUID 是已绑定目标的 UID。
	//
	// 目标被删除并重建后会带有不同的 UID，绑定关系借此区分两者。
	//
	// +optional
	TargetUID string `json:"targetUID,omitempty" protobuf:"bytes,1,opt,name=targetUID"`

	// TargetName 是已绑定目标的名称。
	//
	// +optional
	TargetName string `json:"targetName,omitempty" protobuf:"bytes,2,opt,name=targetName"`

	// Term 是扩缩器写入时所处的绑定任期。
	//
	// +optional
	Term int64 `json:"term" protobuf:"varint,3,opt,name=term"`
}

// ModelDeploymentScalerRoleStatus 表示扩缩器针对单个角色观测到的状态与建议结果。
type ModelDeploymentScalerRoleStatus struct {
	// Name 是该状态所属的角色名称。
	//
	// +required
	// +k8s:validation:minLength=1
	// +k8s:validation:maxLength=253
	Name string `json:"name" protobuf:"bytes,1,name=name"`

	// CurrentReplicas 是目标为该角色上报的当前副本数。
	//
	// +optional
	CurrentReplicas int32 `json:"currentReplicas,omitempty" protobuf:"varint,2,opt,name=currentReplicas"`

	// ReadyReplicas 是该角色中已就绪并可提供服务的副本数。
	//
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty" protobuf:"varint,3,opt,name=readyReplicas"`

	// SignalState 表示扩缩器对该角色观测结果的可信程度。
	//
	// 当上一时间窗口内所有必需信号均已读取时为 Fresh；当已读取角色状态且当前无负载时为 Idle；
	// 当缺少必需信号时为 Missing；当所属时间窗口已过期时为 Stale。
	//
	// +optional
	SignalState string `json:"signalState,omitempty" protobuf:"bytes,4,opt,name=signalState"`

	// PromptTokensPerSecond 是扩缩器观测到的 prompt token 速率。
	//
	// +optional
	PromptTokensPerSecond *resource.Quantity `json:"promptTokensPerSecond,omitempty" protobuf:"bytes,5,opt,name=promptTokensPerSecond"`

	// GenerationTokensPerSecond 是扩缩器观测到的生成 token 速率。
	//
	// +optional
	GenerationTokensPerSecond *resource.Quantity `json:"generationTokensPerSecond,omitempty" protobuf:"bytes,6,opt,name=generationTokensPerSecond"`

	// RunningRequests 是该角色正在处理的请求数。
	//
	// +optional
	RunningRequests int32 `json:"runningRequests,omitempty" protobuf:"varint,7,opt,name=runningRequests"`

	// WaitingRequests 是该角色正在等待处理的请求数。
	//
	// +optional
	WaitingRequests int32 `json:"waitingRequests,omitempty" protobuf:"varint,8,opt,name=waitingRequests"`

	// KVCacheUsage 是该角色当前的 KVCache 使用量。
	//
	// +optional
	KVCacheUsage *resource.Quantity `json:"kvCacheUsage,omitempty" protobuf:"bytes,9,opt,name=kvCacheUsage"`

	// TTFTMilliseconds 是扩缩器观测到的首 token 延迟（TTFT）。
	//
	// +optional
	TTFTMilliseconds *resource.Quantity `json:"ttftMilliseconds,omitempty" protobuf:"bytes,10,opt,name=ttftMilliseconds"`

	// ITLMilliseconds 是扩缩器观测到的 token 间延迟（ITL）。
	//
	// +optional
	ITLMilliseconds *resource.Quantity `json:"itlMilliseconds,omitempty" protobuf:"bytes,11,opt,name=itlMilliseconds"`

	// RawRecommendedReplicas 是算法在应用上下界与比例约束之前计算出的建议副本数。
	//
	// +optional
	RawRecommendedReplicas int32 `json:"rawRecommendedReplicas,omitempty" protobuf:"varint,12,opt,name=rawRecommendedReplicas"`

	// ConstrainedReplicas 是规划器在应用上下界与比例约束之后最终确定的副本数。
	//
	// +optional
	ConstrainedReplicas int32 `json:"constrainedReplicas,omitempty" protobuf:"varint,13,opt,name=constrainedReplicas"`

	// Reason 说明给出当前建议副本数的原因。
	//
	// +optional
	Reason string `json:"reason,omitempty" protobuf:"bytes,14,opt,name=reason"`

	// LastScaleUpTime 是该角色上一次扩容的时间。
	//
	// +optional
	LastScaleUpTime *meta.Time `json:"lastScaleUpTime,omitempty" protobuf:"bytes,15,opt,name=lastScaleUpTime"`

	// StabilizationHistory 记录该角色在最近一个时间窗口内收到的建议副本数。
	//
	// 该历史记录容量有上限，用于在重启后保留缩容下限状态。
	//
	// +optional
	// +listType=atomic
	// +k8s:validation:maxItems=256
	StabilizationHistory []ModelDeploymentScalerStabilizationSample `json:"stabilizationHistory,omitempty" protobuf:"bytes,16,rep,name=stabilizationHistory"` // nolint: lll
}

// ModelDeploymentScalerStabilizationSample 表示角色收到的一条副本数建议采样。
type ModelDeploymentScalerStabilizationSample struct {
	// Timestamp 是生成该建议的时间。
	//
	// +required
	Timestamp meta.Time `json:"timestamp" protobuf:"bytes,1,name=timestamp"`

	// Replicas 是该建议请求的副本数。
	//
	// +required
	Replicas int32 `json:"replicas" protobuf:"varint,2,opt,name=replicas"`
}

// ModelDeploymentScalerCommitStatus 表示扩缩器最近一次提交的决策。
type ModelDeploymentScalerCommitStatus struct {
	// DecisionID 用于唯一标识该决策。
	//
	// +optional
	DecisionID string `json:"decisionID,omitempty" protobuf:"bytes,1,opt,name=decisionID"`

	// Term 是写入该决策时所处的绑定任期。
	//
	// +optional
	Term int64 `json:"term" protobuf:"varint,2,opt,name=term"`

	// Vector 是该决策提交的各角色副本数向量。
	//
	// +optional
	Vector map[string]int32 `json:"vector,omitempty" protobuf:"bytes,3,rep,name=vector"`

	// BaselineVector 是该决策开始执行前各角色的基准副本数。
	//
	// 当决策必须撤回时，扩缩器会恢复到该向量。
	//
	// +optional
	BaselineVector map[string]int32 `json:"baselineVector,omitempty" protobuf:"bytes,4,rep,name=baselineVector"`

	// Timestamp 是该决策提交的时间。
	//
	// +optional
	Timestamp *meta.Time `json:"timestamp,omitempty" protobuf:"bytes,5,opt,name=timestamp"`

	// TargetGeneration 是计算该决策所依据的目标资源 generation。
	//
	// +optional
	TargetGeneration int64 `json:"targetGeneration,omitempty" protobuf:"varint,6,opt,name=targetGeneration"`
}

// ModelDeploymentScalerPendingCohortStatus 表示扩缩器正在等待完成的扩容批次。
type ModelDeploymentScalerPendingCohortStatus struct {
	// DecisionID 用于标识添加该扩容批次的决策。
	//
	// +optional
	DecisionID string `json:"decisionID,omitempty" protobuf:"bytes,1,opt,name=decisionID"`

	// Term 是写入该决策时所处的绑定任期。
	//
	// +optional
	Term int64 `json:"term" protobuf:"varint,2,opt,name=term"`

	// BaselineVector 是添加该扩容批次前各角色的基准副本数。
	//
	// 当该批次被撤回时，扩缩器会恢复到该向量。
	//
	// +optional
	BaselineVector map[string]int32 `json:"baselineVector,omitempty" protobuf:"bytes,3,rep,name=baselineVector"`

	// TargetVector 是该扩容批次请求的各角色目标副本数。
	//
	// +optional
	TargetVector map[string]int32 `json:"targetVector,omitempty" protobuf:"bytes,4,rep,name=targetVector"`

	// StartedAt 是该扩容批次提交的时间。
	//
	// +optional
	StartedAt *meta.Time `json:"startedAt,omitempty" protobuf:"bytes,5,opt,name=startedAt"`
}

// ModelDeploymentScalerRebalanceStatus 表示一个角色向另一个角色让渡副本的再均衡事务。
type ModelDeploymentScalerRebalanceStatus struct {
	// ReceiverRole 是接收副本的角色。
	//
	// +optional
	ReceiverRole string `json:"receiverRole,omitempty" protobuf:"bytes,1,opt,name=receiverRole"`

	// DonorRole 是让渡副本的角色。
	//
	// +optional
	DonorRole string `json:"donorRole,omitempty" protobuf:"bytes,2,opt,name=donorRole"`

	// DonorTargetReplicas 是让渡方角色正在调整的目标副本数。
	//
	// +optional
	DonorTargetReplicas int32 `json:"donorTargetReplicas,omitempty" protobuf:"varint,3,opt,name=donorTargetReplicas"`

	// ReceiverTargetReplicas 是接收方角色正在调整的目标副本数。
	//
	// +optional
	ReceiverTargetReplicas int32 `json:"receiverTargetReplicas,omitempty" protobuf:"varint,4,opt,name=receiverTargetReplicas"`

	// StartedAt 是该事务开始的时间。
	//
	// +optional
	StartedAt *meta.Time `json:"startedAt,omitempty" protobuf:"bytes,5,opt,name=startedAt"`

	// ExhaustionEpisodeGeneration 记录该事务所属的资源耗尽周期代数。
	//
	// 它用于防止单个耗尽周期过度抽干让渡方角色的副本。
	//
	// +optional
	ExhaustionEpisodeGeneration int64 `json:"exhaustionEpisodeGeneration,omitempty" protobuf:"varint,6,opt,name=exhaustionEpisodeGeneration"`

	// DonationAttempted 表示是否已向让渡方请求过让渡副本。
	//
	// +optional
	DonationAttempted bool `json:"donationAttempted,omitempty" protobuf:"varint,7,opt,name=donationAttempted"`

	// Reason 说明该事务处于当前状态的原因。
	//
	// +optional
	Reason string `json:"reason,omitempty" protobuf:"bytes,8,opt,name=reason"`
}

// ModelDeploymentScalerScaleDownStatus 表示最近一次缩容的结果。
type ModelDeploymentScalerScaleDownStatus struct {
	// Role 是本次缩容从中移除副本的角色。
	//
	// +optional
	Role string `json:"role,omitempty" protobuf:"bytes,1,opt,name=role"`

	// Ordinal 是本次缩容移除的副本序号。
	//
	// +optional
	Ordinal int32 `json:"ordinal,omitempty" protobuf:"varint,2,opt,name=ordinal"`

	// PreScaleDownVector 是本次缩容前各角色的副本数。
	//
	// 当副本下线中止时，扩缩器会恢复到该向量。
	//
	// +optional
	PreScaleDownVector map[string]int32 `json:"preScaleDownVector,omitempty" protobuf:"bytes,3,rep,name=preScaleDownVector"`

	// Outcome 表示本次缩容的最终结果。
	//
	// 副本下线进行中时为 Committed；下线失败且扩缩器恢复副本数向量后为 Aborted。
	//
	// +optional
	Outcome string `json:"outcome,omitempty" protobuf:"bytes,4,opt,name=outcome"`

	// AbortedAt 是副本下线中止的时间。
	//
	// +optional
	AbortedAt *meta.Time `json:"abortedAt,omitempty" protobuf:"bytes,5,opt,name=abortedAt"`

	// CooldownUntil 是该角色允许再次缩容的冷却截止时间。
	//
	// +optional
	CooldownUntil *meta.Time `json:"cooldownUntil,omitempty" protobuf:"bytes,6,opt,name=cooldownUntil"`
}

// ModelDeploymentScalerPredictorStatus 表示单个角色预测器的状态。
type ModelDeploymentScalerPredictorStatus struct {
	// Name 是该预测器所属的角色名称。
	//
	// +required
	// +k8s:validation:minLength=1
	// +k8s:validation:maxLength=253
	Name string `json:"name" protobuf:"bytes,1,name=name"`

	// Level 是预测器当前估计的需求水平。
	//
	// +optional
	Level *resource.Quantity `json:"level,omitempty" protobuf:"bytes,2,opt,name=level"`

	// Trend 是预测器估计的需求变化速率。
	//
	// +optional
	Trend *resource.Quantity `json:"trend,omitempty" protobuf:"bytes,3,opt,name=trend"`

	// Covariance 表示预测器对其自身估计的置信程度。
	//
	// +optional
	Covariance *ModelDeploymentScalerKalmanCovariance `json:"covariance,omitempty" protobuf:"bytes,4,opt,name=covariance"`

	// SampleCount 是预测器已观测到的样本数量。
	//
	// +optional
	SampleCount int32 `json:"sampleCount,omitempty" protobuf:"varint,5,opt,name=sampleCount"`

	// History 是预测器已观测到的历史需求。
	//
	// 其长度是有界的，因为预测器只需保留近期数据。
	//
	// +optional
	// +listType=atomic
	// +k8s:validation:maxItems=256
	History []ModelDeploymentScalerDemandSample `json:"history,omitempty" protobuf:"bytes,6,rep,name=history"`
}

// ModelDeploymentScalerKalmanCovariance 是预测器的协方差矩阵。
//
// 它是一个按 level、trend 顺序排列的 2x2 矩阵，因此四个元素依次为：
// level 的方差、level 与 trend 的协方差、trend 与 level 的协方差，以及 trend 的方差。
type ModelDeploymentScalerKalmanCovariance struct {
	// P00 是 level 的方差。
	//
	// +optional
	P00 *resource.Quantity `json:"p00,omitempty" protobuf:"bytes,1,opt,name=p00"`

	// P01 是 level 与 trend 的协方差。
	//
	// +optional
	P01 *resource.Quantity `json:"p01,omitempty" protobuf:"bytes,2,opt,name=p01"`

	// P10 是 trend 与 level 的协方差。
	//
	// +optional
	P10 *resource.Quantity `json:"p10,omitempty" protobuf:"bytes,3,opt,name=p10"`

	// P11 是 trend 的方差。
	//
	// +optional
	P11 *resource.Quantity `json:"p11,omitempty" protobuf:"bytes,4,opt,name=p11"`
}

// ModelDeploymentScalerDemandSample 是预测器观测到的一条样本。
type ModelDeploymentScalerDemandSample struct {
	// Timestamp 是观测发生的时间。
	//
	// +required
	Timestamp meta.Time `json:"timestamp" protobuf:"bytes,1,name=timestamp"`

	// TokensPerSecond 是观测到的需求速率。
	//
	// +required
	TokensPerSecond resource.Quantity `json:"tokensPerSecond" protobuf:"bytes,2,name=tokensPerSecond"`
}

// ModelDeploymentScalerCapacityFieldSource 表示生效的容量字段值来源于何处。
type ModelDeploymentScalerCapacityFieldSource string

const (
	// ModelDeploymentScalerCapacityFieldSourceExplicit 表示该容量字段在 scaler spec 中显式设置。
	ModelDeploymentScalerCapacityFieldSourceExplicit ModelDeploymentScalerCapacityFieldSource = "Explicit"

	// ModelDeploymentScalerCapacityFieldSourceCatalog 表示该容量字段由 catalog profile 推导得出。
	ModelDeploymentScalerCapacityFieldSourceCatalog ModelDeploymentScalerCapacityFieldSource = "Catalog"
)

// ModelDeploymentScalerResolvedCapacityPriorStatus 记录单个角色已解析的容量先验依据及各生效字段值。
type ModelDeploymentScalerResolvedCapacityPriorStatus struct {
	// Name 是此状态所属的角色。
	//
	// +required
	// +k8s:validation:minLength=1
	// +k8s:validation:maxLength=253
	Name string `json:"name" protobuf:"bytes,1,name=name"`

	// SourceID 是该 profile 所属的 catalog 来源标识符。
	//
	// +optional
	// +k8s:validation:maxLength=253
	SourceID string `json:"sourceID,omitempty" protobuf:"bytes,3,opt,name=sourceID"`

	// ProfileDigest 是 catalog profile 的内容摘要。
	//
	// +optional
	// +k8s:validation:maxLength=253
	ProfileDigest string `json:"profileDigest,omitempty" protobuf:"bytes,4,opt,name=profileDigest"`

	// CompatibilityFingerprint 涵盖此解析所依赖的 target、role、model、engine、hardware、quantization、
	// topology、workload、catalog digest 以及延迟目标。
	//
	// +optional
	// +k8s:validation:maxLength=253
	CompatibilityFingerprint string `json:"compatibilityFingerprint,omitempty" protobuf:"bytes,5,opt,name=compatibilityFingerprint"` // nolint: lll

	// Metric 是用于解析容量的吞吐量指标分子。
	//
	// +optional
	// +k8s:validation:maxLength=253
	Metric string `json:"metric,omitempty" protobuf:"bytes,6,opt,name=metric"`

	// Quantile 是用于校验该角色延迟约束的延迟分位数统计量。
	//
	// +optional
	// +k8s:validation:maxLength=253
	Quantile string `json:"quantile,omitempty" protobuf:"bytes,7,opt,name=quantile"`

	// WorkloadID 是测量该 profile 时所处工作负载包络的标识符。
	//
	// +optional
	// +k8s:validation:maxLength=253
	WorkloadID string `json:"workloadID,omitempty" protobuf:"bytes,8,opt,name=workloadID"`

	// Denominator 是为单个扩缩容副本重建的测量分母。
	//
	// +optional
	// +k8s:validation:maxLength=253
	Denominator string `json:"denominator,omitempty" protobuf:"bytes,9,opt,name=denominator"`

	// Headroom 是应用于 catalog 种子容量的安全余量。
	//
	// +optional
	Headroom *resource.Quantity `json:"headroom,omitempty" protobuf:"bytes,10,opt,name=headroom"`

	// SeedTokensPerSecond 是应用安全余量之前的单副本 catalog 种子容量。
	//
	// +optional
	SeedTokensPerSecond *resource.Quantity `json:"seedTokensPerSecond,omitempty" protobuf:"bytes,11,opt,name=seedTokensPerSecond"` // nolint: lll

	// Fields 记录此角色每个必需容量字段的生效值及来源。
	//
	// +optional
	// +listType=map
	// +listMapKey=field
	// +k8s:validation:maxItems=8
	Fields []ModelDeploymentScalerResolvedCapacityFieldStatus `json:"fields,omitempty" protobuf:"bytes,12,rep,name=fields"` // nolint: lll

	// CatalogDigest 是从中解析出该 profile 的 catalog 内容摘要。
	//
	// +optional
	// +k8s:validation:maxLength=253
	CatalogDigest string `json:"catalogDigest,omitempty" protobuf:"bytes,13,opt,name=catalogDigest"`

	// SourceRevision 锁定导入该 catalog profile 的上游源修订版本。
	//
	// +optional
	// +k8s:validation:maxLength=253
	SourceRevision string `json:"sourceRevision,omitempty" protobuf:"bytes,14,opt,name=sourceRevision"`

	// WorkloadRevision 锁定测量该 profile 时所依据的工作负载定义版本。
	//
	// +optional
	// +k8s:validation:maxLength=253
	WorkloadRevision string `json:"workloadRevision,omitempty" protobuf:"bytes,15,opt,name=workloadRevision"`

	// GPUsPerReplica 是单个扩缩容副本中的加速器数量，用于重建 PerGPU 测量分母。
	//
	// +optional
	// +k8s:validation:minimum=0
	GPUsPerReplica int32 `json:"gpusPerReplica,omitempty" protobuf:"varint,16,opt,name=gpusPerReplica"`

	// MeasuredReplicas 是被测系统中的扩缩容副本数量，用于重建 WholeSystem 测量分母。
	//
	// +optional
	// +k8s:validation:minimum=0
	MeasuredReplicas int32 `json:"measuredReplicas,omitempty" protobuf:"varint,17,opt,name=measuredReplicas"`

	// LatencyBounds 记录该角色每个必需延迟指标的分位数及保守上界依据。
	//
	// +optional
	// +listType=map
	// +listMapKey=metric
	// +k8s:validation:maxItems=8
	LatencyBounds []ModelDeploymentScalerResolvedLatencyBoundStatus `json:"latencyBounds,omitempty" protobuf:"bytes,18,rep,name=latencyBounds"` // nolint: lll

	// RequestedQuantile 是解析时所遵循的延迟分位数。
	//
	// 它是扩缩容控制器所请求的分位数，而非 catalog 实际测量的分位数；
	// 当 catalog 测量的分位数高于请求所需分位数时，两者会有所不同。
	//
	// +optional
	// +k8s:validation:maxLength=253
	RequestedQuantile string `json:"requestedQuantile,omitempty" protobuf:"bytes,19,opt,name=requestedQuantile"`
}

// ModelDeploymentScalerResolvedLatencyBoundStatus 记录单个必需延迟指标的实测分位数依据。
type ModelDeploymentScalerResolvedLatencyBoundStatus struct {
	// Metric 是必需的延迟指标名称，例如 TTFT 或 ITL。
	//
	// +required
	// +k8s:validation:minLength=1
	// +k8s:validation:maxLength=64
	Metric string `json:"metric" protobuf:"bytes,1,name=metric"`

	// Quantile 是用于界定该指标上界的实测延迟分位数统计量。
	//
	// +required
	// +k8s:validation:minLength=1
	// +k8s:validation:maxLength=64
	Quantile string `json:"quantile" protobuf:"bytes,2,name=quantile"`

	// Conservative 表示是否采用了高于所需分位数的分位数作为保守上界。
	//
	// +optional
	Conservative bool `json:"conservative,omitempty" protobuf:"varint,3,opt,name=conservative"`
}

// ModelDeploymentScalerResolvedCapacityFieldStatus 记录单个算法容量字段的生效值及来源。
type ModelDeploymentScalerResolvedCapacityFieldStatus struct {
	// Field 是容量字段的名称。
	//
	// +required
	// +k8s:validation:minLength=1
	// +k8s:validation:maxLength=253
	Field string `json:"field" protobuf:"bytes,1,name=field"`

	// Source 表示生效值是来自显式 spec 字段还是来自 catalog。
	//
	// +required
	// +k8s:validation:enum=["Explicit","Catalog"]
	Source ModelDeploymentScalerCapacityFieldSource `json:"source" protobuf:"bytes,2,name=source"`

	// EffectiveTokensPerSecond 是此字段使用的单副本有效 token 容量。
	//
	// +required
	EffectiveTokensPerSecond resource.Quantity `json:"effectiveTokensPerSecond" protobuf:"bytes,3,name=effectiveTokensPerSecond"` // nolint: lll
}

// ModelDeploymentScalerCapacityPriorCatalog 是记录各负载水平下实测单副本 token 容量及
// 延迟分位数的离线目录。
//
// +protobuf=false
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type ModelDeploymentScalerCapacityPriorCatalog struct {
	meta.TypeMeta `json:",inline"`

	// Profiles 保存此 catalog 中的实测 profile 列表。
	//
	// +optional
	// +listType=atomic
	Profiles []ModelDeploymentScalerCapacityPriorProfile `json:"profiles" yaml:"profiles"`
}

var _ runtime.Object = (*ModelDeploymentScalerCapacityPriorCatalog)(nil)

// ModelDeploymentScalerCapacityPriorProfile 是容量先验 catalog 中的一条实测推理服务 profile。
//
// +protobuf=false
type ModelDeploymentScalerCapacityPriorProfile struct {
	// Role 是该测量数据所描述的服务角色。
	//
	// +required
	Role string `json:"role" yaml:"role"`

	// Model 是所提供服务的模型标识符。
	//
	// +required
	Model string `json:"model" yaml:"model"`

	// Quantization 是模型量化方案。
	//
	// +required
	Quantization string `json:"quantization" yaml:"quantization"`

	// Engine 是推理引擎系列。
	//
	// +required
	Engine string `json:"engine" yaml:"engine"`

	// Hardware 是加速器硬件系列。
	//
	// +required
	Hardware string `json:"hardware" yaml:"hardware"`

	// Topology 是单个扩缩容副本的并行拓扑形态。
	//
	// +required
	Topology ModelDeploymentScalerCapacityPriorTopology `json:"topology" yaml:"topology"`

	// Points 是为此 profile 测得的负载数据点列表。
	//
	// +required
	// +listType=atomic
	Points []ModelDeploymentScalerCapacityPriorPoint `json:"points" yaml:"points"`
}

// ModelDeploymentScalerCapacityPriorTopology 是容量先验 profile 中单个扩缩容副本的并行拓扑形态。
//
// +protobuf=false
type ModelDeploymentScalerCapacityPriorTopology struct {
	// GPUsPerReplica 是单个扩缩容副本所使用的加速器数量。
	//
	// +optional
	GPUsPerReplica int `json:"gpusPerReplica,omitempty" yaml:"gpusPerReplica,omitempty"`

	// TensorParallel 表示张量并行宽度。
	//
	// +optional
	TensorParallel int `json:"tensorParallel,omitempty" yaml:"tensorParallel,omitempty"`

	// PipelineParallel 表示流水线并行宽度。
	//
	// +optional
	PipelineParallel int `json:"pipelineParallel,omitempty" yaml:"pipelineParallel,omitempty"`

	// DataParallel 表示数据并行宽度。
	//
	// +optional
	DataParallel int `json:"dataParallel,omitempty" yaml:"dataParallel,omitempty"`

	// LocalDataParallel 表示本地数据并行宽度。
	//
	// +optional
	LocalDataParallel int `json:"localDataParallel,omitempty" yaml:"localDataParallel,omitempty"`

	// ContextParallel 表示上下文并行宽度。
	//
	// +optional
	ContextParallel int `json:"contextParallel,omitempty" yaml:"contextParallel,omitempty"`

	// ExpertParallel 表示专家并行宽度。
	//
	// +optional
	ExpertParallel int `json:"expertParallel,omitempty" yaml:"expertParallel,omitempty"`
}

// ModelDeploymentScalerCapacityPriorPoint 表示容量先验配置中的一个实测负载点。
//
// +protobuf=false
type ModelDeploymentScalerCapacityPriorPoint struct {
	// Concurrency 表示同时运行的请求数量。
	//
	// +required
	Concurrency int `json:"concurrency" yaml:"concurrency"`

	// TotalTokensPerSecond 表示单个副本的提示词和生成 token 总速率。
	//
	// +required
	TotalTokensPerSecond float64 `json:"totalTokensPerSecond" yaml:"totalTokensPerSecond"`

	// InputTokensPerSecond 表示单个副本的提示词 token 速率。
	//
	// +required
	InputTokensPerSecond float64 `json:"inputTokensPerSecond" yaml:"inputTokensPerSecond"`

	// OutputTokensPerSecond 表示单个副本的生成 token 速率。
	//
	// +required
	OutputTokensPerSecond float64 `json:"outputTokensPerSecond" yaml:"outputTokensPerSecond"`

	// TTFTMilliseconds 将延迟分位键（如 p25、p50、p70、p90、p95 或 p99）映射到
	// 实测首 token 延迟（单位：毫秒）。
	//
	// +optional
	TTFTMilliseconds map[string]float64 `json:"ttftMilliseconds,omitempty" yaml:"ttftMilliseconds,omitempty"`

	// ITLMilliseconds 将延迟分位键（如 p25、p50、p70、p90、p95 或 p99）映射到
	// 实测 token 间延迟（单位：毫秒）。
	//
	// +optional
	ITLMilliseconds map[string]float64 `json:"itlMilliseconds,omitempty" yaml:"itlMilliseconds,omitempty"`
}
