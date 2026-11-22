package v1alpha1

import (
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	gpustack "gpustack.ai/gpustack/api/v1"
)

// TopologySource 是用于构建 Kueue 层级结构的集群级拓扑清单。
//
// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:crd-gen:resource:scope="Cluster",categories=["gpustack"],shortName=["toposrc"],subResources=["status"]
// +k8s:crd-gen:printcolumn:name="Kind",type="string",jsonPath=".status.sourceKind"
// +k8s:crd-gen:printcolumn:name="Revision",type="string",jsonPath=".status.lastSuccessfulRevision"
// +k8s:crd-gen:printcolumn:name="Ready",type="string",jsonPath=".status.conditions[?(@.type=='Ready')].status"
type TopologySource struct {
	meta.TypeMeta   `json:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Spec   TopologySourceSpec   `json:"spec" protobuf:"bytes,2,name=spec"`
	Status TopologySourceStatus `json:"status,omitempty" protobuf:"bytes,3,name=status"`
}

var _ runtime.Object = (*TopologySource)(nil)

// TopologySourceSpec 选择 Node 以及一个清单来源。Levels 按从粗到细的顺序排列。
type TopologySourceSpec struct {
	// NodeSelector 选择此来源可描述的 Node。
	// +required
	NodeSelector meta.LabelSelector `json:"nodeSelector" protobuf:"bytes,1,name=nodeSelector"`

	// Levels 是有序的拓扑层级。kubernetes.io/hostname 为隐式层级，禁止在此处指定。
	// +required
	// +k8s:validation:minItems=1
	// +k8s:validation:maxItems=16
	// +listType=atomic
	Levels []string `json:"levels" protobuf:"bytes,2,rep,name=levels"`

	// AdditionalWritePrefix 允许 ConfigMap 和 Webhook 快照在管理员拥有的 DNS 前缀下写入标签。\n// 该值包含末尾的斜杠。
	// +optional
	AdditionalWritePrefix string `json:"additionalWritePrefix,omitempty" protobuf:"bytes,6,opt,name=additionalWritePrefix"`

	// NodeLabels 直接使用所选 Node 现有的标签，而不对其进行修改。
	// +optional
	NodeLabels *TopologySourceNodeLabels `json:"nodeLabels,omitempty" protobuf:"bytes,3,name=nodeLabels"`

	// ConfigMap 从 worker Namespace 读取带版本的清单快照。
	// +optional
	ConfigMap *TopologySourceConfigMap `json:"configMap,omitempty" protobuf:"bytes,4,name=configMap"`

	// Webhook 通过经过身份验证的 HTTPS 读取带版本的清单快照。
	// +optional
	Webhook *TopologySourceWebhook `json:"webhook,omitempty" protobuf:"bytes,5,name=webhook"`
}

// TopologySourceNodeLabels 是只读来源分支。指定该字段即表示选用 Node 上已有的标签。
type TopologySourceNodeLabels struct{}

// TopologySourceObjectReference 标识一个 Namespace 作用域的凭据或配置对象。
type TopologySourceObjectReference struct {
	// +required
	Namespace string `json:"namespace" protobuf:"bytes,1,name=namespace"`
	// +required
	Name string `json:"name" protobuf:"bytes,2,name=name"`
}

// TopologySourceConfigMap 从单个 ConfigMap 读取快照键。
type TopologySourceConfigMap struct {
	// +required
	ConfigMapRef TopologySourceObjectReference `json:"configMapRef" protobuf:"bytes,1,name=configMapRef"`
	// +required
	Key string `json:"key" protobuf:"bytes,2,name=key"`
	// MaxStaleness 指定在读取失败后，最近一次有效快照仍可使用的时长。
	// +required
	MaxStaleness meta.Duration `json:"maxStaleness" protobuf:"bytes,3,name=maxStaleness"`
}

// TopologySourceWebhook 从 HTTPS 端点读取快照。必须且只能指定一个凭据分支。
type TopologySourceWebhook struct {
	// +required
	URL string `json:"url" protobuf:"bytes,1,name=url"`
	// +required
	PollInterval meta.Duration `json:"pollInterval" protobuf:"bytes,2,name=pollInterval"`
	// +required
	Timeout meta.Duration `json:"timeout" protobuf:"bytes,3,name=timeout"`
	// +required
	MaxStaleness meta.Duration `json:"maxStaleness" protobuf:"bytes,4,name=maxStaleness"`
	// CABundleConfigMapRef 可选地从 worker Namespace 提供端点的 CA 证书包。\n// 该 ConfigMap 必须在 ca.crt 键下存储 PEM 证书包。
	// +optional
	CABundleConfigMapRef *TopologySourceObjectReference `json:"caBundleConfigMapRef,omitempty" protobuf:"bytes,5,name=caBundleConfigMapRef"`
	// BearerTokenSecretRef 选择 Bearer Token 认证方式。Secret 中的键为 token。
	// +optional
	BearerTokenSecretRef *TopologySourceObjectReference `json:"bearerTokenSecretRef,omitempty" protobuf:"bytes,6,name=bearerTokenSecretRef"`
	// TLSClientCertificateSecretRef 选择 mTLS 认证方式。Secret 中的键为 tls.crt 和 tls.key。
	// +optional
	//nolint:lll
	TLSClientCertificateSecretRef *TopologySourceObjectReference `json:"tlsClientCertificateSecretRef,omitempty" protobuf:"bytes,7,name=tlsClientCertificateSecretRef"`
}

// TopologySourceStatus 是最近一次观测到的拓扑清单状态。
type TopologySourceStatus struct {
	ObservedGeneration        int64      `json:"observedGeneration,omitempty" protobuf:"varint,1,name=observedGeneration"`
	SourceKind                string     `json:"sourceKind,omitempty" protobuf:"bytes,2,name=sourceKind"`
	LastSuccessfulRevision    string     `json:"lastSuccessfulRevision,omitempty" protobuf:"bytes,3,name=lastSuccessfulRevision"`
	LastSuccessfulRefreshTime *meta.Time `json:"lastSuccessfulRefreshTime,omitempty" protobuf:"bytes,4,name=lastSuccessfulRefreshTime"`
	SelectedNodes             int32      `json:"selectedNodes,omitempty" protobuf:"varint,5,name=selectedNodes"`
	MutatedNodes              int32      `json:"mutatedNodes,omitempty" protobuf:"varint,6,name=mutatedNodes"`
	ConflictedNodes           int32      `json:"conflictedNodes,omitempty" protobuf:"varint,7,name=conflictedNodes"`
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []gpustack.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,8,rep,name=conditions"`
}

// TopologySourceList 包含 TopologySource 对象列表。
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type TopologySourceList struct {
	meta.TypeMeta `json:",inline"`
	meta.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	Items         []TopologySource `json:"items" protobuf:"bytes,2,rep,name=items"`
}

var _ runtime.Object = (*TopologySourceList)(nil)

// TopologySourceConfiguration 是从 ConfigMap 或 Webhook 读取的带版本拓扑快照。
//
// +protobuf=false
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type TopologySourceConfiguration struct {
	meta.TypeMeta `json:",inline"`

	// Revision 标识在 TopologySourceStatus 中上报的快照版本。
	// +required
	Revision string `json:"revision"`

	// Nodes 将每个 Node 名称映射到其拓扑标签键值对。
	// +optional
	Nodes map[string]map[string]string `json:"nodes,omitempty"`
}

var _ runtime.Object = (*TopologySourceConfiguration)(nil)
