package v1alpha1

import (
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	gpustack "gpustack.ai/gpustack/api/v1"
)

// ModelArtifact 是 worker.gpustack.ai 的架构定义。
//
// 它定义了模型权重的来源以及读取权重所用的凭据，且是唯一声明该信息的对象：
// ModelDeployment 或 Instance 仅通过名称引用它，自身不携带任何 URI、版本（revision）
// 或令牌。同一 Namespace 内的所有使用方共享同一份声明。
//
// 它代表一种不可变身份，因此其整个 Spec 在创建后即不可变（由 Webhook 强制校验）。
// 不同的来源或版本对应不同的 ModelArtifact，应新建而非修改。也正因如此，对其的
// 冻结引用才能真正起到锁定作用：一个不可变的引用若指向可变的资源，则什么也锁定不了。
//
// Hugging Face 来源仅解析一次：分支或标签会被解析为 40 位 commit 哈希，该 commit
// 下的文件列表构成规范清单（manifest），清单摘要则成为内容寻址标识。此后不会再跟踪
// 分支更新。访问权限会定期重新校验；权限丢失会阻止新的消费，但绝不会影响运行中的 Pod。
//
// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:crd-gen:resource:scope="Namespaced",categories=["gpustack"],shortName=["mart"],subResources=["status"]
// +k8s:crd-gen:printcolumn:name="Revision",type="string",jsonPath=".status.resolved.revision"
// +k8s:crd-gen:printcolumn:name="Size",type="integer",jsonPath=".status.resolved.sizeBytes"
// +k8s:crd-gen:printcolumn:name="Resolved",type="string",jsonPath=".status.conditions[?(@.type=='Resolved')].status"
// +k8s:crd-gen:printcolumn:name="Ready",type="integer",jsonPath=".status.nodes.ready"
// +k8s:crd-gen:printcolumn:name="Age",type="date",jsonPath=".metadata.creationTimestamp"
type ModelArtifact struct {
	meta.TypeMeta   `json:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Spec   ModelArtifactSpec   `json:"spec" protobuf:"bytes,2,name=spec"`
	Status ModelArtifactStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

var _ runtime.Object = (*ModelArtifact)(nil)

// ModelArtifactSpec 定义 ModelArtifact 的期望状态。
type ModelArtifactSpec struct {
	// Source 指定模型权重的来源。
	//
	// +required
	Source ModelArtifactSource `json:"source" protobuf:"bytes,1,name=source"`

	// AllowPatterns 和 IgnorePatterns 用于从模型仓库（hub）来源中选择构成该 Artifact
	// 的文件，语义与 huggingface_hub 的 allow_patterns 和 ignore_patterns 一致：
	// 采用 Python 的 fnmatch.fnmatchcase 规则，其中 "*" 和 "?" 可跨 "/" 匹配；以 "/"
	// 结尾的模式匹配该目录下的内容；未设置 AllowPatterns 时保留所有文件；IgnorePatterns
	// 的优先级高于 AllowPatterns。
	//
	// 文件筛选在构建清单之前进行，因此摘要、文件数量和大小均对应筛选后的文件，而匹配
	// 模式本身不参与摘要计算：两个选出相同文件集合的 Artifact 会共享同一摘要，未配置
	// 任何模式的 Artifact 则拥有整个 commit 的摘要。仅节点级分发能遵循文件筛选；由引擎
	// 自行下载权重时，引擎会按自身逻辑选择文件。PVC 来源不接受任何筛选模式（由 Webhook
	// 强制校验），且每个模式的长度须为 1 到 256 个字符，不能包含控制字符。
	//
	// +optional
	// +listType=atomic
	// +k8s:validation:maxItems=32
	AllowPatterns []string `json:"allowPatterns,omitempty" protobuf:"bytes,2,rep,name=allowPatterns"`

	// +optional
	// +listType=atomic
	// +k8s:validation:maxItems=32
	IgnorePatterns []string `json:"ignorePatterns,omitempty" protobuf:"bytes,3,rep,name=ignorePatterns"`

	// ExpectedDigest 可选地断言该 Artifact 必须解析到的清单摘要：格式为 "sha256:"
	// 加上 64 位小写十六进制字符串，即 status.resolved.manifestDigest 所携带的内容地址。
	// 准入校验仅在 hub 来源上接受该字段：PVC 的内容取决于挂载时卷内实际存放的数据
	// （动态制备的 PVC 每次制备内容都可能不同），其身份由用户确认的 PVC 本身决定；
	// 镜像的身份则由其引用的摘要决定。若解析出的摘要不匹配，则会以 DigestMismatch 拒绝；
	// 若无法访问 hub，则无需经过 hub 直接解析到该锚点，并在 status.resolved.digestSource
	// 中记录为 "Expected"。该字段随整个 Spec 一同不可变。
	//
	// +optional
	// +k8s:validation:pattern="^sha256:[a-f0-9]{64}$"
	// +k8s:validation:maxLength=71
	ExpectedDigest string `json:"expectedDigest,omitempty" protobuf:"bytes,4,opt,name=expectedDigest"`
}

// ModelArtifactSource 是一个标签联合体：有且仅有一个成员被设置，由 Webhook 强制校验。
type ModelArtifactSource struct {
	// HuggingFace 表示指定版本（revision）的 Hugging Face 模型仓库。
	//
	// +optional
	HuggingFace *ModelArtifactHubSource `json:"huggingFace,omitempty" protobuf:"bytes,1,opt,name=huggingFace"`

	// ModelScope 是第二个支持的模型仓库（hub）。其仓库与版本规则与 Hugging Face 相同，
	// 默认版本为 "master"，文件筛选模式的规则也完全一致。不同之处在于解析方式：
	// 完整 commit 哈希按原样使用；分支或标签通过 commits API 解析并与 git 交叉校验；
	// 当 API 达到静默截断阈值时改为按目录重新列举文件；且引擎运行器需要支持以 commit
	// 作为版本的 ModelScope SDK。
	//
	// +optional
	ModelScope *ModelArtifactHubSource `json:"modelScope,omitempty" protobuf:"bytes,2,opt,name=modelScope"`

	// PersistentVolumeClaim 表示当前 Namespace 下某个 PVC 中的一个目录。Operator
	// 不会读取 PVC 的内容，因此该 Artifact 没有版本（revision）和摘要，目录中存放
	// 的内容及其任何变更均由用户自行负责。
	//
	// +optional
	PersistentVolumeClaim *ModelArtifactPersistentVolumeClaimSource `json:"persistentVolumeClaim,omitempty" protobuf:"bytes,3,opt,name=persistentVolumeClaim"` // nolint: lll

	// Image 是存放模型权重的 OCI 镜像引用。该引用必须固定到摘要：
	// "registry/repository@sha256:<64 hex>"。由于标签（tag）是可变的，同一个
	// Artifact 在不同次拉取时可能会交付不同的权重，因此准入校验会拒绝使用标签。
	//
	// 该摘要锁定的是镜像的字节内容，而非 hub 来源所解析出的清单摘要。Operator 不会
	// 读取镜像仓库，因此也不验证镜像内的具体内容：权重需位于镜像根目录，确保目录结构
	// 符合要求是镜像构建方的契约。分发机制通过 Kubernetes 的镜像卷（image volume）
	// 以只读方式挂载镜像根目录，这要求 apiserver 和 kubelet 版本不低于 1.35 且
	// containerd 版本不低于 2.1；在更低版本的 apiserver 上准入校验会拒绝该来源。
	//
	// +optional
	Image *ModelArtifactImageSource `json:"image,omitempty" protobuf:"bytes,4,opt,name=image"`
}

// ModelArtifactImageSource 表示在根目录存放模型权重的 OCI 镜像引用。
type ModelArtifactImageSource struct {
	// Reference 是固定到摘要的镜像引用：
	// "registry/repository@sha256:<64 lowercase hex>"。该摘要即为此 Artifact 的完整
	// 身份标识；与 PVC 来源一样，镜像来源的 status.resolved 保持为空，不可变 Spec 中的
	// Reference 是该 Artifact 所交付内容的唯一记录。
	//
	// +required
	// +k8s:validation:minLength=1
	// +k8s:validation:maxLength=1024
	Reference string `json:"reference" protobuf:"bytes,1,name=reference"`
}

// ModelArtifactHubSource 表示模型仓库（hub）上处于特定版本的一个仓库。
type ModelArtifactHubSource struct {
	// Repository 是仓库 ID，格式为 "owner/name" 或简短规范名称。
	//
	// +required
	// +k8s:validation:minLength=1
	// +k8s:validation:maxLength=256
	Repository string `json:"repository" protobuf:"bytes,1,name=repository"`

	// Revision 可以是分支、标签或 commit。准入校验将其默认设为 "main"。它会在创建时
	// 一次性解析为完整 commit，并写入 status.resolved.revision。
	//
	// +optional
	// +k8s:validation:maxLength=255
	Revision string `json:"revision,omitempty" protobuf:"bytes,2,opt,name=revision"`

	// SecretRef 引用当前 Namespace 中一个包含 "token" 键的 Secret，作为访问 hub 的令牌。
	// 控制器在解析与重新校验时会读取该 Secret；对于自行下载权重的引擎，则通过引用该
	// Secret 的环境变量传递给引擎，从而使令牌值绝不会进入 Pod Spec、Status 或 Event。
	//
	// 准入时该 Secret 不必已经存在；若缺失，会在 Status 中报告。
	//
	// +optional
	SecretRef *core.LocalObjectReference `json:"secretRef,omitempty" protobuf:"bytes,3,opt,name=secretRef"`
}

// ModelArtifactPersistentVolumeClaimSource 表示 PVC 内的一个目录。
type ModelArtifactPersistentVolumeClaimSource struct {
	// ClaimName 指定当前 Namespace 中的 PersistentVolumeClaim 名称。准入时该 PVC
	// 不必已经存在；若缺失，会在 Status 中报告。
	//
	// +required
	// +k8s:validation:minLength=1
	// +k8s:validation:maxLength=253
	ClaimName string `json:"claimName" protobuf:"bytes,1,name=claimName"`

	// Path 是卷内相对于根目录的目录路径，留空表示根目录。路径不能为绝对路径，且不能
	// 包含 ".." 路径段；后一项限制由准入校验负责，因为该 schema 的正则模式无法表达此约束。
	//
	// +optional
	// +k8s:validation:pattern="^[^/].*$"
	// +k8s:validation:maxLength=1024
	Path string `json:"path,omitempty" protobuf:"bytes,2,opt,name=path"`
}

// ModelArtifactStatus 定义 ModelArtifact 的观测状态。
type ModelArtifactStatus struct {
	// ObservedGeneration 是生成当前状态所对应的资源代数（generation）。
	ObservedGeneration int64 `json:"observedGeneration,omitempty" protobuf:"varint,1,opt,name=observedGeneration"`

	// Resolved 记录源的解析结果。该字段仅在首次解析成功后写入一次，此后不再改变：
	// 后续的重新校验只会更新 LastValidatedTime 和 Conditions。在首次解析成功之前
	// 该字段缺失。
	//
	// +optional
	Resolved *ModelArtifactResolved `json:"resolved,omitempty" protobuf:"bytes,2,opt,name=resolved"`

	// Nodes 记录内容的分布情况：有多少节点已就绪、正在下载或下载失败，以及正在下载
	// 节点的整体进度。它按内容（即清单摘要）统计，因此具有相同摘要的 Artifact 会看到
	// 相同的节点状态；该字段仅包含计数和进度数值。对于 PVC 来源以及首次解析之前，该字段缺失。
	//
	// +optional
	Nodes *ModelArtifactNodes `json:"nodes,omitempty" protobuf:"bytes,4,opt,name=nodes"`

	// Conditions：Resolved 表示源是否已绑定到不可变身份且最近一次访问校验已通过；
	// Degraded 表示解析或重新校验出现故障，包括尚未导致访问失效的故障。
	//
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []gpustack.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,3,rep,name=conditions"` // nolint: lll
}

// ModelArtifactNodes 按状态统计 NodeModelStore 中列有某 Artifact 摘要的节点数量。
// 仅当计数变化或下载百分比跨入下一个步进值时才会写入，且至多每 30 秒更新一次，
// 避免大规模节点同步下载时每次节点上报都触发 Artifact 重写。
type ModelArtifactNodes struct {
	// Ready 统计已就绪并发布该内容的节点数。
	//
	// +required
	// +k8s:validation:minimum=0
	Ready int32 `json:"ready" protobuf:"varint,1,name=ready"`

	// Downloading 统计正在下载该内容的节点数。
	//
	// +required
	// +k8s:validation:minimum=0
	Downloading int32 `json:"downloading" protobuf:"varint,2,name=downloading"`

	// Failed 统计最近一次尝试失败并等待重试的节点数。
	//
	// +required
	// +k8s:validation:minimum=0
	Failed int32 `json:"failed" protobuf:"varint,3,name=failed"`

	// DownloadingPercent 是正在下载节点的平均进度（每个节点对应一份完整副本），
	// 向下取整到 5 的倍数。Ready 节点只计入数量，不参与进度平均。当没有节点在下载时
	// 该字段缺失。
	//
	// +optional
	// +k8s:validation:minimum=0
	// +k8s:validation:maximum=100
	DownloadingPercent *int32 `json:"downloadingPercent,omitempty" protobuf:"varint,4,opt,name=downloadingPercent"`
}

// ModelArtifactResolved 是来源解析后得到的身份信息。
type ModelArtifactResolved struct {
	// Revision 是 hub 来源解析出的完整 40 位 commit 哈希。对于 PVC 来源，该字段缺失。
	//
	// +optional
	Revision string `json:"revision,omitempty" protobuf:"bytes,1,opt,name=revision"`

	// ManifestDigest 是 hub 来源的内容地址：由 "sha256:" 加上 Revision 处所有文件
	// 规范清单的 SHA-256 哈希组成，格式由 pkg/modelartifact 定义。
	//
	// 它绝不能作为授权凭证。它纯粹是内容地址：包含相同文件的公开仓库与私有仓库会拥有
	// 相同的摘要，因此知道该摘要并不能证明拥有访问权限。对于 PVC 来源，该字段缺失。
	//
	// +optional
	ManifestDigest string `json:"manifestDigest,omitempty" protobuf:"bytes,2,opt,name=manifestDigest"`

	// FileCount 和 SizeBytes 分别为清单的文件数量和总字节大小。对于 PVC 来源，这两个字段缺失。
	//
	// +optional
	FileCount int64 `json:"fileCount,omitempty" protobuf:"varint,3,opt,name=fileCount"`

	// +optional
	SizeBytes int64 `json:"sizeBytes,omitempty" protobuf:"varint,4,opt,name=sizeBytes"`

	// ResolvedTime 是解析成功的时间。
	ResolvedTime meta.Time `json:"resolvedTime" protobuf:"bytes,5,name=resolvedTime"`

	// LastValidatedTime 是最近一次确认访问权限的时间。对于 PVC 来源该字段缺失，因为
	// PVC 自身没有访问权限校验。
	//
	// +optional
	LastValidatedTime *meta.Time `json:"lastValidatedTime,omitempty" protobuf:"bytes,6,opt,name=lastValidatedTime"`

	// DigestSource 说明 ManifestDigest 的来源：Hub 表示来自 hub 自身的文件列表；
	// Expected 表示在确认无法访问 hub 后写入的 spec.expectedDigest。采用 Expected
	// 身份时不会携带 Revision、FileCount 或 SizeBytes，且之后不再请求 hub。
	//
	// +optional
	DigestSource ModelArtifactDigestSource `json:"digestSource,omitempty" protobuf:"bytes,7,opt,name=digestSource"`
}

// ModelArtifactDigestSource 表示已解析清单摘要的来源。
type ModelArtifactDigestSource string

const (
	// ModelArtifactDigestSourceHub 表示由 hub 自身文件列表生成的内容摘要。
	ModelArtifactDigestSourceHub ModelArtifactDigestSource = "Hub"

	// ModelArtifactDigestSourceExpected 表示在确认 hub 不可用后，作为身份写入的
	// Spec 中的 expectedDigest。
	ModelArtifactDigestSourceExpected ModelArtifactDigestSource = "Expected"
)

// ModelArtifactList 包含 ModelArtifact 的列表。
//
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type ModelArtifactList struct {
	meta.TypeMeta `json:",inline"`
	meta.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Items []ModelArtifact `json:"items" protobuf:"bytes,2,rep,name=items"`
}

var _ runtime.Object = (*ModelArtifactList)(nil)
