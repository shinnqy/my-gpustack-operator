package mooncake

import (
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/kubemeta"
	"gpustack.ai/gpustack/pkg/systemmeta"
	"gpustack.ai/gpustack/pkg/worker/kuberess"
	"gpustack.ai/gpustack/pkg/worker/kvcache"
)

const (
	// QuotaPolicyFileName 是策略文档存储的键，也是当 ConfigMap 被挂载时该文件对应的文件名。
	//
	// 这是一个常量，而非字面值，因为“TWO”有两种含义：负责生成文档的人，以及负责将其挂载到主系统上的人。
	// 他们是不同的控制器，其中 하나遵循不同的规范。
	QuotaPolicyFileName = "tenant-quota-policy.yaml"

	// QuotaPolicyDir 是 Master 的策略文件可写入的目录，QuotaPolicyFilePath 则是连接器 URI 所指向的路径。
	//
	// 可写性是整个要求。Master 在应用之前会将其对连接器的管理 API 更改持久化到其中，而文件连接器则通过
	// 创建同级临时文件并将其重命名为目标文件来保存数据——因此，不仅文件本身需要可写，整个目录也必须是可
	// 写的。这排除了在此处挂载 ConfigMap 的可能性，取而代之的是将 QuotaPolicySeedDir 放置于此处。
	QuotaPolicyDir      = "/var/lib/mooncake"
	QuotaPolicyFilePath = QuotaPolicyDir + "/" + QuotaPolicyFileName

	// QuotaPolicySeedDir 是 ConfigMap 挂载的目录，由 initContainer 读取一次，而 Master 不会读取。它是一
	// 个第二位置，而非唯一位置，因为 Master 无法从只读挂载点运行，且也不是 Master 的，因为指向此位置的
	// URI 在首次配额写入时会失败，而非在启动时失败。
	QuotaPolicySeedDir      = "/etc/mooncake/tenant-quota-policy"
	QuotaPolicySeedFilePath = QuotaPolicySeedDir + "/" + QuotaPolicyFileName

	// quotaPolicyResourceNoteRole 用于区分该对象与主节点及成员在资源说明中每个渲染对象所携带的内容。
	quotaPolicyResourceNoteRole = "tenant-quota-policy"
)

// QuotaPolicyObjectName 返回保存某个后端租户配额策略的 ConfigMap 名称。
//
// 该名称源自 BACKEND，而非任何资源池，这正是其设计初衷：策略文件属于为该 BACKEND 提供服务的 Master，而多个
// 资源池可能共享该文件。如果采用每个资源池独立的名称，则每个资源池会拥有自己的文档；每次处理时也会只写入描
// 述其自身租户的文件——在服务于所有租户的 Master 上则会抹除其他租户的文件。
func QuotaPolicyObjectName(kvcb *workercore.KVCacheBackend) string {
	return kvcb.Name + "-tenant-quota-policy"
}

// RenderQuotaPolicyConfigMap 将完整策略写入 ConfigMap，供 master 初始化自己的策略文件。
//
// 每轮从期望状态生成整份文档。master 在 Admin API 变更后会重写自己的副本，尝试在此前内容上增量合并容易使两
// 套写入互相覆盖。
//
// 将 ConfigMap 的所有者指向后端。Pool 控制器负责写入它，但 Pool 可能比后端存活得更久，依靠后端的所有者引用回收这份策略。
func RenderQuotaPolicyConfigMap(
	kvcb *workercore.KVCacheBackend, document []byte,
) *core.ConfigMap {
	cm := &core.ConfigMap{
		ObjectMeta: meta.ObjectMeta{
			Name:      QuotaPolicyObjectName(kvcb),
			Namespace: kuberess.SystemNamespaceName,
		},
		Data: map[string]string{
			QuotaPolicyFileName: string(document),
		},
	}

	systemmeta.NoteResource(cm, kvcache.ResourceType, map[string]string{
		kvcache.ResourceNoteBackend: kvcb.Name,
		"role":                      quotaPolicyResourceNoteRole,
	})
	kubemeta.ControlOnWithoutBlock(cm, kvcb,
		workercore.SchemeGroupVersion.WithKind("KVCacheBackend"))

	return cm
}
