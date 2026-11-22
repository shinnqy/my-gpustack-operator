package worker

import (
	"context"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	ctrlreconcile "sigs.k8s.io/controller-runtime/pkg/reconcile"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/kubemeta"
	"gpustack.ai/gpustack/pkg/modelstore"
)

// modelArtifactNodesWindow 是单个 artifact 的 status.nodes 两次写入之间的最短间隔。集群中一批节点
// 完成同一次下载时每个节点都会使计数变化一次，且同一摘要可能被多个 namespace 中的 artifact 引用；
// 无论集群规模如何，该时间窗口都能限制每个 artifact 的写入频率。
const modelArtifactNodesWindow = 30 * time.Second

// modelArtifactNodes 返回 artifact 内容在各节点上的分布情况；对于无可统计分布的情况（如 claim
// 或尚未解析的 hub 来源），则返回 nil。
func modelArtifactNodes(ma *workercore.ModelArtifact, stores []workercore.NodeModelStore) *workercore.ModelArtifactNodes {
	r := ma.Status.Resolved
	if ma.Spec.Source.HuggingFace == nil || r == nil || r.ManifestDigest == "" {
		return nil
	}
	a := modelstore.AggregateEntries(modelstore.NodeEntries(stores, r.ManifestDigest))

	return &workercore.ModelArtifactNodes{
		Ready: a.Ready, Downloading: a.Downloading, Failed: a.Failed, DownloadingPercent: a.SteppedPercent(),
	}
}

// reconcileNodes 根据 NodeModelStore 更新 status.nodes；若该 artifact 的节点信息距离上次写入
// 尚未超过时间窗口，则保留已存储的值，并在返回结果中请求于窗口结束时再触发一次写入。
func (r *ModelArtifactReconciler) reconcileNodes(
	ctx context.Context, ma *workercore.ModelArtifact, stored *workercore.ModelArtifactNodes,
) ctrl.Result {
	// 仅当 artifact 确实存在可统计的节点分布时才读取 store：无论集群中有多少节点缓存，
	// modelArtifactNodes 对 claim 或未解析的 hub 来源都会返回 nil。
	var stores []workercore.NodeModelStore
	if res := ma.Status.Resolved; ma.Spec.Source.HuggingFace != nil && res != nil && res.ManifestDigest != "" {
		list := new(workercore.NodeModelStoreList)
		if err := r.Client.List(ctx, list,
			ctrlcli.MatchingFields{IndexingNodeModelStoreByModelDigest: res.ManifestDigest}); err != nil {
			ctrllog.FromContext(ctx).Error(err, "list node model stores; the artifact's nodes stay as they are")
			ma.Status.Nodes = stored
			return ctrl.Result{RequeueAfter: modelArtifactNodesWindow}
		}
		stores = list.Items
	}
	next := modelArtifactNodes(ma, stores)
	if kubemeta.DeepEqual(next, stored) {
		ma.Status.Nodes = stored
		return ctrl.Result{}
	}
	if last, ok := r.nodesWritten.Load(ma.UID); ok {
		if wait := modelArtifactNodesWindow - r.now().Sub(last.(time.Time)); wait > 0 {
			ma.Status.Nodes = stored
			return ctrl.Result{RequeueAfter: wait}
		}
	}
	ma.Status.Nodes = next

	return ctrl.Result{}
}

// IndexingModelArtifactByManifestDigest 按解析所绑定的摘要为 ModelArtifact 建立索引，使
// NodeModelStore 事件能够精确触发引用其所持摘要的 artifact，而不必在每次事件时列举全部 artifact。
const IndexingModelArtifactByManifestDigest = "modelartifacts.worker.gpustack.ai/manifest-digest"

// indexModelArtifactByManifestDigest 提取 IndexingModelArtifactByManifestDigest 索引键。
func indexModelArtifactByManifestDigest(obj ctrlcli.Object) []string {
	ma, ok := obj.(*workercore.ModelArtifact)
	if !ok {
		return nil
	}
	if res := ma.Status.Resolved; res != nil && res.ManifestDigest != "" {
		return []string{res.ManifestDigest}
	}

	return nil
}

// IndexingNodeModelStoreByModelDigest 按 status 中记录的摘要为 NodeModelStore 建立索引，使
// artifact 调谐时仅读取持有该摘要的 store，而无需扫描全集群。
const IndexingNodeModelStoreByModelDigest = "nodemodelstores.worker.gpustack.ai/model-digest"

// indexNodeModelStoreByModelDigest 提取 IndexingNodeModelStoreByModelDigest 索引键。
func indexNodeModelStoreByModelDigest(obj ctrlcli.Object) []string {
	nms, ok := obj.(*workercore.NodeModelStore)
	if !ok {
		return nil
	}
	digests := make([]string, 0, len(nms.Status.Models))
	for _, m := range nms.Status.Models {
		digests = append(digests, m.Digest)
	}

	return digests
}

// mapModelArtifactNodeModelStore 将 NodeModelStore 映射到所有 namespace 中引用其所持摘要的
// artifact。每次 List 都是基于 artifact 摘要索引的查找；watch 会按与控制器其他 watch 相同的秒级
// 去重窗口对其进行包装。
func (r *ModelArtifactReconciler) mapModelArtifactNodeModelStore(
	ctx context.Context, obj ctrlcli.Object,
) []ctrlreconcile.Request {
	nms, ok := obj.(*workercore.NodeModelStore)
	if !ok {
		return nil
	}
	var requests []ctrlreconcile.Request
	for _, m := range nms.Status.Models {
		mas := new(workercore.ModelArtifactList)
		if err := r.Client.List(ctx, mas, ctrlcli.MatchingFields{IndexingModelArtifactByManifestDigest: m.Digest}); err != nil {
			ctrllog.FromContext(ctx).Error(err, "list model artifacts for a node model store")
			continue
		}
		for i := range mas.Items {
			requests = append(requests, ctrlreconcile.Request{NamespacedName: ctrlcli.ObjectKeyFromObject(&mas.Items[i])})
		}
	}

	return requests
}
