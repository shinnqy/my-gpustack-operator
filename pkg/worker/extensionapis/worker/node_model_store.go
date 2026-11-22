package worker

import (
	"context"
	"strconv"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/registry/rest"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"

	worker "gpustack.ai/gpustack/api/worker/v1"
	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/extensionapi"
)

// NodeModelStoreHandler 暴露 v1alpha1.NodeModelStore 的 v1 视图：仅支持 get、list、watch 和 delete，
// 不支持 create 或 update。其写入方（worker 和节点插件）直接写入 v1alpha1 资源，
// 该资源的 status 守卫按各自身份进行校验；若代理可写，则会以 worker 的身份执行写入。
//
// 提供 DELETE 是为了支持垃圾回收器。垃圾回收器按 API 组的首选版本（此处为 v1）监听各资源，
// 且仅删除该版本支持的资源：若没有 delete，NodeModelStore 不会随其 Node 一起被回收（在 Kubernetes 1.36 上实测）。
// 只要节点仍在运行该插件，被删除的对象会由 worker 重新创建。
type NodeModelStoreHandler struct {
	extensionapi.ObjectInfo
	extensionapi.ListWatchOperation
	extensionapi.GetOperation
	extensionapi.DeleteOperation
}

func (h *NodeModelStoreHandler) SetupHandler(
	_ context.Context, opts extensionapi.SetupOptions,
) (schema.GroupVersionResource, map[string]rest.Storage, error) {
	gvr := worker.SchemeGroupVersionResource("nodemodelstores")
	table, err := extensionapi.NewJSONPathTemplateTableConvertor(
		extensionapi.JSONPathTemplateColumn("Ready", "{.status.conditions[?(@.type=='Ready')].status}"),
		extensionapi.JSONPathTemplateColumn("Used", "{.status.capacity.usedPercent}"),
		extensionapi.RenderColumn("Models", func(obj *worker.NodeModelStore) string {
			return strconv.Itoa(len(obj.Status.Models))
		}),
		extensionapi.RenderColumn("Downloading", func(obj *worker.NodeModelStore) string {
			n := 0
			for _, m := range obj.Status.Models {
				if m.State == workercore.NodeModelStoreModelStateDownloading {
					n++
				}
			}
			return strconv.Itoa(n)
		}),
	)
	if err != nil {
		return gvr, nil, err
	}
	h.ObjectInfo = &worker.NodeModelStore{}
	// 该代理实现所有动词；仅对外暴露读取和删除操作。
	ops := extensionapi.WithCurdProxy[
		*worker.NodeModelStore, *worker.NodeModelStoreList,
		*workercore.NodeModelStore, *workercore.NodeModelStoreList,
	](table, h, opts.Manager.GetClient().(ctrlcli.WithWatch), opts.Manager.GetAPIReader())
	h.ListWatchOperation, h.GetOperation = ops.ListWatchOperation, ops.GetOperation
	h.DeleteOperation = ops.DeleteOperation

	return gvr, nil, nil
}

func (h *NodeModelStoreHandler) New() runtime.Object     { return &worker.NodeModelStore{} }
func (h *NodeModelStoreHandler) Destroy()                {}
func (h *NodeModelStoreHandler) NewList() runtime.Object { return &worker.NodeModelStoreList{} }
func (h *NodeModelStoreHandler) NewListForProxy() runtime.Object {
	return &workercore.NodeModelStoreList{}
}

func (h *NodeModelStoreHandler) CastObjectTo(obj *worker.NodeModelStore) *workercore.NodeModelStore {
	return (*workercore.NodeModelStore)(obj)
}

func (h *NodeModelStoreHandler) CastObjectFrom(
	_ context.Context, obj *workercore.NodeModelStore,
) *worker.NodeModelStore {
	return (*worker.NodeModelStore)(obj)
}
