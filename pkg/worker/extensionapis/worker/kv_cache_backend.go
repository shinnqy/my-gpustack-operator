package worker

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/registry/rest"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"

	worker "gpustack.ai/gpustack/api/worker/v1"
	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/extensionapi"
)

// KVCacheBackendHandler 将 v1alpha1.KVCacheBackend 代理为公开的 KVCacheBackend 资源。
//
// 此处不暴露 status 子资源代理，因为 status 记录了后端控制器的端点发布与成员统计信息，
// 而代理以 worker 身份执行写入。底层存储资源的 status 子资源会在主资源写入时保持原状，
// 因此 status 通过主资源读取。
type KVCacheBackendHandler struct {
	extensionapi.ObjectInfo
	extensionapi.CurdOperations
}

func (h *KVCacheBackendHandler) SetupHandler(
	_ context.Context, opts extensionapi.SetupOptions,
) (schema.GroupVersionResource, map[string]rest.Storage, error) {
	gvr := worker.SchemeGroupVersionResource("kvcachebackends")
	table, err := extensionapi.NewJSONPathTemplateTableConvertor(
		extensionapi.JSONPathTemplateColumn("Type", "{.spec.type}"),
		extensionapi.JSONPathTemplateColumn("Phase", "{.status.phase}"),
		extensionapi.JSONPathTemplateColumn("Endpoint", "{.status.endpoints[?(@.name=='Client')].address}"),
		extensionapi.JSONPathTemplateColumn("Capacity", "{.status.capacity.total}"),
	)
	if err != nil {
		return gvr, nil, err
	}
	h.ObjectInfo = &worker.KVCacheBackend{}
	h.CurdOperations = extensionapi.WithCurdProxy[
		*worker.KVCacheBackend, *worker.KVCacheBackendList,
		*workercore.KVCacheBackend, *workercore.KVCacheBackendList,
	](table, h, opts.Manager.GetClient().(ctrlcli.WithWatch), opts.Manager.GetAPIReader())
	return gvr, map[string]rest.Storage{}, nil
}

func (h *KVCacheBackendHandler) New() runtime.Object     { return &worker.KVCacheBackend{} }
func (h *KVCacheBackendHandler) Destroy()                {}
func (h *KVCacheBackendHandler) NewList() runtime.Object { return &worker.KVCacheBackendList{} }
func (h *KVCacheBackendHandler) NewListForProxy() runtime.Object {
	return &workercore.KVCacheBackendList{}
}

func (h *KVCacheBackendHandler) CastObjectTo(obj *worker.KVCacheBackend) *workercore.KVCacheBackend {
	return (*workercore.KVCacheBackend)(obj)
}

func (h *KVCacheBackendHandler) CastObjectFrom(
	_ context.Context, obj *workercore.KVCacheBackend,
) *worker.KVCacheBackend {
	return (*worker.KVCacheBackend)(obj)
}
