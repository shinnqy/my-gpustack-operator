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

// KVCachePoolHandler 将 v1alpha1.KVCachePool 代理为公开的 KVCachePool 资源。
//
// 此处不暴露 status 子资源代理，因为 status 承载了由资源池控制器写入的已授配额、
// 使用量以及资源池 finalizer 校验所依赖的域注册表，而代理以 worker 身份执行写入。
// 底层存储资源的 status 子资源会在主资源写入时保持原状，因此 status 通过主资源读取。
type KVCachePoolHandler struct {
	extensionapi.ObjectInfo
	extensionapi.CurdOperations
}

func (h *KVCachePoolHandler) SetupHandler(
	_ context.Context, opts extensionapi.SetupOptions,
) (schema.GroupVersionResource, map[string]rest.Storage, error) {
	gvr := worker.SchemeGroupVersionResource("kvcachepools")
	table, err := extensionapi.NewJSONPathTemplateTableConvertor(
		extensionapi.JSONPathTemplateColumn("Quota", "{.spec.quota.total}"),
		extensionapi.JSONPathTemplateColumn("Usage", "{.status.usage.total}"),
		extensionapi.JSONPathTemplateColumn("Phase", "{.status.phase}"),
		extensionapi.JSONPathTemplateColumn("Endpoint", "{.status.clientEndpoint}"),
	)
	if err != nil {
		return gvr, nil, err
	}
	h.ObjectInfo = &worker.KVCachePool{}
	h.CurdOperations = extensionapi.WithCurdProxy[
		*worker.KVCachePool, *worker.KVCachePoolList,
		*workercore.KVCachePool, *workercore.KVCachePoolList,
	](table, h, opts.Manager.GetClient().(ctrlcli.WithWatch), opts.Manager.GetAPIReader())
	return gvr, map[string]rest.Storage{}, nil
}

func (h *KVCachePoolHandler) New() runtime.Object     { return &worker.KVCachePool{} }
func (h *KVCachePoolHandler) Destroy()                {}
func (h *KVCachePoolHandler) NewList() runtime.Object { return &worker.KVCachePoolList{} }
func (h *KVCachePoolHandler) NewListForProxy() runtime.Object {
	return &workercore.KVCachePoolList{}
}

func (h *KVCachePoolHandler) CastObjectTo(obj *worker.KVCachePool) *workercore.KVCachePool {
	return (*workercore.KVCachePool)(obj)
}

func (h *KVCachePoolHandler) CastObjectFrom(
	_ context.Context, obj *workercore.KVCachePool,
) *worker.KVCachePool {
	return (*worker.KVCachePool)(obj)
}
