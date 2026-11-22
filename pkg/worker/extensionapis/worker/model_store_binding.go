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

// ModelStoreBindingHandler 将 v1alpha1.ModelStoreBinding 代理为公开的 ModelStoreBinding 资源。
//
// 这里不暴露 status 代理，因为 status 记录了 Namespace 相对于其预算的资源消耗（由预热控制器写入），
// 而代理以 worker 的身份执行写入。底层存储资源的 status 子资源会在主资源写入时保持 status 不变，
// 因此 status 通过主资源读取。
type ModelStoreBindingHandler struct {
	extensionapi.ObjectInfo
	extensionapi.CurdOperations
}

func (h *ModelStoreBindingHandler) SetupHandler(
	_ context.Context, opts extensionapi.SetupOptions,
) (schema.GroupVersionResource, map[string]rest.Storage, error) {
	gvr := worker.SchemeGroupVersionResource("modelstorebindings")
	table, err := extensionapi.NewJSONPathTemplateTableConvertor(
		extensionapi.JSONPathTemplateColumn("Quota", "{.spec.quota.bytes}"),
		extensionapi.JSONPathTemplateColumn("AllowPinned", "{.spec.allowPinned}"),
		extensionapi.JSONPathTemplateColumn("Used", "{.status.usedBytes}"),
		extensionapi.JSONPathTemplateColumn("Phase", "{.status.phase}"),
	)
	if err != nil {
		return gvr, nil, err
	}
	h.ObjectInfo = &worker.ModelStoreBinding{}
	h.CurdOperations = extensionapi.WithCurdProxy[
		*worker.ModelStoreBinding, *worker.ModelStoreBindingList,
		*workercore.ModelStoreBinding, *workercore.ModelStoreBindingList,
	](table, h, opts.Manager.GetClient().(ctrlcli.WithWatch), opts.Manager.GetAPIReader())
	return gvr, map[string]rest.Storage{}, nil
}

func (h *ModelStoreBindingHandler) New() runtime.Object     { return &worker.ModelStoreBinding{} }
func (h *ModelStoreBindingHandler) Destroy()                {}
func (h *ModelStoreBindingHandler) NewList() runtime.Object { return &worker.ModelStoreBindingList{} }

func (h *ModelStoreBindingHandler) NewListForProxy() runtime.Object {
	return &workercore.ModelStoreBindingList{}
}

func (h *ModelStoreBindingHandler) CastObjectTo(
	obj *worker.ModelStoreBinding,
) *workercore.ModelStoreBinding {
	return (*workercore.ModelStoreBinding)(obj)
}

func (h *ModelStoreBindingHandler) CastObjectFrom(
	_ context.Context, obj *workercore.ModelStoreBinding,
) *worker.ModelStoreBinding {
	return (*worker.ModelStoreBinding)(obj)
}
