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

// TopologySourceHandler 将 v1alpha1.TopologySource 代理为公开的 TopologySource 资源。
//
// 这里不暴露 status 代理，因为 status 是拓扑控制器记录的资产清单，
// 而代理以 worker 的身份执行写入。底层存储资源的 status 子资源会在主资源写入时保持 status 不变，
// 因此 status 通过主资源读取。
type TopologySourceHandler struct {
	extensionapi.ObjectInfo
	extensionapi.CurdOperations
}

func (h *TopologySourceHandler) SetupHandler(
	_ context.Context, opts extensionapi.SetupOptions,
) (schema.GroupVersionResource, map[string]rest.Storage, error) {
	gvr := worker.SchemeGroupVersionResource("topologysources")
	table, err := extensionapi.NewJSONPathTemplateTableConvertor(
		extensionapi.JSONPathTemplateColumn("Kind", "{.status.sourceKind}"),
		extensionapi.JSONPathTemplateColumn("Revision", "{.status.lastSuccessfulRevision}"),
		extensionapi.JSONPathTemplateColumn("Ready", "{.status.conditions[?(@.type=='Ready')].status}"),
	)
	if err != nil {
		return gvr, nil, err
	}
	h.ObjectInfo = &worker.TopologySource{}
	h.CurdOperations = extensionapi.WithCurdProxy[
		*worker.TopologySource, *worker.TopologySourceList,
		*workercore.TopologySource, *workercore.TopologySourceList,
	](table, h, opts.Manager.GetClient().(ctrlcli.WithWatch), opts.Manager.GetAPIReader())
	return gvr, map[string]rest.Storage{}, nil
}

func (h *TopologySourceHandler) New() runtime.Object     { return &worker.TopologySource{} }
func (h *TopologySourceHandler) Destroy()                {}
func (h *TopologySourceHandler) NewList() runtime.Object { return &worker.TopologySourceList{} }
func (h *TopologySourceHandler) NewListForProxy() runtime.Object {
	return &workercore.TopologySourceList{}
}

func (h *TopologySourceHandler) CastObjectTo(obj *worker.TopologySource) *workercore.TopologySource {
	return (*workercore.TopologySource)(obj)
}

func (h *TopologySourceHandler) CastObjectFrom(
	_ context.Context, obj *workercore.TopologySource,
) *worker.TopologySource {
	return (*worker.TopologySource)(obj)
}
