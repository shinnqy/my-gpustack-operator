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

// ModelDeploymentScalerHandler 暴露 v1alpha1.ModelDeploymentScaler 的 v1 视图。
//
// scaler 持有对其目标的绑定，因此写入频率低于部署本身，且不提供自有子资源：
// 它将观测结果记录在自身 status 中，而读取的指标属于目标的 Pod。
type ModelDeploymentScalerHandler struct {
	extensionapi.ObjectInfo
	extensionapi.CurdOperations
}

func (h *ModelDeploymentScalerHandler) SetupHandler(
	_ context.Context, opts extensionapi.SetupOptions,
) (schema.GroupVersionResource, map[string]rest.Storage, error) {
	gvr := worker.SchemeGroupVersionResource("modeldeploymentscalers")
	table, err := modelDeploymentScalerTable()
	if err != nil {
		return gvr, nil, err
	}
	h.ObjectInfo = &worker.ModelDeploymentScaler{}
	h.CurdOperations = extensionapi.WithCurdProxy[
		*worker.ModelDeploymentScaler, *worker.ModelDeploymentScalerList,
		*workercore.ModelDeploymentScaler, *workercore.ModelDeploymentScalerList,
	](table, h, opts.Manager.GetClient().(ctrlcli.WithWatch), opts.Manager.GetAPIReader())
	return gvr, nil, nil
}

func modelDeploymentScalerTable() (rest.TableConvertor, error) {
	return extensionapi.NewJSONPathTemplateTableConvertor(
		extensionapi.JSONPathTemplateColumn("Target", "{.spec.targetRef.name}"),
		extensionapi.JSONPathTemplateColumn("Mode", "{.spec.mode}"),
		extensionapi.JSONPathTemplateColumn("Algorithm", "{.spec.algorithm.type}"),
		extensionapi.JSONPathTemplateColumn("Bound", "{.status.conditions[?(@.type=='Bound')].status}"),
		extensionapi.JSONPathTemplateColumn("Active", "{.status.conditions[?(@.type=='Active')].status}"),
		extensionapi.JSONPathTemplateColumn("Blocked", "{.status.conditions[?(@.type=='Blocked')].status}"),
	)
}

func (h *ModelDeploymentScalerHandler) New() runtime.Object { return &worker.ModelDeploymentScaler{} }
func (h *ModelDeploymentScalerHandler) Destroy()            {}
func (h *ModelDeploymentScalerHandler) NewList() runtime.Object {
	return &worker.ModelDeploymentScalerList{}
}

func (h *ModelDeploymentScalerHandler) NewListForProxy() runtime.Object {
	return &workercore.ModelDeploymentScalerList{}
}

func (h *ModelDeploymentScalerHandler) CastObjectTo(obj *worker.ModelDeploymentScaler) *workercore.ModelDeploymentScaler {
	return (*workercore.ModelDeploymentScaler)(obj)
}

func (h *ModelDeploymentScalerHandler) CastObjectFrom(
	_ context.Context, obj *workercore.ModelDeploymentScaler,
) *worker.ModelDeploymentScaler {
	return (*worker.ModelDeploymentScaler)(obj)
}
