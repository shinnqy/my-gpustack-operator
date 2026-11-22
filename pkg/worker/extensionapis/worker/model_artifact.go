package worker

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/registry/rest"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"

	worker "gpustack.ai/gpustack/api/worker/v1"
	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/extensionapi"
)

// ModelArtifactHandler 暴露 v1alpha1.ModelArtifact 的 v1 视图及其 progress 子资源。
//
// 不提供 STATUS 子资源：代理使用 worker 身份写入（ModelArtifact status 守卫允许该身份），
// 因此可写的 v1 status 会让任意调用方篡改挂载授权依据。通过主资源更新会保持 status
// 原状不变，这与资源的 status 子资源在 API server 中的行为一致。
type ModelArtifactHandler struct {
	extensionapi.ObjectInfo
	extensionapi.CurdOperations
}

func (h *ModelArtifactHandler) SetupHandler(
	_ context.Context, opts extensionapi.SetupOptions,
) (schema.GroupVersionResource, map[string]rest.Storage, error) {
	gvr := worker.SchemeGroupVersionResource("modelartifacts")
	table, err := extensionapi.NewJSONPathTemplateTableConvertor(
		extensionapi.RenderColumn("Source", modelArtifactSourceColumn),
		extensionapi.RenderColumn("Revision", func(obj *worker.ModelArtifact) string {
			if r := obj.Status.Resolved; r != nil && len(r.Revision) > 12 {
				return r.Revision[:12]
			}
			return ""
		}),
		extensionapi.JSONPathTemplateColumn("Size", "{.status.resolved.sizeBytes}"),
		extensionapi.JSONPathTemplateColumn("Ready", "{.status.nodes.ready}"),
		extensionapi.JSONPathTemplateColumn("Downloading", "{.status.nodes.downloading}"),
		extensionapi.JSONPathTemplateColumn("Resolved", "{.status.conditions[?(@.type=='Resolved')].status}"),
	)
	if err != nil {
		return gvr, nil, err
	}
	h.ObjectInfo = &worker.ModelArtifact{}
	h.CurdOperations = extensionapi.WithCurdProxy[
		*worker.ModelArtifact, *worker.ModelArtifactList,
		*workercore.ModelArtifact, *workercore.ModelArtifactList,
	](table, h, opts.Manager.GetClient().(ctrlcli.WithWatch), opts.Manager.GetAPIReader())
	return gvr, map[string]rest.Storage{
		"progress": newModelArtifactProgressHandler(h.ObjectInfo, opts),
	}, nil
}

// modelArtifactSourceColumn 返回模型仓库、声明或镜像引用的名称。
func modelArtifactSourceColumn(obj *worker.ModelArtifact) string {
	switch s := obj.Spec.Source; {
	case s.HuggingFace != nil:
		return s.HuggingFace.Repository
	case s.ModelScope != nil:
		return s.ModelScope.Repository
	case s.PersistentVolumeClaim != nil:
		return fmt.Sprintf("claim/%s", s.PersistentVolumeClaim.ClaimName)
	case s.Image != nil:
		return s.Image.Reference
	}
	return ""
}

func (h *ModelArtifactHandler) New() runtime.Object     { return &worker.ModelArtifact{} }
func (h *ModelArtifactHandler) Destroy()                {}
func (h *ModelArtifactHandler) NewList() runtime.Object { return &worker.ModelArtifactList{} }
func (h *ModelArtifactHandler) NewListForProxy() runtime.Object {
	return &workercore.ModelArtifactList{}
}

func (h *ModelArtifactHandler) CastObjectTo(obj *worker.ModelArtifact) *workercore.ModelArtifact {
	return (*workercore.ModelArtifact)(obj)
}

func (h *ModelArtifactHandler) CastObjectFrom(
	_ context.Context, obj *workercore.ModelArtifact,
) *worker.ModelArtifact {
	return (*worker.ModelArtifact)(obj)
}
