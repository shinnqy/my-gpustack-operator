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

const _DevicesResource = "devices"

// DevicesHandler 处理 v1.Devices 对象。
//
// DevicesHandler 代理服务端的 v1alpha1.Devices 对象。
type DevicesHandler struct {
	extensionapi.ObjectInfo
	extensionapi.CurdOperations
}

func (h *DevicesHandler) SetupHandler(
	ctx context.Context,
	opts extensionapi.SetupOptions,
) (gvr schema.GroupVersionResource, srs map[string]rest.Storage, err error) {
	// 配置字段索引器。
	fi := opts.Manager.GetFieldIndexer()
	err = fi.IndexField(ctx, &workercore.Devices{}, "metadata.name",
		func(obj ctrlcli.Object) []string {
			if obj == nil {
				return nil
			}
			return []string{obj.GetName()}
		})
	if err != nil {
		return schema.GroupVersionResource{}, srs, err
	}

	// 声明 GVR。
	gvr = worker.SchemeGroupVersionResource(_DevicesResource)

	// 创建表格转换器以美化 kubectl 的输出。
	tc, err := extensionapi.NewJSONPathTableConvertor()
	if err != nil {
		return gvr, srs, err
	}

	// 作为存储后端。
	h.ObjectInfo = &worker.Devices{}
	h.CurdOperations = extensionapi.WithCurdProxy[
		*worker.Devices, *worker.DevicesList, *workercore.Devices, *workercore.DevicesList,
	](tc, h, opts.Manager.GetClient().(ctrlcli.WithWatch), opts.Manager.GetAPIReader())

	return gvr, srs, err
}

var (
	_ rest.Storage           = (*DevicesHandler)(nil)
	_ rest.Creater           = (*DevicesHandler)(nil)
	_ rest.Lister            = (*DevicesHandler)(nil)
	_ rest.Watcher           = (*DevicesHandler)(nil)
	_ rest.Getter            = (*DevicesHandler)(nil)
	_ rest.Updater           = (*DevicesHandler)(nil)
	_ rest.Patcher           = (*DevicesHandler)(nil)
	_ rest.GracefulDeleter   = (*DevicesHandler)(nil)
	_ rest.CollectionDeleter = (*DevicesHandler)(nil)
)

func (h *DevicesHandler) New() runtime.Object {
	return &worker.Devices{}
}

func (h *DevicesHandler) Destroy() {
}

func (h *DevicesHandler) NewList() runtime.Object {
	return &worker.DevicesList{}
}

func (h *DevicesHandler) NewListForProxy() runtime.Object {
	return &workercore.DevicesList{}
}

func (h *DevicesHandler) CastObjectTo(do *worker.Devices) (uo *workercore.Devices) {
	return (*workercore.Devices)(do)
}

func (h *DevicesHandler) CastObjectFrom(_ context.Context, uo *workercore.Devices) (do *worker.Devices) {
	return (*worker.Devices)(uo)
}
