package worker

import (
	"context"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/registry/rest"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"

	worker "gpustack.ai/gpustack/api/worker/v1"
	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/extensionapi"
)

const (
	_InstanceTypeResource = "instancetypes"
)

// InstanceTypeHandler 处理 v1.InstanceType 对象。
//
// InstanceTypeHandler 将 v1.InstanceType 代理到 v1alpha1.InstanceType CRD。
// 背后的 ClusterQueue 与四视图状态由 InstanceTypeReconciler 负责调谐；
// 本处理器仅提供 CRD 存储服务。
type InstanceTypeHandler struct {
	extensionapi.ObjectInfo
	extensionapi.CurdOperations
}

func (h *InstanceTypeHandler) SetupHandler(
	ctx context.Context,
	opts extensionapi.SetupOptions,
) (gvr schema.GroupVersionResource, srs map[string]rest.Storage, err error) {
	// 配置字段索引器。
	fi := opts.Manager.GetFieldIndexer()
	err = fi.IndexField(ctx, &workercore.InstanceType{}, "metadata.name",
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
	gvr = worker.SchemeGroupVersionResource(_InstanceTypeResource)

	// 四视图列只能将分区视图显示为单个标量值，这是在竞争同一物理切分的多个 profile
	// 之间取最优情况的结果。本列展示其背后的逐 profile 列表——即该资源池仍可容纳哪些
	// profile；其中资源池提供但当前无法构建的 profile 会显示为零。优先级 1 将其限制在
	// `-o wide` 下显示：该列内容较长，且对于没有分区卡的资源池均为空。
	partitions := extensionapi.RenderColumn("Partitions", func(obj *worker.InstanceType) string {
		profiles := obj.Status.AcceleratorPartitioned.RemainingProfiles
		if len(profiles) == 0 {
			// 没有分区卡的资源池不提供可选列表。渲染与 jsonpath 列在缺省值时相同的占位符，
			// 以保持宽表格式统一。
			return "-"
		}
		parts := make([]string, 0, len(profiles))
		for i := range profiles {
			parts = append(parts, fmt.Sprintf("%s:%d", profiles[i].Name, profiles[i].Count))
		}
		return strings.Join(parts, " ")
	})
	partitions.Priority = 1

	// 创建表格转换器以美化 kubectl 的输出。
	tc, err := extensionapi.NewJSONPathTemplateTableConvertor(
		extensionapi.JSONPathTemplateColumn("Entrance", "{.status.entrance}"),
		extensionapi.JSONPathTemplateColumn("Unit(CPU/RAM)/Storage",
			"{.spec.unitResources.cpu}/{.spec.unitResources.ram}/{.spec.localStorage}"),
		// 将独占/共享/切片/分区四类视图折叠为一列：按列头所示顺序排列的
		// 四组以空格分隔的 onceMaxRequest/remaining 数值。
		extensionapi.JSONPathTemplateColumn("Accelerator(EX/SH/SL/PT)",
			"{.status.accelerator.onceMaxRequest}/{.status.accelerator.remaining} "+
				"{.status.acceleratorShared.onceMaxRequest}/{.status.acceleratorShared.remaining} "+
				"{.status.acceleratorSliced.onceMaxRequest}/{.status.acceleratorSliced.remaining} "+
				"{.status.acceleratorPartitioned.onceMaxRequest}/{.status.acceleratorPartitioned.remaining}"),
		partitions,
		extensionapi.JSONPathTemplateColumn("CPU", "{.status.cpu.onceMaxRequest}/{.status.cpu.remaining}"),
		extensionapi.JSONPathTemplateColumn("Phase", "{.status.phase}"),
	)
	if err != nil {
		return gvr, srs, err
	}

	// 作为存储实现。
	h.ObjectInfo = &worker.InstanceType{}
	h.CurdOperations = extensionapi.WithCurdProxy[
		*worker.InstanceType, *worker.InstanceTypeList, *workercore.InstanceType, *workercore.InstanceTypeList,
	](tc, h, opts.Manager.GetClient().(ctrlcli.WithWatch), opts.Manager.GetAPIReader())

	return gvr, srs, err
}

var (
	_ rest.Storage           = (*InstanceTypeHandler)(nil)
	_ rest.Creater           = (*InstanceTypeHandler)(nil)
	_ rest.Lister            = (*InstanceTypeHandler)(nil)
	_ rest.Watcher           = (*InstanceTypeHandler)(nil)
	_ rest.Getter            = (*InstanceTypeHandler)(nil)
	_ rest.Updater           = (*InstanceTypeHandler)(nil)
	_ rest.Patcher           = (*InstanceTypeHandler)(nil)
	_ rest.GracefulDeleter   = (*InstanceTypeHandler)(nil)
	_ rest.CollectionDeleter = (*InstanceTypeHandler)(nil)
)

func (h *InstanceTypeHandler) New() runtime.Object {
	return &worker.InstanceType{}
}

func (h *InstanceTypeHandler) Destroy() {}

func (h *InstanceTypeHandler) NewList() runtime.Object {
	return &worker.InstanceTypeList{}
}

func (h *InstanceTypeHandler) NewListForProxy() runtime.Object {
	return &workercore.InstanceTypeList{}
}

func (h *InstanceTypeHandler) CastObjectTo(do *worker.InstanceType) (uo *workercore.InstanceType) {
	return (*workercore.InstanceType)(do)
}

func (h *InstanceTypeHandler) CastObjectFrom(_ context.Context, uo *workercore.InstanceType) (do *worker.InstanceType) {
	return (*worker.InstanceType)(uo)
}
