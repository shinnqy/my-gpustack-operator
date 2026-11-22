package worker

import (
	"context"
	"fmt"
	"strconv"

	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/registry/rest"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"

	worker "gpustack.ai/gpustack/api/worker/v1"
	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/extensionapi"
	"gpustack.ai/gpustack/pkg/setting"
	workersettings "gpustack.ai/gpustack/pkg/worker/settings"
)

// 该视图自有列所占用的单元格位置。框架会将对象名称排在首位、存在时间排在末位，
// 因此两者之间的列按位置索引。
const (
	modelStoreHighCell = 2
	modelStoreLowCell  = 3
)

// ModelStoreHandler 将 v1alpha1.ModelStore 代理为公开的 ModelStore 资源。
//
// 这里不暴露 status 代理，因为 status 是控制器对所匹配节点驻留情况的观测结果，
// 而代理以 worker 的身份执行写入。底层存储资源的 status 子资源会在主资源写入时保持 status 不变，
// 因此 status 通过主资源读取。
type ModelStoreHandler struct {
	extensionapi.ObjectInfo
	extensionapi.CurdOperations
}

func (h *ModelStoreHandler) SetupHandler(
	_ context.Context, opts extensionapi.SetupOptions,
) (schema.GroupVersionResource, map[string]rest.Storage, error) {
	gvr := worker.SchemeGroupVersionResource("modelstores")
	table, err := modelStoreTable(opts.Manager.GetAPIReader())
	if err != nil {
		return gvr, nil, err
	}
	h.ObjectInfo = &worker.ModelStore{}
	h.CurdOperations = extensionapi.WithCurdProxy[
		*worker.ModelStore, *worker.ModelStoreList,
		*workercore.ModelStore, *workercore.ModelStoreList,
	](table, h, opts.Manager.GetClient().(ctrlcli.WithWatch), opts.Manager.GetAPIReader())
	return gvr, map[string]rest.Storage{}, nil
}

// modelStoreTable 是该视图的 kubectl 表格：展示 ModelStore 对象自身的列，
// 并在存储池未指定水位线时从集群 Settings 中补全两个水位线单元格。
//
// 它反映存储池自身的缓存策略（位于集群默认值之上的一层），而非节点实际生效的报告：
// 节点自身的 kubelet 阈值可能会将高水位线限制在存储池设定之下，只有该节点的 NodeModelStore 才显示最终结果。
func modelStoreTable(reader ctrlcli.Reader) (rest.TableConvertor, error) {
	columns, err := extensionapi.NewJSONPathTemplateTableConvertor(
		extensionapi.JSONPathTemplateColumn("Nodes", "{.status.nodes}"),
		extensionapi.JSONPathTemplateColumn("High", "{.spec.watermarks.highPercent}"),
		extensionapi.JSONPathTemplateColumn("Low", "{.spec.watermarks.lowPercent}"),
	)
	if err != nil {
		return nil, err
	}
	return modelStoreTableConvertor{columns: columns, reader: reader}, nil
}

// modelStoreTableConvertor 通过单次读取集群 Settings，为存储池未设置水位线的行填充两个水位线单元格。
//
// 它包装视图的自有列而非替换它们，因此表头、列表元数据以及每行携带的对象均保持表格框架原有输出。
// 集群值仅在首个需要的行读取一次并由其余行共享，因此没有任何继承的请求完全不会读取 Secret。
// 修改仅限单元格内容：读取来源的存储池对象保持原样，因此采用继承的存储池仍未显式声明水位线，
// 表格中携带的对象副本亦如此。
type modelStoreTableConvertor struct {
	columns rest.TableConvertor
	reader  ctrlcli.Reader
}

func (c modelStoreTableConvertor) ConvertToTable(
	ctx context.Context, obj, tableOptions runtime.Object,
) (*meta.Table, error) {
	table, err := c.columns.ConvertToTable(ctx, obj, tableOptions)
	if err != nil {
		return nil, err
	}

	var (
		cluster workercore.NodeModelStoreWatermarks
		read    bool
	)
	for i := range table.Rows {
		store, ok := table.Rows[i].Object.Object.(*worker.ModelStore)
		if !ok || store.Spec.Watermarks != nil {
			continue
		}
		if !read {
			if cluster, err = readClusterWatermarks(ctx, c.reader); err != nil {
				return nil, err
			}
			read = true
		}
		table.Rows[i].Cells[modelStoreHighCell] = strconv.Itoa(int(cluster.HighPercent))
		table.Rows[i].Cells[modelStoreLowCell] = strconv.Itoa(int(cluster.LowPercent))
	}
	return table, nil
}

// readClusterWatermarks 读取集群 Settings 中保存的水位线。
//
// 它通过 API reader 而非 Settings 包读取，因为后者的读缓存有效期可达三十秒，
// 而表格应展示当前存储的值。不存在的 Secret 以及其中缺失的键会回退到 Setting 自身的默认值，
// 即存储池未显式覆盖时所依据的集群层设置。其他任何读取失败以及非百分比的存储值均视为错误：
// 若表格无法确认当前生效的集群值，绝不能将默认值当作运维人员显式选择的值来展示。
func readClusterWatermarks(ctx context.Context, reader ctrlcli.Reader) (workercore.NodeModelStoreWatermarks, error) {
	sec := new(core.Secret)
	key := ctrlcli.ObjectKey{Namespace: setting.DelegatedSecretNamespace, Name: setting.DelegatedSecretName}
	if err := reader.Get(ctx, key, sec); ctrlcli.IgnoreNotFound(err) != nil {
		return workercore.NodeModelStoreWatermarks{}, fmt.Errorf("read the settings secret: %w", err)
	}
	// 未找到 Secret 时 sec 保持为空，因此所有键都会回退到各自的默认值。

	var wm workercore.NodeModelStoreWatermarks
	var err error
	if wm.HighPercent, err = watermarkPercent(sec.Data, workersettings.ModelStoreHighWatermark); err != nil {
		return workercore.NodeModelStoreWatermarks{}, err
	}
	if wm.LowPercent, err = watermarkPercent(sec.Data, workersettings.ModelStoreLowWatermark); err != nil {
		return workercore.NodeModelStoreWatermarks{}, err
	}
	return wm, nil
}

// watermarkPercent 读取单个水位线的存储值；当 Secret 中未携带该键时返回 Setting 的默认值。
// 这里仅读取两个水位线键，因此其他 Setting 中格式错误的值仅影响其自身，不会阻碍表格渲染。
func watermarkPercent(data map[string][]byte, s setting.Setting) (int32, error) {
	value := s.DefaultValue()
	if v, ok := data[s.Name()]; ok {
		value = string(v)
	}
	percent, err := strconv.ParseInt(value, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("setting %s: %w", s.Name(), err)
	}
	return int32(percent), nil
}

func (h *ModelStoreHandler) New() runtime.Object     { return &worker.ModelStore{} }
func (h *ModelStoreHandler) Destroy()                {}
func (h *ModelStoreHandler) NewList() runtime.Object { return &worker.ModelStoreList{} }
func (h *ModelStoreHandler) NewListForProxy() runtime.Object {
	return &workercore.ModelStoreList{}
}

func (h *ModelStoreHandler) CastObjectTo(obj *worker.ModelStore) *workercore.ModelStore {
	return (*workercore.ModelStore)(obj)
}

func (h *ModelStoreHandler) CastObjectFrom(
	_ context.Context, obj *workercore.ModelStore,
) *worker.ModelStore {
	return (*worker.ModelStore)(obj)
}
