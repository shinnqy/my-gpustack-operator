package worker

import (
	"cmp"
	"context"
	"fmt"
	"slices"

	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/registry/rest"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	worker "gpustack.ai/gpustack/api/worker/v1"
	"gpustack.ai/gpustack/pkg/extensionapi"
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/systemmeta"
	"gpustack.ai/gpustack/pkg/utils/gox"
	"gpustack.ai/gpustack/pkg/worker/settings"
)

const (
	_InstanceTypeFlavorResource = "instancetypeflavors"

	// _ResourceFlavorResType 是该 handler 所聚合的、归属 operator 的 Kueue
	// ResourceFlavor 的 systemmeta 资源类型（由 NodeFlavorReconciler 标记）。
	_ResourceFlavorResType = "nodes"
)

// InstanceTypeFlavorHandler 以只读方式提供 v1.InstanceTypeFlavor 对象（get、list 和 watch）。
//
// 它没有底层 CRD：OnList 聚合归属 operator 的 Kueue ResourceFlavor，
// 将每个资源池的 note.gpustack.ai/* 注解解析为一个 InstanceTypeFlavor，
// 对相同条目去重，并依次按厂商、产品型号、显存排序。OnWatch 将
// ResourceFlavor 的 watch 事件投影到同一去重目录中，仅当某个 flavor 的首个底层
// ResourceFlavor 出现时发出 ADDED 事件，仅当最后一个消失时发出 DELETED 事件。
// OnGet 基于相同的 OnList 聚合结果按名称解析单个 flavor。
type InstanceTypeFlavorHandler struct {
	extensionapi.ObjectInfo
	extensionapi.ListWatchOperation
	extensionapi.GetOperation

	Client    ctrlcli.WithWatch
	APIReader ctrlcli.Reader
}

func (h *InstanceTypeFlavorHandler) SetupHandler(
	_ context.Context,
	opts extensionapi.SetupOptions,
) (gvr schema.GroupVersionResource, srs map[string]rest.Storage, err error) {
	// 声明 GVR。
	gvr = worker.SchemeGroupVersionResource(_InstanceTypeFlavorResource)

	// 创建表格转换器以美化 kubectl 的输出。
	tc, err := extensionapi.NewJSONPathTableConvertor(
		extensionapi.JSONPathColumn("GeneralGroup", ".spec.generalGroup"),
		extensionapi.JSONPathColumn("AcceleratorGroup", ".spec.acceleratorGroup"),
		extensionapi.JSONPathColumn("Acceleratable", ".spec.acceleratable"),
		extensionapi.JSONPathColumn("Manufacturer", ".spec.manufacturer"),
		extensionapi.JSONPathColumn("Product", ".spec.product"),
		extensionapi.JSONPathColumn("Memory", ".spec.memory"),
		extensionapi.JSONPathColumn("Cores", ".spec.cores"),
	)
	if err != nil {
		return gvr, srs, err
	}

	// 作为存储实现。
	h.ObjectInfo = &worker.InstanceTypeFlavor{}
	h.ListWatchOperation = extensionapi.WithListWatch(tc, h)
	h.GetOperation = extensionapi.WithGet(h)
	h.Client = opts.Manager.GetClient().(ctrlcli.WithWatch)
	h.APIReader = opts.Manager.GetAPIReader()

	return gvr, srs, err
}

var (
	_ rest.Storage = (*InstanceTypeFlavorHandler)(nil)
	_ rest.Lister  = (*InstanceTypeFlavorHandler)(nil)
	_ rest.Watcher = (*InstanceTypeFlavorHandler)(nil)
	_ rest.Getter  = (*InstanceTypeFlavorHandler)(nil)
)

func (h *InstanceTypeFlavorHandler) New() runtime.Object {
	return &worker.InstanceTypeFlavor{}
}

func (h *InstanceTypeFlavorHandler) Destroy() {}

func (h *InstanceTypeFlavorHandler) NewList() runtime.Object {
	return &worker.InstanceTypeFlavorList{}
}

// OnList 将归属 operator 的 ResourceFlavor 聚合为去重并排序的 InstanceTypeFlavor，
// 分组方式与派生的 ClusterQueue/InstanceType 相同，受 instance-type-aware-cpu-manufacturer 控制
// （从远端读取，因为聚合 apiserver 没有本地配置索引器）。关闭该感知时，通用池折叠为一行
// "generic"，加速池按每种加速器一行（忽略 CPU）；开启时，两者均按 CPU 键拆分。
// 相同规格会合并为一条，因此跨越多个节点或 os/arch 的型号仅出现一次。
func (h *InstanceTypeFlavorHandler) OnList(ctx context.Context, opts ctrlcli.ListOptions) (runtime.Object, error) {
	// 列出资源。
	rfList := new(kueue.ResourceFlavorList)
	err := h.APIReader.List(ctx, rfList,
		convertResourceFlavorListOptsFromInstanceTypeFlavorListOpts(opts),
		ctrlcli.UnsafeDisableDeepCopy)
	if err != nil {
		return nil, err
	}

	// 按布尔值读取感知设置（与 reconciler 的 ShouldValueBool 一致），而非字面量 "true"：
	// 该设置的 AllowBool 准入还接受 "1"/"TRUE" 等值，仅与 "true" 比较会导致目录仍处于
	// 折叠状态，而调谐后的 ClusterQueue/InstanceType 却已拆分。
	cpuAware := settings.InstanceTypeAwareCPUManufacturer.ShouldValueBoolFromRemote(ctx)

	visited := sets.New[worker.InstanceTypeFlavorSpec]()
	list := &worker.InstanceTypeFlavorList{Items: make([]worker.InstanceTypeFlavor, 0, len(rfList.Items))}
	for i := range rfList.Items {
		spec, ok := resourceFlavorToSpec(&rfList.Items[i], cpuAware)
		if !ok {
			continue
		}
		if visited.Has(spec) {
			continue
		}
		visited.Insert(spec)
		list.Items = append(list.Items, worker.InstanceTypeFlavor{
			ObjectMeta: meta.ObjectMeta{Name: instanceTypeFlavorName(spec)},
			Spec:       spec,
		})
	}

	slices.SortFunc(list.Items, compareInstanceTypeFlavor)

	return list, nil
}

// compareInstanceTypeFlavor 依次按厂商、产品型号和显存对目录排序，并以分组标识作为
// 决胜键，以确保按 CPU 拆分的行（开启感知时）具有确定性顺序。
//
// 它在每个键上都满足反对称性，这是 slices.SortFunc 的要求：compare(x, y) 始终等于
// -compare(y, x)。显存键需要特别处理——它通过自定义的双向比较函数对数量字符串排序，
// 而两个不同的字符串可能解析出相同的数量（如 "16Gi" 与 "16384Mi"），此时双方互不小于。
// 因此需先双向比较，再回退到字符串本身（不等式守卫已保证二者不同）；若改为返回固定符号，
// 会导致双向比较结果一致，从而使排序结果未定义。
func compareInstanceTypeFlavor(x, y worker.InstanceTypeFlavor) int {
	a, b := &x.Spec, &y.Spec
	if a.Manufacturer != b.Manufacturer {
		return cmp.Compare(a.Manufacturer, b.Manufacturer)
	}
	if a.Product != b.Product {
		return cmp.Compare(a.Product, b.Product)
	}
	if a.Memory != b.Memory {
		if lessInstanceTypeFlavorMemory(a.Memory, b.Memory) {
			return -1
		}
		if lessInstanceTypeFlavorMemory(b.Memory, a.Memory) {
			return 1
		}
		return cmp.Compare(a.Memory, b.Memory)
	}
	if a.GeneralGroup != b.GeneralGroup {
		return cmp.Compare(a.GeneralGroup, b.GeneralGroup)
	}
	return cmp.Compare(a.AcceleratorGroup, b.AcceleratorGroup)
}

// OnGet 基于 OnList 生成的同一聚合目录按名称解析单个 flavor。该目录是派生出来的，
// 没有按对象存储的后端，因此 Get 先执行列表查询再按名称过滤。flavor 名称仅由分组标识派生，
// 不保证唯一，因此返回 OnList 确定性排序（厂商、产品型号、显存）中的首个匹配项；
// 若无匹配的资源池，则返回 NotFound。
func (h *InstanceTypeFlavorHandler) OnGet(ctx context.Context, key types.NamespacedName, _ ctrlcli.GetOptions) (runtime.Object, error) {
	listObj, err := h.OnList(ctx, ctrlcli.ListOptions{})
	if err != nil {
		return nil, err
	}

	list := listObj.(*worker.InstanceTypeFlavorList)
	for i := range list.Items {
		if list.Items[i].Name == key.Name {
			return &list.Items[i], nil
		}
	}
	return nil, kerrors.NewNotFound(worker.Resource(_InstanceTypeFlavorResource), key.Name)
}

// OnWatch 将归属 operator 的 ResourceFlavor watch 事件投影到去重的 InstanceTypeFlavor 目录上。
// 由于是多 ResourceFlavor 到单 flavor 的投影，事件无法 1:1 映射：仅当某 flavor 的首个底层
// ResourceFlavor 出现时触发 ADDED，仅当最后一个底层 ResourceFlavor 消失时触发 DELETED。
// 它先根据当前 flavor 初始化一个底层引用计数多重集，以抑制上游 watch 的初始重放
// （客户端已通过 list 获取该状态），随后将每个 ResourceFlavor 事件折叠进该多重集并输出
// 相应的 flavor 增量事件。
//
// 其机制与 SettingHandler.OnWatch 一致：感知设置在 watch 启动时读取一次（若后续变更，
// 客户端重连时会重新派生），外层 ListWatchOperation.Watch 包装器负责资源版本去重与
// bookmark 透传，因此发出的每个 flavor 都携带对应底层 ResourceFlavor 的资源版本。
func (h *InstanceTypeFlavorHandler) OnWatch(ctx context.Context, opts ctrlcli.ListOptions) (watch.Interface, error) {
	// 在 watch 启动时读取一次感知设置（与 OnList 的读取方式一致）。
	cpuAware := settings.InstanceTypeAwareCPUManufacturer.ShouldValueBoolFromRemote(ctx)

	// 根据当前 ResourceFlavor 初始化底层计数多重集，使上游 watch 的初始重放
	// （对每个已有 flavor 触发的 ADDED）被视为已知状态而抑制。
	rfList := new(kueue.ResourceFlavorList)
	err := h.APIReader.List(ctx, rfList,
		convertResourceFlavorListOptsFromInstanceTypeFlavorListOpts(opts),
		ctrlcli.UnsafeDisableDeepCopy)
	if err != nil {
		return nil, err
	}
	state := newInstanceTypeFlavorWatchState()
	for i := range rfList.Items {
		if spec, ok := resourceFlavorToSpec(&rfList.Items[i], cpuAware); ok {
			state.seed(rfList.Items[i].Name, spec)
		}
	}

	// Watch 归属 operator 的 ResourceFlavor（与 list 使用相同的作用域）。
	uw, err := h.Client.Watch(ctx, new(kueue.ResourceFlavorList),
		convertResourceFlavorListOptsFromInstanceTypeFlavorListOpts(opts))
	if err != nil {
		return nil, err
	}

	c := make(chan watch.Event)
	dw := watch.NewProxyWatcher(c)
	gox.Go(func() {
		defer close(c)
		defer uw.Stop()

		for {
			select {
			case <-ctx.Done():
				// 因上下文取消而退出。
				return
			case <-dw.StopChan():
				// 因下游停止而退出。
				return
			case e, ok := <-uw.ResultChan():
				if !ok {
					// 因上游关闭而退出。
					return
				}

				// 无需处理。
				if e.Object == nil {
					c <- e
					continue
				}

				// 类型断言。
				rf, ok := e.Object.(*kueue.ResourceFlavor)
				if !ok {
					c <- e
					continue
				}

				// 处理 bookmark：通过占位 flavor 透传资源版本。
				if e.Type == watch.Bookmark {
					e.Object = &worker.InstanceTypeFlavor{
						ObjectMeta: meta.ObjectMeta{ResourceVersion: rf.ResourceVersion},
					}
					c <- e
					continue
				}

				// 将 ResourceFlavor 事件折叠进去重多重集并分发增量事件。
				for _, fe := range state.apply(e.Type, rf, cpuAware) {
					c <- fe
				}
			}
		}
	})

	return dw, nil
}

// instanceTypeFlavorWatchState 跟踪当前每个目录 flavor spec 对应的底层 ResourceFlavor，
// 使多 ResourceFlavor 到单 flavor 的投影仅在首个底层 ResourceFlavor 出现时发出 ADDED，
// 仅在最后一个消失时发出 DELETED。它由单个 OnWatch goroutine 独占，无需加锁。
type instanceTypeFlavorWatchState struct {
	rfSpec   map[string]worker.InstanceTypeFlavorSpec // ResourceFlavor 名称 -> 其对应的 spec
	specRefs map[worker.InstanceTypeFlavorSpec]int    // flavor spec -> 底层 ResourceFlavor 数量
}

func newInstanceTypeFlavorWatchState() *instanceTypeFlavorWatchState {
	return &instanceTypeFlavorWatchState{
		rfSpec:   make(map[string]worker.InstanceTypeFlavorSpec),
		specRefs: make(map[worker.InstanceTypeFlavorSpec]int),
	}
}

// seed 记录一条底层绑定关系而不发出事件，从而避免上游 watch 对已列出 flavor 的
// 初始重放产生重复的 ADDED 事件。
func (s *instanceTypeFlavorWatchState) seed(rfName string, spec worker.InstanceTypeFlavorSpec) {
	s.rfSpec[rfName] = spec
	s.specRefs[spec]++
}

// apply 将一个 ResourceFlavor 事件折叠进多重集，并返回由此触发的 flavor 事件。
// 当 spec 获得首个底层绑定时触发 ADDED；失去最后一个底层绑定时触发 DELETED。
// 派生 spec 未变的底层对象不会产生事件；spec 发生变更时，会释放旧 spec（可能触发 DELETED）
// 并获取新 spec（可能触发 ADDED）。
//
// 当单个 ResourceFlavor 事件同时产生这两个事件（因 spec 变更而迁移）时，它们共享同一
// 底层 ResourceFlavor 的资源版本，会导致外层 ListWatchOperation.Watch 包装器的版本去重丢弃
// 第二个事件。以下两点确保两者都能保留：先发出 ADDED 以在版本替换中生效，且每个 DELETED
// 都带有 DeletionTimestamp（通过 flavorWatchEvent 设置），包装器会无视版本号将其放行。
// 发出的 flavor 均携带对应 ResourceFlavor 的资源版本。
func (s *instanceTypeFlavorWatchState) apply(evtType watch.EventType, rf *kueue.ResourceFlavor, cpuAware bool) []watch.Event {
	newSpec, present := resourceFlavorToSpec(rf, cpuAware)
	oldSpec, tracked := s.rfSpec[rf.Name]

	var added, deleted []watch.Event
	release := func() {
		if !tracked {
			return
		}
		delete(s.rfSpec, rf.Name)
		s.specRefs[oldSpec]--
		if s.specRefs[oldSpec] <= 0 {
			delete(s.specRefs, oldSpec)
			deleted = append(deleted, flavorWatchEvent(watch.Deleted, oldSpec, rf.ResourceVersion))
		}
	}
	acquire := func() {
		s.rfSpec[rf.Name] = newSpec
		s.specRefs[newSpec]++
		if s.specRefs[newSpec] == 1 {
			added = append(added, flavorWatchEvent(watch.Added, newSpec, rf.ResourceVersion))
		}
	}

	switch evtType {
	case watch.Deleted:
		release()
	case watch.Added, watch.Modified:
		switch {
		case !present:
			// 该 ResourceFlavor 正在排空或已失去资源池标识：移除其底层绑定。
			release()
		case tracked && oldSpec == newSpec:
			// 底层绑定未变化：去重集合保持不变。
		default:
			release() // 先扣减旧 spec，确保新 spec 写入前其条目已移除
			acquire() // 增加新 spec 计数（可能发出 ADDED）
		}
	}

	// ADDED 在 DELETED 之前发出，确保迁移时两者都能通过包装器的版本去重（参见上方注释）。
	return append(added, deleted...)
}

// flavorWatchEvent 为单个目录 flavor 构建 watch 事件，并标记底层 ResourceFlavor 的资源版本，
// 使外层 ListWatchOperation.Watch 包装器的版本去重将其放行。DELETED 事件还会携带
// DeletionTimestamp，这样即使它与前一个事件共享同一资源版本（spec 变更迁移会在同一版本下
// 发出两个事件），包装器也会将其放行。
func flavorWatchEvent(t watch.EventType, spec worker.InstanceTypeFlavorSpec, rv string) watch.Event {
	obj := &worker.InstanceTypeFlavor{
		ObjectMeta: meta.ObjectMeta{
			Name:            instanceTypeFlavorName(spec),
			ResourceVersion: rv,
		},
		Spec: spec,
	}
	if t == watch.Deleted {
		now := meta.Now()
		obj.DeletionTimestamp = &now
	}
	return watch.Event{Type: t, Object: obj}
}

// resourceFlavorToSpec 推导 ResourceFlavor 对应的目录 spec；若该 flavor 正在排空
// （已设置 DeletionTimestamp，目录不应通告正在下线的资源池）或缺少 operator 资源池标识，
// 则返回 ok=false。
func resourceFlavorToSpec(rf *kueue.ResourceFlavor, cpuAware bool) (worker.InstanceTypeFlavorSpec, bool) {
	if rf.DeletionTimestamp != nil {
		return worker.InstanceTypeFlavorSpec{}, false
	}
	_, notes := systemmeta.DescribeResource(rf)
	spec := instanceTypeFlavorSpec(notes, cpuAware)
	if spec.GeneralGroup == "" && spec.AcceleratorGroup == "" {
		return worker.InstanceTypeFlavorSpec{}, false
	}
	return spec, true
}

// instanceTypeFlavorSpec 根据 ResourceFlavor 的 notes 构建一条目录行，并按感知设置分组。
// 非加速 flavor 会成为按 CPU 区分的行（开启感知）或单一的 CPU 无关 "generic" 行（关闭感知）；
// 加速 flavor 保留其设备描述符（不同 CPU 变体下设备描述符相同，因此可去重），且仅在开启感知时
// 携带 CPU 键。
func instanceTypeFlavorSpec(notes map[string]string, cpuAware bool) worker.InstanceTypeFlavorSpec {
	if notes["acceleratable"] != "true" {
		if cpuAware {
			return worker.InstanceTypeFlavorSpec{
				GeneralGroup: notes["generalGroup"],
				Manufacturer: notes["manufacturer"],
				Product:      notes["product"],
				Family:       notes["family"],
			}
		}
		return worker.InstanceTypeFlavorSpec{
			GeneralGroup: nodefeature.GeneralGroupGeneric,
		}
	}
	spec := worker.InstanceTypeFlavorSpec{
		AcceleratorGroup: notes["acceleratorGroup"],
		Acceleratable:    true,
		Manufacturer:     notes["manufacturer"],
		Product:          notes["product"],
		Family:           notes["family"],
		Memory:           notes["memory"],
		Cores:            notes["cores"],
	}
	if cpuAware {
		spec.GeneralGroup = notes["generalGroup"]
	}
	return spec
}

// instanceTypeFlavorName 根据分组标识为目录行命名，与派生的 InstanceType 名称规则一致：
// gpustack--${aKey} / gpustack--${gKey}--${aKey}（加速型，关闭/开启感知）以及
// gpustack--generic / gpustack--${gKey}（通用型，关闭/开启感知）。
func instanceTypeFlavorName(spec worker.InstanceTypeFlavorSpec) string {
	if spec.Acceleratable {
		if spec.GeneralGroup != "" {
			return fmt.Sprintf("gpustack--%s--%s", spec.GeneralGroup, spec.AcceleratorGroup)
		}
		return fmt.Sprintf("gpustack--%s", spec.AcceleratorGroup)
	}
	return fmt.Sprintf("gpustack--%s", spec.GeneralGroup)
}

// lessInstanceTypeFlavorMemory 在两条显存注解都能解析为数量时按数值比较排序
// （例如 "8Gi" < "16Gi"），否则回退为字典序比较（例如通用类型的空显存值）。
func lessInstanceTypeFlavorMemory(a, b string) bool {
	qa, ea := resource.ParseQuantity(a)
	qb, eb := resource.ParseQuantity(b)
	if ea == nil && eb == nil {
		return qa.Cmp(qb) < 0
	}
	return a < b
}

func convertResourceFlavorListOptsFromInstanceTypeFlavorListOpts(in ctrlcli.ListOptions) (out *ctrlcli.ListOptions) {
	// 添加必要的标签选择器。
	if lbs := systemmeta.GetResourcesLabelSelectorOfType(_ResourceFlavorResType); in.LabelSelector == nil {
		in.LabelSelector = lbs
	} else {
		reqs, _ := lbs.Requirements()
		in.LabelSelector = in.LabelSelector.DeepCopySelector().Add(reqs...)
	}

	return &in
}
