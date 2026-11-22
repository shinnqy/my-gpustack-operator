package extensionapi

import (
	"context"
	"errors"
	"fmt"

	core "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/registry/rest"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"

	gpustack "gpustack.ai/gpustack/api/v1"
	"gpustack.ai/gpustack/pkg/kubemeta"
	"gpustack.ai/gpustack/pkg/setting"
	"gpustack.ai/gpustack/pkg/systemmeta"
	"gpustack.ai/gpustack/pkg/utils/gox"
)

const _SettingResource = "settings"

// SettingHandler 处理 v1.Setting 对象。
//
// SettingHandler 将所有 v1.Setting 对象映射到名为 "gpustack-system/gpustack-settings" 的
// Kubernetes Secret 资源。
//
// 每个 v1.Setting 对象都作为键值对记录在 Secret 的 Data 字段中。
type SettingHandler struct {
	index setting.IndexFunc

	ObjectInfo
	ListWatchOperation
	GetOperation
	UpdateOperation

	Client    ctrlcli.Client
	APIReader ctrlcli.Reader
}

// NewSettingHandler 创建一个新的 SettingHandler。
func NewSettingHandler(index setting.IndexFunc) *SettingHandler {
	return &SettingHandler{
		index: index,
	}
}

func (h *SettingHandler) SetupHandler(
	ctx context.Context,
	opts SetupOptions,
) (gvr schema.GroupVersionResource, srs map[string]rest.Storage, err error) {
	// 配置字段索引器。
	fi := opts.Manager.GetFieldIndexer()
	err = fi.IndexField(ctx, &core.Secret{}, "metadata.name",
		func(obj ctrlcli.Object) []string {
			if obj == nil {
				return nil
			}
			return []string{obj.GetName()}
		})
	if err != nil {
		return gvr, srs, fmt.Errorf("index secret 'metadata.name': %w", err)
	}

	// 声明 GVR。
	gvr = gpustack.SchemeGroupVersionResource(_SettingResource)

	// 创建表格转换器以美化 kubectl 输出。
	tc, err := NewJSONPathTableConvertor(
		JSONPathColumn("Value", ".status.value"),
	)
	if err != nil {
		return gvr, srs, err
	}

	// 作为存储。
	h.ObjectInfo = &gpustack.Setting{}
	h.ListWatchOperation = WithListWatch(tc, h)
	h.GetOperation = WithGet(h)
	h.UpdateOperation = WithUpdate(h)

	// 设置客户端。
	h.Client = opts.Manager.GetClient()
	h.APIReader = opts.Manager.GetAPIReader()

	return gvr, srs, err
}

var (
	_ rest.Storage = (*SettingHandler)(nil)
	_ rest.Lister  = (*SettingHandler)(nil)
	_ rest.Watcher = (*SettingHandler)(nil)
	_ rest.Getter  = (*SettingHandler)(nil)
	_ rest.Updater = (*SettingHandler)(nil)
	_ rest.Patcher = (*SettingHandler)(nil)
)

func (h *SettingHandler) New() runtime.Object {
	return &gpustack.Setting{}
}

func (h *SettingHandler) Destroy() {}

func (h *SettingHandler) NewList() runtime.Object {
	return &gpustack.SettingList{}
}

func (h *SettingHandler) OnList(ctx context.Context, opts ctrlcli.ListOptions) (runtime.Object, error) {
	// 支持通过 `kubectl get -A` 进行 watch。
	if opts.Namespace == "" {
		opts.Namespace = setting.DelegatedSecretNamespace
	}

	// 获取。
	sec := &core.Secret{
		ObjectMeta: meta.ObjectMeta{
			Namespace: opts.Namespace,
			Name:      setting.DelegatedSecretName,
		},
	}
	err := h.APIReader.Get(ctx, ctrlcli.ObjectKeyFromObject(sec), sec)
	if err != nil {
		if !kerrors.IsNotFound(err) {
			return nil, err
		}
		// 如果未找到 Secret，则返回空列表。
		return &gpustack.SettingList{}, nil
	}

	// 转换。
	sList := convertSettingListFromSecret(sec, opts, h.index)
	return sList, nil
}

func (h *SettingHandler) OnWatch(ctx context.Context, opts ctrlcli.ListOptions) (watch.Interface, error) {
	// 支持通过 `kubectl get -A` 进行 watch。
	if opts.Namespace == "" {
		opts.Namespace = setting.DelegatedSecretNamespace
	}

	// 列出并建立索引。
	setIndexer := map[string]gpustack.Setting{} // [sn] -> set
	{
		listObj, err := h.OnList(ctx, opts)
		if err != nil {
			return nil, err
		}
		sList := listObj.(*gpustack.SettingList)
		for i := range sList.Items {
			setIndexKey := sList.Items[i].Name
			setIndexer[setIndexKey] = sList.Items[i]
		}
	}

	// 监听。
	uw, err := h.Client.(ctrlcli.WithWatch).Watch(ctx, new(core.SecretList),
		convertSecretListOptsFromSettingListOpts(opts))
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
				// 因 context 取消。
				return
			case <-dw.StopChan():
				// 由下游停止。
				return
			case e, ok := <-uw.ResultChan():
				if !ok {
					// 由上游关闭。
					return
				}

				// 无需处理。
				if e.Object == nil {
					c <- e
					continue
				}

				// 类型断言。
				sec, ok := e.Object.(*core.Secret)
				if !ok {
					c <- e
					continue
				}

				// 处理 bookmark。
				if e.Type == watch.Bookmark {
					systemmeta.UnnoteResource(sec)
					e.Object = &gpustack.Setting{ObjectMeta: sec.ObjectMeta}
					c <- e
					continue
				}

				// 发送。
				for name := range sec.Data {
					// 如果未被 `kubectl get --field-selector=metadata.name=...` 选中，则忽略。
					if !settingMatchFieldSelector(opts, name) {
						continue
					}

					// 转换。
					set := convertSettingFromSecret(sec, name, h.index)
					if set == nil {
						continue
					}

					// 如果与之前相同，则忽略。
					setIndexKey := set.Name
					prevSet, ok := setIndexer[setIndexKey]
					switch {
					default:
						// 忽略
						continue
					case !ok:
						// 插入
						setIndexer[setIndexKey] = *set
					case set.Status.Value_ != prevSet.Status.Value_:
						// 更新
						setIndexer[setIndexKey] = *set
					}

					// 分发。
					e2 := e.DeepCopy()
					e2.Object = set
					c <- *e2
				}
			}
		}
	})

	return dw, nil
}

func (h *SettingHandler) OnGet(ctx context.Context, key types.NamespacedName, opts ctrlcli.GetOptions) (runtime.Object, error) {
	// 获取。
	sec := &core.Secret{
		ObjectMeta: meta.ObjectMeta{
			Namespace: key.Namespace,
			Name:      setting.DelegatedSecretName,
		},
	}
	err := h.APIReader.Get(ctx, ctrlcli.ObjectKeyFromObject(sec), sec)
	if err != nil {
		return nil, err
	}

	// 转换。
	set := convertSettingFromSecret(sec, key.Name, h.index)
	if set == nil {
		return nil, kerrors.NewNotFound(gpustack.Resource(_SettingResource), key.Name)
	}
	return set, nil
}

func (h *SettingHandler) OnUpdate(ctx context.Context, obj, _ runtime.Object, opts ctrlcli.UpdateOptions) (runtime.Object, error) {
	// 校验。
	set := obj.(*gpustack.Setting)
	if set.Namespace != setting.DelegatedSecretNamespace {
		return nil, kerrors.NewNotFound(gpustack.Resource(_SettingResource), set.Name)
	}
	s, ok := h.index(set.Name)
	if !ok || !s.Editable() {
		return nil, kerrors.NewForbidden(gpustack.Resource(_SettingResource), set.Name,
			errors.New("setting is not editable"))
	}

	// 更新。
	if set.Spec.Value != nil {
		err := s.Configure(ctx, *set.Spec.Value)
		if err != nil {
			return nil, kerrors.NewConflict(gpustack.Resource(_SettingResource), set.Name, err)
		}
	}

	// 获取。
	return h.OnGet(ctx, ctrlcli.ObjectKeyFromObject(set),
		ctrlcli.GetOptions{
			Raw: &meta.GetOptions{
				ResourceVersion: "0",
			},
		})
}

func convertSecretListOptsFromSettingListOpts(in ctrlcli.ListOptions) (out *ctrlcli.ListOptions) {
	// 锁定字段选择器。
	in.FieldSelector = fields.SelectorFromSet(fields.Set{
		"metadata.namespace": in.Namespace,
		"metadata.name":      setting.DelegatedSecretName,
	})

	// 添加必要的标签选择器。
	if lbs := systemmeta.GetResourcesLabelSelectorOfType(_SettingResource); in.LabelSelector == nil {
		in.LabelSelector = lbs
	} else {
		reqs, _ := lbs.Requirements()
		in.LabelSelector = in.LabelSelector.DeepCopySelector().Add(reqs...)
	}

	return &in
}

func convertSettingFromSecret(sec *core.Secret, reqName string, index setting.IndexFunc) *gpustack.Setting {
	resType := systemmeta.DescribeResourceType(sec)
	if resType != _SettingResource {
		return nil
	}

	// 过滤掉。
	s, ok := index(reqName)
	if !ok || s.Private() || sec.Data == nil {
		return nil
	}

	uid := sec.UID
	if uidS := systemmeta.DescribeResourceNote(sec, reqName+"-uid"); len(uidS) != 0 {
		uid = types.UID(uidS)
	}
	var (
		value  = []byte("")
		value_ = sec.Data[reqName]
	)
	if len(value_) != 0 && s.Sensitive() {
		value = []byte("(sensitive)")
	} else if len(value_) != 0 {
		value = value_
	}

	set := &gpustack.Setting{
		ObjectMeta: meta.ObjectMeta{
			Namespace:         sec.Namespace,
			Name:              reqName,
			UID:               uid,
			ResourceVersion:   sec.ResourceVersion,
			CreationTimestamp: sec.CreationTimestamp,
			DeletionTimestamp: sec.DeletionTimestamp,
		},
		Status: gpustack.SettingStatus{
			Description: s.Description(),
			Editable:    s.Editable(),
			Sensitive:   s.Sensitive(),
			Value:       string(value),
			Value_:      string(value_),
		},
	}

	kubemeta.OverwriteLastAppliedAnnotation(set)
	return set
}

func convertSettingListFromSecret(sec *core.Secret, opts ctrlcli.ListOptions, index setting.IndexFunc) *gpustack.SettingList {
	resType, secNotes := systemmeta.DescribeResource(sec)
	if resType != _SettingResource {
		return &gpustack.SettingList{}
	}

	sList := &gpustack.SettingList{
		ListMeta: meta.ListMeta{
			ResourceVersion: sec.ResourceVersion,
		},
		Items: make([]gpustack.Setting, 0, len(sec.Data)),
	}

	// 按名称排序。
	for _, name := range sets.List(sets.KeySet(sec.Data)) {
		// 如果未被 `kubectl get --field-selector=metadata.name=...` 选中，则忽略。
		if !settingMatchFieldSelector(opts, name) {
			continue
		}

		// 过滤掉。
		s, ok := index(name)
		if !ok || s.Private() {
			continue
		}

		uid := sec.UID
		if uidS := secNotes[name+"-uid"]; len(uidS) != 0 {
			uid = types.UID(uidS)
		}
		var (
			value  = []byte("")
			value_ = sec.Data[name]
		)
		if len(value_) != 0 && s.Sensitive() {
			value = []byte("(sensitive)")
		} else if len(value_) != 0 {
			value = value_
		}

		set := &gpustack.Setting{
			ObjectMeta: meta.ObjectMeta{
				Namespace:         sec.Namespace,
				Name:              name,
				UID:               uid,
				ResourceVersion:   sec.ResourceVersion,
				CreationTimestamp: sec.CreationTimestamp,
				DeletionTimestamp: sec.DeletionTimestamp,
			},
			Status: gpustack.SettingStatus{
				Description: s.Description(),
				Editable:    s.Editable(),
				Sensitive:   s.Sensitive(),
				Value:       string(value),
				Value_:      string(value_),
			},
		}

		kubemeta.ConfigureLastAppliedAnnotation(set)
		sList.Items = append(sList.Items, *set)
	}

	return sList
}

// settingMatchFieldSelector 检查 Setting 是否匹配列表选项中的字段选择器。
func settingMatchFieldSelector(opts ctrlcli.ListOptions, name string) bool {
	fs := opts.FieldSelector
	if fs == nil {
		return true
	}
	return fs.Matches(fields.Set{"metadata.name": name})
}
