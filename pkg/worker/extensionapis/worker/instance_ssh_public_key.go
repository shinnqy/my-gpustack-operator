package worker

import (
	"context"
	"strings"
	"unicode"

	core "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/registry/rest"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"

	worker "gpustack.ai/gpustack/api/worker/v1"
	"gpustack.ai/gpustack/pkg/extensionapi"
	"gpustack.ai/gpustack/pkg/systemmeta"
	"gpustack.ai/gpustack/pkg/utils/gox"
	"gpustack.ai/gpustack/pkg/utils/stringx"
)

const _InstanceSSHPublicKeyResource = "instancesshpublickeys"

// InstanceSSHPublicKeyHandler 处理 v1.InstanceSSHPublicKey 对象。
//
// InstanceSSHPublicKeyHandler 将 v1.InstanceSSHPublicKey 映射为一个 Kubernetes Secret 资源，
// 该 Secret 的名称与 InstanceSSHPublicKey 相同。
type InstanceSSHPublicKeyHandler struct {
	extensionapi.ObjectInfo
	extensionapi.CurdOperations

	Client    ctrlcli.Client
	APIReader ctrlcli.Reader
}

func (h *InstanceSSHPublicKeyHandler) SetupHandler(
	ctx context.Context,
	opts extensionapi.SetupOptions,
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
		return schema.GroupVersionResource{}, srs, err
	}

	// 声明 GVR。
	gvr = worker.SchemeGroupVersionResource(_InstanceSSHPublicKeyResource)

	// 创建表转换器以美化 kubectl 输出。
	tc, err := extensionapi.NewJSONPathTemplateTableConvertor(
		extensionapi.RenderColumn("Data", func(obj *worker.InstanceSSHPublicKey) string {
			const tailLen = 27
			data := obj.Spec.Data
			if len(data) > tailLen {
				data = "..." + data[len(data)-tailLen:]
			}
			return strings.TrimRightFunc(data, unicode.IsSpace)
		}),
	)
	if err != nil {
		return gvr, srs, err
	}

	// 作为存储。
	h.ObjectInfo = &worker.InstanceSSHPublicKey{}
	h.CurdOperations = extensionapi.WithCurd(tc, h)

	// 设置客户端。
	h.Client = opts.Manager.GetClient()
	h.APIReader = opts.Manager.GetAPIReader()

	return gvr, srs, err
}

var (
	_ rest.Storage           = (*InstanceSSHPublicKeyHandler)(nil)
	_ rest.Creater           = (*InstanceSSHPublicKeyHandler)(nil)
	_ rest.Lister            = (*InstanceSSHPublicKeyHandler)(nil)
	_ rest.Watcher           = (*InstanceSSHPublicKeyHandler)(nil)
	_ rest.Getter            = (*InstanceSSHPublicKeyHandler)(nil)
	_ rest.Updater           = (*InstanceSSHPublicKeyHandler)(nil)
	_ rest.Patcher           = (*InstanceSSHPublicKeyHandler)(nil)
	_ rest.GracefulDeleter   = (*InstanceSSHPublicKeyHandler)(nil)
	_ rest.CollectionDeleter = (*InstanceSSHPublicKeyHandler)(nil)

	_ extensionapi.DryRunHandler = (*InstanceSSHPublicKeyHandler)(nil)
)

func (h *InstanceSSHPublicKeyHandler) New() runtime.Object {
	return &worker.InstanceSSHPublicKey{}
}

func (h *InstanceSSHPublicKeyHandler) Destroy() {
}

// SupportsDryRun 返回 true：OnCreate 和 OnUpdate 各自写入一次 Secret，
// 并将选项透传给该写入操作。
func (h *InstanceSSHPublicKeyHandler) SupportsDryRun() bool {
	return true
}

func (h *InstanceSSHPublicKeyHandler) OnCreate(ctx context.Context, obj runtime.Object, opts ctrlcli.CreateOptions) (runtime.Object, error) {
	instSSHKey := obj.(*worker.InstanceSSHPublicKey)

	// 创建。
	sec := convertSecretFromInstanceSSHPublicKey(instSSHKey)
	err := h.Client.Create(ctx, sec, &opts)
	if err != nil {
		return nil, err
	}

	instSSHKey = convertInstanceSSHPublicKeyFromSecret(sec)
	return instSSHKey, nil
}

func (h *InstanceSSHPublicKeyHandler) NewList() runtime.Object {
	return &worker.InstanceSSHPublicKeyList{}
}

func (h *InstanceSSHPublicKeyHandler) OnList(ctx context.Context, opts ctrlcli.ListOptions) (runtime.Object, error) {
	// 列出。
	secList := new(core.SecretList)
	err := h.APIReader.List(ctx, secList,
		convertSecretListOptsFromInstanceSSHPublicKeyListOpts(opts))
	if err != nil {
		return nil, err
	}

	// 转换。
	instSSHKeyList := convertInstanceSSHPublicKeyListFromSecretList(secList, opts)
	return instSSHKeyList, nil
}

func (h *InstanceSSHPublicKeyHandler) OnWatch(ctx context.Context, opts ctrlcli.ListOptions) (watch.Interface, error) {
	// 监听。
	uw, err := h.Client.(ctrlcli.WithWatch).Watch(ctx, new(core.SecretList),
		convertSecretListOptsFromInstanceSSHPublicKeyListOpts(opts))
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
				// 上下文取消。
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

				// 处理 Bookmark 事件。
				if e.Type == watch.Bookmark {
					systemmeta.UnnoteResource(sec)
					e.Object = &worker.InstanceSSHPublicKey{ObjectMeta: sec.ObjectMeta}
					c <- e
					continue
				}

				// 转换。
				instSSHKey := convertInstanceSSHPublicKeyFromSecret(sec)
				if instSSHKey == nil {
					continue
				}

				// 若未被 `kubectl get --field-selector=metadata.namespace=...` 选中则忽略。
				if !instanceSSHPublicKeyMatchFieldSelector(opts, instSSHKey) {
					continue
				}

				// 分发。
				e.Object = instSSHKey
				c <- e
			}
		}
	})

	return dw, nil
}

func (h *InstanceSSHPublicKeyHandler) OnGet(ctx context.Context, key types.NamespacedName, opts ctrlcli.GetOptions) (runtime.Object, error) {
	// 获取。
	sec := &core.Secret{
		ObjectMeta: meta.ObjectMeta{
			Name:      key.Name,
			Namespace: key.Namespace,
		},
	}
	err := h.APIReader.Get(ctx, ctrlcli.ObjectKeyFromObject(sec), sec, &opts)
	if err != nil {
		return nil, err
	}

	// 转换。
	instSSHKey := convertInstanceSSHPublicKeyFromSecret(sec)
	if instSSHKey == nil {
		return nil, kerrors.NewNotFound(worker.Resource(_InstanceSSHPublicKeyResource), key.Name)
	}

	return instSSHKey, nil
}

func (h *InstanceSSHPublicKeyHandler) OnUpdate(
	ctx context.Context, obj, oldObj runtime.Object, opts ctrlcli.UpdateOptions,
) (runtime.Object, error) {
	instSSHKey := obj.(*worker.InstanceSSHPublicKey)

	// 更新。
	{
		sec := convertSecretFromInstanceSSHPublicKey(instSSHKey)
		err := h.Client.Update(ctx, sec, &opts)
		if err != nil {
			return nil, err
		}
		instSSHKey = convertInstanceSSHPublicKeyFromSecret(sec)
	}

	return instSSHKey, nil
}

func (h *InstanceSSHPublicKeyHandler) OnDelete(ctx context.Context, obj runtime.Object, opts ctrlcli.DeleteOptions) error {
	instSSHKey := obj.(*worker.InstanceSSHPublicKey)

	// 删除。
	sec := &core.Secret{ObjectMeta: instSSHKey.ObjectMeta}
	return h.Client.Delete(ctx, sec, &opts)
}

func convertSecretListOptsFromInstanceSSHPublicKeyListOpts(in ctrlcli.ListOptions) (out *ctrlcli.ListOptions) {
	// 添加必要的标签选择器。
	if lbs := systemmeta.GetResourcesLabelSelectorOfType(_InstanceSSHPublicKeyResource); in.LabelSelector == nil {
		in.LabelSelector = lbs
	} else {
		reqs, _ := lbs.Requirements()
		in.LabelSelector = in.LabelSelector.DeepCopySelector().Add(reqs...)
	}

	return &in
}

func convertSecretFromInstanceSSHPublicKey(instSSHKey *worker.InstanceSSHPublicKey) *core.Secret {
	sec := &core.Secret{
		ObjectMeta: instSSHKey.ObjectMeta,
		Data:       map[string][]byte{"authorized_keys": stringx.ToBytes(&instSSHKey.Spec.Data)},
	}

	systemmeta.NoteResource(sec, _InstanceSSHPublicKeyResource, map[string]string{
		"displayName": instSSHKey.Spec.DisplayName,
		"description": instSSHKey.Spec.Description,
	})

	return sec
}

func convertInstanceSSHPublicKeyFromSecret(sec *core.Secret) *worker.InstanceSSHPublicKey {
	if sec == nil {
		return nil
	}

	resType, notes := systemmeta.UnnoteResource(sec)
	if resType != _InstanceSSHPublicKeyResource {
		return nil
	}

	var secData string
	if v, ok := sec.Data["authorized_keys"]; ok {
		secData = stringx.FromBytes(&v)
	}

	return &worker.InstanceSSHPublicKey{
		ObjectMeta: sec.ObjectMeta,
		Spec: worker.InstanceSSHPublicKeySpec{
			DisplayName: notes["displayName"],
			Description: notes["description"],
			Data:        secData,
		},
	}
}

func convertInstanceSSHPublicKeyListFromSecretList(secList *core.SecretList, opts ctrlcli.ListOptions) *worker.InstanceSSHPublicKeyList {
	if secList == nil {
		return &worker.InstanceSSHPublicKeyList{}
	}

	instSSHKeyList := &worker.InstanceSSHPublicKeyList{
		ListMeta: secList.ListMeta,
		Items:    make([]worker.InstanceSSHPublicKey, 0, len(secList.Items)),
	}

	for i := range secList.Items {
		instSSHKey := convertInstanceSSHPublicKeyFromSecret(&secList.Items[i])
		if instSSHKey == nil {
			continue
		}

		// 若未被 `kubectl get --field-selector=metadata.namespace=...` 选中则忽略。
		if !instanceSSHPublicKeyMatchFieldSelector(opts, instSSHKey) {
			continue
		}

		instSSHKeyList.Items = append(instSSHKeyList.Items, *instSSHKey)
	}

	return instSSHKeyList
}

// instanceSSHPublicKeyMatchFieldSelector 检查 InstanceSSHPublicKey 是否匹配列表选项中的字段选择器。
func instanceSSHPublicKeyMatchFieldSelector(opts ctrlcli.ListOptions, instSSHKey *worker.InstanceSSHPublicKey) bool {
	fs := opts.FieldSelector
	if fs == nil {
		return true
	}
	return fs.Matches(fields.Set{"metadata.namespace": instSSHKey.Namespace, "metadata.name": instSSHKey.Name})
}
