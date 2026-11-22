package worker

import (
	"context"

	core "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/registry/rest"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"

	worker "gpustack.ai/gpustack/api/worker/v1"
	"gpustack.ai/gpustack/pkg/extensionapi"
	"gpustack.ai/gpustack/pkg/systemmeta"
	"gpustack.ai/gpustack/pkg/utils/gox"
	"gpustack.ai/gpustack/pkg/utils/json"
	"gpustack.ai/gpustack/pkg/utils/stringx"
)

const _InstanceImagePullSecretResource = "instanceimagepullsecrets"

// InstanceImagePullSecretHandler 处理 v1.InstanceImagePullSecret 对象。
//
// InstanceImagePullSecretHandler 将 v1.InstanceImagePullSecret 映射为 Kubernetes Secret 资源，
// 该 Secret 与 InstanceImagePullSecret 同名。
type InstanceImagePullSecretHandler struct {
	extensionapi.ObjectInfo
	extensionapi.CurdOperations

	Client    ctrlcli.Client
	APIReader ctrlcli.Reader
}

func (h *InstanceImagePullSecretHandler) SetupHandler(
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
	gvr = worker.SchemeGroupVersionResource(_InstanceImagePullSecretResource)

	// 创建表格转换器以美化 kubectl 输出。
	tc, err := extensionapi.NewJSONPathTableConvertor()
	if err != nil {
		return gvr, srs, err
	}

	// 作为存储。
	h.ObjectInfo = &worker.InstanceImagePullSecret{}
	h.CurdOperations = extensionapi.WithCurd(tc, h)

	// 设置客户端。
	h.Client = opts.Manager.GetClient()
	h.APIReader = opts.Manager.GetAPIReader()

	return gvr, srs, err
}

var (
	_ rest.Storage           = (*InstanceImagePullSecretHandler)(nil)
	_ rest.Creater           = (*InstanceImagePullSecretHandler)(nil)
	_ rest.Lister            = (*InstanceImagePullSecretHandler)(nil)
	_ rest.Watcher           = (*InstanceImagePullSecretHandler)(nil)
	_ rest.Getter            = (*InstanceImagePullSecretHandler)(nil)
	_ rest.Updater           = (*InstanceImagePullSecretHandler)(nil)
	_ rest.Patcher           = (*InstanceImagePullSecretHandler)(nil)
	_ rest.GracefulDeleter   = (*InstanceImagePullSecretHandler)(nil)
	_ rest.CollectionDeleter = (*InstanceImagePullSecretHandler)(nil)

	_ extensionapi.DryRunHandler = (*InstanceImagePullSecretHandler)(nil)
)

func (h *InstanceImagePullSecretHandler) New() runtime.Object {
	return &worker.InstanceImagePullSecret{}
}

func (h *InstanceImagePullSecretHandler) Destroy() {
}

// SupportsDryRun 返回 true：OnCreate 和 OnUpdate 各自仅写入一次 Secret，并将请求选项透传给该写入操作。
func (h *InstanceImagePullSecretHandler) SupportsDryRun() bool {
	return true
}

func (h *InstanceImagePullSecretHandler) OnCreate(ctx context.Context, obj runtime.Object, opts ctrlcli.CreateOptions) (runtime.Object, error) {
	instImgPullSec := obj.(*worker.InstanceImagePullSecret)

	// 校验。
	if instImgPullSec.Spec.Registry == "" {
		return nil, field.Required(field.NewPath("spec.registry"), "registry is required")
	}
	if instImgPullSec.Spec.Username == "" {
		return nil, field.Required(field.NewPath("spec.username"), "username is required")
	}
	if instImgPullSec.Spec.Password == "" {
		return nil, field.Required(field.NewPath("spec.password"), "password is required")
	}

	// 创建。
	sec := convertSecretFromInstanceImagePullSecret(instImgPullSec)
	err := h.Client.Create(ctx, sec, &opts)
	if err != nil {
		return nil, err
	}

	instImgPullSec = convertInstanceImagePullSecretFromSecret(sec)
	return instImgPullSec, nil
}

func (h *InstanceImagePullSecretHandler) NewList() runtime.Object {
	return &worker.InstanceImagePullSecretList{}
}

func (h *InstanceImagePullSecretHandler) OnList(ctx context.Context, opts ctrlcli.ListOptions) (runtime.Object, error) {
	// 查询列表。
	secList := new(core.SecretList)
	err := h.APIReader.List(ctx, secList, &opts)
	if err != nil {
		return nil, err
	}

	// 转换。
	itList := convertInstanceImagePullSecretListFromSecretList(secList, opts)
	return itList, nil
}

func (h *InstanceImagePullSecretHandler) OnWatch(ctx context.Context, opts ctrlcli.ListOptions) (watch.Interface, error) {
	// 监听。
	uw, err := h.Client.(ctrlcli.WithWatch).Watch(ctx, new(core.SecretList), &opts)
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
				// 因 context 取消而退出。
				return
			case <-dw.StopChan():
				// 因下游停止。
				return
			case e, ok := <-uw.ResultChan():
				if !ok {
					// 因上游关闭。
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

				// 处理书签事件。
				if e.Type == watch.Bookmark {
					systemmeta.UnnoteResource(sec)
					e.Object = &worker.InstanceImagePullSecret{ObjectMeta: sec.ObjectMeta}
					c <- e
					continue
				}

				// 转换。
				instImgPullSec := convertInstanceImagePullSecretFromSecret(sec)
				if instImgPullSec == nil {
					continue
				}

				// 如果未被 `kubectl get --field-selector=metadata.namespace=...` 选中则忽略。
				if !instanceImagePullSecretMatchFieldSelector(opts, instImgPullSec) {
					continue
				}

				// 分发。
				e.Object = instImgPullSec
				c <- e
			}
		}
	})

	return dw, nil
}

func (h *InstanceImagePullSecretHandler) OnGet(ctx context.Context, key types.NamespacedName, opts ctrlcli.GetOptions) (runtime.Object, error) {
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
	instImgPullSec := convertInstanceImagePullSecretFromSecret(sec)
	if instImgPullSec == nil {
		return nil, kerrors.NewNotFound(worker.Resource(_InstanceImagePullSecretResource), key.Name)
	}

	return instImgPullSec, nil
}

func (h *InstanceImagePullSecretHandler) OnUpdate(
	ctx context.Context, obj, oldObj runtime.Object, opts ctrlcli.UpdateOptions,
) (runtime.Object, error) {
	instImgPullSec := obj.(*worker.InstanceImagePullSecret)

	// 校验。
	if instImgPullSec.Spec.Registry == "" {
		return nil, field.Required(field.NewPath("spec.registry"), "registry is required")
	}
	if instImgPullSec.Spec.Username == "" {
		return nil, field.Required(field.NewPath("spec.username"), "username is required")
	}
	if instImgPullSec.Spec.Password == "" {
		return nil, field.Required(field.NewPath("spec.password"), "password is required")
	}

	// 更新。
	{
		sec := convertSecretFromInstanceImagePullSecret(instImgPullSec)
		err := h.Client.Update(ctx, sec, &opts)
		if err != nil {
			return nil, err
		}
		instImgPullSec = convertInstanceImagePullSecretFromSecret(sec)
	}

	return instImgPullSec, nil
}

func (h *InstanceImagePullSecretHandler) OnDelete(ctx context.Context, obj runtime.Object, opts ctrlcli.DeleteOptions) error {
	instImgPullSec := obj.(*worker.InstanceImagePullSecret)

	// 删除。
	sec := &core.Secret{ObjectMeta: instImgPullSec.ObjectMeta}
	return h.Client.Delete(ctx, sec, &opts)
}

func convertSecretFromInstanceImagePullSecret(instImgPullSec *worker.InstanceImagePullSecret) *core.Secret {
	dcrCfg := map[string]any{
		"auths": map[string]any{
			instImgPullSec.Spec.Registry: func() (auth map[string]string) {
				auth = map[string]string{
					"auth": stringx.EncodeBase64(instImgPullSec.Spec.Username + ":" + instImgPullSec.Spec.Password),
				}
				if instImgPullSec.Spec.Email != "" {
					auth["email"] = instImgPullSec.Spec.Email
				}
				return auth
			}(),
		},
	}
	dcrCfgJson := json.MustMarshal(dcrCfg)
	// 示例:
	// {"auths":{"your.private.registry.example.com":{"email":"jdoe@example.com","auth":"c3R...zE2"}}}

	sec := &core.Secret{
		ObjectMeta: instImgPullSec.ObjectMeta,
		Type:       core.SecretTypeDockerConfigJson,
		StringData: map[string]string{
			core.DockerConfigJsonKey: stringx.FromBytes(&dcrCfgJson),
		},
	}

	systemmeta.NoteResource(sec, "", map[string]string{
		"displayName": instImgPullSec.Spec.DisplayName,
		"description": instImgPullSec.Spec.Description,
	})

	return sec
}

func convertInstanceImagePullSecretFromSecret(sec *core.Secret) *worker.InstanceImagePullSecret {
	if sec == nil {
		return nil
	}
	if sec.Type != core.SecretTypeDockerConfigJson {
		return nil
	}

	_, notes := systemmeta.UnnoteResource(sec)

	instImgPullSec := &worker.InstanceImagePullSecret{
		ObjectMeta: sec.ObjectMeta,
		Spec: worker.InstanceImagePullSecretSpec{
			DisplayName: notes["displayName"],
			Description: notes["description"],
		},
	}
	return instImgPullSec
}

func convertInstanceImagePullSecretListFromSecretList(secList *core.SecretList, opts ctrlcli.ListOptions) *worker.InstanceImagePullSecretList {
	if secList == nil {
		return &worker.InstanceImagePullSecretList{}
	}

	instImgPullSecList := &worker.InstanceImagePullSecretList{
		ListMeta: secList.ListMeta,
		Items:    make([]worker.InstanceImagePullSecret, 0, len(secList.Items)),
	}

	for i := range secList.Items {
		instImgPullSec := convertInstanceImagePullSecretFromSecret(&secList.Items[i])
		if instImgPullSec == nil {
			continue
		}

		// 如果未被 `kubectl get --field-selector=metadata.namespace=...` 选中则忽略。
		if !instanceImagePullSecretMatchFieldSelector(opts, instImgPullSec) {
			continue
		}

		instImgPullSecList.Items = append(instImgPullSecList.Items, *instImgPullSec)
	}

	return instImgPullSecList
}

// instanceImagePullSecretMatchFieldSelector 检查 InstanceImagePullSecret 是否匹配列表选项中的字段选择器。
func instanceImagePullSecretMatchFieldSelector(opts ctrlcli.ListOptions, instImgPullSec *worker.InstanceImagePullSecret) bool {
	fs := opts.FieldSelector
	if fs == nil {
		return true
	}
	return fs.Matches(fields.Set{"metadata.namespace": instImgPullSec.Namespace, "metadata.name": instImgPullSec.Name})
}
