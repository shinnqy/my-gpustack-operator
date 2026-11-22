package worker

import (
	"context"
	"fmt"

	core "k8s.io/api/core/v1"
	storage "k8s.io/api/storage/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/utils/ptr"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"

	worker "gpustack.ai/gpustack/api/worker/v1"
	"gpustack.ai/gpustack/pkg/extensionapi"
	"gpustack.ai/gpustack/pkg/systemmeta"
	"gpustack.ai/gpustack/pkg/utils/ctrlclix"
	"gpustack.ai/gpustack/pkg/utils/gox"
)

const (
	_InstancePersistentVolumeResource = "instancepersistentvolumes"
	_InstancePersistentVolumeKind     = "InstancePersistentVolume"
)

// InstancePersistentVolumeHandler 处理 v1.InstancePersistentVolume 对象。
//
// InstancePersistentVolumeHandler 将 v1.InstancePersistentVolume 映射到 Kubernetes PersistentVolumeClaim 资源，
// 该资源与 InstancePersistentVolume 同名。
type InstancePersistentVolumeHandler struct {
	extensionapi.ObjectInfo
	extensionapi.CurdOperations

	Client    ctrlcli.Client
	APIReader ctrlcli.Reader
}

func (h *InstancePersistentVolumeHandler) SetupHandler(
	ctx context.Context,
	opts extensionapi.SetupOptions,
) (gvr schema.GroupVersionResource, srs map[string]rest.Storage, err error) {
	// 配置字段索引器。
	fi := opts.Manager.GetFieldIndexer()
	err = fi.IndexField(ctx, &core.PersistentVolumeClaim{}, "metadata.name",
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
	gvr = worker.SchemeGroupVersionResource(_InstancePersistentVolumeResource)

	// 创建表格转换器以美化 kubectl 的输出。
	tc, err := extensionapi.NewJSONPathTableConvertor(
		extensionapi.JSONPathColumn("Type", ".spec.type"),
		extensionapi.JSONPathColumn("Capacity", ".spec.capacity"),
		extensionapi.JSONPathColumn("Access-Mode", ".spec.accessMode"),
		extensionapi.JSONPathColumn("Reclaim-Policy", ".spec.reclaimPolicy"),
		extensionapi.JSONPathColumn("Phase", ".status.phase"),
	)
	if err != nil {
		return gvr, srs, err
	}

	// 作为存储。
	h.ObjectInfo = &worker.InstancePersistentVolume{}
	h.CurdOperations = extensionapi.WithCurd(tc, h)

	// 设置客户端。
	h.Client = opts.Manager.GetClient()
	h.APIReader = opts.Manager.GetAPIReader()

	// 创建子资源处理器。
	srs = map[string]rest.Storage{
		"events": newInstancePersistentVolumeEventsHandler(h.ObjectInfo, opts),
	}

	return gvr, srs, err
}

var (
	_ rest.Storage           = (*InstancePersistentVolumeHandler)(nil)
	_ rest.Creater           = (*InstancePersistentVolumeHandler)(nil)
	_ rest.Lister            = (*InstancePersistentVolumeHandler)(nil)
	_ rest.Watcher           = (*InstancePersistentVolumeHandler)(nil)
	_ rest.Getter            = (*InstancePersistentVolumeHandler)(nil)
	_ rest.Updater           = (*InstancePersistentVolumeHandler)(nil)
	_ rest.Patcher           = (*InstancePersistentVolumeHandler)(nil)
	_ rest.GracefulDeleter   = (*InstancePersistentVolumeHandler)(nil)
	_ rest.CollectionDeleter = (*InstancePersistentVolumeHandler)(nil)

	_ extensionapi.DryRunHandler = (*InstancePersistentVolumeHandler)(nil)
)

func (h *InstancePersistentVolumeHandler) New() runtime.Object {
	return &worker.InstancePersistentVolume{}
}

func (h *InstancePersistentVolumeHandler) Destroy() {
}

// SupportsDryRun 返回 true：OnCreate 和 OnUpdate 各自只写入一次 PersistentVolumeClaim，
// 并将选项传递给该写入操作。
func (h *InstancePersistentVolumeHandler) SupportsDryRun() bool {
	return true
}

const (
	_IsDefaultStorageClassAnnotation     = "storageclass.kubernetes.io/is-default-class"
	_BetaIsDefaultStorageClassAnnotation = "storageclass.beta.kubernetes.io/is-default-class"
)

func (h *InstancePersistentVolumeHandler) OnCreate(ctx context.Context, obj runtime.Object, opts ctrlcli.CreateOptions) (runtime.Object, error) {
	instPV := obj.(*worker.InstancePersistentVolume)

	// 校验。
	var stgClass *storage.StorageClass
	if ptr.Deref(instPV.Spec.Type, "") == "" {
		stgClassList := new(storage.StorageClassList)
		err := h.Client.List(ctx, stgClassList,
			ctrlclix.WithoutQuorum)
		if err != nil {
			return nil, field.InternalError(
				field.NewPath("spec.type"), fmt.Errorf("list storage classes: %w", err),
			)
		}
		for i := range stgClassList.Items {
			if len(stgClassList.Items[i].Annotations) == 0 {
				continue
			}
			if stgClassList.Items[i].Annotations[_IsDefaultStorageClassAnnotation] == "true" ||
				stgClassList.Items[i].Annotations[_BetaIsDefaultStorageClassAnnotation] == "true" {
				stgClass = &stgClassList.Items[i]
				break
			}
		}
		if stgClass == nil {
			return nil, field.Required(
				field.NewPath("spec.type"), "type is required when there is no default storage class",
			)
		}
	} else {
		stgClass = new(storage.StorageClass)
		err := h.Client.Get(ctx, types.NamespacedName{Name: *instPV.Spec.Type}, stgClass)
		if err != nil {
			if !kerrors.IsNotFound(err) {
				return nil, field.InternalError(
					field.NewPath("spec.type"), fmt.Errorf("get storage class: %w", err),
				)
			}
			return nil, field.NotFound(
				field.NewPath("spec.type"), *instPV.Spec.Type,
			)
		}
	}

	// 设置默认值。
	if instPV.Spec.Capacity.IsZero() {
		instPV.Spec.Capacity = *resource.NewQuantity(20*1<<30, resource.BinarySI)
	}
	if instPV.Spec.AccessMode == nil {
		if ptr.Deref(stgClass.VolumeBindingMode, storage.VolumeBindingImmediate) == storage.VolumeBindingImmediate {
			if ptr.Deref(stgClass.AllowVolumeExpansion, false) {
				instPV.Spec.AccessMode = ptr.To(core.ReadWriteOnce)
			} else {
				instPV.Spec.AccessMode = ptr.To(core.ReadWriteMany)
			}
		} else {
			instPV.Spec.AccessMode = ptr.To(core.ReadWriteOnce)
		}
	}

	// 创建。
	pvc := convertPersistentVolumeClaimFromInstancePersistentVolume(instPV)
	err := h.Client.Create(ctx, pvc, &opts)
	if err != nil {
		return nil, err
	}

	instPV = convertInstancePersistentVolumeFromPersistentVolumeClaim(pvc)
	return instPV, nil
}

func (h *InstancePersistentVolumeHandler) NewList() runtime.Object {
	return &worker.InstancePersistentVolumeList{}
}

func (h *InstancePersistentVolumeHandler) OnList(ctx context.Context, opts ctrlcli.ListOptions) (runtime.Object, error) {
	// 列出。
	pvcList := new(core.PersistentVolumeClaimList)
	err := h.APIReader.List(ctx, pvcList,
		convertPersistentVolumeClaimListOptsFromInstancePersistentVolumeListOpts(opts))
	if err != nil {
		return nil, err
	}

	// 转换。
	itList := convertInstancePersistentVolumeListFromPersistentVolumeClaimList(pvcList, opts)
	return itList, nil
}

func (h *InstancePersistentVolumeHandler) OnWatch(ctx context.Context, opts ctrlcli.ListOptions) (watch.Interface, error) {
	// 监听。
	uw, err := h.Client.(ctrlcli.WithWatch).Watch(ctx, new(core.PersistentVolumeClaimList),
		convertPersistentVolumeClaimListOptsFromInstancePersistentVolumeListOpts(opts))
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
				// 由上下文取消。
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
				pvc, ok := e.Object.(*core.PersistentVolumeClaim)
				if !ok {
					c <- e
					continue
				}

				// 处理 bookmark 事件。
				if e.Type == watch.Bookmark {
					systemmeta.UnnoteResource(pvc)
					e.Object = &worker.InstancePersistentVolume{ObjectMeta: pvc.ObjectMeta}
					c <- e
					continue
				}

				// 转换。
				instPV := convertInstancePersistentVolumeFromPersistentVolumeClaim(pvc)
				if instPV == nil {
					continue
				}

				// 如果未被 `kubectl get --field-selector=metadata.namespace=...` 选中则忽略。
				if !instancePersistentVolumeMatchFieldSelector(opts, instPV) {
					continue
				}

				// 分发。
				e.Object = instPV
				c <- e
			}
		}
	})

	return dw, nil
}

func (h *InstancePersistentVolumeHandler) OnGet(ctx context.Context, key types.NamespacedName, opts ctrlcli.GetOptions) (runtime.Object, error) {
	// 获取。
	pvc := &core.PersistentVolumeClaim{
		ObjectMeta: meta.ObjectMeta{
			Name:      key.Name,
			Namespace: key.Namespace,
		},
	}
	err := h.APIReader.Get(ctx, ctrlcli.ObjectKeyFromObject(pvc), pvc, &opts)
	if err != nil {
		return nil, err
	}

	// 转换。
	instPV := convertInstancePersistentVolumeFromPersistentVolumeClaim(pvc)
	if instPV == nil {
		return nil, kerrors.NewNotFound(worker.Resource(_InstancePersistentVolumeResource), key.Name)
	}

	return instPV, nil
}

func (h *InstancePersistentVolumeHandler) OnUpdate(
	ctx context.Context, obj, oldObj runtime.Object, opts ctrlcli.UpdateOptions,
) (runtime.Object, error) {
	instPV, instPVOld := obj.(*worker.InstancePersistentVolume), oldObj.(*worker.InstancePersistentVolume)

	// 校验。
	var errs field.ErrorList
	if !ptr.Equal(instPV.Spec.Type, instPVOld.Spec.Type) {
		errs = append(errs, field.Forbidden(
			field.NewPath("spec.type"), "field is immutable",
		))
	}
	if !instPV.Spec.Capacity.Equal(instPVOld.Spec.Capacity) {
		errs = append(errs, field.Forbidden(
			field.NewPath("spec.capacity"), "field is immutable",
		))
	}
	if !ptr.Equal(instPV.Spec.AccessMode, instPVOld.Spec.AccessMode) {
		errs = append(errs, field.Forbidden(
			field.NewPath("spec.accessMode"), "field is immutable",
		))
	}
	if len(errs) > 0 {
		return nil, kerrors.NewInvalid(worker.Kind("InstancePersistentVolume"), instPV.Name, errs)
	}

	// 更新。
	oldPvc := new(core.PersistentVolumeClaim)
	err := h.APIReader.Get(ctx, ctrlcli.ObjectKeyFromObject(instPV), oldPvc,
		ctrlclix.WithoutQuorum)
	if err != nil {
		return nil, err
	}

	pvc := oldPvc.DeepCopy()
	pvc.ObjectMeta = instPV.ObjectMeta
	systemmeta.NoteResource(pvc, _InstancePersistentVolumeResource, map[string]string{
		"displayName": instPV.Spec.DisplayName,
		"description": instPV.Spec.Description,
	})

	err = h.Client.Patch(ctx, pvc, ctrlcli.MergeFrom(oldPvc),
		ctrlclix.ToPatchOptions(opts))
	if err != nil {
		return nil, kerrors.NewInternalError(fmt.Errorf("update corresponding persistent volume claim: %w", err))
	}

	instPV = convertInstancePersistentVolumeFromPersistentVolumeClaim(pvc)
	return instPV, nil
}

func (h *InstancePersistentVolumeHandler) OnDelete(ctx context.Context, obj runtime.Object, opts ctrlcli.DeleteOptions) error {
	instPV := obj.(*worker.InstancePersistentVolume)

	// 删除。
	pvc := &core.PersistentVolumeClaim{ObjectMeta: instPV.ObjectMeta}
	return h.Client.Delete(ctx, pvc, &opts)
}

func convertPersistentVolumeClaimListOptsFromInstancePersistentVolumeListOpts(in ctrlcli.ListOptions) (out *ctrlcli.ListOptions) {
	// 添加必要的标签选择器。
	if lbs := systemmeta.GetResourcesLabelSelectorOfType(_InstancePersistentVolumeResource); in.LabelSelector == nil {
		in.LabelSelector = lbs
	} else {
		reqs, _ := lbs.Requirements()
		in.LabelSelector = in.LabelSelector.DeepCopySelector().Add(reqs...)
	}

	return &in
}

func convertPersistentVolumeClaimFromInstancePersistentVolume(instPV *worker.InstancePersistentVolume) *core.PersistentVolumeClaim {
	pvc := &core.PersistentVolumeClaim{
		ObjectMeta: instPV.ObjectMeta,
		Spec: core.PersistentVolumeClaimSpec{
			StorageClassName: instPV.Spec.Type,
			AccessModes:      []core.PersistentVolumeAccessMode{*instPV.Spec.AccessMode},
			Resources: core.VolumeResourceRequirements{
				Requests: core.ResourceList{
					core.ResourceStorage: instPV.Spec.Capacity,
				},
			},
		},
	}

	systemmeta.NoteResource(pvc, _InstancePersistentVolumeResource, map[string]string{
		"displayName": instPV.Spec.DisplayName,
		"description": instPV.Spec.Description,
	})

	return pvc
}

func convertInstancePersistentVolumeFromPersistentVolumeClaim(pvc *core.PersistentVolumeClaim) *worker.InstancePersistentVolume {
	if pvc == nil {
		return nil
	}

	resType, notes := systemmeta.UnnoteResource(pvc)
	if resType != _InstancePersistentVolumeResource {
		return nil
	}

	var phase string
	switch pvc.Status.Phase {
	case core.ClaimBound:
		phase = "Available"
	case core.ClaimLost:
		phase = "Unavailable"
	default:
		phase = "Pending"
	}

	return &worker.InstancePersistentVolume{
		ObjectMeta: pvc.ObjectMeta,
		Spec: worker.InstancePersistentVolumeSpec{
			DisplayName: notes["displayName"],
			Description: notes["description"],
			Type:        pvc.Spec.StorageClassName,
			Capacity:    pvc.Spec.Resources.Requests[core.ResourceStorage],
			AccessMode:  &pvc.Spec.AccessModes[0],
		},
		Status: worker.InstancePersistentVolumeStatus{
			Phase: phase,
			Volume: func() *core.ObjectReference {
				if pvc.Spec.VolumeName == "" {
					return nil
				}
				return &core.ObjectReference{
					Name: pvc.Spec.VolumeName,
				}
			}(),
		},
	}
}

func convertInstancePersistentVolumeListFromPersistentVolumeClaimList(
	pvcList *core.PersistentVolumeClaimList, opts ctrlcli.ListOptions,
) *worker.InstancePersistentVolumeList {
	if pvcList == nil {
		return &worker.InstancePersistentVolumeList{}
	}

	instPVList := &worker.InstancePersistentVolumeList{
		ListMeta: pvcList.ListMeta,
		Items:    make([]worker.InstancePersistentVolume, 0, len(pvcList.Items)),
	}

	for i := range pvcList.Items {
		instPV := convertInstancePersistentVolumeFromPersistentVolumeClaim(&pvcList.Items[i])
		if instPV == nil {
			continue
		}

		// 如果未被 `kubectl get --field-selector=metadata.namespace=...` 选中则忽略。
		if !instancePersistentVolumeMatchFieldSelector(opts, instPV) {
			continue
		}

		instPVList.Items = append(instPVList.Items, *instPV)
	}

	return instPVList
}

func instancePersistentVolumeMatchFieldSelector(opts ctrlcli.ListOptions, instPV *worker.InstancePersistentVolume) bool {
	fs := opts.FieldSelector
	if fs == nil {
		return true
	}
	return fs.Matches(fields.Set{"metadata.namespace": instPV.Namespace, "metadata.name": instPV.Name})
}
