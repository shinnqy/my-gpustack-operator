package extensionapi

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	kerrors "k8s.io/apimachinery/pkg/api/errors"
	kmeta "k8s.io/apimachinery/pkg/api/meta"
	metainternal "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/endpoints/handlers/fieldmanager"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/apiserver/pkg/util/dryrun"
	"k8s.io/utils/ptr"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlapiutil "sigs.k8s.io/controller-runtime/pkg/client/apiutil"

	"gpustack.ai/gpustack/pkg/utils/funcx"
	"gpustack.ai/gpustack/pkg/utils/gox"
)

// DryRunHandler 是处理器的接口，其 OnCreate 和 OnUpdate 会将 dry-run 选项传递给所执行的每次写操作。
//
// 仅当处理器支持 dry-run 时，dry-run 创建或更新请求才会进入 OnCreate 或 OnUpdate：
// 其后端的 API Server 随后会执行包含 Webhook 在内的准入控制且不持久化任何数据，
// 因此在真实请求会被拒绝的场景下，dry-run 请求同样会被拒绝。对于其他处理器，
// dry-run 会在到达 OnCreate 或 OnUpdate 之前停止并跳过该准入控制。这是必需的：
// 若其中的写操作丢弃了该选项，就会将调用方仅请求校验的内容持久化。
type DryRunHandler interface {
	// SupportsDryRun 返回 OnCreate 和 OnUpdate 是否将 dry-run 选项传递给每次写操作。
	SupportsDryRun() bool
}

func supportsDryRun(h any) bool {
	drh, ok := h.(DryRunHandler)
	return ok && drh.SupportsDryRun()
}

type (
	// CreateHandler 是创建处理器的接口。
	CreateHandler interface {
		rest.Scoper
		// New 返回一个新对象。
		New() runtime.Object
		// OnCreate 使用给定选项创建对象，
		// 并返回创建后的对象。
		OnCreate(ctx context.Context, object runtime.Object, opts ctrlcli.CreateOptions) (created runtime.Object, err error)
	}

	// CreateOperation 实现了 rest.Creater 接口。
	CreateOperation struct {
		Handler CreateHandler
	}
)

var _ rest.Creater = (*CreateOperation)(nil)

// WithCreate 返回给定 CreateHandler 对应的 CreateOperation。
func WithCreate(h CreateHandler) CreateOperation {
	return CreateOperation{Handler: h}
}

func (s CreateOperation) New() runtime.Object {
	return s.Handler.New()
}

func (s CreateOperation) Create(
	ctx context.Context,
	obj runtime.Object,
	createValidation rest.ValidateObjectFunc,
	options *meta.CreateOptions,
) (runtime.Object, error) {
	if options == nil {
		options = &meta.CreateOptions{}
	}

	om, err := kmeta.Accessor(obj)
	if err != nil {
		return nil, kerrors.NewInternalError(err)
	}
	rest.FillObjectMetaSystemFields(om)
	if gn := om.GetGenerateName(); len(gn) > 0 && len(om.GetName()) == 0 {
		om.SetName(names.SimpleNameGenerator.GenerateName(gn))
	}

	if createValidation != nil {
		err = createValidation(ctx, obj.DeepCopyObject())
		if err != nil {
			return nil, err
		}
	}

	if dryrun.IsDryRun(options.DryRun) && !supportsDryRun(s.Handler) {
		getter, ok := s.Handler.(rest.Getter)
		if !ok {
			// 若处理器不支持 get 操作，则无法检查对象是否存在。
			return obj, nil
		}

		_, err = getter.Get(ctx, om.GetName(),
			&meta.GetOptions{
				ResourceVersion: "0",
			})
		if err != nil {
			if !kerrors.IsNotFound(err) {
				return nil, wrapError(ctx, om.GetName(), err)
			}
			return obj, nil
		}

		return nil, kerrors.NewAlreadyExists(qualifiedResourceFromContext(ctx), om.GetName())
	}

	obj, err = s.Handler.OnCreate(ctx, obj, convertCtrlCreateOptionsFromMeta(options))
	if err != nil {
		return nil, wrapError(ctx, om.GetName(), err)
	}
	return obj, nil
}

type (
	// ListHandler 是列表查询处理器的接口。
	ListHandler interface {
		// NewList 返回一个可用于 List 调用的空对象。
		NewList() runtime.Object
		// OnList 返回给定选项对应的对象列表集合。
		OnList(ctx context.Context, opts ctrlcli.ListOptions) (list runtime.Object, err error)
	}

	// ListOperation 实现了 rest.Lister 接口。
	ListOperation struct {
		WatchBookmark  WatchBookmark
		Handler        ListHandler
		TableConvertor rest.TableConvertor
	}
)

var _ rest.Lister = (*ListOperation)(nil)

// WithList 返回给定 rest.TableConvertor 和 ListHandler 对应的 ListOperation。
func WithList(tc rest.TableConvertor, h ListHandler) ListOperation {
	if tc == nil {
		tc = NewDefaultTableConvertor()
	}
	return ListOperation{
		WatchBookmark:  NewWatchBookmark(),
		Handler:        h,
		TableConvertor: tc,
	}
}

func (s ListOperation) ConvertToTable(ctx context.Context, object, tableOptions runtime.Object) (*meta.Table, error) {
	return s.TableConvertor.ConvertToTable(ctx, object, tableOptions)
}

func (s ListOperation) NewList() runtime.Object {
	return s.Handler.NewList()
}

func (s ListOperation) List(
	ctx context.Context,
	options *metainternal.ListOptions,
) (runtime.Object, error) {
	if options == nil {
		options = &metainternal.ListOptions{ResourceVersion: "0"}
	}

	listObj, err := s.Handler.OnList(ctx, convertCtrlListOptionsFromMeta(ctx, options))
	if err != nil {
		return nil, wrapError(ctx, "", err)
	}

	if listObj == nil {
		listObj = s.Handler.NewList()
	} else {
		// 尝试从列表元数据中获取 resource version。
		var rv string
		lm, err := kmeta.ListAccessor(listObj)
		if err == nil {
			rv = lm.GetResourceVersion()
		}
		// 否则，尝试从最后一个列表项中获取 resource version。
		if rv == "" {
			li, _ := kmeta.ExtractList(listObj)
			if len(li) != 0 {
				om, err := kmeta.Accessor(li[len(li)-1])
				if err == nil {
					rv = om.GetResourceVersion()
				}
			}
		}
		if rv != "" {
			s.WatchBookmark.SwapResourceVersion(rv)
		}
	}

	return listObj, nil
}

type (
	// WatchHandler 是监听处理器的接口。
	WatchHandler interface {
		// OnWatch 返回用于监听对象列表的 watch.Interface。
		OnWatch(ctx context.Context, opts ctrlcli.ListOptions) (watch.Interface, error)
	}

	// ListWatchHandler 是列表查询处理器的接口。
	ListWatchHandler interface {
		ListHandler
		WatchHandler
	}

	// ListWatchOperation 实现了 rest.Lister 和 rest.Watcher 接口。
	ListWatchOperation struct {
		ListOperation
		Handler WatchHandler
	}
)

var (
	_ rest.Lister  = (*ListWatchOperation)(nil)
	_ rest.Watcher = (*ListWatchOperation)(nil)
)

// WithListWatch 返回给定 rest.TableConvertor 和 ListWatchHandler 对应的 ListWatchOperation。
func WithListWatch(tc rest.TableConvertor, h ListWatchHandler) ListWatchOperation {
	return ListWatchOperation{
		ListOperation: WithList(tc, h),
		Handler:       h,
	}
}

func (s ListWatchOperation) Watch(
	ctx context.Context,
	options *metainternal.ListOptions,
) (watch.Interface, error) {
	if options == nil {
		options = &metainternal.ListOptions{ResourceVersion: "0", Watch: true}
	}

	uw, err := s.Handler.OnWatch(ctx, convertCtrlListOptionsFromMeta(ctx, options))
	if err != nil {
		return nil, wrapError(ctx, "", err)
	}

	wm := s.WatchBookmark.DeepCopy()
	c := make(chan watch.Event)
	dw := watch.NewProxyWatcher(c)
	gox.Go(func() {
		defer close(c)
		defer uw.Stop()

		for {
			select {
			case <-ctx.Done():
				// 由 context 取消。
				return
			case <-dw.StopChan():
				// 由下游取消。
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

				// 记录资源版本号。
				om, err := kmeta.Accessor(e.Object)
				if err == nil {
					// 如果 resource version 未更新，
					// 且事件对应的对象未被删除，则忽略该事件。
					swp := wm.SwapResourceVersion(om.GetResourceVersion())
					if !swp && om.GetDeletionTimestamp() == nil {
						continue
					}
				}

				c <- e
			}
		}
	})

	return dw, nil
}

type (
	// GetHandler 是获取处理器的接口。
	GetHandler interface {
		rest.Scoper
		// OnGet 返回给定键和选项对应的对象。
		OnGet(ctx context.Context, key types.NamespacedName, opts ctrlcli.GetOptions) (object runtime.Object, err error)
	}

	// GetOperation 实现了 rest.Getter 接口。
	GetOperation struct {
		Handler GetHandler
	}
)

var _ rest.Getter = (*GetOperation)(nil)

// WithGet 返回给定 GetHandler 对应的 GetOperation。
func WithGet(h GetHandler) GetOperation {
	return GetOperation{Handler: h}
}

type (
	// SubResourceGetHandler 是子资源获取处理器的接口。
	SubResourceGetHandler interface {
		// OnGet 返回给定键和选项对应的对象。
		OnGet(ctx context.Context, key types.NamespacedName, opts ctrlcli.GetOptions) (object runtime.Object, err error)
	}

	subResourceGetHandlerAdapter struct {
		rest.Scoper
		SubResourceGetHandler
	}
)

// WithSubResourceGet 返回给定 SubResourceGetHandler 对应的 GetOperation。
func WithSubResourceGet(parent rest.Scoper, h SubResourceGetHandler) GetOperation {
	return GetOperation{
		Handler: subResourceGetHandlerAdapter{
			Scoper:                parent,
			SubResourceGetHandler: h,
		},
	}
}

func (s GetOperation) Get(
	ctx context.Context,
	name string,
	options *meta.GetOptions,
) (runtime.Object, error) {
	if options == nil {
		options = &meta.GetOptions{
			ResourceVersion: "0",
		}
	}

	keyFunc := KeyFuncForClusterScope
	if s.Handler.NamespaceScoped() {
		keyFunc = KeyFuncForNamespacedScope
	}
	key, err := keyFunc(ctx, name)
	if err != nil {
		return nil, err
	}

	obj, err := s.Handler.OnGet(ctx, key, convertCtrlGetOptionsFromMeta(options))
	if err != nil {
		return nil, wrapError(ctx, name, err)
	}
	return obj, nil
}

type (
	// UpdateHandler 是更新处理器的接口。
	UpdateHandler interface {
		GetHandler
		// New 返回一个新对象。
		New() runtime.Object
		// OnUpdate 使用给定选项更新对象，
		// 并返回更新后的对象。
		OnUpdate(ctx context.Context, object, oldObject runtime.Object, opts ctrlcli.UpdateOptions) (updated runtime.Object, err error)
	}

	// BeforeUpdateFunc 是在更新前调用的函数。
	BeforeUpdateFunc = func(ctx context.Context, object, existing runtime.Object) (runtime.Object, error)

	// UpdateOperation 实现了 rest.Updater 接口。
	UpdateOperation struct {
		Handler      UpdateHandler
		BeforeUpdate BeforeUpdateFunc
	}
)

var _ rest.Updater = (*UpdateOperation)(nil)

// WithUpdate 返回给定 UpdateHandler 对应的 UpdateOperation。
func WithUpdate(h UpdateHandler) UpdateOperation {
	var bf BeforeUpdateFunc
	if _, ok := h.New().(ObjectWithStatusSubResource); ok {
		bf = beforeUpdateFuncForPreventStatusModify
	}
	return UpdateOperation{
		Handler:      h,
		BeforeUpdate: bf,
	}
}

func (s UpdateOperation) New() runtime.Object {
	return s.Handler.New()
}

func (s UpdateOperation) Update(
	ctx context.Context,
	name string,
	objInfo rest.UpdatedObjectInfo,
	createValidation rest.ValidateObjectFunc,
	updateValidation rest.ValidateObjectUpdateFunc,
	forceAllowCreate bool,
	options *meta.UpdateOptions,
) (runtime.Object, bool, error) {
	if options == nil {
		options = &meta.UpdateOptions{}
	}

	keyFunc := KeyFuncForClusterScope
	if s.Handler.NamespaceScoped() {
		keyFunc = KeyFuncForNamespacedScope
	}
	key, err := keyFunc(ctx, name)
	if err != nil {
		return nil, false, err
	}

	var creating bool
	existing, err := s.Handler.OnGet(ctx, key,
		ctrlcli.GetOptions{
			Raw: &meta.GetOptions{
				ResourceVersion: "0",
			},
		})
	if err != nil {
		if !kerrors.IsNotFound(err) {
			return nil, false, wrapError(ctx, name, err)
		}
		if !forceAllowCreate {
			return nil, false, kerrors.NewNotFound(qualifiedResourceFromContext(ctx), name)
		}
		existing = s.Handler.New()
		creating = true
	}

	obj, err := objInfo.UpdatedObject(ctx, existing)
	if err != nil {
		return nil, false, err
	}

	if creating {
		createHandler, ok := s.Handler.(CreateHandler)
		if !ok {
			// 若处理器不支持 get 操作，则直接返回错误。
			return nil, false, kerrors.NewNotFound(qualifiedResourceFromContext(ctx), name)
		}

		creator := WithCreate(createHandler)
		obj, err = creator.Create(ctx, obj, createValidation, newCreateOptionsFromUpdateOptions(options))
		if err != nil {
			return nil, false, err
		}
		return obj, true, nil
	}

	om, err := kmeta.Accessor(obj)
	if err != nil {
		return nil, false, kerrors.NewInternalError(err)
	}

	if om.GetResourceVersion() == "0" {
		gk := qualifiedKindFromContext(ctx)
		errs := field.ErrorList{field.Invalid(
			field.NewPath("metadata.resourceVersion"), 0, "must be specified for an update",
		)}
		return nil, false, kerrors.NewInvalid(gk, name, errs)
	}

	if s.BeforeUpdate != nil {
		obj, err = s.BeforeUpdate(ctx, obj, existing)
		if err != nil {
			return nil, false, kerrors.NewInternalError(err)
		}
	}

	obj, err = fieldmanager.IgnoreManagedFieldsTimestampsTransformer(ctx, obj, existing)
	if err != nil {
		return nil, false, err
	}

	if updateValidation != nil {
		err = updateValidation(ctx, obj.DeepCopyObject(), existing.DeepCopyObject())
		if err != nil {
			return nil, false, err
		}
	}

	if dryrun.IsDryRun(options.DryRun) && !supportsDryRun(s.Handler) {
		return obj, false, nil
	}

	obj, err = s.Handler.OnUpdate(ctx, obj, existing, convertCtrlUpdateOptionsFromMeta(options))
	if err != nil {
		return nil, false, wrapError(ctx, name, err)
	}
	return obj, false, nil
}

func (s UpdateOperation) GetStatusSubResourceUpdater() UpdateOperation {
	s.BeforeUpdate = beforeUpdateFuncForStatusModifyOnly
	return s
}

type (
	// CreateUpdateHandler 是创建和更新处理器的接口。
	CreateUpdateHandler interface {
		GetHandler
		// New 返回一个新对象。
		New() runtime.Object
		// OnCreate 使用给定选项创建对象，
		// 并返回创建后的对象。
		OnCreate(ctx context.Context, object runtime.Object, opts ctrlcli.CreateOptions) (created runtime.Object, err error)
		// OnUpdate 使用给定选项更新对象，
		// 并返回更新后的对象。
		OnUpdate(ctx context.Context, object, oldObject runtime.Object, opts ctrlcli.UpdateOptions) (updated runtime.Object, err error)
	}

	// CreateUpdateOperation 实现了 rest.Creater 和 rest.Updater 接口。
	CreateUpdateOperation struct {
		UpdateOperation
		CreateOperation CreateOperation
	}
)

var (
	_ rest.Creater = (*CreateUpdateOperation)(nil)
	_ rest.Updater = (*CreateUpdateOperation)(nil)
)

// WithCreateUpdate 返回给定 CreateUpdateHandler 对应的 CreateUpdateOperation。
func WithCreateUpdate(h CreateUpdateHandler) CreateUpdateOperation {
	return CreateUpdateOperation{
		UpdateOperation: WithUpdate(h),
		CreateOperation: WithCreate(h),
	}
}

func (s CreateUpdateOperation) Create(
	ctx context.Context,
	obj runtime.Object,
	createValidation rest.ValidateObjectFunc,
	options *meta.CreateOptions,
) (runtime.Object, error) {
	return s.CreateOperation.Create(ctx, obj, createValidation, options)
}

func (s CreateUpdateOperation) Update(
	ctx context.Context,
	name string,
	objInfo rest.UpdatedObjectInfo,
	createValidation rest.ValidateObjectFunc,
	updateValidation rest.ValidateObjectUpdateFunc,
	forceAllowCreate bool,
	options *meta.UpdateOptions,
) (runtime.Object, bool, error) {
	return s.UpdateOperation.Update(ctx, name, objInfo, createValidation, updateValidation, forceAllowCreate, options)
}

type (
	// DeleteHandler 是删除处理器的接口。
	DeleteHandler = interface {
		GetHandler
		// OnDelete 使用给定选项删除对象。
		OnDelete(ctx context.Context, object runtime.Object, opts ctrlcli.DeleteOptions) error
	}

	// DeleteOperation 实现了 rest.Deleter 接口。
	DeleteOperation struct {
		Handler DeleteHandler
	}
)

var _ rest.GracefulDeleter = (*DeleteOperation)(nil)

// WithDelete 返回给定 DeleteHandler 对应的 DeleteOperation。
func WithDelete(h DeleteHandler) DeleteOperation {
	return DeleteOperation{
		Handler: h,
	}
}

func (s DeleteOperation) Delete(
	ctx context.Context,
	name string,
	deleteValidation rest.ValidateObjectFunc,
	options *meta.DeleteOptions,
) (runtime.Object, bool, error) {
	if options == nil {
		options = &meta.DeleteOptions{GracePeriodSeconds: ptr.To[int64](0)}
	}

	keyFunc := KeyFuncForClusterScope
	if s.Handler.NamespaceScoped() {
		keyFunc = KeyFuncForNamespacedScope
	}
	key, err := keyFunc(ctx, name)
	if err != nil {
		return nil, false, err
	}

	existing, err := s.Handler.OnGet(ctx, key,
		ctrlcli.GetOptions{
			Raw: &meta.GetOptions{
				ResourceVersion: "0",
			},
		})
	if err != nil {
		if !kerrors.IsNotFound(err) {
			return nil, false, wrapError(ctx, name, err)
		}
		return nil, false, kerrors.NewNotFound(qualifiedResourceFromContext(ctx), name)
	}

	om, err := kmeta.Accessor(existing)
	if err != nil {
		return nil, false, kerrors.NewInternalError(err)
	}

	if deleteValidation != nil {
		err = deleteValidation(ctx, existing.DeepCopyObject())
		if err != nil {
			return nil, false, err
		}
	}

	gk := qualifiedKindFromContext(ctx)
	st := &meta.Status{
		Status: meta.StatusSuccess,
		Details: &meta.StatusDetails{
			Group: gk.Group,
			Kind:  gk.Kind,
			Name:  name,
			UID:   om.GetUID(),
		},
	}
	async := ptr.Deref(options.PropagationPolicy, meta.DeletePropagationBackground) == meta.DeletePropagationBackground
	if dryrun.IsDryRun(options.DryRun) {
		// TODO(thxCode): 是否支持删除操作的 dry-run？
		return st, async, nil
	}

	err = s.Handler.OnDelete(ctx, existing, convertCtrlDeleteOptionsFromMeta(options))
	if err != nil {
		return nil, false, wrapError(ctx, name, err)
	}
	return st, async, nil
}

// CollectionDeleteOperation 实现了 rest.Lister、rest.GracefulDeleter 和 rest.CollectionDeleter 接口。
type CollectionDeleteOperation struct {
	ListWatchOperation
	DeleteOperation
}

var (
	_ rest.Lister            = (*CollectionDeleteOperation)(nil)
	_ rest.GracefulDeleter   = (*CollectionDeleteOperation)(nil)
	_ rest.CollectionDeleter = (*CollectionDeleteOperation)(nil)
)

// WithCollectionDelete 返回给定 ListWatchHandler 和 DeleteHandler 对应的 CollectionDeleteOperation。
func WithCollectionDelete(l ListWatchOperation, d DeleteOperation) CollectionDeleteOperation {
	return CollectionDeleteOperation{
		ListWatchOperation: l,
		DeleteOperation:    d,
	}
}

func (s CollectionDeleteOperation) DeleteCollection(
	ctx context.Context,
	deleteValidation rest.ValidateObjectFunc,
	options *meta.DeleteOptions,
	listOptions *metainternal.ListOptions,
) (runtime.Object, error) {
	if options == nil {
		options = meta.NewDeleteOptions(0)
	}
	if listOptions == nil {
		listOptions = &metainternal.ListOptions{ResourceVersion: "0"}
	} else {
		listOptions = listOptions.DeepCopy()
	}

	hasLimit := listOptions.Limit > 0
	if listOptions.Limit == 0 {
		listOptions.Limit = 1000
	}

	var (
		items   []runtime.Object
		itemsCh = make(chan runtime.Object, 256)
	)
	gp := gox.GroupWithContextIn(ctx)
	gp.Go(func(ctx context.Context) error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			listObj, err := s.List(ctx, listOptions)
			if err != nil {
				return err
			}
			if listObj == nil {
				return nil
			}

			newItems, err := kmeta.ExtractList(listObj)
			if err != nil {
				return kerrors.NewInternalError(err)
			}
			items = append(items, newItems...)

			for i := 0; i < len(newItems); i++ {
				select {
				case itemsCh <- newItems[i]:
				case <-ctx.Done():
					return ctx.Err()
				}
			}

			// 已达到数量上限，处理完成。
			if hasLimit {
				close(itemsCh)
				return nil
			}

			// 或者已无更多条目，处理完成。
			om, err := kmeta.ListAccessor(listObj)
			if err != nil {
				return kerrors.NewInternalError(err)
			}
			if om.GetContinue() == "" {
				close(itemsCh)
				return nil
			}

			// 否则继续。
			listOptions.Continue = om.GetContinue()
			listOptions.ResourceVersion = ""
			listOptions.ResourceVersionMatch = ""
		}
	})
	gp.Go(func(ctx context.Context) error {
		for {
			var (
				item runtime.Object
				ok   bool
			)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case item, ok = <-itemsCh:
				if !ok {
					return nil
				}
			}

			om, err := kmeta.Accessor(item)
			if err != nil {
				return kerrors.NewInternalError(err)
			}

			_, _, err = s.Delete(ctx, om.GetName(), deleteValidation, options.DeepCopy())
			if err != nil && !kerrors.IsNotFound(err) {
				return err
			}
		}
	})
	err := gp.Wait()
	if err != nil {
		return nil, kerrors.NewInternalError(err)
	}

	listObj := s.NewList()
	_ = kmeta.SetList(listObj, items)
	return listObj, nil
}

type (
	// CurdOperationsHandler 是 CURD 处理器的接口。
	CurdOperationsHandler interface {
		CreateUpdateHandler
		ListWatchHandler
		GetHandler
		DeleteHandler
	}

	// CurdOperations 实现了 rest.Creater、rest.Lister、rest.Watcher、rest.Getter、
	// rest.Updater、rest.GracefulDeleter 和 rest.CollectionDeleter 接口。
	CurdOperations struct {
		CreateUpdateOperation
		ListWatchOperation
		GetOperation
		DeleteOperation
		CollectionDeleteOperation
	}
)

var (
	_ rest.Creater           = (*CurdOperations)(nil)
	_ rest.Lister            = (*CurdOperations)(nil)
	_ rest.Watcher           = (*CurdOperations)(nil)
	_ rest.Getter            = (*CurdOperations)(nil)
	_ rest.Updater           = (*CurdOperations)(nil)
	_ rest.Patcher           = (*CurdOperations)(nil)
	_ rest.GracefulDeleter   = (*CurdOperations)(nil)
	_ rest.CollectionDeleter = (*CurdOperations)(nil)
)

// WithCurd 根据给定的 rest.TableConvertor 和 CurdOperationsHandler 返回一个 CurdOperations，
// 其实现了 rest.Creater、rest.Lister、rest.Watcher、rest.Getter、rest.Updater、rest.GracefulDeleter
// 和 rest.CollectionDeleter 接口。
func WithCurd(tc rest.TableConvertor, h CurdOperationsHandler) CurdOperations {
	cu := WithCreateUpdate(h)
	l := WithListWatch(tc, h)
	g := WithGet(h)
	d := WithDelete(h)
	return CurdOperations{
		CreateUpdateOperation:     cu,
		ListWatchOperation:        l,
		GetOperation:              g,
		DeleteOperation:           d,
		CollectionDeleteOperation: WithCollectionDelete(l, d),
	}
}

type (
	// CurdProxyHandler 是代理 CURD 处理器的接口。
	CurdProxyHandler[DO MetaObject, DOL MetaObjectList, UO MetaObject, UOL MetaObjectList] interface {
		rest.Scoper
		rest.Storage
		// NewList 返回一个可用于 List 调用的空对象。
		NewList() runtime.Object
		// NewListForProxy 返回一个可用于代理 List 调用的空对象。
		NewListForProxy() runtime.Object
		// CastObjectTo 将对象从下游转换为上游。
		//
		// 通常建议在此方法中执行类型转换，
		// 例如 `return UO(do)`。
		CastObjectTo(do DO) UO
		// CastObjectFrom 将对象从上游转换为下游。
		//
		// 通常建议在此方法中执行类型转换，
		// 例如 `return DO(uo)`。
		CastObjectFrom(ctx context.Context, uo UO) DO
	}

	// CurdProxyOnCreateBeforeAdvice 是在 OnCreate 之前进行拦截的接口。
	CurdProxyOnCreateBeforeAdvice[DO MetaObject] interface {
		// BeforeOnCreate 在创建前调用，
		// 用于修改待创建的对象和选项。
		BeforeOnCreate(ctx context.Context, obj DO, opts *ctrlcli.CreateOptions) error
	}

	// CurdProxyOnListWatchBeforeAdvice 是在 OnList/OnWatch 之前进行拦截的接口。
	CurdProxyOnListWatchBeforeAdvice interface {
		// BeforeOnListWatch 在 list/watch 前调用，
		// 用于修改 list/watch 选项。
		BeforeOnListWatch(ctx context.Context, opts *ctrlcli.ListOptions) error
	}

	// CurdProxyOnListAfterAdvice 是在 OnList 之后进行拦截的接口。
	CurdProxyOnListAfterAdvice[DOL MetaObjectList] interface {
		// AfterOnList 在 list 后调用，
		// 用于修改列表结果。
		AfterOnList(dol DOL, opts *ctrlcli.ListOptions) (mdol DOL)
	}

	// CurdProxyOnWatchDuringAdvice 是在 OnWatch 期间进行拦截的接口。
	CurdProxyOnWatchDuringAdvice[DO MetaObject] interface {
		// DuringWatching 在 watch 期间调用，
		// 用于修改接收到的结果。
		//
		// 如果返回值 `send` 为 true，
		// 该结果将被发送给客户端。
		DuringWatching(do DO, opts *ctrlcli.ListOptions) (mdo DO, send bool)
	}

	// CurdProxyOnGetBeforeAdvice 是在 OnGet 之前进行拦截的接口。
	CurdProxyOnGetBeforeAdvice[DO MetaObject] interface {
		// BeforeOnGet 在 get 前调用，
		// 用于修改获取选项。
		BeforeOnGet(ctx context.Context, key types.NamespacedName, opts *ctrlcli.GetOptions) error
	}

	// CurdProxyOnUpdateBeforeAdvice 是在 OnUpdate 之前进行拦截的接口。
	CurdProxyOnUpdateBeforeAdvice[DO MetaObject] interface {
		// BeforeOnUpdate 在更新前调用，
		// 用于修改待更新的对象和选项。
		BeforeOnUpdate(ctx context.Context, obj, oldObj DO, opts *ctrlcli.UpdateOptions) error
	}

	// CurdProxyOnDeleteBeforeAdvice 是在 OnDelete 之前进行拦截的接口。
	CurdProxyOnDeleteBeforeAdvice[DO MetaObject] interface {
		// BeforeOnDelete 在删除前调用，
		// 用于修改待删除的对象和选项。
		BeforeOnDelete(ctx context.Context, obj DO, opts *ctrlcli.DeleteOptions) error
	}

	// _CurdProxyHandler 是实现 CurdProxyHandler 的私有结构体。
	_CurdProxyHandler[DO MetaObject, DOL MetaObjectList, UO MetaObject, UOL MetaObjectList] struct {
		CurdProxyHandler[DO, DOL, UO, UOL]
		CtrlClient   ctrlcli.WithWatch
		CtrlReader   ctrlcli.Reader
		DOAPIVersion string
		DOKind       string
		UOAPIVersion string
		UOKind       string
	}
)

// WithCurdProxy 使用给定的 CurdProxyHandler 返回一个 CurdOperations。
func WithCurdProxy[DO MetaObject, DOL MetaObjectList, UO MetaObject, UOL MetaObjectList](
	tc rest.TableConvertor,
	h CurdProxyHandler[DO, DOL, UO, UOL],
	ctrlClient ctrlcli.WithWatch,
	ctrlReader ctrlcli.Reader,
) CurdOperations {
	do := h.New().(DO)
	uo := h.CastObjectTo(do)
	doGvk := funcx.MustNoError(ctrlapiutil.GVKForObject(do, ctrlClient.Scheme()))
	uoGvk := funcx.MustNoError(ctrlapiutil.GVKForObject(uo, ctrlClient.Scheme()))
	ph := _CurdProxyHandler[DO, DOL, UO, UOL]{
		CurdProxyHandler: h,
		CtrlClient:       ctrlClient,
		CtrlReader:       ctrlReader,
		DOAPIVersion:     doGvk.GroupVersion().String(),
		DOKind:           doGvk.Kind,
		UOAPIVersion:     uoGvk.GroupVersion().String(),
		UOKind:           uoGvk.Kind,
	}
	return WithCurd(tc, ph)
}

func (h _CurdProxyHandler[DO, DOL, UO, UOL]) CastObjectTo(do DO) UO {
	uo := h.CurdProxyHandler.CastObjectTo(do)
	uot, err := kmeta.TypeAccessor(uo)
	if err == nil {
		uot.SetAPIVersion(h.UOAPIVersion)
		uot.SetKind(h.UOKind)
	}
	return uo
}

func (h _CurdProxyHandler[DO, DOL, UO, UOL]) CastObjectFrom(ctx context.Context, uo UO) DO {
	do := h.CurdProxyHandler.CastObjectFrom(ctx, uo)
	dot, err := kmeta.TypeAccessor(do)
	if err == nil {
		dot.SetAPIVersion(h.DOAPIVersion)
		dot.SetKind(h.DOKind)
	}
	return do
}

// _CreateHandlerWithoutNew 是不含 New 函数的创建处理器接口。
type _CreateHandlerWithoutNew interface {
	// OnCreate 使用给定选项创建对象，
	// 并返回创建后的对象。
	OnCreate(ctx context.Context, object runtime.Object, opts ctrlcli.CreateOptions) (created runtime.Object, err error)
}

func (h _CurdProxyHandler[DO, DOL, UO, UOL]) OnCreate(
	ctx context.Context,
	obj runtime.Object,
	opts ctrlcli.CreateOptions,
) (runtime.Object, error) {
	// 如果处理器实现了 CreateHandler，则绕过当前逻辑。
	if gh, ok := h.CurdProxyHandler.(_CreateHandlerWithoutNew); ok {
		return gh.OnCreate(ctx, obj, opts)
	}

	// 前置拦截。
	if bf, ok := h.CurdProxyHandler.(CurdProxyOnCreateBeforeAdvice[DO]); ok {
		err := bf.BeforeOnCreate(ctx, obj.(DO), &opts)
		if err != nil {
			return nil, err
		}
	}

	uo := h.CastObjectTo(obj.(DO))
	err := h.CtrlClient.Create(ctx, uo, &opts)
	if err != nil {
		return nil, err
	}
	return h.CastObjectFrom(ctx, uo), nil
}

func (h _CurdProxyHandler[DO, DOL, UO, UOL]) OnList(
	ctx context.Context,
	opts ctrlcli.ListOptions,
) (runtime.Object, error) {
	// 如果处理器实现了 ListHandler，则绕过当前逻辑。
	if gh, ok := h.CurdProxyHandler.(ListHandler); ok {
		return gh.OnList(ctx, opts)
	}

	// 前置拦截。
	uopts := copyListOptions(opts)
	if bf, ok := h.CurdProxyHandler.(CurdProxyOnListWatchBeforeAdvice); ok {
		err := bf.BeforeOnListWatch(ctx, uopts)
		if err != nil {
			return nil, err
		}
	}

	uol := h.NewListForProxy().(UOL)
	err := h.CtrlReader.List(ctx, uol, uopts)
	if err != nil {
		return nil, err
	}

	uos, err := kmeta.ExtractList(uol)
	if err != nil {
		return nil, fmt.Errorf("get upstream list: %w", err)
	}
	dos := make([]runtime.Object, 0, len(uos))
	for i := range uos {
		dos = append(dos, h.CastObjectFrom(ctx, uos[i].(UO)))
	}

	dol := h.NewList()
	err = kmeta.SetList(dol, dos)
	if err != nil {
		return nil, fmt.Errorf("set downstream list: %w", err)
	}

	// 设置列表元数据。
	{
		uolm, err := kmeta.ListAccessor(uol)
		if err != nil {
			return nil, fmt.Errorf("get upstream list accessor: %w", err)
		}
		dolm, err := kmeta.ListAccessor(dol)
		if err != nil {
			return nil, fmt.Errorf("get downstream list accessor: %w", err)
		}
		dolm.SetResourceVersion(uolm.GetResourceVersion())
		dolm.SetContinue(uolm.GetContinue())
		dolm.SetRemainingItemCount(uolm.GetRemainingItemCount())
		dolm.SetSelfLink(uolm.GetSelfLink())
	}

	// 后置拦截。
	if af, ok := h.CurdProxyHandler.(CurdProxyOnListAfterAdvice[DOL]); ok {
		dol = af.AfterOnList(dol.(DOL), &opts)
	}

	return dol, nil
}

func (h _CurdProxyHandler[DO, DOL, UO, UOL]) OnWatch(
	ctx context.Context,
	opts ctrlcli.ListOptions,
) (watch.Interface, error) {
	// 如果处理器实现了 WatchHandler，则绕过当前逻辑。
	if gh, ok := h.CurdProxyHandler.(WatchHandler); ok {
		return gh.OnWatch(ctx, opts)
	}

	// 前置拦截。
	uopts := copyListOptions(opts)
	if bf, ok := h.CurdProxyHandler.(CurdProxyOnListWatchBeforeAdvice); ok {
		err := bf.BeforeOnListWatch(ctx, uopts)
		if err != nil {
			return nil, err
		}
	}

	uol := h.NewListForProxy().(UOL)
	uw, err := h.CtrlClient.Watch(ctx, uol, uopts)
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
				// 由 context 取消。
				return
			case <-dw.StopChan():
				// 由下游取消。
				return
			case e, ok := <-uw.ResultChan():
				if !ok {
					// 由上游关闭。
					return
				}

				if e.Object == nil {
					// 无需处理。
					c <- e
					continue
				}

				// 类型断言。
				uo, ok := e.Object.(UO)
				if !ok {
					continue
				}

				// 类型转换。
				do := h.CastObjectFrom(ctx, uo)

				// 过程拦截。
				if da, ok := h.CurdProxyHandler.(CurdProxyOnWatchDuringAdvice[DO]); ok {
					do, ok = da.DuringWatching(do, &opts)
					if !ok {
						continue
					}
				}

				e.Object = do
				c <- e
			}
		}
	})

	return dw, nil
}

func copyListOptions(opts ctrlcli.ListOptions) *ctrlcli.ListOptions {
	if opts.LabelSelector != nil {
		opts.LabelSelector = opts.LabelSelector.DeepCopySelector()
	}
	if opts.FieldSelector != nil {
		opts.FieldSelector = opts.FieldSelector.DeepCopySelector()
	}
	if opts.UnsafeDisableDeepCopy != nil {
		opts.UnsafeDisableDeepCopy = ptr.To(ptr.Deref(opts.UnsafeDisableDeepCopy, false))
	}
	if opts.Raw != nil {
		opts.Raw = opts.Raw.DeepCopy()
	}
	return &opts
}

func (h _CurdProxyHandler[DO, DOL, UO, UOL]) OnGet(
	ctx context.Context,
	key types.NamespacedName,
	opts ctrlcli.GetOptions,
) (runtime.Object, error) {
	// 如果处理器实现了 GetHandler，则绕过当前逻辑。
	if gh, ok := h.CurdProxyHandler.(GetHandler); ok {
		return gh.OnGet(ctx, key, opts)
	}

	// 前置拦截。
	if bf, ok := h.CurdProxyHandler.(CurdProxyOnGetBeforeAdvice[DO]); ok {
		err := bf.BeforeOnGet(ctx, key, &opts)
		if err != nil {
			return nil, err
		}
	}
	uo := h.CastObjectTo(h.New().(DO))
	err := h.CtrlReader.Get(ctx, key, uo, &opts)
	if err != nil {
		return nil, err
	}
	return h.CastObjectFrom(ctx, uo), nil
}

// _UpdateHandlerWithoutNew 是不含 New 函数的更新处理器接口。
type _UpdateHandlerWithoutNew interface {
	// OnUpdate 使用给定选项更新对象，
	// 并返回更新后的对象。
	OnUpdate(ctx context.Context, object, oldObject runtime.Object, opts ctrlcli.UpdateOptions) (updated runtime.Object, err error)
}

func (h _CurdProxyHandler[DO, DOL, UO, UOL]) OnUpdate(
	ctx context.Context,
	obj, oldObj runtime.Object,
	opts ctrlcli.UpdateOptions,
) (runtime.Object, error) {
	// 如果处理器实现了 UpdateHandler，则绕过当前逻辑。
	if gh, ok := h.CurdProxyHandler.(_UpdateHandlerWithoutNew); ok {
		return gh.OnUpdate(ctx, obj, oldObj, opts)
	}

	// 前置拦截。
	if bf, ok := h.CurdProxyHandler.(CurdProxyOnUpdateBeforeAdvice[DO]); ok {
		err := bf.BeforeOnUpdate(ctx, obj.(DO), oldObj.(DO), &opts)
		if err != nil {
			return nil, err
		}
	}
	uo := h.CastObjectTo(obj.(DO))
	var err error
	ri, ok := genericapirequest.RequestInfoFrom(ctx)
	if ok && ri.Subresource != "" {
		err = h.CtrlClient.SubResource(ri.Subresource).Update(ctx, uo, &ctrlcli.SubResourceUpdateOptions{UpdateOptions: opts})
	} else {
		err = h.CtrlClient.Update(ctx, uo, &opts)
	}
	if err != nil {
		return nil, err
	}
	return h.CastObjectFrom(ctx, uo), nil
}

// SupportsDryRun 返回 true，除非处理器替换了 OnCreate 或 OnUpdate（此时由处理器自身决定）：
// 代理自身的 OnCreate 和 OnUpdate 各自只执行一次上游写入，
// 并将选项传递给上游。
func (h _CurdProxyHandler[DO, DOL, UO, UOL]) SupportsDryRun() bool {
	_, replacesCreate := h.CurdProxyHandler.(_CreateHandlerWithoutNew)
	_, replacesUpdate := h.CurdProxyHandler.(_UpdateHandlerWithoutNew)
	if replacesCreate || replacesUpdate {
		return supportsDryRun(h.CurdProxyHandler)
	}
	return true
}

func (h _CurdProxyHandler[DO, DOL, UO, UOL]) OnDelete(
	ctx context.Context,
	obj runtime.Object,
	opts ctrlcli.DeleteOptions,
) error {
	// 如果处理器实现了 DeleteHandler，则绕过当前逻辑。
	if gh, ok := h.CurdProxyHandler.(DeleteHandler); ok {
		return gh.OnDelete(ctx, obj, opts)
	}

	// 前置拦截。
	if bf, ok := h.CurdProxyHandler.(CurdProxyOnDeleteBeforeAdvice[DO]); ok {
		err := bf.BeforeOnDelete(ctx, obj.(DO), &opts)
		if err != nil {
			return err
		}
	}
	uo := h.CastObjectTo(obj.(DO))
	return h.CtrlClient.Delete(ctx, uo, &opts)
}

func qualifiedResourceFromContext(ctx context.Context) schema.GroupResource {
	ri, _ := genericapirequest.RequestInfoFrom(ctx)
	return schema.GroupResource{
		Group:    ri.APIGroup,
		Resource: ri.Resource,
	}
}

func qualifiedKindFromContext(ctx context.Context) schema.GroupKind {
	ri, _ := genericapirequest.RequestInfoFrom(ctx)
	return schema.GroupKind{
		Group: ri.APIGroup,
		Kind:  ri.Resource, // 使用资源名称作为 Kind 名称。
	}
}

func wrapError(ctx context.Context, name string, err error) error {
	if err == nil {
		return nil
	}

	gk := qualifiedKindFromContext(ctx)

	if apiStatus, ok := err.(kerrors.APIStatus); ok || errors.As(err, &apiStatus) {
		st := apiStatus.Status()
		std := st.Details
		if std == nil {
			std = &meta.StatusDetails{}
		}
		return &kerrors.StatusError{
			ErrStatus: meta.Status{
				Status: st.Status,
				Code:   st.Code,
				Reason: st.Reason,
				Details: &meta.StatusDetails{
					Name:              name,
					Group:             gk.Group,
					Kind:              gk.Kind,
					UID:               std.UID,
					Causes:            std.Causes,
					RetryAfterSeconds: std.RetryAfterSeconds,
				},
				Message: st.Message,
			},
		}
	}

	var fieldErr *field.Error
	if errors.As(err, &fieldErr) {
		return &kerrors.StatusError{
			ErrStatus: meta.Status{
				Status: meta.StatusFailure,
				Code:   http.StatusBadRequest,
				Reason: meta.StatusReasonInvalid,
				Details: &meta.StatusDetails{
					Name:  name,
					Group: gk.Group,
					Kind:  gk.Kind,
					Causes: []meta.StatusCause{
						{
							Type:    meta.CauseType(fieldErr.Type), // 1-1 mapping.
							Message: fieldErr.ErrorBody(),
							Field:   fieldErr.Field,
						},
					},
				},
			},
		}
	}

	return &kerrors.StatusError{
		ErrStatus: meta.Status{
			Status: meta.StatusFailure,
			Code:   http.StatusInternalServerError,
			Reason: meta.StatusReasonInternalError,
			Details: &meta.StatusDetails{
				Name:  name,
				Group: gk.Group,
				Kind:  gk.Kind,
				Causes: []meta.StatusCause{
					{
						Type:    meta.CauseTypeInternal,
						Message: err.Error(),
					},
				},
			},
			Message: fmt.Sprintf("Internal error occurred: %v", err),
		},
	}
}
