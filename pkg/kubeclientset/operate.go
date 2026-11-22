package kubeclientset

import (
	"context"
	"errors"
	"reflect"
	"time"

	kerrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
)

type (
	// MetaObject 是带有元数据的资源对象接口。
	MetaObject = ctrlcli.Object

	// AlignWithFn 用于比较实际对象与期望对象，并在两者不一致时返回对齐后的对象。
	AlignWithFn[T MetaObject] func(actualCopied T) (aligned T, skip bool, err error)

	// CompareWithFn 用于比较实际对象与期望对象，若两者一致则返回 true。
	CompareWithFn[T MetaObject] func(actualCopied T) (skip bool)
)

type (
	GetClient[T MetaObject] interface {
		Get(ctx context.Context, name string, opts meta.GetOptions) (T, error)
	}

	CreateClient[T MetaObject] interface {
		Create(ctx context.Context, obj T, opts meta.CreateOptions) (T, error)
	}

	_CreateOptions[T MetaObject] struct {
		meta.CreateOptions
		UpdateAlignFunc     AlignWithFn[T]
		RecreateCompareFunc CompareWithFn[T]
	}

	CreateOption[T MetaObject] func(*_CreateOptions[T])
)

// WithUpdateIfExisted 配置对齐函数，以便在资源已存在时执行更新。
//
// WithUpdateIfExisted 与 WithRecreateIfDuplicated 互斥；若同时提供，则优先使用 WithUpdateIfExisted。
func WithUpdateIfExisted[T MetaObject](fn AlignWithFn[T]) CreateOption[T] {
	return func(co *_CreateOptions[T]) {
		co.UpdateAlignFunc = fn
	}
}

// WithRecreateIfDuplicated 配置比较函数，以便在资源存在差异时重建该资源。
//
// WithRecreateIfDuplicated 与 WithUpdateIfExisted 互斥；若同时提供，则优先使用 WithUpdateIfExisted。
func WithRecreateIfDuplicated[T MetaObject](fn CompareWithFn[T]) CreateOption[T] {
	return func(co *_CreateOptions[T]) {
		co.RecreateCompareFunc = fn
	}
}

// Create 类似于 Apply，在资源不存在时创建该资源。
//
// 若提供了 WithUpdateIfExisted，Create 会更新已有资源；若提供了 WithRecreateIfDuplicated，则会重建资源。
// WithUpdateIfExisted 与 WithRecreateIfDuplicated 只能二选一；若同时提供，则优先使用 WithUpdateIfExisted。
func Create[T MetaObject](ctx context.Context, cli CreateClient[T], expected T, opts ...CreateOption[T]) (T, error) {
	if reflect.ValueOf(expected).IsZero() {
		return expected, errors.New("expected is nil")
	}

	var co _CreateOptions[T]
	for i := range opts {
		opts[i](&co)
	}

	var (
		name   = expected.GetName()
		err    = errors.New("resource name may not be empty")
		actual T
	)

	if name != "" {
		if getter, ok := cli.(GetClient[T]); ok {
			actual, err = getter.Get(ctx, name,
				readOptions(ctx))
			if err != nil && !kerrors.IsNotFound(err) {
				if rctx, ok := retryTransient(ctx, err); ok {
					return Create(rctx, cli, expected, opts...)
				}
				return actual, err
			}
		}
	}

	// 若未找到资源或资源正在删除中，则尝试创建。
	if err != nil || actual.GetDeletionTimestamp() != nil {
		deleting := err == nil && actual.GetDeletionTimestamp() != nil && len(actual.GetFinalizers()) == 0
		if deleting {
			// NB(thxCode): 短暂休眠以避免服务端状态抖动。
			time.Sleep(10 * time.Millisecond)
		}
		actual, err = cli.Create(ctx, expected, meta.CreateOptions{
			DryRun: co.DryRun,
		})
		if err != nil {
			switch {
			case isRetryError(err):
				if rctx, ok := retryTransient(ctx, err); ok {
					return Create(rctx, cli, expected, opts...)
				}
			case kerrors.IsAlreadyExists(err):
				// 在以下情况下遇到资源已存在错误时进行重试：
				// - 配置了对齐函数；
				// - 配置了比较函数；
				// - 资源正在删除且没有 finalizers。
				if co.UpdateAlignFunc != nil || co.RecreateCompareFunc != nil || deleting {
					if rctx, ok := nextAttempt(ctx, err); ok {
						return Create(rctx, cli, expected, opts...)
					}
				} else {
					err = nil
				}
			}
		}
		return actual, err
	}

	switch {
	case co.UpdateAlignFunc != nil:
		var (
			copied T
			skip   bool
		)
		copied, skip, err = co.UpdateAlignFunc(actual.DeepCopyObject().(T))
		if err != nil {
			return actual, err
		}
		if skip {
			return actual, nil
		}

		updater, ok := cli.(UpdateClient[T])
		if !ok {
			return actual, errors.New("client does not support update")
		}

		// 复制 resourceVersion 以用于更新。
		//
		// 若期望对象中未设置 labels、annotations、finalizers 和 ownerReferences，则保留原有值。
		// 如需清空上述字段，请在期望对象中将其显式置为空。
		copiedOm, actualOm := copied, actual
		copiedOm.SetResourceVersion(actualOm.GetResourceVersion())
		if copiedOm.GetLabels() == nil {
			copiedOm.SetLabels(actualOm.GetLabels())
		}
		if copiedOm.GetAnnotations() == nil {
			copiedOm.SetAnnotations(actualOm.GetAnnotations())
		}
		if copiedOm.GetFinalizers() == nil {
			copiedOm.SetFinalizers(actualOm.GetFinalizers())
		}
		if copiedOm.GetOwnerReferences() == nil {
			copiedOm.SetOwnerReferences(actualOm.GetOwnerReferences())
		}

		updated, err := updater.Update(ctx, copied, meta.UpdateOptions{
			DryRun: co.DryRun,
		})
		if err == nil {
			return updated, nil
		}

		// 当服务端要求退避，或发生并发写入冲突时进行重试：
		// 下一轮会重新读取实际资源并在其基础上对齐状态，使并发更新落败的一方能够收敛而非直接失败。
		//
		// 满足其中任一原因即可触发重试，因此使用逻辑或判断：这些条件彼此互斥，同时满足永远不可能成立。
		if rctx, ok := retryToConverge(ctx, err); ok {
			return Create(rctx, cli, expected, opts...)
		}

		return updated, err
	case co.RecreateCompareFunc != nil:
		skip := co.RecreateCompareFunc(actual.DeepCopyObject().(T))
		if skip {
			return actual, nil
		}

		deleter, ok := cli.(DeleteClient)
		if !ok {
			return actual, errors.New("client does not support delete")
		}

		err = deleter.Delete(ctx, name, meta.DeleteOptions{
			DryRun:            co.DryRun,
			PropagationPolicy: ptr.To(meta.DeletePropagationForeground),
		})
		if err != nil && !kerrors.IsNotFound(err) && !isRetryError(err) {
			return actual, err
		}

		// 重建资源，并与其他重入路径一样受重试次数上限约束：否则删除后持续遇到存活对象会导致无限递归。
		// 该路径没有返回服务端错误，因此耗尽重试预算时需返回其自身的错误。
		if rctx, ok := nextAttempt(ctx, nil); ok {
			return Create(rctx, cli, expected, opts...)
		}

		return actual, errTooManyAttempts
	}

	return actual, nil
}

// CreateWithCtrlClient 与 Create 类似，但使用 ctrl 客户端。
func CreateWithCtrlClient[T MetaObject](ctx context.Context, cli ctrlcli.Client, expected T, opts ...CreateOption[T]) (T, error) {
	if reflect.ValueOf(expected).IsZero() {
		return expected, errors.New("expected is nil")
	}

	var co _CreateOptions[T]
	for i := range opts {
		opts[i](&co)
	}

	var (
		name   = expected.GetName()
		err    = errors.New("resource name may not be empty")
		actual = expected.DeepCopyObject().(T)
	)

	if name != "" {
		err = cli.Get(ctx, ctrlcli.ObjectKeyFromObject(expected), actual)
		if err != nil && !kerrors.IsNotFound(err) {
			if rctx, ok := retryTransient(ctx, err); ok {
				return CreateWithCtrlClient[T](rctx, cli, expected, opts...)
			}
			return actual, err
		}
	}

	// 若未找到资源或资源正在删除中，则尝试创建。
	if err != nil || actual.GetDeletionTimestamp() != nil {
		deleting := err == nil && actual.GetDeletionTimestamp() != nil && len(actual.GetFinalizers()) == 0
		if deleting {
			// NB(thxCode): 短暂休眠以避免服务端状态抖动。
			time.Sleep(10 * time.Millisecond)
		}
		err = cli.Create(ctx, expected, &ctrlcli.CreateOptions{
			DryRun:       co.DryRun,
			FieldManager: co.FieldManager,
			Raw:          ptr.To(co.CreateOptions),
		})
		if err == nil {
			return expected, nil
		}
		switch {
		case isRetryError(err):
			if rctx, ok := retryTransient(ctx, err); ok {
				return CreateWithCtrlClient[T](rctx, cli, expected, opts...)
			}
		case kerrors.IsAlreadyExists(err):
			// 在以下情况下遇到资源已存在错误时进行重试：
			// - 配置了对齐函数；
			// - 配置了比较函数；
			// - 资源正在删除且没有 finalizers。
			if co.UpdateAlignFunc != nil || co.RecreateCompareFunc != nil || deleting {
				if rctx, ok := nextAttempt(ctx, err); ok {
					return CreateWithCtrlClient[T](rctx, cli, expected, opts...)
				}
			} else {
				err = nil
			}
		}
		return actual, err
	}

	switch {
	case co.UpdateAlignFunc != nil:
		var (
			copied T
			skip   bool
		)
		copied, skip, err = co.UpdateAlignFunc(actual.DeepCopyObject().(T))
		if err != nil {
			return actual, err
		}
		if skip {
			return actual, nil
		}

		// 复制 resourceVersion 以用于更新。
		//
		// 若期望对象中未设置 labels、annotations、finalizers 和 ownerReferences，则保留原有值。
		// 如需清空上述字段，请在期望对象中将其显式置为空。
		copiedOm, actualOm := copied, actual
		copiedOm.SetResourceVersion(actualOm.GetResourceVersion())
		if copiedOm.GetLabels() == nil {
			copiedOm.SetLabels(actualOm.GetLabels())
		}
		if copiedOm.GetAnnotations() == nil {
			copiedOm.SetAnnotations(actualOm.GetAnnotations())
		}
		if copiedOm.GetFinalizers() == nil {
			copiedOm.SetFinalizers(actualOm.GetFinalizers())
		}
		if copiedOm.GetOwnerReferences() == nil {
			copiedOm.SetOwnerReferences(actualOm.GetOwnerReferences())
		}

		err = cli.Update(ctx, copied, &ctrlcli.UpdateOptions{
			DryRun: co.DryRun,
			Raw:    &meta.UpdateOptions{DryRun: co.DryRun},
		})
		if err == nil {
			return copied, nil
		}

		// 当服务端要求退避或发生并发写入冲突时进行重试，逻辑与 Create 相同。
		if rctx, ok := retryToConverge(ctx, err); ok {
			return CreateWithCtrlClient[T](rctx, cli, expected, opts...)
		}

		return copied, err
	case co.RecreateCompareFunc != nil:
		skip := co.RecreateCompareFunc(actual.DeepCopyObject().(T))
		if skip {
			return actual, nil
		}

		err = cli.Delete(ctx, expected, &ctrlcli.DeleteOptions{
			DryRun:            co.DryRun,
			PropagationPolicy: ptr.To(meta.DeletePropagationForeground),
		})
		if err != nil && !kerrors.IsNotFound(err) && !isRetryError(err) {
			return actual, err
		}

		// 重建资源，并与其他重入路径一样受重试次数上限约束：否则删除后持续遇到存活对象会导致无限递归。
		// 该路径没有返回服务端错误，因此耗尽重试预算时需返回其自身的错误。
		if rctx, ok := nextAttempt(ctx, nil); ok {
			return CreateWithCtrlClient[T](rctx, cli, expected, opts...)
		}

		return actual, errTooManyAttempts
	}

	return actual, nil
}

type (
	UpdateClient[T MetaObject] interface {
		Update(ctx context.Context, obj T, opts meta.UpdateOptions) (T, error)
		Get(ctx context.Context, name string, opts meta.GetOptions) (T, error)
	}

	_UpdateOptions[T MetaObject] struct {
		meta.UpdateOptions
		AlignFunc          AlignWithFn[T]
		CreateIfNotExisted bool
	}

	UpdateOption[T MetaObject] func(*_UpdateOptions[T])
)

// WithUpdateAlign 配置用于更新资源的对齐函数。
func WithUpdateAlign[T MetaObject](fn AlignWithFn[T]) UpdateOption[T] {
	return func(uo *_UpdateOptions[T]) {
		uo.AlignFunc = fn
	}
}

// WithCreateIfNotExisted 会在资源不存在时创建该资源。
func WithCreateIfNotExisted[T MetaObject]() UpdateOption[T] {
	return func(uo *_UpdateOptions[T]) {
		uo.CreateIfNotExisted = true
	}
}

// Update 在资源存在时更新该资源，并返回更新后的资源对象。
//
// 若资源未找到或更新失败，Update 将返回 error。
//
// 当提供了 AlignWithFn 且更新发生冲突时，Update 会自动重试。
func Update[T MetaObject](ctx context.Context, cli UpdateClient[T], expected T, opts ...UpdateOption[T]) (T, error) {
	if reflect.ValueOf(expected).IsZero() {
		return expected, errors.New("expected is nil")
	}

	var uo _UpdateOptions[T]
	for i := range opts {
		opts[i](&uo)
	}

	name := expected.GetName()
	if name == "" {
		return expected, errors.New("resource name may not be empty")
	}

	actual, err := cli.Get(ctx, name,
		readOptions(ctx))
	if err != nil {
		if kerrors.IsNotFound(err) && uo.CreateIfNotExisted {
			creator, ok := cli.(CreateClient[T])
			if !ok {
				return actual, errors.New("client does not support create")
			}
			actual, err = creator.Create(ctx, expected, meta.CreateOptions{
				DryRun: uo.DryRun,
			})
			if err != nil && kerrors.IsAlreadyExists(err) {
				// 若资源已存在则重试。
				if rctx, ok := nextAttempt(ctx, err); ok {
					return Update(rctx, cli, expected, opts...)
				}
			}
		}
		if rctx, ok := retryTransient(ctx, err); ok {
			return Update(rctx, cli, expected, opts...)
		}
		return actual, err
	}

	var copied T
	if uo.AlignFunc != nil {
		var skip bool
		copied, skip, err = uo.AlignFunc(actual.DeepCopyObject().(T))
		if err != nil {
			return actual, err
		}
		if skip {
			return actual, nil
		}
	} else {
		copied = expected.DeepCopyObject().(T)
		// 复制 resourceVersion 以用于更新。
		//
		// 若期望对象中未设置 labels、annotations、finalizers 和 ownerReferences，则保留原有值。
		// 如需清空上述字段，请在期望对象中将其显式置为空。
		copiedOm, actualOm := copied, actual
		copiedOm.SetResourceVersion(actualOm.GetResourceVersion())
		if copiedOm.GetLabels() == nil {
			copiedOm.SetLabels(actualOm.GetLabels())
		}
		if copiedOm.GetAnnotations() == nil {
			copiedOm.SetAnnotations(actualOm.GetAnnotations())
		}
		if copiedOm.GetFinalizers() == nil {
			copiedOm.SetFinalizers(actualOm.GetFinalizers())
		}
		if copiedOm.GetOwnerReferences() == nil {
			copiedOm.SetOwnerReferences(actualOm.GetOwnerReferences())
		}
	}

	updated, err := cli.Update(ctx, copied, meta.UpdateOptions{
		DryRun: uo.DryRun,
	})
	if err != nil {
		if rctx, ok := retryTransient(ctx, err); ok {
			return Update(rctx, cli, expected, opts...)
		}

		if !kerrors.IsConflict(err) && !kerrors.IsNotAcceptable(err) {
			return actual, err
		}

		// 当提供了对齐函数且更新发生冲突时进行重试。
		if uo.AlignFunc != nil {
			if rctx, ok := nextAttempt(ctx, err); ok {
				return Update(rctx, cli, expected, opts...)
			}
		}
	}

	return updated, err
}

// UpdateWithCtrlClient 与 Update 类似，但使用 ctrl 客户端。
func UpdateWithCtrlClient[T MetaObject](ctx context.Context, cli ctrlcli.Client, expected T, opts ...UpdateOption[T]) (T, error) {
	if reflect.ValueOf(expected).IsZero() {
		return expected, errors.New("expected is nil")
	}

	var uo _UpdateOptions[T]
	for i := range opts {
		opts[i](&uo)
	}

	name := expected.GetName()
	if name == "" {
		return expected, errors.New("resource name may not be empty")
	}

	actual := expected.DeepCopyObject().(T)
	err := cli.Get(ctx, ctrlcli.ObjectKeyFromObject(expected), actual)
	if err != nil {
		if kerrors.IsNotFound(err) && uo.CreateIfNotExisted {
			actual = expected.DeepCopyObject().(T)
			err = cli.Create(ctx, actual, &ctrlcli.CreateOptions{
				DryRun:       uo.DryRun,
				FieldManager: uo.FieldManager,
			})
			if err != nil && kerrors.IsAlreadyExists(err) {
				// 若资源已存在则重试。
				if rctx, ok := nextAttempt(ctx, err); ok {
					return UpdateWithCtrlClient[T](rctx, cli, expected, opts...)
				}
			}
		}
		if rctx, ok := retryTransient(ctx, err); ok {
			return UpdateWithCtrlClient[T](rctx, cli, expected, opts...)
		}
		return actual, err
	}

	var copied T
	if uo.AlignFunc != nil {
		var skip bool
		copied, skip, err = uo.AlignFunc(actual.DeepCopyObject().(T))
		if err != nil {
			return actual, err
		}
		if skip {
			return actual, nil
		}
	} else {
		copied = expected.DeepCopyObject().(T)
		// 复制 resourceVersion 以用于更新。
		//
		// 若期望对象中未设置 labels、annotations、finalizers 和 ownerReferences，则保留原有值。
		// 如需清空上述字段，请在期望对象中将其显式置为空。
		copiedOm, actualOm := copied, actual
		copiedOm.SetResourceVersion(actualOm.GetResourceVersion())
		if copiedOm.GetLabels() == nil {
			copiedOm.SetLabels(actualOm.GetLabels())
		}
		if copiedOm.GetAnnotations() == nil {
			copiedOm.SetAnnotations(actualOm.GetAnnotations())
		}
		if copiedOm.GetFinalizers() == nil {
			copiedOm.SetFinalizers(actualOm.GetFinalizers())
		}
		if copiedOm.GetOwnerReferences() == nil {
			copiedOm.SetOwnerReferences(actualOm.GetOwnerReferences())
		}
	}

	updated := copied
	err = cli.Update(ctx, updated, &ctrlcli.UpdateOptions{
		DryRun:       uo.DryRun,
		FieldManager: uo.FieldManager,
		Raw:          ptr.To(uo.UpdateOptions),
	})
	if err != nil {
		if rctx, ok := retryTransient(ctx, err); ok {
			return UpdateWithCtrlClient[T](rctx, cli, expected, opts...)
		}

		if !kerrors.IsConflict(err) && !kerrors.IsNotAcceptable(err) {
			return actual, err
		}

		// 当提供了对齐函数且更新发生冲突时进行重试。
		if uo.AlignFunc != nil {
			if rctx, ok := nextAttempt(ctx, err); ok {
				return UpdateWithCtrlClient[T](rctx, cli, expected, opts...)
			}
		}
	}

	return updated, err
}

type (
	DeleteClient interface {
		Delete(ctx context.Context, name string, opts meta.DeleteOptions) error
	}
)

// isRetryError 报告服务端本身是否要求重试。这是一个纯判断函数：
// 等待退避逻辑归属于 nextAttempt，其他因服务端未明确指明的原因而触发重试的路径也会经过该函数。
func isRetryError(err error) bool {
	if kerrors.IsTooManyRequests(err) || kerrors.IsGone(err) || kerrors.IsTimeout(err) || kerrors.IsServerTimeout(err) {
		return true
	}
	_, ok := kerrors.SuggestsClientDelay(err)

	return ok
}

// errTooManyAttempts 用于重入是唯一推进方式但重试预算已耗尽的场景，
// 此时无法报告最后一次服务端错误，因为该执行路径并未产生服务端错误。
var errTooManyAttempts = errors.New("too many attempts")

// retryAttemptKey 在 context 中记录操作的重入次数。
type retryAttemptKey struct{}

const (
	// maxAttempts 限制单个操作允许自身重入的最大次数。此处的每个操作都通过重新读取
	// 实时资源并在其上对齐状态来实现收敛，相当于以递归形式编写的重试循环。若不加限制，
	// 持续获胜的并发写入方会导致其无限递归：调用栈不断增长直至进程中止，而调用方永远
	// 无法收到可用于上报或设置 condition 的 error。
	maxAttempts = 8
	// attemptBackoff 是首次重入前的等待时间，之后每次重试按指数翻倍。Conflict 错误
	// 不携带服务端建议的延迟，因此若无此退避，下一次尝试会立即执行，从而使发生竞争的
	// 对象退化为针对 API Server 的热点死循环。
	attemptBackoff = 10 * time.Millisecond
)

// nextAttempt 在等待完本次尝试的退避时间后，返回用于重入操作的 context，并报告
// 重试预算是否允许此次重入。若服务端给出了建议延迟，则优先采用服务端延迟，因为
// 服务端明确指定了具体数值，而本地退避仅是估算值。
//
// 重入次数通过 context 传递，因为此处每个操作在自身递归过程中都已贯穿传递了
// context；而各操作的选项类型各不相同，若不修改所有函数签名，则无法通过共享选项传递该计数。
func nextAttempt(ctx context.Context, err error) (context.Context, bool) {
	attempt, _ := ctx.Value(retryAttemptKey{}).(int)
	if attempt >= maxAttempts {
		return ctx, false
	}

	wait := attemptBackoff << attempt
	if s, ok := kerrors.SuggestsClientDelay(err); ok {
		wait = time.Duration(s) * time.Second
	}

	select {
	case <-ctx.Done():
		return ctx, false
	case <-time.After(wait):
	}

	return context.WithValue(ctx, retryAttemptKey{}, attempt+1), true
}

// retryTransient 对应仅因服务端要求重试而进行重试的调用点。
func retryTransient(ctx context.Context, err error) (context.Context, bool) {
	if !isRetryError(err) {
		return ctx, false
	}

	return nextAttempt(ctx, err)
}

// retryToConverge 对应在写入竞争失败时也会重试的调用点：下一轮会重新读取实际
// 资源并在其上对齐状态，从而使在并发更新中失败的写入方最终收敛而非直接失败。
//
// 这些条件满足其一即可，因此使用逻辑或：它们彼此互斥，不可能同时成立。
func retryToConverge(ctx context.Context, err error) (context.Context, bool) {
	if !isRetryError(err) && !kerrors.IsConflict(err) && !kerrors.IsNotAcceptable(err) {
		return ctx, false
	}

	return nextAttempt(ctx, err)
}

// readOptions 是操作在写入前执行读取时使用的选项。首次读取可由 API Server 的
// watch 缓存提供（这仅是一种优化）；但重入时不可使用缓存，因为在刚刚遭遇冲突的
// 同一份陈旧对象上继续对齐状态只会导致再次冲突，而缓存恰恰会返回这份陈旧对象。
func readOptions(ctx context.Context) meta.GetOptions {
	if attempt, _ := ctx.Value(retryAttemptKey{}).(int); attempt > 0 {
		return meta.GetOptions{}
	}

	return meta.GetOptions{ResourceVersion: "0"}
}
