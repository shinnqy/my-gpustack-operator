package ctrlhandlerx

import (
	"context"
	"reflect"
	"sync"
	"time"

	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/ptr"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/priorityqueue"
	"sigs.k8s.io/controller-runtime/pkg/event"
	ctrlhandler "sigs.k8s.io/controller-runtime/pkg/handler"
	ctrlreconcile "sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// DedupEnqueueRequestsFromMapFunc 复制自 controller-runtime 的 EnqueueRequestsFromMapFunc，
// 但支持配置 TTL，以便在指定时间窗口内对同一对象的请求去重。
//
// 在最近一次触发后的 TTL 窗口内，同一请求的后续事件会通过 q.AddAfter
// 合并为一次安排在窗口结束时的延迟入队——因此最新事件始终会被覆盖，
// 绝不会静默丢弃。
func DedupEnqueueRequestsFromMapFunc(
	ttl time.Duration,
	fn ctrlhandler.MapFunc,
) ctrlhandler.EventHandler {
	return TypedDedupEnqueueRequestsFromMapFunc(ttl, fn)
}

// DedupEnqueueRequestsFromMapFuncWithWindow 与 DedupEnqueueRequestsFromMapFunc 相同，
// 但允许来自不同 watch 的处理器共享同一个去重窗口，并将重复请求合并在一起。
func DedupEnqueueRequestsFromMapFuncWithWindow(
	ttl time.Duration,
	window *DedupWindow[ctrlreconcile.Request],
	fn ctrlhandler.MapFunc,
) ctrlhandler.EventHandler {
	return TypedDedupEnqueueRequestsFromMapFuncWithWindow(ttl, window, fn)
}

// TypedDedupEnqueueRequestsFromMapFunc 复制自 controller-runtime 的 TypedEnqueueRequestsFromMapFunc，
// 但支持配置 TTL，以便在指定时间窗口内对同一对象的请求去重。
//
// 在最近一次触发后的 TTL 窗口内，同一请求的后续事件会通过 q.AddAfter
// 合并为一次安排在窗口结束时的延迟入队——因此最新事件始终会被覆盖，
// 绝不会静默丢弃。
func TypedDedupEnqueueRequestsFromMapFunc[object any, request comparable](
	ttl time.Duration,
	fn ctrlhandler.TypedMapFunc[object, request],
) ctrlhandler.TypedEventHandler[object, request] {
	return TypedDedupEnqueueRequestsFromMapFuncWithWindow(ttl, nil, fn)
}

// TypedDedupEnqueueRequestsFromMapFuncWithWindow 与 TypedDedupEnqueueRequestsFromMapFunc 相同，
// 但允许在处理器之间共享去重窗口，从而实现跨 watch 去重。
func TypedDedupEnqueueRequestsFromMapFuncWithWindow[object any, request comparable](
	ttl time.Duration,
	window *DedupWindow[request],
	fn ctrlhandler.TypedMapFunc[object, request],
) ctrlhandler.TypedEventHandler[object, request] {
	if window == nil {
		window = NewDedupWindow[request]()
	}
	return &dedupEnqueueRequestsFromMapFunc[object, request]{
		ttl:                          ttl,
		window:                       window,
		toRequests:                   fn,
		objectImplementsClientObject: implementsClientObject[object](),
	}
}

// DedupWindow 跟踪每个请求的最后一次入队/调度时间。
// 在多个处理器间复用同一个 DedupWindow 可实现全局重复请求去重。
type DedupWindow[request comparable] struct {
	m     sync.RWMutex
	until map[request]time.Time
}

// NewDedupWindow 创建一个新的 DedupWindow，
// 用于跟踪每个请求的最后一次入队/调度时间以进行去重。
func NewDedupWindow[request comparable]() *DedupWindow[request] {
	return &DedupWindow[request]{
		until: make(map[request]time.Time),
	}
}

var _ ctrlhandler.EventHandler = &dedupEnqueueRequestsFromMapFunc[ctrlcli.Object, ctrlreconcile.Request]{}

type dedupEnqueueRequestsFromMapFunc[object any, request comparable] struct {
	ttl                          time.Duration
	window                       *DedupWindow[request]
	toRequests                   ctrlhandler.TypedMapFunc[object, request]
	objectImplementsClientObject bool
}

// Create 实现 EventHandler 接口。
func (e *dedupEnqueueRequestsFromMapFunc[object, request]) Create(
	ctx context.Context,
	evt event.TypedCreateEvent[object],
	q workqueue.TypedRateLimitingInterface[request],
) {
	reqs := map[request]empty{}

	var lowPriority bool
	if isPriorityQueue(q) && !isNil(evt.Object) {
		if evt.IsInInitialList {
			lowPriority = true
		}
	}
	e.mapAndEnqueue(ctx, q, evt.Object, reqs, lowPriority)
}

// Update 实现 EventHandler 接口。
func (e *dedupEnqueueRequestsFromMapFunc[object, request]) Update(
	ctx context.Context,
	evt event.TypedUpdateEvent[object],
	q workqueue.TypedRateLimitingInterface[request],
) {
	var lowPriority bool
	if e.objectImplementsClientObject && isPriorityQueue(q) && !isNil(evt.ObjectOld) && !isNil(evt.ObjectNew) {
		lowPriority = any(evt.ObjectOld).(ctrlcli.Object).GetResourceVersion() == any(evt.ObjectNew).(ctrlcli.Object).GetResourceVersion()
	}
	reqs := map[request]empty{}
	e.mapAndEnqueue(ctx, q, evt.ObjectOld, reqs, lowPriority)
	e.mapAndEnqueue(ctx, q, evt.ObjectNew, reqs, lowPriority)
}

// Delete 实现 EventHandler 接口。
func (e *dedupEnqueueRequestsFromMapFunc[object, request]) Delete(
	ctx context.Context,
	evt event.TypedDeleteEvent[object],
	q workqueue.TypedRateLimitingInterface[request],
) {
	reqs := map[request]empty{}
	e.mapAndEnqueue(ctx, q, evt.Object, reqs, false)
}

// Generic 实现 EventHandler 接口。
func (e *dedupEnqueueRequestsFromMapFunc[object, request]) Generic(
	ctx context.Context,
	evt event.TypedGenericEvent[object],
	q workqueue.TypedRateLimitingInterface[request],
) {
	reqs := map[request]empty{}
	e.mapAndEnqueue(ctx, q, evt.Object, reqs, false)
}

func (e *dedupEnqueueRequestsFromMapFunc[object, request]) mapAndEnqueue(
	ctx context.Context,
	q workqueue.TypedRateLimitingInterface[request],
	o object,
	reqs map[request]empty,
	lowPriority bool,
) {
	requests := e.toRequests(ctx, o)
	if len(requests) == 0 {
		return
	}

	now := time.Now()

	type pending struct {
		req   request
		delay time.Duration
	}
	pendings := make([]pending, 0, len(requests))

	e.window.m.Lock()
	for _, req := range requests {
		if _, dup := reqs[req]; dup {
			continue
		}
		reqs[req] = empty{}

		// e.until[req] 是 req 最近一次入队（或已调度的未来入队）的时间戳。
		// 有效的 TTL 窗口为 [t, t+ttl)。
		t, exists := e.window.until[req]
		switch {
		case !exists || !now.Before(t.Add(e.ttl)):
			// 此前未触发或 TTL 窗口已过期——立即入队
			// 并开启从 now 开始的新窗口。
			e.window.until[req] = now
			pendings = append(pendings, pending{req: req})
		case now.Before(t):
			// 已在未来的时间点 t 安排了延迟入队——
			// 最新事件必然会被该入队覆盖，因此
			// 可以安全地合并（丢弃）本次事件。
		default:
			// 处于 TTL 窗口内 (t <= now < t+ttl)——将入队延迟到
			// 窗口结束时执行，以确保最新事件仍会被调谐。
			// 将 e.until[req] 推进到计划触发时间，
			// 使同一窗口内的后续事件落入上面的 "now.Before(t)" 分支并被合并。
			delay := t.Add(e.ttl).Sub(now)
			e.window.until[req] = now.Add(delay)
			pendings = append(pendings, pending{req: req, delay: delay})
		}
	}

	// 按需清理 TTL 窗口结束已超过至少一个 TTL 的条目——
	// 它们的下一次事件无论如何都会立即触发，因此保留该条目
	// 没有任何意义。跳过计划在未来触发的条目 (now.Sub(t) < 0)。
	for req, t := range e.window.until {
		if now.Sub(t) >= 2*e.ttl {
			delete(e.window.until, req)
		}
	}
	e.window.m.Unlock()

	for _, p := range pendings {
		e.enqueue(q, p.req, p.delay, lowPriority)
	}
}

// enqueue 将 req 添加到 q。当 delay > 0 时，延迟添加由
// time.AfterFunc 驱动，而不是使用队列自身的 After 选项。
//
// 我们绝不能将 delay 传给 controller-runtime 的 priorityqueue.PriorityQueue：
// 它会通过保留最早的 ReadyAt 来合并同一 key 的并发添加，
// 因此当相同的 req 已经以 ReadyAt==nil 在队列中排队时（例如先前的
// 立即添加正停留在 w.ready 中等待忙碌的 worker），后续的
// AddWithOpts({After: X}) 会静默退化为立即入队，从而永久丢失
// 去重窗口的延迟入队。priorityqueue.AddAfter 共用同一代码路径，因此存在同样的问题。
func (e *dedupEnqueueRequestsFromMapFunc[object, request]) enqueue(
	q workqueue.TypedRateLimitingInterface[request],
	req request,
	delay time.Duration,
	lowPriority bool,
) {
	add := func() {
		if pq, ok := q.(priorityqueue.PriorityQueue[request]); ok {
			var opts priorityqueue.AddOpts
			if lowPriority {
				opts.Priority = ptr.To(-100)
			}
			pq.AddWithOpts(opts, req)
			return
		}
		q.Add(req)
	}
	if delay > 0 {
		time.AfterFunc(delay, add)
		return
	}
	add()
}

var typeForClientObject = reflect.TypeFor[ctrlcli.Object]()

func implementsClientObject[object any]() bool {
	return reflect.TypeFor[object]().Implements(typeForClientObject)
}

func isPriorityQueue[request comparable](q workqueue.TypedRateLimitingInterface[request]) bool {
	_, ok := q.(priorityqueue.PriorityQueue[request])
	return ok
}

type empty struct{}

func isNil(arg any) bool {
	if v := reflect.ValueOf(arg); !v.IsValid() || ((v.Kind() == reflect.Pointer ||
		v.Kind() == reflect.Interface ||
		v.Kind() == reflect.Slice ||
		v.Kind() == reflect.Map ||
		v.Kind() == reflect.Chan ||
		v.Kind() == reflect.Func) && v.IsNil()) {
		return true
	}
	return false
}
