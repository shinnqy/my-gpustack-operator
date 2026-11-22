package kubeapp

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	coordination "k8s.io/api/coordination/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	klog "k8s.io/klog/v2"
	"k8s.io/utils/ptr"

	coordinationv1 "gpustack.ai/gpustack/pkg/kubeclients/kubernetes/typed/coordination/v1"
)

const (
	// defaultLockDuration 是锁持有声明在未续期情况下的有效时长，
	// 即等待方在接管一个已消失持有者的锁声明前需要等待的时间。
	defaultLockDuration = time.Minute
	// defaultLockTimeout 限制等待获取锁的超时时间。放弃等待会向调用方报告失败：
	// 对于靠自身重启实现重试的进程而言，这就是一次重试。该值设置得足够长，以覆盖正在执行耗时操作的持有者
	// （例如等待镜像拉取的 Helm 操作会持续数分钟）；同时又足够短，以便在通常运行该逻辑的容器启动探针超时前留出余量，
	// 因为启动探针才是真正决定启动最多能等待多久的上限。
	defaultLockTimeout = 12 * time.Minute
	// maxLockRetryInterval 限制等待方两次读取 Lease 之间的最大间隔，
	// 以便无论锁声明本身的有效期多长，被释放的锁都能被及时接手。
	maxLockRetryInterval = 5 * time.Second
)

// errLockTakenOver 表示其他并发进程已接管当前进程正在续期的锁声明。
var errLockTakenOver = errors.New("taken over")

// Lock 使用 coordination.k8s.io 的 Lease 在多进程间串行化执行工作。
//
// 这里有意不采用 client-go 的 leaderelection：该包会在 ctx 被取消时释放锁，
// 而这只有在被保护的工作到那时已经停止的情况下才安全。Helm 在 ctx 取消后仍会继续执行应用操作，
// 因此该锁严格在被保护的函数返回后才释放；取消只会缩短工作时间，
// 而不会在工作尚未结束时把锁交给其他并发进程导致并行执行。
type Lock struct {
	// Leases 是锁所在 Namespace 的 Lease 客户端。
	Leases coordinationv1.LeaseInterface
	// Name 是 Lease 对象的名称。
	Name string
	// Holder 用于向其他并发进程标识当前进程。每个进程必须唯一；
	// 两个共享同一标识的进程都会认为自己持有锁。
	Holder string
	// Duration 是当前进程的锁声明在未续期情况下的有效时长。零值时使用 defaultLockDuration。
	Duration time.Duration
	// Timeout 限制等待获取锁的超时时间。零值时使用 defaultLockTimeout。
	Timeout time.Duration
}

// Do 在持有锁期间运行 fn，并返回 fn 的 error。
//
// 它会一直阻塞，直到 Lease 空闲（从未被申请、已释放，或由已停止续期的进程持有）或超时；
// 超时则会直接返回 error 而不运行 fn。在 fn 运行期间，锁声明会在后台持续续期；
// 无论 fn 是否失败或 ctx 是否被取消，锁都会在 fn 返回后释放。
//
// fn 会收到被接管的原持有者名称，若获取时锁原本就是空闲的则收到 ""。
// 这是该锁能提供的关于前任持有者的最强信息：获取时为空闲说明锁是被主动释放的，
// 而锁只会在被保护的调用自行结束时才被释放，因此前任进程肯定已不再运行。
// 接管而来的锁声明则恰恰相反，其持有者只是不再续期，这并不能证明它已停止工作。
// 至于已知的该前任持有者是否确实已经退出，则需由调用方自行判断。
func (l Lock) Do(ctx context.Context, fn func(ctx context.Context, predecessor string) error) error {
	if l.Leases == nil || l.Name == "" || l.Holder == "" {
		return fmt.Errorf("lock client, name and holder are required")
	}

	duration, timeout := l.Duration, l.Timeout
	if duration <= 0 {
		duration = defaultLockDuration
	}
	if timeout <= 0 {
		timeout = defaultLockTimeout
	}

	predecessor, err := l.acquire(ctx, duration, timeout)
	if err != nil {
		return err
	}
	klog.InfoS("acquired lock", "lease", l.Name, "holder", l.Holder, "tookOverFrom", predecessor)

	// 续期和释放有意比调用方的 ctx 存活更久：被取消的 ctx 并不会立即中止该锁保护的工作，
	// 因此也不能就此放弃锁声明。
	held := context.WithoutCancel(ctx)

	// 失去锁声明会取消被保护的工作。这是一种收尾通知，而非强隔离栅栏：
	// 已经开始应用的 Helm 操作在 ctx 取消后仍会继续执行，因此这只是缩小与接管者之间的重叠窗口，而无法完全消除重叠。
	guarded, lost := context.WithCancel(ctx)
	defer lost()

	stop, renewed := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(renewed)
		l.renew(held, duration, stop, lost)
	}()

	defer func() {
		// 在释放前等待续期停止，可确保在 Lease 被清理期间没有其他协程再写入它。
		close(stop)
		<-renewed

		// 仅在受保护的调用自行结束时才释放持有权。被中断的调用不一定真正停止了（例如 Helm 在取消时就会返回，而其 apply 操作仍在继续），
		// 如果此时释放持有权，会向下一个持有者发出前任已完成工作的错误信号。保留原状让其自然过期，才符合实际已知的状态。
		//
		// 这里检查的是受保护的 context，而非调用方的 context：它在调用方取消或当前进程失去持有权时都会被终止，
		// 而调用方对此一无所知。因续约停滞而非被其他节点抢占导致失去持有权，才是产生这一区别的关键——
		// 此时尚无其他节点写入该 Lease，其中记录的仍是当前持有者，若执行释放就会真正解除锁定。
		// 上方的等待已确保续约协程返回，因此这里读取的是其最终结果，不存在竞态。
		if guarded.Err() != nil {
			klog.InfoS("leaving the claim to expire", "lease", l.Name, "holder", l.Holder)
			return
		}

		l.release(held, duration)
	}()

	return fn(guarded, predecessor)
}

// acquire 申领 Lease，若当前有存活的持有者则等待直至超时，并返回接管前的原持有者名称；若发现锁处于空闲状态则返回空字符串。
//
// 失败时返回最后一次尝试失败的原因，而非仅仅提示等待超时：读取错误方需要知道当前究竟是哪个节点持有该锁。
func (l Lock) acquire(ctx context.Context, duration, timeout time.Duration) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var seen observation
	for {
		predecessor, err := l.claim(ctx, duration, &seen)
		if err == nil {
			return predecessor, nil
		}

		// 只有锁竞争才值得等待重试。因权限不足或请求内容本身无效而被拒绝的请求每次都会以同样的方式失败，
		// 持续重试只会将真正原因掩盖到整个超时结束。
		if kerrors.IsForbidden(err) || kerrors.IsUnauthorized(err) || kerrors.IsInvalid(err) {
			return "", fmt.Errorf("acquire lock %s as %s: %w", l.Name, l.Holder, err)
		}

		select {
		case <-ctx.Done():
			return "", fmt.Errorf("acquire lock %s as %s: %w", l.Name, l.Holder, err)
		case <-time.After(retryInterval(duration)):
		}
	}
}

// claim 尝试申领一次 Lease，并记录观察到的其他节点持有情况，以便后续尝试区分锁是停滞未动还是仍在续约。
// 若从其他节点接管了锁，则返回该原持有者名称。
//
// 单次尝试受锁自身持有时长限制：挂起时间超过该时长的请求会比其竞争的锁生命周期更长，
// 而客户端自身的超时时间通常可能远长于此。
func (l Lock) claim(ctx context.Context, duration time.Duration, seen *observation) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()

	lease, err := l.Leases.Get(ctx, l.Name, meta.GetOptions{})
	if kerrors.IsNotFound(err) {
		_, err = l.Leases.Create(ctx, l.claimed(nil, duration), meta.CreateOptions{})
		return "", err
	}
	if err != nil {
		return "", err
	}

	// 若发现当前持有者正是本进程自身，则直接回收而非等待过期，并视同锁原为空闲：
	// 这意味着它是同一身份的上一个实例留下的，而该实例已经退出。
	holder := ptr.Deref(lease.Spec.HolderIdentity, "")
	predecessor := holder
	if holder == l.Holder {
		predecessor = ""
	}
	if predecessor != "" && !seen.standingFor(lease, duration) {
		return "", fmt.Errorf("held by %s", holder)
	}

	// 更新请求携带了刚读取到的 resourceVersion，因此当两个进程同时发现同一个空闲 Lease 时，
	// 只会有一个成功，另一个则进入重试。
	_, err = l.Leases.Update(ctx, l.claimed(lease, duration), meta.UpdateOptions{})

	return predecessor, err
}

// renew 持续为锁续约，直到 stop 被关闭（持有者在受保护函数返回后关闭该通道）或持有权丢失；
// 一旦丢失持有权便立即调用 lost，使受锁保护的工作及时收尾退出，避免与其他节点并发执行。
func (l Lock) renew(ctx context.Context, duration time.Duration, stop <-chan struct{}, lost func()) {
	ticker := time.NewTicker(duration / 3)
	defer ticker.Stop()

	held := time.Now()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
		}

		// 将单次续约尝试限制在续约间隔内，避免挂起的请求超过其所续约锁的剩余有效期而导致丢锁未被察觉。
		attempt, cancel := context.WithTimeout(ctx, duration/3)
		err := l.renewOnce(attempt, duration)
		cancel()

		if err == nil {
			held = time.Now()
			continue
		}
		klog.InfoS("renewing lock", "lease", l.Name, "err", err)

		// 锁在整个有效期内都未能续约时，其他节点随时可能将其接管，因此即使尚未观测到被抢占，也应视为已丢失。
		if errors.Is(err, errLockTakenOver) || time.Since(held) > duration {
			klog.InfoS("lost lock", "lease", l.Name, "holder", l.Holder, "err", err)
			lost()

			return
		}
	}
}

// renewOnce 重新读取 Lease 并写入本进程的持有声明；若发现已被其他节点持有，则返回 errLockTakenOver。
func (l Lock) renewOnce(ctx context.Context, duration time.Duration) error {
	lease, err := l.Leases.Get(ctx, l.Name, meta.GetOptions{})
	if err != nil {
		return err
	}
	if holder := ptr.Deref(lease.Spec.HolderIdentity, ""); holder != l.Holder {
		return fmt.Errorf("%w by %s", errLockTakenOver, holder)
	}

	_, err = l.Leases.Update(ctx, l.claimed(lease, duration), meta.UpdateOptions{})

	return err
}

// release 释放 Lease，但若期间已有其他进程接管，则保留对方的持有状态。
func (l Lock) release(ctx context.Context, duration time.Duration) {
	ctx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()

	lease, err := l.Leases.Get(ctx, l.Name, meta.GetOptions{})
	if err != nil {
		klog.InfoS("releasing lock", "lease", l.Name, "err", err)
		return
	}
	if ptr.Deref(lease.Spec.HolderIdentity, "") != l.Holder {
		return
	}

	lease.Spec.HolderIdentity = ptr.To("")
	if _, err = l.Leases.Update(ctx, lease, meta.UpdateOptions{}); err != nil {
		// 保持当前持有状态等待其自然过期，这会使等待方多耗费一个 Duration 的时间。
		klog.InfoS("releasing lock", "lease", l.Name, "err", err)
		return
	}
	klog.InfoS("released lock", "lease", l.Name, "holder", l.Holder)
}

// claimed 返回已标记为本进程持有的指定 Lease；若传入为 nil，则创建一个新的 Lease。
func (l Lock) claimed(lease *coordination.Lease, duration time.Duration) *coordination.Lease {
	now := meta.NewMicroTime(time.Now())
	if lease == nil {
		lease = &coordination.Lease{ObjectMeta: meta.ObjectMeta{Name: l.Name}}
	}

	if ptr.Deref(lease.Spec.HolderIdentity, "") != l.Holder {
		lease.Spec.AcquireTime = &now
	}
	lease.Spec.HolderIdentity = ptr.To(l.Holder)
	// 该字段单位为整秒，因此需要向上取整：若直接截断，亚秒级时长会变成 0 秒，导致刚写入就被判定为已过期。
	lease.Spec.LeaseDurationSeconds = ptr.To(int32(math.Ceil(duration.Seconds())))
	lease.Spec.RenewTime = &now

	return lease
}

// observation 记录本进程最近一次观测到的其他节点持锁状态及观测时间。
//
// 锁是否过期完全根据本进程持续观测到同一持锁状态未发生变化的时长来判断，
// 绝不将本地时钟与 Lease 中的续约时间戳作比较——该时间戳由另一台主机写入，
// 若两台主机时钟偏差超过锁的有效期，双方都会将对方的存活锁误判为已过期并同时继续执行。
type observation struct {
	holder    string
	renewedAt meta.MicroTime
	seenAt    time.Time
}

// standingFor 报告指定 Lease 是否与正在观测的持锁状态一致且已保持未变至少达到指定时长。
// 首次观测到的锁绝不会满足条件，因为此时尚无法判断其持有者是否仍在续约。
func (o *observation) standingFor(lease *coordination.Lease, duration time.Duration) bool {
	holder := ptr.Deref(lease.Spec.HolderIdentity, "")
	renewedAt := ptr.Deref(lease.Spec.RenewTime, meta.MicroTime{})

	if o.holder != holder || !o.renewedAt.Equal(&renewedAt) {
		*o = observation{holder: holder, renewedAt: renewedAt, seenAt: time.Now()}
		return false
	}

	return time.Since(o.seenAt) >= duration
}

// retryInterval 表示等待方两次读取 Lease 之间的间隔时间。该间隔绝不会超过所观测锁的有效期：
// 必须在其有效期内至少读取两次以上，状态停滞未变才具有判定意义。
func retryInterval(duration time.Duration) time.Duration {
	if step := duration / 3; step < maxLockRetryInterval {
		return step
	}

	return maxLockRetryInterval
}
