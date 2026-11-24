package manager

import (
	"context"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"

	worker "gpustack.ai/gpustack/api/worker/v1"
	"gpustack.ai/gpustack/pkg/kubeclients/kubernetes"
)

// TestWorkerSubscribe_BlocksWithoutInformers 验证空 GVK 订阅也会保留注册。
// 后台任务应等到取消后退出，让网关能持续对该集群执行实时 List。
func TestWorkerSubscribe_BlocksWithoutInformers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	wk := &_Worker{
		Context:   ctx,
		Cancel:    cancel,
		Cluster:   "test",
		Informers: make(map[schema.GroupVersionKind]cache.SharedIndexInformer),
	}

	done := make(chan error, 1)
	go func() { done <- wk.Subscribe() }()

	// 没有 informer 的订阅也不能自行返回，否则管理器会反复删除和重新注册 Worker。
	select {
	case err := <-done:
		t.Fatalf("Subscribe returned before cancel: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	assert.True(t, wk.AllReady.Load(), "a zero-informer worker should report ready")

	// 取消订阅必须使 Subscribe 结束等待。
	wk.Cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Subscribe did not return after cancel")
	}
}

// _ProbeRecorder 记录指向待就绪集群的真实 HTTP 探测请求。每个请求一直等到调用方取消，
// 因此正在处理的请求数能反映同时运行的就绪检查数。
type _ProbeRecorder struct {
	Started  atomic.Int64
	InFlight atomic.Int64

	arrived chan struct{}
}

func newProbeRecorder() *_ProbeRecorder {
	return &_ProbeRecorder{arrived: make(chan struct{}, 1)}
}

func (p *_ProbeRecorder) ServeHTTP(_ http.ResponseWriter, r *http.Request) {
	p.Started.Add(1)
	p.InFlight.Add(1)
	defer p.InFlight.Add(-1)

	select {
	case p.arrived <- struct{}{}:
	default:
	}

	<-r.Context().Done()
}

// WaitForProbe 等到收到一次探测，并消费这次到达通知。
func (p *_ProbeRecorder) WaitForProbe(t *testing.T) {
	t.Helper()

	select {
	case <-p.arrived:
	case <-time.After(10 * time.Second):
		t.Fatal("no readiness probe arrived")
	}
}

func newTestManager(t *testing.T, rec *_ProbeRecorder) *_Manager {
	t.Helper()

	srv := httptest.NewServer(rec)
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	cfg := &Config{
		ConstructRestConfig: func(_, _ string) (*rest.Config, error) {
			return &rest.Config{Host: srv.URL}, nil
		},
		ResyncPeriod: time.Minute,
	}
	m, err := cfg.Apply(ctx)
	require.NoError(t, err)

	return m.(*_Manager)
}

// TestSubscribeWorker_UnsubscribePendingStopsProbing 验证集群尚未就绪时取消订阅，
// 正在进行的探测会终止，之后也不会重新探测。
func TestSubscribeWorker_UnsubscribePendingStopsProbing(t *testing.T) {
	rec := newProbeRecorder()
	wm := newTestManager(t, rec)

	require.NoError(t, wm.SubscribeWorker(context.Background(), "6", "token", nil, false))
	rec.WaitForProbe(t)
	assert.EqualValues(t, 1, rec.InFlight.Load(), "exactly one readiness probe in flight")

	wm.UnsubscribeWorker(context.Background(), "6")

	assert.Eventually(t, func() bool { return rec.InFlight.Load() == 0 },
		5*time.Second, 20*time.Millisecond, "unsubscribe must abort the in-flight probe")
}

// TestSubscribeWorker_RepeatedSubscribeStartsOneLoop 验证同一集群已有等待中的订阅时，
// 重复请求不会再启动一条就绪循环。
func TestSubscribeWorker_RepeatedSubscribeStartsOneLoop(t *testing.T) {
	rec := newProbeRecorder()
	wm := newTestManager(t, rec)

	for i := 0; i < 3; i++ {
		require.NoError(t, wm.SubscribeWorker(context.Background(), "6", "token", nil, false))
	}

	rec.WaitForProbe(t)
	// 留出额外探测本来可能到达的时间，以便检测是否出现了第二条循环。
	time.Sleep(500 * time.Millisecond)

	assert.EqualValues(t, 1, rec.Started.Load(), "only one readiness loop may run per cluster")
	assert.EqualValues(t, 1, rec.InFlight.Load())
}

// TestSubscribeWorker_StalledCheckIsAbandonedAndRetried 验证单轮就绪检查的时限。
// 上游收了请求却不应答时，网关应结束这次请求，退避后再试。
func TestSubscribeWorker_StalledCheckIsAbandonedAndRetried(t *testing.T) {
	rec := newProbeRecorder()
	wm := newTestManager(t, rec)
	// 测试 REST 配置没有客户端超时，因此这里的超时只能来自就绪检查本身。
	wm.ReadinessCheckTimeout = 50 * time.Millisecond
	wm.ReadinessBackoff = wait.Backoff{
		Duration: 10 * time.Millisecond,
		Factor:   1,
		Steps:    math.MaxInt32,
	}

	require.NoError(t, wm.SubscribeWorker(context.Background(), "6", "token", nil, false))

	// 测试端点始终没有返回结果。能够收到后续探测，说明前一轮确实已结束。
	assert.Eventually(t, func() bool { return rec.Started.Load() >= 3 },
		5*time.Second, 20*time.Millisecond,
		"a stalled readiness check must be given up on and retried")

	wm.UnsubscribeWorker(context.Background(), "6")
	assert.Eventually(t, func() bool { return rec.InFlight.Load() == 0 },
		5*time.Second, 20*time.Millisecond, "no abandoned check may stay in flight")
}

// _FakeReadiness 记录每次就绪检查的开始时间，用于独立验证管理器的重试节奏。
type _FakeReadiness struct {
	mu     sync.Mutex
	starts []time.Time

	// err 是每一轮检查返回的结果。
	err error
}

func (f *_FakeReadiness) Check(_ context.Context, _ kubernetes.Interface) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.starts = append(f.starts, time.Now())

	return f.err
}

func (f *_FakeReadiness) Attempts() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return len(f.starts)
}

func (f *_FakeReadiness) Gaps() []time.Duration {
	f.mu.Lock()
	defer f.mu.Unlock()

	gaps := make([]time.Duration, 0, len(f.starts))
	for i := 1; i < len(f.starts); i++ {
		gaps = append(gaps, f.starts[i].Sub(f.starts[i-1]))
	}
	return gaps
}

// newFakeReadinessManager 给测试管理器注入可观察的就绪检查和较短退避，便于验证循环本身的行为。
func newFakeReadinessManager(t *testing.T, fake *_FakeReadiness) *_Manager {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	m, err := New(ctx, &Config{
		ConstructRestConfig: func(_, _ string) (*rest.Config, error) {
			return &rest.Config{Host: "https://worker.invalid"}, nil
		},
		ResyncPeriod: time.Minute,
	})
	require.NoError(t, err)

	wm := m.(*_Manager)
	wm.IsServicesReady = fake.Check
	wm.ReadinessBackoff = wait.Backoff{
		Duration: 20 * time.Millisecond,
		Factor:   2,
		Cap:      time.Second,
		Steps:    math.MaxInt32,
	}

	return wm
}

// TestSubscribeWorker_FailedReadinessRetriesUntilUnsubscribed 验证集群持续未就绪时会重试，
// 取消订阅后重试停止。
func TestSubscribeWorker_FailedReadinessRetriesUntilUnsubscribed(t *testing.T) {
	fake := &_FakeReadiness{err: errors.New("api services are not ready")}
	wm := newFakeReadinessManager(t, fake)

	require.NoError(t, wm.SubscribeWorker(context.Background(), "6", "token", nil, false))
	assert.Eventually(t, func() bool { return fake.Attempts() >= 3 },
		5*time.Second, 10*time.Millisecond, "a failed readiness wait must be retried")

	wm.UnsubscribeWorker(context.Background(), "6")

	// 取消时某一轮可能正进入等待，先让它结束再记录尝试数。仍存活的循环会使计数继续增长。
	time.Sleep(200 * time.Millisecond)
	settled := fake.Attempts()
	time.Sleep(500 * time.Millisecond)
	assert.Equal(t, settled, fake.Attempts(), "unsubscribe must stop the retries")
}

// TestSubscribeWorker_BacksOffBetweenFailedAttempts 验证各次失败之间的退避间隔，
// 防止重试变成紧密循环。
func TestSubscribeWorker_BacksOffBetweenFailedAttempts(t *testing.T) {
	fake := &_FakeReadiness{err: errors.New("api services are not ready")}
	wm := newFakeReadinessManager(t, fake)

	require.NoError(t, wm.SubscribeWorker(context.Background(), "6", "token", nil, false))
	assert.Eventually(t, func() bool { return fake.Attempts() >= 4 },
		5*time.Second, 10*time.Millisecond)
	wm.UnsubscribeWorker(context.Background(), "6")

	// 定时器调度可能使实际间隔变长，因此与该次应有的最小退避比较，不比较相邻两次的实测值。
	step := wm.ReadinessBackoff.Duration
	gaps := fake.Gaps()
	require.GreaterOrEqual(t, len(gaps), 3)
	assert.GreaterOrEqual(t, gaps[0], step, "the first retry must wait its first step")
	assert.GreaterOrEqual(t, gaps[2], 4*step, "each retry must wait longer than the last")
}

// TestReadinessBackoff_GrowsAndCapsAtAMinute 核对默认退避从短间隔开始增长，最后维持在一分钟。
func TestReadinessBackoff_GrowsAndCapsAtAMinute(t *testing.T) {
	backoff := newReadinessBackoff()

	first := backoff.Step()
	second := backoff.Step()
	third := backoff.Step()

	assert.Greater(t, second, first, "the gap between attempts must grow")
	assert.Greater(t, third, first, "the gap between attempts must keep growing")

	// 即使集群长期未就绪，达到上限后每分钟仍检查一次。
	var last time.Duration
	for i := 0; i < 10; i++ {
		last = backoff.Step()
		assert.LessOrEqual(t, last, time.Minute, "the gap must not grow past the cap")
	}
	assert.EqualValues(t, time.Minute, last, "the gap must settle at the cap")
}

// newEmptyListServer 为 List 请求返回类型正确的空清单，并记录收到的请求数量。
func newEmptyListServer(t *testing.T, kind string) (kubernetes.Interface, *atomic.Int64) {
	t.Helper()

	var lists atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		lists.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"apiVersion":"` + worker.SchemeGroupVersion.String() +
			`","kind":"` + kind + `List","items":[]}`))
	}))
	t.Cleanup(srv.Close)

	cli, err := kubernetes.NewForConfig(&rest.Config{Host: srv.URL})
	require.NoError(t, err)

	return cli, &lists
}

// TestIterateWorkers_VisitsEachRegisteredClusterOnce 验证不指定 cluster 时，
// 默认列表来自当前注册的 Worker，每个集群恰好访问一次。
func TestIterateWorkers_VisitsEachRegisteredClusterOnce(t *testing.T) {
	m, err := New(context.Background(), &Config{})
	require.NoError(t, err)
	wm := m.(*_Manager)

	counts := make(map[Cluster]*atomic.Int64, 2)
	for _, cluster := range []Cluster{"a", "b"} {
		cli, lists := newEmptyListServer(t, "Devices")
		counts[cluster] = lists
		wm.Workers[cluster] = &_Worker{
			Context:   context.Background(),
			Cancel:    func() {},
			Cluster:   cluster,
			Client:    cli,
			Informers: make(map[schema.GroupVersionKind]cache.SharedIndexInformer),
		}
	}

	// 测试中没有为该 GVK 建立 informer，每次访问都应触发该集群的一次实时 List。
	err = wm.IterateWorkers(context.Background(), nil, worker.SchemeGroupVersionKind("Devices"),
		IteratorOptions{}, func(string, runtime.Object) error { return nil })
	require.NoError(t, err)

	for cluster, lists := range counts {
		assert.EqualValues(t, 1, lists.Load(), "cluster %q must be visited exactly once", cluster)
	}
}

// TestDefaultInformerFactories_IncludesInstanceTypeFlavor 确认硬件池的 GVK 有对应 informer 工厂，
// 使订阅该类型后能持续收到变化。
func TestDefaultInformerFactories_IncludesInstanceTypeFlavor(t *testing.T) {
	gvk := worker.SchemeGroupVersionKind("InstanceTypeFlavor")
	_, ok := defaultInformerFactories[gvk]
	assert.True(t, ok, "InstanceTypeFlavor must have an informer factory registered")
}

// loopback 从同一份 kubeconfig 连接 API。请求中的集群标签和 token 不会把查询转向另一套集群。
func TestLoopbackUsesTheConfiguredCluster(t *testing.T) {
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	path := filepath.Join(t.TempDir(), "config")
	require.NoError(t, os.WriteFile(path, []byte(`apiVersion: v1
kind: Config
clusters:
- name: local
  cluster:
    server: https://127.0.0.1:6443
contexts:
- name: lab
  context:
    cluster: local
    user: reader
current-context: lab
users:
- name: reader
  user:
    token: kubeconfig-test-token
`), 0o600))
	old := clientcmd.RecommendedHomeFile
	clientcmd.RecommendedHomeFile = path
	t.Cleanup(func() { clientcmd.RecommendedHomeFile = old })
	options := NewOptions()
	options.WorkerConnMode = "loopback"
	config, err := options.Complete(t.Context())
	require.NoError(t, err)
	for _, cluster := range []string{"edge-a", "edge-b"} {
		upstream, err := config.ConstructRestConfig(cluster, "request-test-token")
		require.NoError(t, err)
		assert.Equal(t, "https://127.0.0.1:6443", upstream.Host)
		assert.Equal(t, "kubeconfig-test-token", upstream.BearerToken)
	}
}
