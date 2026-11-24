package worker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlrecord "k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/kubeclients/kubernetes/scheme"
	"gpustack.ai/gpustack/pkg/modelartifact"
	"gpustack.ai/gpustack/pkg/utils/json"
)

const (
	testArtifactCommit = "0123456789abcdef0123456789abcdef01234567"
	testArtifactToken  = "artifact-token-never-in-status"
)

// testArtifactHub 模拟 owner/repo 的 HTTP 接口，记录调用次数，并允许用例在两轮协调之间改变源站答复。
type testArtifactHub struct {
	server   *httptest.Server
	whoami   atomic.Int32
	revision atomic.Int32 // 查询版本时返回的 HTTP 状态码。

	requests atomic.Int64
	auth     atomic.Value
}

func newTestArtifactHub(t *testing.T) *testArtifactHub {
	t.Helper()
	h := &testArtifactHub{}
	h.revision.Store(http.StatusOK)
	h.whoami.Store(http.StatusOK)
	h.auth.Store("")
	h.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.requests.Add(1)
		h.auth.Store(r.Header.Get("Authorization"))
		switch r.URL.Path {
		case "/api/models/owner/repo/revision/main":
			if status := int(h.revision.Load()); status != http.StatusOK {
				w.WriteHeader(status)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]string{"sha": testArtifactCommit})
		case "/api/models/owner/repo/tree/" + testArtifactCommit:
			_ = json.NewEncoder(w).Encode([]map[string]any{
				{"type": "file", "path": "config.json", "size": 10, "oid": strings.Repeat("b", 40)},
				{
					"type": "file", "path": "model.safetensors", "size": 100, "oid": strings.Repeat("9", 40),
					"lfs": map[string]any{"oid": strings.Repeat("a", 64), "size": 100},
				},
			})
		case "/owner/repo/resolve/" + testArtifactCommit + "/config.json":
			w.WriteHeader(http.StatusOK)
		case "/api/whoami-v2":
			w.WriteHeader(int(h.whoami.Load()))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(h.server.Close)

	return h
}

// testArtifactClock 允许测试直接推进确认和重验所用的时钟。
type testArtifactClock struct{ now time.Time }

func (c *testArtifactClock) Now() time.Time { return c.now }

type testArtifactEnv struct {
	r     *ModelArtifactReconciler
	cli   ctrlcli.Client
	hub   *testArtifactHub
	clock *testArtifactClock
}

func newTestArtifactEnv(t *testing.T, objs ...ctrlcli.Object) *testArtifactEnv {
	t.Helper()
	hub := newTestArtifactHub(t)
	cli := ctrlfake.NewClientBuilder().WithScheme(scheme.Scheme).
		WithStatusSubresource(&workercore.ModelArtifact{}).
		WithIndex(&workercore.ModelArtifact{}, IndexingModelArtifactByManifestDigest, indexModelArtifactByManifestDigest).
		WithIndex(&workercore.NodeModelStore{}, IndexingNodeModelStoreByModelDigest, indexNodeModelStoreByModelDigest).
		WithObjects(objs...).Build()
	clock := &testArtifactClock{now: time.Date(2026, 9, 25, 6, 0, 0, 0, time.UTC)}
	r := &ModelArtifactReconciler{
		Client: cli,
		Now:    clock.Now,
		NewHuggingFace: func(context.Context) (*modelartifact.HuggingFace, error) {
			return &modelartifact.HuggingFace{Endpoint: hub.server.URL, Client: hub.server.Client()}, nil
		},
	}

	return &testArtifactEnv{r: r, cli: cli, hub: hub, clock: clock}
}

func (e *testArtifactEnv) reconcile(t *testing.T, name string) (*workercore.ModelArtifact, ctrl.Result) {
	t.Helper()
	key := ctrlcli.ObjectKey{Namespace: "team-a", Name: name}
	res, err := e.r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key})
	require.NoError(t, err)
	// 新制品的第一轮协调先写入 Resolving，然后重新排队解析。
	for i := 0; res.Requeue && i < 3; i++ {
		res, err = e.r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key})
		require.NoError(t, err)
	}
	ma := new(workercore.ModelArtifact)
	require.NoError(t, e.cli.Get(context.Background(), key, ma))

	return ma, res
}

func testHubArtifact(secret string) *workercore.ModelArtifact {
	ma := &workercore.ModelArtifact{
		ObjectMeta: meta.ObjectMeta{Namespace: "team-a", Name: "qwen", UID: "uid-qwen", Generation: 1},
		Spec: workercore.ModelArtifactSpec{Source: workercore.ModelArtifactSource{
			HuggingFace: &workercore.ModelArtifactHubSource{Repository: "owner/repo", Revision: "main"},
		}},
	}
	if secret != "" {
		ma.Spec.Source.HuggingFace.SecretRef = &core.LocalObjectReference{Name: secret}
	}

	return ma
}

func testTokenSecret(token string) *core.Secret {
	return &core.Secret{
		ObjectMeta: meta.ObjectMeta{Namespace: "team-a", Name: "hf-token"},
		Data:       map[string][]byte{"token": []byte(token)},
	}
}

// testModelScopeArtifact 构造 ModelScope 来源的制品，可选择引用同命名空间的 Secret。
func testModelScopeArtifact(secret string) *workercore.ModelArtifact {
	ma := &workercore.ModelArtifact{
		ObjectMeta: meta.ObjectMeta{Namespace: "team-a", Name: "qwen", UID: "uid-qwen", Generation: 1},
		Spec: workercore.ModelArtifactSpec{Source: workercore.ModelArtifactSource{
			ModelScope: &workercore.ModelArtifactHubSource{Repository: "qwen/repo", Revision: "master"},
		}},
	}
	if secret != "" {
		ma.Spec.Source.ModelScope.SecretRef = &core.LocalObjectReference{Name: secret}
	}

	return ma
}

// testModelScopeHub 记录预检与解析调用，按用例设置的结果响应 ModelScope 请求。
type testModelScopeHub struct {
	mu         sync.Mutex
	resolution modelartifact.Resolution
	resolveErr error

	list []modelartifact.Manifest // 每次 ListManifest 取一份清单，用完后重复最后一份。

	asks        int // 累计访问 Hub 的次数，包括失败的请求。
	lists       int
	revalidates int
	lastRepo    string
	lastRev     string
	lastToken   string

	lastCommit string
}

func (h *testModelScopeHub) Resolve(_ context.Context, repository, revision, token string, _ modelartifact.Filter) (modelartifact.Resolution, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.asks++
	h.lastRepo, h.lastRev, h.lastToken = repository, revision, token
	if h.resolveErr != nil {
		return modelartifact.Resolution{}, h.resolveErr
	}

	return h.resolution, nil
}

func (h *testModelScopeHub) Revalidate(_ context.Context, repository, commit, token string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.asks++
	h.revalidates++
	h.lastRepo, h.lastCommit, h.lastToken = repository, commit, token
	return nil
}

func (h *testModelScopeHub) ListManifest(_ context.Context, repository, commit, token string, _ modelartifact.Filter) (modelartifact.Manifest, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.asks++
	h.lists++
	h.lastRepo, h.lastCommit, h.lastToken = repository, commit, token
	if len(h.list) == 0 {
		return h.resolution.Manifest, nil
	}
	manifest := h.list[0]
	if len(h.list) > 1 {
		h.list = h.list[1:]
	}

	return manifest, nil
}

func (h *testModelScopeHub) ValidToken(_ context.Context, _ string) (bool, error) {
	return true, nil
}

// newTestModelScopeEnv 建立制品控制器的测试环境，并接入该用例指定的 ModelScope 客户端。
func newTestModelScopeEnv(t *testing.T, hub *testModelScopeHub, objs ...ctrlcli.Object) *testArtifactEnv {
	t.Helper()
	env := newTestArtifactEnv(t, objs...)
	env.r.NewModelScope = func(context.Context) (modelArtifactHub, error) { return hub, nil }

	return env
}

func TestModelArtifactReconcileResolvesModelScope(t *testing.T) {
	digest, err := modelartifact.NewManifest([]modelartifact.ManifestEntry{
		{Path: "README.md", Size: 3, Digest: modelartifact.DigestSHA256 + ":" + strings.Repeat("a", 64)},
	})
	require.NoError(t, err)
	hub := &testModelScopeHub{resolution: modelartifact.Resolution{
		Commit: testArtifactCommit, Manifest: digest,
	}}
	env := newTestModelScopeEnv(t, hub, testModelScopeArtifact(""))

	ma, res := env.reconcile(t, "qwen")
	assert.True(t, ModelArtifactConditionResolved.IsTrue(ma))
	assert.Equal(t, testArtifactCommit, ma.Status.Resolved.Revision)
	assert.Equal(t, digest.Digest, ma.Status.Resolved.ManifestDigest)
	assert.Positive(t, ma.Status.Resolved.FileCount)
	assert.Positive(t, res.RequeueAfter)
	hub.mu.Lock()
	defer hub.mu.Unlock()
	assert.Equal(t, "qwen/repo", hub.lastRepo)
	assert.Equal(t, "master", hub.lastRev)
	assert.Equal(t, "", hub.lastToken, "an artifact without a SecretRef sends no token")
}

func TestModelArtifactReconcileResolvesHuggingFace(t *testing.T) {
	cases := []struct {
		name         string
		objs         []ctrlcli.Object
		revision     int
		wantResolved string
		wantReason   string
		wantDegraded bool
	}{
		{
			name:         "a public repository",
			objs:         []ctrlcli.Object{testHubArtifact("")},
			revision:     http.StatusOK,
			wantResolved: "True", wantReason: "Resolved",
		},
		{
			name:         "a private repository with its token",
			objs:         []ctrlcli.Object{testHubArtifact("hf-token"), testTokenSecret(testArtifactToken)},
			revision:     http.StatusOK,
			wantResolved: "True", wantReason: "Resolved",
		},
		{
			name:         "a private repository without a token",
			objs:         []ctrlcli.Object{testHubArtifact("")},
			revision:     http.StatusUnauthorized,
			wantResolved: "False", wantReason: modelartifact.ReasonAccessDenied, wantDegraded: true,
		},
		{
			name:         "a Secret that does not exist yet",
			objs:         []ctrlcli.Object{testHubArtifact("hf-token")},
			revision:     http.StatusOK,
			wantResolved: "False", wantReason: "SecretNotFound", wantDegraded: true,
		},
		{
			name:         "a Secret without the token key",
			objs:         []ctrlcli.Object{testHubArtifact("hf-token"), testTokenSecret("")},
			revision:     http.StatusOK,
			wantResolved: "False", wantReason: "SecretNotFound", wantDegraded: true,
		},
		{
			name:         "an outage",
			objs:         []ctrlcli.Object{testHubArtifact("")},
			revision:     http.StatusBadGateway,
			wantResolved: "False", wantReason: modelartifact.ReasonSourceUnavailable, wantDegraded: true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			env := newTestArtifactEnv(t, c.objs...)
			env.hub.revision.Store(int32(c.revision))

			ma, res := env.reconcile(t, "qwen")
			assert.Contains(t, ma.Finalizers, ModelArtifactProtectionFinalizer)
			resolved := testCondition(ma.Status.Conditions, string(ModelArtifactConditionResolved))
			require.NotNil(t, resolved)
			assert.Equal(t, c.wantResolved, string(resolved.Status))
			assert.Equal(t, c.wantReason, ModelArtifactConditionResolved.GetReason(ma))
			assert.Equal(t, c.wantDegraded, ModelArtifactConditionDegraded.IsTrue(ma))
			assert.Positive(t, res.RequeueAfter)
			raw, err := json.Marshal(ma.Status)
			require.NoError(t, err)
			assert.NotContains(t, string(raw), testArtifactToken)
			if c.wantResolved != "True" {
				assert.Nil(t, ma.Status.Resolved)
				return
			}
			require.NotNil(t, ma.Status.Resolved)
			assert.Equal(t, testArtifactCommit, ma.Status.Resolved.Revision)
			assert.True(t, strings.HasPrefix(ma.Status.Resolved.ManifestDigest, "sha256:"))
			assert.Equal(t, int64(2), ma.Status.Resolved.FileCount)
			assert.Equal(t, int64(110), ma.Status.Resolved.SizeBytes)
			assert.Equal(t, int64(1), ma.Status.ObservedGeneration)
		})
	}
}

func TestModelArtifactReconcileFiltersTheManifest(t *testing.T) {
	cases := []struct {
		name         string
		allow        []string
		ignore       []string
		wantResolved string
		wantReason   string
		wantFiles    int64
		wantSize     int64
	}{
		{name: "no pattern is the whole commit", wantResolved: "True", wantReason: "Resolved", wantFiles: 2, wantSize: 110},
		{
			name: "an allow pattern keeps its files", allow: []string{"*.safetensors"},
			wantResolved: "True", wantReason: "Resolved", wantFiles: 1, wantSize: 100,
		},
		{
			name: "an ignore pattern drops its files", ignore: []string{"*.safetensors"},
			wantResolved: "True", wantReason: "Resolved", wantFiles: 1, wantSize: 10,
		},
		{
			name: "a filter keeping nothing is an empty manifest", allow: []string{"*.gguf"},
			wantResolved: "False", wantReason: modelartifact.ReasonEmptyManifest,
		},
	}
	var wholeDigest string
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ma := testHubArtifact("")
			ma.Spec.AllowPatterns, ma.Spec.IgnorePatterns = c.allow, c.ignore
			env := newTestArtifactEnv(t, ma)

			got, _ := env.reconcile(t, "qwen")
			resolved := testCondition(got.Status.Conditions, string(ModelArtifactConditionResolved))
			require.NotNil(t, resolved)
			assert.Equal(t, c.wantResolved, string(resolved.Status))
			assert.Equal(t, c.wantReason, ModelArtifactConditionResolved.GetReason(got))
			if c.wantResolved != "True" {
				assert.Nil(t, got.Status.Resolved)
				return
			}
			require.NotNil(t, got.Status.Resolved)
			assert.Equal(t, c.wantFiles, got.Status.Resolved.FileCount)
			assert.Equal(t, c.wantSize, got.Status.Resolved.SizeBytes)
			if c.allow == nil && c.ignore == nil {
				wholeDigest = got.Status.Resolved.ManifestDigest
				return
			}
			assert.NotEqual(t, wholeDigest, got.Status.Resolved.ManifestDigest)
		})
	}
}

func TestModelArtifactReconcileSendsTheNamespaceToken(t *testing.T) {
	env := newTestArtifactEnv(t, testHubArtifact("hf-token"), testTokenSecret(testArtifactToken))

	env.reconcile(t, "qwen")
	assert.Equal(t, "Bearer "+testArtifactToken, env.hub.auth.Load())
}

func TestModelArtifactReconcileWarnsOnARejectedToken(t *testing.T) {
	env := newTestArtifactEnv(t, testHubArtifact("hf-token"), testTokenSecret("mistyped"))
	env.hub.whoami.Store(http.StatusUnauthorized)
	recorder := ctrlrecord.NewFakeRecorder(1)
	env.r.Recorder = recorder

	ma, _ := env.reconcile(t, "qwen")
	require.True(t, ModelArtifactConditionResolved.IsTrue(ma))
	require.Len(t, recorder.Events, 1)
	event := <-recorder.Events
	assert.Contains(t, event, "Warning InvalidToken")
	assert.NotContains(t, event, "mistyped")
}

func TestModelArtifactReconcileResolvesAClaim(t *testing.T) {
	claimArtifact := &workercore.ModelArtifact{
		ObjectMeta: meta.ObjectMeta{Namespace: "team-a", Name: "qwen", UID: "uid-qwen"},
		Spec: workercore.ModelArtifactSpec{Source: workercore.ModelArtifactSource{
			PersistentVolumeClaim: &workercore.ModelArtifactPersistentVolumeClaimSource{ClaimName: "models", Path: "qwen"},
		}},
	}
	claim := &core.PersistentVolumeClaim{ObjectMeta: meta.ObjectMeta{Namespace: "team-a", Name: "models"}}
	cases := []struct {
		name       string
		objs       []ctrlcli.Object
		wantStatus string
		wantReason string
	}{
		{name: "the claim exists", objs: []ctrlcli.Object{claimArtifact.DeepCopy(), claim.DeepCopy()}, wantStatus: "True", wantReason: "Resolved"},
		{name: "the claim does not exist yet", objs: []ctrlcli.Object{claimArtifact.DeepCopy()}, wantStatus: "False", wantReason: "ClaimNotFound"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			env := newTestArtifactEnv(t, c.objs...)

			ma, _ := env.reconcile(t, "qwen")
			resolved := testCondition(ma.Status.Conditions, string(ModelArtifactConditionResolved))
			require.NotNil(t, resolved)
			assert.Equal(t, c.wantStatus, string(resolved.Status))
			assert.Equal(t, c.wantReason, ModelArtifactConditionResolved.GetReason(ma))
			assert.Zero(t, env.hub.requests.Load())
			if c.wantStatus == "True" {
				require.NotNil(t, ma.Status.Resolved)
				assert.Empty(t, ma.Status.Resolved.Revision)
				assert.Empty(t, ma.Status.Resolved.ManifestDigest)
			}
		})
	}
}

// TestModelArtifactReconcileImage 验证 OCI 镜像来源可以直接确认带摘要的引用，
// 不访问仓库、不汇总缓存节点，也无需定时重验。
func TestModelArtifactReconcileImage(t *testing.T) {
	testImageReference := "registry.example.com/team/qwen@sha256:" + strings.Repeat("a", 64)
	ma := testHubArtifact("")
	ma.Spec.Source = workercore.ModelArtifactSource{Image: &workercore.ModelArtifactImageSource{
		Reference: testImageReference,
	}}
	env := newTestArtifactEnv(t, ma)

	// 第一轮只公布正在解析，第二轮写入解析结果。
	_, err := env.r.Reconcile(context.Background(),
		ctrl.Request{NamespacedName: ctrlcli.ObjectKey{Namespace: "team-a", Name: "qwen"}})
	require.NoError(t, err)
	result, err := env.r.Reconcile(context.Background(),
		ctrl.Request{NamespacedName: ctrlcli.ObjectKey{Namespace: "team-a", Name: "qwen"}})
	require.NoError(t, err)
	assert.False(t, result.Requeue)
	assert.Zero(t, result.RequeueAfter, "there is nothing to ask again: no registry, no revalidation")

	got := new(workercore.ModelArtifact)
	require.NoError(t, env.cli.Get(context.Background(),
		ctrlcli.ObjectKey{Namespace: "team-a", Name: "qwen"}, got))
	require.NotNil(t, got.Status.Resolved)
	assert.NotNil(t, got.Status.Resolved.ResolvedTime)
	assert.Empty(t, got.Status.Resolved.Revision, "an image source has no commit")
	assert.Empty(t, got.Status.Resolved.ManifestDigest, "the reference is the whole identity")
	assert.Nil(t, got.Status.Nodes, "no node ever aggregates an image")
	assert.True(t, ModelArtifactConditionResolved.IsTrue(got))
	assert.Equal(t, "Resolved", ModelArtifactConditionResolved.GetReason(got))
	assert.True(t, ModelArtifactConditionDegraded.IsFalse(got))
}

// testAnchoredModelScopeArtifact 构造带 expectedDigest 的 ModelScope 制品，
// 并按需指定访问凭据。
func testAnchoredModelScopeArtifact(digest, secret string) *workercore.ModelArtifact {
	ma := testModelScopeArtifact(secret)
	ma.Spec.ExpectedDigest = digest

	return ma
}

func testManifest(path, hash string, size int64) (modelartifact.Manifest, error) {
	return modelartifact.NewManifest([]modelartifact.ManifestEntry{
		{Path: path, Size: size, Digest: modelartifact.DigestSHA256 + ":" + hash},
	})
}

// TestModelArtifactReconcileAssertsTheExpectedDigest 覆盖摘要匹配、拒绝不匹配内容和重复确认故障后采用用户摘要的情况。
// 还会核对后续重验如何处理内容变化，以及采用 Expected 后不再访问 Hub。
func TestModelArtifactReconcileAssertsTheExpectedDigest(t *testing.T) {
	good, err := testManifest("README.md", strings.Repeat("a", 64), 3)
	require.NoError(t, err)
	drifted, err := testManifest("README.md", strings.Repeat("f", 64), 3)
	require.NoError(t, err)

	t.Run("a resolution matching the anchor stands", func(t *testing.T) {
		hub := &testModelScopeHub{resolution: modelartifact.Resolution{Commit: testArtifactCommit, Manifest: good}}
		env := newTestModelScopeEnv(t, hub, testAnchoredModelScopeArtifact(good.Digest, ""))

		ma, _ := env.reconcile(t, "qwen")
		assert.True(t, ModelArtifactConditionResolved.IsTrue(ma))
		require.NotNil(t, ma.Status.Resolved)
		assert.Equal(t, good.Digest, ma.Status.Resolved.ManifestDigest)
		assert.Equal(t, testArtifactCommit, ma.Status.Resolved.Revision)
		assert.Equal(t, workercore.ModelArtifactDigestSourceHub, ma.Status.Resolved.DigestSource)
	})
	t.Run("a resolution missing the anchor is refused with both digests", func(t *testing.T) {
		hub := &testModelScopeHub{resolution: modelartifact.Resolution{Commit: testArtifactCommit, Manifest: good}}
		env := newTestModelScopeEnv(t, hub, testAnchoredModelScopeArtifact(drifted.Digest, ""))

		ma, _ := env.reconcile(t, "qwen")
		assert.False(t, ModelArtifactConditionResolved.IsTrue(ma))
		assert.Equal(t, modelartifact.ReasonDigestMismatch, ModelArtifactConditionResolved.GetReason(ma))
		assert.Nil(t, ma.Status.Resolved, "a refused resolution writes no identity")
		assert.True(t, ModelArtifactConditionDegraded.IsTrue(ma))
		message := ModelArtifactConditionResolved.GetMessage(ma)
		assert.Contains(t, message, drifted.Digest)
		assert.Contains(t, message, good.Digest)
	})
	t.Run("a drift at the resolved commit walks the staircase", func(t *testing.T) {
		hub := &testModelScopeHub{
			resolution: modelartifact.Resolution{Commit: testArtifactCommit, Manifest: good},
			list:       []modelartifact.Manifest{drifted, drifted},
		}
		env := newTestModelScopeEnv(t, hub, testAnchoredModelScopeArtifact(good.Digest, ""))
		resolved, _ := env.reconcile(t, "qwen")
		require.True(t, ModelArtifactConditionResolved.IsTrue(resolved))

		day := 24 * time.Hour
		env.clock.now = env.clock.now.Add(day)
		first, _ := env.reconcile(t, "qwen")
		assert.True(t, ModelArtifactConditionResolved.IsTrue(first), "the first mismatch degrades, not revokes")
		assert.Equal(t, modelartifact.ReasonDigestMismatch, ModelArtifactConditionDegraded.GetReason(first))
		hub.mu.Lock()
		assert.Equal(t, testArtifactCommit, hub.lastCommit, "the revalidation lists at the resolved commit")
		assert.Equal(t, 1, hub.lists, "the anchored revalidation is one listing")
		assert.Equal(t, 0, hub.revalidates, "the anchored revalidation is not the HEAD probe")
		hub.mu.Unlock()

		env.clock.now = env.clock.now.Add(10 * time.Second)
		env.reconcile(t, "qwen")
		env.clock.now = env.clock.now.Add(time.Minute)
		revoked, _ := env.reconcile(t, "qwen")
		assert.False(t, ModelArtifactConditionResolved.IsTrue(revoked), "the confirmed mismatch revokes")
		assert.Equal(t, modelartifact.ReasonDigestMismatch, ModelArtifactConditionResolved.GetReason(revoked))
		require.NotNil(t, revoked.Status.Resolved)
		assert.Equal(t, testArtifactCommit, revoked.Status.Resolved.Revision, "the identity stays for audit")
	})
	t.Run("a confirmed outage resolves to the anchor without the hub", func(t *testing.T) {
		outage := &modelartifact.SourceError{Reason: modelartifact.ReasonSourceUnavailable, Message: "HTTP 503"}
		hub := &testModelScopeHub{resolveErr: outage}
		env := newTestModelScopeEnv(t, hub, testAnchoredModelScopeArtifact(good.Digest, ""))

		env.reconcile(t, "qwen")
		got := mustGet(t, env.cli)
		assert.False(t, ModelArtifactConditionResolved.IsTrue(got), "one blip is not a verdict")
		assert.Nil(t, got.Status.Resolved)

		env.clock.now = env.clock.now.Add(2 * modelArtifactUnavailableRetry)
		env.reconcile(t, "qwen")
		got = mustGet(t, env.cli)
		assert.True(t, ModelArtifactConditionResolved.IsTrue(got), "the confirmed outage anchors the identity")
		require.NotNil(t, got.Status.Resolved)
		assert.Equal(t, good.Digest, got.Status.Resolved.ManifestDigest)
		assert.Equal(t, workercore.ModelArtifactDigestSourceExpected, got.Status.Resolved.DigestSource)
		assert.Empty(t, got.Status.Resolved.Revision, "an Expected identity has no commit")
		assert.Zero(t, got.Status.Resolved.FileCount)
		assert.Zero(t, got.Status.Resolved.SizeBytes)

		hub.mu.Lock()
		asksBefore := hub.asks
		hub.mu.Unlock()
		env.clock.now = env.clock.now.Add(25 * time.Hour)
		env.reconcile(t, "qwen")
		hub.mu.Lock()
		defer hub.mu.Unlock()
		assert.Equal(t, asksBefore, hub.asks, "an Expected artifact asks the hub for nothing, ever")
	})
	t.Run("a hub that answered is never anchored around", func(t *testing.T) {
		refused := &modelartifact.SourceError{Reason: modelartifact.ReasonAccessDenied, Message: "private"}
		hub := &testModelScopeHub{resolveErr: refused}
		env := newTestModelScopeEnv(t, hub, testAnchoredModelScopeArtifact(good.Digest, ""))

		env.reconcile(t, "qwen")
		env.clock.now = env.clock.now.Add(2 * modelArtifactUnavailableRetry)
		env.reconcile(t, "qwen")

		ma := new(workercore.ModelArtifact)
		require.NoError(t, env.cli.Get(context.Background(),
			ctrlcli.ObjectKey{Namespace: "team-a", Name: "qwen"}, ma))
		assert.False(t, ModelArtifactConditionResolved.IsTrue(ma))
		assert.Equal(t, modelartifact.ReasonAccessDenied, ModelArtifactConditionResolved.GetReason(ma))
		assert.Nil(t, ma.Status.Resolved, "an answered hub's refusal is the verdict")
	})
	t.Run("a single blip does not freeze the identity", func(t *testing.T) {
		outage := &modelartifact.SourceError{Reason: modelartifact.ReasonSourceUnavailable, Message: "HTTP 503"}
		hub := &testModelScopeHub{resolveErr: outage}
		env := newTestModelScopeEnv(t, hub, testAnchoredModelScopeArtifact(good.Digest, ""))

		env.reconcile(t, "qwen")
		hub.mu.Lock()
		hub.resolveErr = nil
		hub.resolution = modelartifact.Resolution{Commit: testArtifactCommit, Manifest: good}
		hub.mu.Unlock()
		env.clock.now = env.clock.now.Add(2 * modelArtifactUnavailableRetry)
		ma, _ := env.reconcile(t, "qwen")

		assert.True(t, ModelArtifactConditionResolved.IsTrue(ma))
		require.NotNil(t, ma.Status.Resolved)
		assert.Equal(t, testArtifactCommit, ma.Status.Resolved.Revision, "the hub answered, so the hub decides")
		assert.Equal(t, workercore.ModelArtifactDigestSourceHub, ma.Status.Resolved.DigestSource)
	})
	t.Run("an unanchored artifact revalidates with the HEAD probe, not a listing", func(t *testing.T) {
		hub := &testModelScopeHub{resolution: modelartifact.Resolution{Commit: testArtifactCommit, Manifest: good}}
		env := newTestModelScopeEnv(t, hub, testModelScopeArtifact(""))
		env.reconcile(t, "qwen")

		env.clock.now = env.clock.now.Add(25 * time.Hour)
		env.reconcile(t, "qwen")
		assert.True(t, ModelArtifactConditionResolved.IsTrue(mustGet(t, env.cli)))
	})
}

func mustGet(t *testing.T, cli ctrlcli.Client) *workercore.ModelArtifact {
	t.Helper()
	ma := new(workercore.ModelArtifact)
	require.NoError(t, cli.Get(context.Background(),
		ctrlcli.ObjectKey{Namespace: "team-a", Name: "qwen"}, ma))

	return ma
}
