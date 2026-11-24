package driver

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	gpustack "gpustack.ai/gpustack/api/v1"
	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/kubeclients/kubernetes/scheme"
	"gpustack.ai/gpustack/pkg/modelmanager/store"
)

var (
	testHex    = strings.Repeat("a", 64)
	testDigest = "sha256:" + testHex
)

// fakeMounter 在内存中记录挂载状态而不执行真实系统调用。
type fakeMounter struct {
	mu      sync.Mutex
	mounted map[string]string // 目标路径 -> 源路径
	binds   int
	bindErr error
}

func (m *fakeMounter) IsMountPoint(path string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.mounted[path]
	return ok, nil
}

func (m *fakeMounter) BindReadOnly(source, target string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.bindErr != nil {
		return m.bindErr
	}
	m.mounted[target] = source
	m.binds++
	return nil
}

func (m *fakeMounter) IsReadOnly(path string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.mounted[path]
	return ok, nil
}

func (m *fakeMounter) Unmount(target string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.mounted, target)
	return nil
}

// fakeMaterializer 返回预设的进度并记录收到的所有物化请求。
type fakeMaterializer struct {
	progress Progress
	requests []Request
	publish  func(hex string)
}

func (m *fakeMaterializer) Ensure(_ context.Context, req Request) Progress {
	m.requests = append(m.requests, req)
	if m.progress.Published && m.publish != nil {
		m.publish(req.Hex)
	}
	return m.progress
}

type fakeEvents struct{ mounted, unmounted []string }

func (e *fakeEvents) Mounted(hex string)   { e.mounted = append(e.mounted, hex) }
func (e *fakeEvents) Unmounted(hex string) { e.unmounted = append(e.unmounted, hex) }

type testEnv struct {
	d            *Driver
	mounter      *fakeMounter
	materializer *fakeMaterializer
	events       *fakeEvents
	kubelet      string
}

func testArtifact(namespace, name, uid, digest string, resolved bool) *workercore.ModelArtifact {
	condStatus := meta.ConditionTrue
	if !resolved {
		condStatus = meta.ConditionFalse
	}
	return &workercore.ModelArtifact{
		ObjectMeta: meta.ObjectMeta{Namespace: namespace, Name: name, UID: types.UID(uid)},
		Spec: workercore.ModelArtifactSpec{Source: workercore.ModelArtifactSource{
			HuggingFace: &workercore.ModelArtifactHubSource{Repository: "owner/repo", Revision: "main"},
		}},
		Status: workercore.ModelArtifactStatus{
			Resolved:   &workercore.ModelArtifactResolved{Revision: strings.Repeat("c", 40), ManifestDigest: digest},
			Conditions: []gpustack.Condition{{Type: "Resolved", Status: condStatus}},
		},
	}
}

func newTestEnv(t *testing.T, objs ...ctrlcli.Object) *testEnv {
	t.Helper()
	root := t.TempDir()
	st, err := store.Open(root)
	require.NoError(t, err)
	// 已发布目录被封存为只读；非 root 用户下测试清理临时目录前需先恢复写权限。
	t.Cleanup(func() {
		_ = filepath.WalkDir(root, func(p string, e os.DirEntry, err error) error {
			if err == nil && e.IsDir() {
				_ = os.Chmod(p, 0o755)
			}
			return nil
		})
	})
	e := &testEnv{
		mounter:      &fakeMounter{mounted: map[string]string{}},
		materializer: &fakeMaterializer{progress: Progress{Code: codes.Aborted, Message: "downloading"}},
		events:       &fakeEvents{},
		kubelet:      t.TempDir(),
	}
	e.d = &Driver{
		Name: "model.csi.gpustack.ai", NodeID: "node-1", KubeletDir: e.kubelet, Store: st,
		Artifacts:    ctrlfake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(objs...).Build(),
		Ready:        func() bool { return true },
		Mounter:      e.mounter,
		Materializer: e.materializer,
		Events:       e.events,
	}
	return e
}

// publish 在测试环境的 store 中发布 hex 对应的目录树。
func (e *testEnv) publish(t *testing.T, hex string) {
	t.Helper()
	a, err := e.d.Store.NewAttempt(hex)
	require.NoError(t, err)
	p, err := a.FilePath("config.json")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(p, []byte("{}"), 0o644))
	require.NoError(t, a.Publish(store.Marker{Digest: "sha256:" + hex}))
}

func (e *testEnv) target(pod string) string {
	return filepath.Join(e.kubelet, "pods", pod, "volumes", "kubernetes.io~csi", "gpustack-model", "mount")
}

// kubeletVolumeContext 按 kubelet 的行为构造卷上下文：先放入 Pod 的 volumeAttributes，
// 再以 kubelet 的 Pod 信息覆盖写入，防止 Pod 伪造受信任键。
func kubeletVolumeContext(attrs map[string]string, podNamespace string) map[string]string {
	vc := map[string]string{}
	for k, v := range attrs {
		vc[k] = v
	}
	for k, v := range map[string]string{
		attrPodNamespace: podNamespace, attrPodName: "p", attrPodUID: "uid-p", attrEphemeral: "true",
	} {
		vc[k] = v
	}
	return vc
}

func publishRequest(target string, vc map[string]string) *csi.NodePublishVolumeRequest {
	return &csi.NodePublishVolumeRequest{
		VolumeId:         "csi-vol-1",
		TargetPath:       target,
		VolumeCapability: &csi.VolumeCapability{AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}}},
		Readonly:         true,
		VolumeContext:    vc,
		Secrets:          map[string]string{"token": "hf_secret"},
	}
}

func hints(artifact, uid, digest string) map[string]string {
	return map[string]string{AttrArtifact: artifact, AttrArtifactUID: uid, AttrManifestDigest: digest}
}

func TestNodePublishMountsPublishedContent(t *testing.T) {
	env := newTestEnv(t, testArtifact("team-a", "qwen", "uid-qwen", testDigest, true))
	env.publish(t, testHex)
	target := env.target("uid-p")
	req := publishRequest(target, kubeletVolumeContext(hints("qwen", "uid-qwen", testDigest), "team-a"))

	_, err := env.d.NodePublishVolume(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, env.d.Store.PublishedTree(testHex), env.mounter.mounted[target])
	assert.Empty(t, env.materializer.requests, "published content is mounted without the materializer")
	ref, found, err := env.d.Store.ReadRef("csi-vol-1")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, store.Ref{VolumeID: "csi-vol-1", TargetPath: target, Hex: testHex, PodNamespace: "team-a", PodName: "p", PodUID: "uid-p"}, ref)
	assert.Equal(t, []string{testHex}, env.events.mounted)

	// kubelet 对已挂载目标再次调用：不会重复执行挂载。
	_, err = env.d.NodePublishVolume(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, 1, env.mounter.binds)
}

func TestNodePublishClaimsTheTreeBeforeBinding(t *testing.T) {
	cases := []struct {
		name string
		// breakRefs 将账本的 refs 目录替换为普通文件，使引用写入必然失败。
		breakRefs bool
		bindErr   error
		wantCode  codes.Code
		wantBinds int
		wantRef   bool
	}{
		{name: "a mounted tree is recorded, so the collector keeps it", wantCode: codes.OK, wantBinds: 1, wantRef: true},
		{name: "a reference that cannot be written fails the mount and binds nothing", breakRefs: true, wantCode: codes.Internal},
		{name: "a bind that fails leaves no reference", bindErr: errors.New("mount: permission denied"), wantCode: codes.Internal},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			env := newTestEnv(t, testArtifact("team-a", "qwen", "uid-qwen", testDigest, true))
			env.publish(t, testHex)
			env.mounter.bindErr = c.bindErr
			if c.breakRefs {
				refs := filepath.Join(env.d.Store.Root(), "ledger", "refs")
				require.NoError(t, os.RemoveAll(refs))
				require.NoError(t, os.WriteFile(refs, nil, 0o644))
			}

			_, err := env.d.NodePublishVolume(context.Background(),
				publishRequest(env.target("uid-p"), kubeletVolumeContext(hints("qwen", "uid-qwen", testDigest), "team-a")))
			assert.Equal(t, c.wantCode, status.Code(err))
			assert.Equal(t, c.wantBinds, env.mounter.binds)
			if c.breakRefs {
				return
			}
			_, found, rerr := env.d.Store.ReadRef("csi-vol-1")
			require.NoError(t, rerr)
			assert.Equal(t, c.wantRef, found)
			removed, rerr := env.d.Store.RemoveUnreferenced(testHex)
			require.NoError(t, rerr)
			assert.Equal(t, !c.wantRef, removed, "the collector removes the tree only when no mount claimed it")
		})
	}
}

func TestNodePublishHandsMissingContentToTheMaterializer(t *testing.T) {
	cases := []struct {
		name      string
		progress  Progress
		wantCode  codes.Code
		wantMount bool
	}{
		{name: "materializing", progress: Progress{Code: codes.Aborted, Message: "downloading sha256:aaaa: 3 of 10 MiB"}, wantCode: codes.Aborted},
		{name: "backing off", progress: Progress{Code: codes.Unavailable, Message: "failed: IntegrityMismatch; retrying at"}, wantCode: codes.Unavailable},
		{name: "no room", progress: Progress{Code: codes.ResourceExhausted, Message: "InsufficientCapacity"}, wantCode: codes.ResourceExhausted},
		{name: "published by this call", progress: Progress{Published: true}, wantMount: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			env := newTestEnv(t, testArtifact("team-a", "qwen", "uid-qwen", testDigest, true))
			env.materializer.progress = c.progress
			env.materializer.publish = func(hex string) { env.publish(t, hex) }
			target := env.target("uid-p")

			_, err := env.d.NodePublishVolume(context.Background(),
				publishRequest(target, kubeletVolumeContext(hints("qwen", "uid-qwen", testDigest), "team-a")))
			require.Len(t, env.materializer.requests, 1)
			got := env.materializer.requests[0]
			assert.Equal(t, testHex, got.Hex)
			assert.Equal(t, "hf_secret", got.Token, "kubelet's Secret of this call is handed over")
			assert.Equal(t, "uid-qwen", string(got.Artifact.UID))
			if !c.wantMount {
				assert.Equal(t, c.wantCode, status.Code(err))
				assert.Contains(t, err.Error(), c.progress.Message)
				assert.NotContains(t, err.Error(), "hf_secret")
				assert.Empty(t, env.mounter.mounted)
				return
			}
			require.NoError(t, err)
			assert.Contains(t, env.mounter.mounted, target)
		})
	}
}

func TestNodeUnpublish(t *testing.T) {
	env := newTestEnv(t, testArtifact("team-a", "qwen", "uid-qwen", testDigest, true))
	env.publish(t, testHex)
	target := env.target("uid-p")
	unpublish := func(target string) error {
		_, err := env.d.NodeUnpublishVolume(context.Background(), &csi.NodeUnpublishVolumeRequest{VolumeId: "csi-vol-1", TargetPath: target})
		return err
	}

	require.NoError(t, unpublish(target), "a target never mounted succeeds")
	assert.Empty(t, env.events.unmounted)

	_, err := env.d.NodePublishVolume(context.Background(),
		publishRequest(target, kubeletVolumeContext(hints("qwen", "uid-qwen", testDigest), "team-a")))
	require.NoError(t, err)
	require.NoError(t, unpublish(target))
	assert.Empty(t, env.mounter.mounted)
	assert.NoDirExists(t, target)
	_, found, err := env.d.Store.ReadRef("csi-vol-1")
	require.NoError(t, err)
	assert.False(t, found)
	assert.Equal(t, []string{testHex}, env.events.unmounted)

	require.NoError(t, unpublish(target), "twice succeeds")
	assert.Equal(t, []string{testHex}, env.events.unmounted)

	_, err = env.d.NodePublishVolume(context.Background(),
		publishRequest(target, kubeletVolumeContext(hints("qwen", "uid-qwen", testDigest), "team-a")))
	require.NoError(t, err)
	require.NoError(t, env.mounter.Unmount(target))
	require.NoError(t, unpublish(target), "a target unmounted by someone else succeeds")
	assert.Equal(t, []string{testHex, testHex}, env.events.unmounted, "and still ends the tree's use")

	_, err = env.d.NodeUnpublishVolume(context.Background(), &csi.NodeUnpublishVolumeRequest{VolumeId: "csi-vol-1", TargetPath: "/etc"})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}
