package worker

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/kubeclients/kubernetes/scheme"
	"gpustack.ai/gpustack/pkg/modelstore"
)

var testDigest = "sha256:" + strings.Repeat("7", 64)

var testNow = time.Date(2026, 9, 27, 6, 0, 0, 0, time.UTC)

func testResolvedArtifact(name string) *workercore.ModelArtifact {
	return &workercore.ModelArtifact{
		ObjectMeta: meta.ObjectMeta{Name: name, Namespace: "team-a", UID: types.UID("artifact-uid")},
		Status: workercore.ModelArtifactStatus{
			Resolved: &workercore.ModelArtifactResolved{ManifestDigest: testDigest},
		},
	}
}

func testPrefetch(name string, mutate func(*workercore.ModelPrefetch)) *workercore.ModelPrefetch {
	pf := &workercore.ModelPrefetch{
		ObjectMeta: meta.ObjectMeta{Name: name, Namespace: "team-a", Generation: 1},
		Spec: workercore.ModelPrefetchSpec{
			ArtifactRef: workercore.ModelPrefetchArtifactReference{Name: "model"},
			BindingRef:  workercore.ModelPrefetchBindingReference{Name: "cache"},
			Placement: &workercore.ModelPrefetchPlacement{
				NodeSelector: &meta.LabelSelector{MatchLabels: map[string]string{"pool": "h100"}},
			},
		},
	}
	if mutate != nil {
		mutate(pf)
	}

	return pf
}

func testNodeModelStoreEntry(node string, lastUsed *meta.Time, pinned ...[]string) *workercore.NodeModelStore {
	nms := &workercore.NodeModelStore{
		ObjectMeta: meta.ObjectMeta{Name: node},
		Status: workercore.NodeModelStoreStatus{
			Models: []workercore.NodeModelStoreModel{{Digest: testDigest, State: workercore.NodeModelStoreModelStateReady, LastUsedTime: lastUsed}},
		},
	}
	if len(pinned) > 0 {
		nms.Spec.Pinned = pinned[0]
	}

	return nms
}

func newTestModelPrefetchEnv(t *testing.T, objs ...ctrlcli.Object) (*ModelPrefetchReconciler, ctrlcli.Client) {
	t.Helper()
	cli := ctrlfake.NewClientBuilder().WithScheme(scheme.Scheme).
		WithStatusSubresource(&workercore.ModelPrefetch{}, &core.Pod{}).
		WithObjects(objs...).Build()

	r := &ModelPrefetchReconciler{Client: cli, Now: func() time.Time { return testNow }}

	return r, cli
}

func reconcileModelPrefetch(t *testing.T, r *ModelPrefetchReconciler, ns, name string) {
	t.Helper()
	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: ctrlcli.ObjectKey{Namespace: ns, Name: name}})
	require.NoError(t, err)
}

func hourAgo(h int) *meta.Time {
	t := meta.NewTime(testNow.Add(-time.Duration(h) * time.Hour))

	return &t
}

func testPinnedBinding() *workercore.ModelStoreBinding {
	tr := true
	return &workercore.ModelStoreBinding{
		ObjectMeta: meta.ObjectMeta{Namespace: "team-a", Name: "cache"},
		Spec: workercore.ModelStoreBindingSpec{
			StoreRefs:   []workercore.ModelStoreBindingStoreReference{{Name: "h100"}},
			AllowPinned: &tr,
		},
	}
}

func TestModelPrefetchRendersTheWarmupShape(t *testing.T) {
	r, cli := newTestModelPrefetchEnv(t,
		testModelNodeLabeled("node-1", map[string]string{"pool": "h100"}),
		testResolvedArtifact("model"),
		testPrefetch("warm", nil),
	)
	reconcileModelPrefetch(t, r, "team-a", "warm")

	pods := new(core.PodList)
	require.NoError(t, cli.List(context.Background(), pods, ctrlcli.InNamespace("team-a")))
	require.Len(t, pods.Items, 1, "一个目标节点应渲染一个预热 Pod")
	pod := &pods.Items[0]
	assert.True(t, strings.HasPrefix(pod.Name, "warm-warmup-node-1-"),
		"生成名应包含预取名和目标节点名，实际得到 %q", pod.Name)

	assert.Equal(t, "node-1", pod.Spec.NodeName, "预热 Pod 应绑定到目标节点")
	assert.Equal(t, core.RestartPolicyNever, pod.Spec.RestartPolicy)
	require.NotNil(t, pod.Spec.SecurityContext)
	assert.Equal(t, ptr.To(true), pod.Spec.SecurityContext.RunAsNonRoot)
	require.Len(t, pod.Spec.Containers, 1)
	assert.Empty(t, pod.Spec.Containers[0].Resources.Limits, "预热 Pod 不应申请加速器资源")
	require.NotNil(t, pod.Spec.Containers[0].SecurityContext)
	assert.Equal(t, ptr.To(false), pod.Spec.Containers[0].SecurityContext.AllowPrivilegeEscalation)

	for k := range pod.Labels {
		assert.NotEqual(t, "kueue.x-k8s.io/queue-name", k, "预热 Pod 不应携带 queue-name 标签")
	}

	require.Len(t, pod.Spec.Volumes, 1)
	require.NotNil(t, pod.Spec.Volumes[0].CSI)
	assert.Equal(t, modelstore.DriverName, pod.Spec.Volumes[0].CSI.Driver)
	assert.Equal(t, map[string]string{
		modelstore.VolumeAttrArtifact:       "model",
		modelstore.VolumeAttrArtifactUID:    "artifact-uid",
		modelstore.VolumeAttrManifestDigest: testDigest,
	}, pod.Spec.Volumes[0].CSI.VolumeAttributes)

	require.Len(t, pod.OwnerReferences, 1)
	assert.Equal(t, "ModelPrefetch", pod.OwnerReferences[0].Kind)
	assert.True(t, *pod.OwnerReferences[0].Controller)
}

func TestModelPrefetchTTLExpiresAndUnpins(t *testing.T) {
	stale := hourAgo(3)
	fresh := hourAgo(1)
	pf := testPrefetch("warm", func(p *workercore.ModelPrefetch) {
		p.Spec.Retention.Pinned = true
		ttl := meta.Duration{Duration: 2 * time.Hour}
		p.Spec.Retention.TTLAfterLastUse = &ttl
	})
	r, cli := newTestModelPrefetchEnv(t,
		testModelNodeLabeled("node-1", map[string]string{"pool": "h100"}),
		testModelNodeLabeled("node-2", map[string]string{"pool": "h100"}),
		testResolvedArtifact("model"),
		testNodeModelStoreEntry("node-1", stale, []string{testDigest}),
		testNodeModelStoreEntry("node-2", fresh),
		testPinnedBinding(),
		pf,
	)
	reconcileModelPrefetch(t, r, "team-a", "warm")

	got := new(workercore.ModelPrefetch)
	require.NoError(t, cli.Get(context.Background(), ctrlcli.ObjectKey{Namespace: "team-a", Name: "warm"}, got))
	assert.Equal(t, int32(1), got.Status.ReadyNodes, "超时节点的副本不再计入就绪节点数")

	nms := new(workercore.NodeModelStore)
	require.NoError(t, cli.Get(context.Background(), ctrlcli.ObjectKey{Name: "node-1"}, nms))
	assert.Empty(t, nms.Spec.Pinned, "超时节点的 pin 应由并集重算释放")

	nms2 := new(workercore.NodeModelStore)
	require.NoError(t, cli.Get(context.Background(), ctrlcli.ObjectKey{Name: "node-2"}, nms2))
	assert.Equal(t, []string{testDigest}, nms2.Spec.Pinned, "未超时节点应保持 pinned")
}

func TestModelPrefetchPinnedUnionKeepsSharedDigests(t *testing.T) {
	// 两个预取对象在同一节点分别固定不同摘要：并集重算应同时保留两者，避免后执行的对象覆盖先前的 pin。
	digestB := "sha256:" + strings.Repeat("b", 64)
	artifactB := testResolvedArtifact("model2")
	artifactB.Status.Resolved.ManifestDigest = digestB
	r, cli := newTestModelPrefetchEnv(t,
		testModelNodeLabeled("node-1", map[string]string{"pool": "h100"}),
		testResolvedArtifact("model"),
		artifactB,
		testPinnedBinding(),
		testPrefetch("warm", func(p *workercore.ModelPrefetch) { p.Spec.Retention.Pinned = true }),
		testPrefetch("warm2", func(p *workercore.ModelPrefetch) {
			p.Spec.Retention.Pinned = true
			p.Spec.ArtifactRef.Name = "model2"
		}),
		&workercore.NodeModelStore{
			ObjectMeta: meta.ObjectMeta{Name: "node-1"},
			Status: workercore.NodeModelStoreStatus{
				Models: []workercore.NodeModelStoreModel{
					{Digest: testDigest, State: workercore.NodeModelStoreModelStateReady},
					{Digest: digestB, State: workercore.NodeModelStoreModelStateReady},
				},
			},
		},
	)
	require.NoError(t, r.recomputePinned(context.Background()))

	nms := new(workercore.NodeModelStore)
	require.NoError(t, cli.Get(context.Background(), ctrlcli.ObjectKey{Name: "node-1"}, nms))
	assert.Equal(t, []string{testDigest, digestB}, nms.Spec.Pinned,
		"节点应按排序后的并集固定两个预取对象各自要求的摘要")
}
