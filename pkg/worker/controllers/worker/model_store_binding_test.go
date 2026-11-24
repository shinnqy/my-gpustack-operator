package worker

import (
	"context"
	"maps"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/kubeclients/kubernetes/scheme"
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/systemmeta"
)

func accountingDigest(c byte) string { return "sha256:" + strings.Repeat(string(c), 64) }

func newAccountingBinding(ns, name, quota string) *workercore.ModelStoreBinding {
	return &workercore.ModelStoreBinding{
		ObjectMeta: meta.ObjectMeta{Namespace: ns, Name: name, Generation: 1},
		Spec: workercore.ModelStoreBindingSpec{
			StoreRefs: []workercore.ModelStoreBindingStoreReference{{Name: "h100"}},
			Quota:     workercore.ModelStoreBindingQuota{Bytes: resource.MustParse(quota)},
		},
	}
}

func testHoldingStore(node, digest string) *workercore.NodeModelStore {
	return &workercore.NodeModelStore{
		ObjectMeta: meta.ObjectMeta{Name: node},
		Status: workercore.NodeModelStoreStatus{
			Models: []workercore.NodeModelStoreModel{{Digest: digest, State: workercore.NodeModelStoreModelStateReady}},
		},
	}
}

func testAccountingNode(name string) *core.Node {
	return &core.Node{ObjectMeta: meta.ObjectMeta{Name: name, Labels: map[string]string{"pool": "h100"}}}
}

func testAccountingPrefetch(ns, artifact string) *workercore.ModelPrefetch {
	return &workercore.ModelPrefetch{
		ObjectMeta: meta.ObjectMeta{Namespace: ns, Name: "warm"},
		Spec: workercore.ModelPrefetchSpec{
			ArtifactRef: workercore.ModelPrefetchArtifactReference{Name: artifact},
			BindingRef:  workercore.ModelPrefetchBindingReference{Name: "cache"},
			Placement: &workercore.ModelPrefetchPlacement{
				NodeSelector: &meta.LabelSelector{MatchLabels: map[string]string{"pool": "h100"}},
			},
		},
	}
}

func newAccountingEnv(t *testing.T, objs ...ctrlcli.Object) (*ModelStoreBindingReconciler, ctrlcli.Client) {
	t.Helper()
	cli := ctrlfake.NewClientBuilder().WithScheme(scheme.Scheme).
		WithStatusSubresource(&workercore.ModelStoreBinding{}).
		WithObjects(objs...).Build()
	fixed := time.Date(2026, 9, 27, 6, 0, 0, 0, time.UTC)

	return &ModelStoreBindingReconciler{Client: cli, Now: func() time.Time { return fixed }}, cli
}

func TestModelStoreBindingAccounting(t *testing.T) {
	digest := accountingDigest('7')
	artifact := func(ns, size string) *workercore.ModelArtifact {
		sizeBytes := int64(0)
		switch size {
		case "5Gi":
			sizeBytes = 5 << 30
		case "20Gi":
			sizeBytes = 20 << 30
		}
		return &workercore.ModelArtifact{
			ObjectMeta: meta.ObjectMeta{Namespace: ns, Name: "model"},
			Status: workercore.ModelArtifactStatus{
				Resolved: &workercore.ModelArtifactResolved{
					ManifestDigest: digest,
					SizeBytes:      sizeBytes,
				},
			},
		}
	}

	t.Run("usedBytes is the size times every node holding the digest", func(t *testing.T) {
		r, cli := newAccountingEnv(t,
			newAccountingBinding("team-a", "cache", "20Gi"),
			testAccountingPrefetch("team-a", "model"),
			artifact("team-a", "5Gi"),
			testAccountingNode("node-1"),
			testAccountingNode("node-2"),
			testHoldingStore("node-1", digest),
			testHoldingStore("node-2", digest),
		)
		_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: ctrlcli.ObjectKey{Namespace: "team-a", Name: "cache"}})
		require.NoError(t, err)

		binding := new(workercore.ModelStoreBinding)
		require.NoError(t, cli.Get(context.Background(), ctrlcli.ObjectKey{Namespace: "team-a", Name: "cache"}, binding))
		require.NotNil(t, binding.Status.UsedBytes)
		assert.Equal(t, "10Gi", binding.Status.UsedBytes.String(), "5Gi on each of two nodes")
		over := testCondition(binding.Status.Conditions, ModelStoreBindingConditionOverQuota)
		require.NotNil(t, over)
		assert.Equal(t, meta.ConditionFalse, over.Status)
	})

	t.Run("a footprint past the grant reads over quota", func(t *testing.T) {
		r, cli := newAccountingEnv(t,
			newAccountingBinding("team-a", "cache", "6Gi"),
			testModelStore("h100", map[string]string{"pool": "h100"}, nil),
			testAccountingPrefetch("team-a", "model"),
			artifact("team-a", "20Gi"),
			testAccountingNode("node-1"),
			testHoldingStore("node-1", digest),
		)
		_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: ctrlcli.ObjectKey{Namespace: "team-a", Name: "cache"}})
		require.NoError(t, err)

		binding := new(workercore.ModelStoreBinding)
		require.NoError(t, cli.Get(context.Background(), ctrlcli.ObjectKey{Namespace: "team-a", Name: "cache"}, binding))
		over := testCondition(binding.Status.Conditions, ModelStoreBindingConditionOverQuota)
		require.NotNil(t, over)
		assert.Equal(t, meta.ConditionTrue, over.Status)
		assert.Equal(t, "OverQuota", binding.Status.Phase, "an over-budget grant reads OverQuota, not Ready")
	})

	t.Run("an unresolved artifact leaves the figure absent, not zero", func(t *testing.T) {
		unresolved := &workercore.ModelArtifact{
			ObjectMeta: meta.ObjectMeta{Namespace: "team-a", Name: "model"},
		}
		r, cli := newAccountingEnv(t,
			newAccountingBinding("team-a", "cache", "20Gi"),
			&workercore.ModelPrefetch{
				ObjectMeta: meta.ObjectMeta{Namespace: "team-a", Name: "warm"},
				Spec: workercore.ModelPrefetchSpec{
					ArtifactRef: workercore.ModelPrefetchArtifactReference{Name: "model"},
					BindingRef:  workercore.ModelPrefetchBindingReference{Name: "cache"},
				},
			},
			unresolved,
			testAccountingNode("node-1"),
			testHoldingStore("node-1", digest),
		)
		_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: ctrlcli.ObjectKey{Namespace: "team-a", Name: "cache"}})
		require.NoError(t, err)

		binding := new(workercore.ModelStoreBinding)
		require.NoError(t, cli.Get(context.Background(), ctrlcli.ObjectKey{Namespace: "team-a", Name: "cache"}, binding))
		assert.Nil(t, binding.Status.UsedBytes, "an unmeasured figure is absent, never zero")
		ready := testCondition(binding.Status.Conditions, ModelStoreBindingConditionReady)
		require.NotNil(t, ready)
		assert.Equal(t, meta.ConditionFalse, ready.Status)
		over := testCondition(binding.Status.Conditions, ModelStoreBindingConditionOverQuota)
		require.NotNil(t, over)
		assert.Equal(t, meta.ConditionUnknown, over.Status)
	})

	t.Run("a grant naming a deleted store reads StoreMissing", func(t *testing.T) {
		ghost := newAccountingBinding("team-a", "cache", "20Gi")
		ghost.Spec.StoreRefs = []workercore.ModelStoreBindingStoreReference{
			{Name: "h100"}, {Name: "ghost"},
		}
		r, cli := newAccountingEnv(t,
			ghost,
			testModelStore("h100", map[string]string{"pool": "h100"}, nil),
			testAccountingPrefetch("team-a", "model"),
			artifact("team-a", "5Gi"),
			testAccountingNode("node-1"),
			testHoldingStore("node-1", digest),
		)
		_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: ctrlcli.ObjectKey{Namespace: "team-a", Name: "cache"}})
		require.NoError(t, err)

		binding := new(workercore.ModelStoreBinding)
		require.NoError(t, cli.Get(context.Background(), ctrlcli.ObjectKey{Namespace: "team-a", Name: "cache"}, binding))
		require.NotNil(t, binding.Status.UsedBytes, "the accounting still counts what it can measure")
		ready := testCondition(binding.Status.Conditions, ModelStoreBindingConditionReady)
		require.NotNil(t, ready)
		assert.Equal(t, meta.ConditionFalse, ready.Status)
		assert.Equal(t, "StoreMissing", ready.Reason)
	})

	t.Run("a prefetch whose placement cannot expand leaves the figure unmeasured", func(t *testing.T) {
		ambiguous := testAccountingPrefetch("team-a", "model")
		ambiguous.Spec.Placement = &workercore.ModelPrefetchPlacement{
			InstanceTypes: []string{"h100-8"},
			NodeSelector:  &meta.LabelSelector{MatchLabels: map[string]string{"pool": "h100"}},
		}
		r, cli := newAccountingEnv(t,
			newAccountingBinding("team-a", "cache", "20Gi"),
			testModelStore("h100", map[string]string{"pool": "h100"}, nil),
			ambiguous,
			artifact("team-a", "5Gi"),
			testAccountingNode("node-1"),
			testHoldingStore("node-1", digest),
		)
		_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: ctrlcli.ObjectKey{Namespace: "team-a", Name: "cache"}})
		require.NoError(t, err)

		binding := new(workercore.ModelStoreBinding)
		require.NoError(t, cli.Get(context.Background(), ctrlcli.ObjectKey{Namespace: "team-a", Name: "cache"}, binding))
		assert.Nil(t, binding.Status.UsedBytes, "an uncountable prefetch makes the figure absent, not short")
		ready := testCondition(binding.Status.Conditions, ModelStoreBindingConditionReady)
		require.NotNil(t, ready)
		assert.Equal(t, meta.ConditionFalse, ready.Status)
	})

	t.Run("two namespaces sharing one tree each count it in full", func(t *testing.T) {
		shared := artifact("team-a", "5Gi")
		sharedB := artifact("team-b", "5Gi")
		sharedB.Status.Resolved.ManifestDigest = shared.Status.Resolved.ManifestDigest
		r, cli := newAccountingEnv(t,
			newAccountingBinding("team-a", "cache", "20Gi"),
			newAccountingBinding("team-b", "cache", "20Gi"),
			testAccountingPrefetch("team-a", "model"),
			testAccountingPrefetch("team-b", "model"),
			shared, sharedB,
			testAccountingNode("node-1"),
			testHoldingStore("node-1", digest),
		)
		for _, ns := range []string{"team-a", "team-b"} {
			_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: ctrlcli.ObjectKey{Namespace: ns, Name: "cache"}})
			require.NoError(t, err)
			binding := new(workercore.ModelStoreBinding)
			require.NoError(t, cli.Get(context.Background(), ctrlcli.ObjectKey{Namespace: ns, Name: "cache"}, binding))
			require.NotNil(t, binding.Status.UsedBytes)
			assert.Equal(t, "5Gi", binding.Status.UsedBytes.String(), "%s counts the shared tree in full", ns)
		}
	})
}

// 两个预热请求命中同一份内容时，按节点去重，下载中的目标副本也按完整大小计入预算。
func TestModelStoreBindingCountsEachTargetCopyOnce(t *testing.T) {
	ctx := t.Context()
	digest := accountingDigest('a')
	artifact := &workercore.ModelArtifact{
		ObjectMeta: meta.ObjectMeta{Namespace: "lab", Name: "weights"},
		Status: workercore.ModelArtifactStatus{Resolved: &workercore.ModelArtifactResolved{
			ManifestDigest: digest, SizeBytes: 5 << 30,
		}},
	}
	alias := artifact.DeepCopy()
	alias.Name = "same-weights"
	first := testAccountingPrefetch("lab", artifact.Name)
	second := testAccountingPrefetch("lab", alias.Name)
	second.Name = "warm-again"
	downloading := testHoldingStore("node-b", digest)
	downloading.Status.Models[0].State = workercore.NodeModelStoreModelStateDownloading
	other := testAccountingNode("node-c")
	other.Labels["pool"] = "other"
	binding := newAccountingBinding("lab", "cache", "9Gi")
	r, cli := newAccountingEnv(t, binding, testModelStore("h100", map[string]string{"pool": "h100"}, nil),
		artifact, alias, first, second,
		testAccountingNode("node-a"), testAccountingNode("node-b"), other,
		testHoldingStore("node-a", digest), downloading, testHoldingStore("node-c", digest))

	check := func(wantBytes int64, wantPhase string) string {
		t.Helper()
		key := ctrlcli.ObjectKeyFromObject(binding)
		_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
		require.NoError(t, err)
		got := new(workercore.ModelStoreBinding)
		require.NoError(t, cli.Get(ctx, key, got))
		require.NotNil(t, got.Status.UsedBytes)
		assert.Equal(t, wantBytes, got.Status.UsedBytes.Value())
		assert.Equal(t, wantPhase, got.Status.Phase)
		return got.ResourceVersion
	}
	version := check(10<<30, "OverQuota")
	assert.Equal(t, version, check(10<<30, "OverQuota"))
	require.NoError(t, cli.Delete(ctx, downloading))
	check(5<<30, "Ready")
	require.NoError(t, cli.Delete(ctx, testHoldingStore("node-a", digest)))
	// 第三台节点仍持有该摘要，但两个预热请求都没有选中它，因此此时能确定用量为零。
	check(0, "Ready")
}

func TestBindingPlacementResolvesNamedAndInheritedTypes(t *testing.T) {
	ctx := t.Context()
	var nodes []core.Node
	for _, name := range []string{"node-b", "node-a", "other-arch"} {
		nd := newUndiscoveredCPUNode(name)
		if name == "other-arch" {
			nd.Labels[core.LabelArchStable] = "arm64"
		}
		maps.Copy(nd.Labels, nodefeature.ConstructNodeCapacityLabels(nd))
		nd.Labels[TopologyProfileLabel] = topologyProfile([]string{core.LabelHostname})
		nodes = append(nodes, *nd)
	}
	names := indexNodeByScheduleFlavor(&nodes[0])
	require.Len(t, names, 1)
	pool := &kueue.ResourceFlavor{ObjectMeta: meta.ObjectMeta{
		Name: names[0], Labels: nodefeature.PoolScheduleLabels(false, false, "generic", "", "linux", "amd64"),
	}}
	systemmeta.NoteResource(pool, _ResourceFlavorResType, nil)
	instanceType := &workercore.InstanceType{ObjectMeta: meta.ObjectMeta{Name: "small"}, Spec: workercore.InstanceTypeSpec{
		GeneralGroup: "generic", OS: "linux", Arch: "amd64",
	}}
	cli := ctrlfake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(instanceType, pool).Build()
	deployment := workercore.ModelDeployment{Spec: workercore.ModelDeploymentSpec{
		Model: workercore.ModelDeploymentModel{ArtifactRef: &core.LocalObjectReference{Name: "weights"}},
		Roles: []workercore.ModelDeploymentRole{{InstanceType: "small"}},
	}}
	for _, tc := range []struct {
		name        string
		placement   *workercore.ModelPrefetchPlacement
		deployments []workercore.ModelDeployment
		want        []string
	}{
		{"显式指定实例规格", &workercore.ModelPrefetchPlacement{InstanceTypes: []string{"small"}}, nil, []string{"node-a", "node-b"}},
		{"继承引用同一制品的部署规格", nil, []workercore.ModelDeployment{deployment}, []string{"node-a", "node-b"}},
		{"未解析的规格没有目标节点", &workercore.ModelPrefetchPlacement{InstanceTypes: []string{"unknown"}}, nil, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pf := testAccountingPrefetch("lab", "weights")
			pf.Spec.Placement = tc.placement
			flavors, err := ResolveInstanceTypeFlavorNames(ctx, cli, pf, tc.deployments)
			require.NoError(t, err)
			got, err := PrefetchTargetNodes(pf, nodes, tc.deployments, flavors)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}
