package worker

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	ctrlreconcile "sigs.k8s.io/controller-runtime/pkg/reconcile"
	nfd "sigs.k8s.io/node-feature-discovery/api/nfd/v1alpha1"

	"gpustack.ai/gpustack/pkg/kubeclients/kubernetes/scheme"
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/systemname"
	"gpustack.ai/gpustack/pkg/worker/kuberess"
)

// 在 NFD 公布其 CPU 身份之前，newUndiscoveredCPUNode 在注册时会构建一个 Node：没有 CPU 模型标签，也没有
// CPU 名称注释。
func newUndiscoveredCPUNode(name string) *core.Node {
	return &core.Node{
		ObjectMeta: meta.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				core.LabelOSStable:   "linux",
				core.LabelArchStable: "amd64",
			},
		},
		Status: core.NodeStatus{
			Capacity: core.ResourceList{
				core.ResourceCPU: *resource.NewQuantity(2, resource.DecimalSI),
			},
		},
	}
}

// discoverCPU 发布节点对应的 CPU 身份 NFD 报告：包括 cpu-model 标签，以及当 withName 被设置为真时，
// cpu-name 注释。
func discoverCPU(nd *core.Node, withName bool) {
	nd.Labels["feature.node.kubernetes.io/cpu-model.vendor_id"] = "AMD"
	nd.Labels["feature.node.kubernetes.io/cpu-model.family"] = "23"
	nd.Labels["feature.node.kubernetes.io/cpu-model.id"] = "1"
	if withName {
		if nd.Annotations == nil {
			nd.Annotations = map[string]string{}
		}
		nd.Annotations[nodefeature.FeatureLabelPrefix+"cpu-name"] = "AMD EPYC 7571"
	}
}

// TestNodeFeatureNodeUpdated 表示当派生标签的输入发生变化时，一个 Node 的更新会重新派生出 NodeFeature，包
// 括 NFD 在节点注册后发布的那些变化。对于那些派生内容从未被读取的变化，该变量则保持静默。
func TestNodeFeatureNodeUpdated(t *testing.T) {
	cases := []struct {
		name   string
		old    func(nd *core.Node)
		new    func(nd *core.Node)
		expect bool
	}{
		{
			name:   "managed label set by an admin",
			new:    func(nd *core.Node) { nd.Labels[systemname.ManagedLabelKey] = "false" },
			expect: true,
		},
		{
			name:   "cpu-model labels published after registration",
			new:    func(nd *core.Node) { discoverCPU(nd, false) },
			expect: true,
		},
		{
			name:   "cpu-name annotation published after the cpu-model labels",
			old:    func(nd *core.Node) { discoverCPU(nd, false) },
			new:    func(nd *core.Node) { discoverCPU(nd, true) },
			expect: true,
		},
		{
			name: "cpu capacity changed",
			new: func(nd *core.Node) {
				nd.Status.Capacity[core.ResourceCPU] = *resource.NewQuantity(4, resource.DecimalSI)
			},
			expect: true,
		},
		{
			name: "unrelated annotation changed",
			old:  func(nd *core.Node) { discoverCPU(nd, true) },
			new: func(nd *core.Node) {
				discoverCPU(nd, true)
				nd.Annotations["node.alpha.kubernetes.io/ttl"] = "0"
			},
			expect: false,
		},
		{
			name:   "unrelated label changed",
			new:    func(nd *core.Node) { nd.Labels["example.com/team"] = "a" },
			expect: false,
		},
		{
			name: "status heartbeat",
			new: func(nd *core.Node) {
				nd.Status.Conditions = []core.NodeCondition{{Type: core.NodeReady, Status: core.ConditionTrue}}
			},
			expect: false,
		},
		{
			name: "deleting node",
			new: func(nd *core.Node) {
				now := meta.Now()
				nd.DeletionTimestamp = &now
				discoverCPU(nd, true)
			},
			expect: false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			oldNd, newNd := newUndiscoveredCPUNode("node-0"), newUndiscoveredCPUNode("node-0")
			if c.old != nil {
				c.old(oldNd)
			}
			if c.new != nil {
				c.new(newNd)
			}
			assert.Equal(t, c.expect, nodeFeatureNodeUpdated(oldNd, newNd))
		})
	}
}

// TestNodeFeatureReconciler_RederivesGeneralGroupAfterLateCPUIdentity 会重演一个在 NFD 发布其 CPU 身份之
// 前注册的节点：第一次推导会退回到通用组，而发布该身份的更新则必须重新推导出真实的身份。
func TestNodeFeatureReconciler_RederivesGeneralGroupAfterLateCPUIdentity(t *testing.T) {
	nd := newUndiscoveredCPUNode("node-0")
	cli := ctrlfake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithObjects(nd).
		Build()
	r := &NodeFeatureReconciler{Client: cli}
	reconcile := func() map[string]string {
		t.Helper()
		_, err := r.Reconcile(context.Background(),
			ctrlreconcile.Request{NamespacedName: ctrlcli.ObjectKey{Name: nd.Name}})
		require.NoError(t, err)
		nf := new(nfd.NodeFeature)
		require.NoError(t, cli.Get(context.Background(),
			ctrlcli.ObjectKey{Namespace: kuberess.SystemNamespaceName, Name: nd.Name + "-gpustack-worker"}, nf))
		return nf.Spec.Labels
	}

	got := reconcile()
	assert.Equal(t, "2", got[nodefeature.GeneralFeatureLabelPrefix+"generic.count"],
		"an undiscovered CPU derives the generic group")

	oldNd := nd.DeepCopy()
	require.NoError(t, cli.Get(context.Background(), ctrlcli.ObjectKeyFromObject(nd), nd))
	discoverCPU(nd, true)
	require.NoError(t, cli.Update(context.Background(), nd))
	require.True(t, nodeFeatureNodeUpdated(oldNd, nd),
		"publishing the CPU identity must trigger a re-derivation")

	got = reconcile()
	assert.Equal(t, "2", got[nodefeature.GeneralFeatureLabelPrefix+"amd-epyc-7571.count"],
		"the re-derivation picks the published CPU name")
	assert.NotContains(t, got, nodefeature.GeneralFeatureLabelPrefix+"generic.count",
		"the generic group is dropped")
}
