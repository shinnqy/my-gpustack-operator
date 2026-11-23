package worker

import (
	"context"
	"maps"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	ctrlreconcile "sigs.k8s.io/controller-runtime/pkg/reconcile"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/kubeclients/kubernetes/scheme"
	"gpustack.ai/gpustack/pkg/nodefeature"
)

func TestNodeTopologyReconcileProfiles(t *testing.T) {
	pending := readyNodeLabelSource("pending", map[string]string{"inventory": "rack"}, "topology.gpustack.ai/rack")
	pending.Status.Conditions = nil
	tests := []struct {
		name       string
		nodeLabels map[string]string
		sources    []*workercore.TopologySource
		wantLevels []string
	}{
		{
			name:       "匹配的来源未就绪，另一个就绪来源选择其他节点",
			nodeLabels: map[string]string{"inventory": "rack", "topology.gpustack.ai/rack": "r1"},
			sources: []*workercore.TopologySource{
				pending,
				readyNodeLabelSource("other", map[string]string{"inventory": "other"}, "topology.gpustack.ai/rack"),
			},
			wantLevels: []string{core.LabelHostname},
		},
		{
			name: "标准区域和可用区",
			nodeLabels: map[string]string{
				"inventory": "cloud", core.LabelTopologyRegion: "region-a", core.LabelTopologyZone: "zone-a",
			},
			sources: []*workercore.TopologySource{readyNodeLabelSource("cloud", map[string]string{"inventory": "cloud"},
				core.LabelTopologyRegion, core.LabelTopologyZone)},
			wantLevels: []string{core.LabelTopologyRegion, core.LabelTopologyZone, core.LabelHostname},
		},
		{
			name: "partial hierarchy uses longest prefix",
			nodeLabels: map[string]string{
				"inventory": "rack", core.LabelTopologyRegion: "region-a", core.LabelTopologyZone: "zone-a",
			},
			sources: []*workercore.TopologySource{readyNodeLabelSource("rack", map[string]string{"inventory": "rack"},
				core.LabelTopologyRegion, core.LabelTopologyZone, "topology.gpustack.ai/rack")},
			wantLevels: []string{core.LabelTopologyRegion, core.LabelTopologyZone, core.LabelHostname},
		},
		{
			name: "中间缺少机架时，停止在区域，忽略仍然存在的 fabric",
			nodeLabels: map[string]string{
				"inventory": "rack", core.LabelTopologyRegion: "region-a", "feature.gpustack.ai/fabric.domain": "fabric-a",
			},
			sources: []*workercore.TopologySource{readyNodeLabelSource("mixed", map[string]string{"inventory": "rack"},
				core.LabelTopologyRegion, "topology.gpustack.ai/rack", "feature.gpustack.ai/fabric.domain")},
			wantLevels: []string{core.LabelTopologyRegion, core.LabelHostname},
		},
		{
			name:       "unclaimed node uses hostname only",
			nodeLabels: map[string]string{"inventory": "none"},
			wantLevels: []string{core.LabelHostname},
		},
		{
			name:       "overlapping sources use hostname only",
			nodeLabels: map[string]string{"inventory": "shared", core.LabelTopologyZone: "zone-a", "topology.gpustack.ai/rack": "rack-a"},
			sources: []*workercore.TopologySource{
				readyNodeLabelSource("zone", map[string]string{"inventory": "shared"}, core.LabelTopologyZone),
				readyNodeLabelSource("rack", map[string]string{"inventory": "shared"}, "topology.gpustack.ai/rack"),
			},
			wantLevels: []string{core.LabelHostname},
		},
		{
			name: "Topograph fabric tiers reverse closest first",
			nodeLabels: map[string]string{
				"inventory": "fabric", "fabric.topograph.run/tier-0": "leaf", "fabric.topograph.run/tier-1": "spine",
			},
			sources: []*workercore.TopologySource{readyNodeLabelSource("fabric", map[string]string{"inventory": "fabric"},
				"fabric.topograph.run/tier-0", "fabric.topograph.run/tier-1")},
			wantLevels: []string{"fabric.topograph.run/tier-1", "fabric.topograph.run/tier-0", core.LabelHostname},
		},
		{
			name: "accelerator domain remains opaque",
			nodeLabels: map[string]string{
				"inventory": "accelerator", "feature.gpustack.ai/fabric.domain": "nvlink-0-1-2",
			},
			sources: []*workercore.TopologySource{readyNodeLabelSource("accelerator", map[string]string{"inventory": "accelerator"},
				"feature.gpustack.ai/fabric.domain")},
			wantLevels: []string{"feature.gpustack.ai/fabric.domain", core.LabelHostname},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			node := &core.Node{ObjectMeta: meta.ObjectMeta{Name: "node-a", Labels: tc.nodeLabels}}
			objects := []ctrlcli.Object{node}
			for _, source := range tc.sources {
				objects = append(objects, source)
			}
			cli := ctrlfake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(objects...).Build()
			r := &NodeTopologyReconciler{Client: cli}

			_, err := r.Reconcile(context.Background(), ctrlreconcile.Request{NamespacedName: ctrlcli.ObjectKey{Name: node.Name}})
			require.NoError(t, err)

			gotNode := new(core.Node)
			require.NoError(t, cli.Get(context.Background(), ctrlcli.ObjectKeyFromObject(node), gotNode))
			profile := gotNode.Labels[TopologyProfileLabel]
			require.Equal(t, topologyProfile(tc.wantLevels), profile)
			topology := new(kueue.Topology)
			require.NoError(t, cli.Get(context.Background(), ctrlcli.ObjectKey{Name: topologyName(profile)}, topology))
			assert.Equal(t, topologyLevelObjects(tc.wantLevels), topology.Spec.Levels)
		})
	}
}

func TestNodeTopologyRejectsImmutableTopologyDrift(t *testing.T) {
	levels := []string{core.LabelHostname}
	profile := topologyProfile(levels)
	node := &core.Node{ObjectMeta: meta.ObjectMeta{Name: "node-a", Labels: map[string]string{}}}
	topology := &kueue.Topology{
		ObjectMeta: meta.ObjectMeta{Name: topologyName(profile)},
		Spec: kueue.TopologySpec{Levels: topologyLevelObjects([]string{
			core.LabelTopologyZone, core.LabelHostname,
		})},
	}
	cli := ctrlfake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(node, topology).Build()
	r := &NodeTopologyReconciler{Client: cli}

	_, err := r.Reconcile(context.Background(), ctrlreconcile.Request{NamespacedName: ctrlcli.ObjectKey{Name: node.Name}})
	require.EqualError(t, err, "managed Topology \""+topology.Name+"\" has immutable level drift")
	gotNode := new(core.Node)
	require.NoError(t, cli.Get(context.Background(), ctrlcli.ObjectKeyFromObject(node), gotNode))
	assert.NotContains(t, gotNode.Labels, TopologyProfileLabel)
}

func TestTopologySourceNodeLabelsRejectsContradictoryHierarchy(t *testing.T) {
	source := readyNodeLabelSource("rack", map[string]string{"inventory": "rack"}, core.LabelTopologyZone, "topology.gpustack.ai/rack")
	source.Status.Conditions = nil
	nodes := []ctrlcli.Object{
		&core.Node{ObjectMeta: meta.ObjectMeta{Name: "node-a", Labels: map[string]string{"inventory": "rack", core.LabelTopologyZone: "zone-a", "topology.gpustack.ai/rack": "rack-shared"}}},
		&core.Node{ObjectMeta: meta.ObjectMeta{Name: "node-b", Labels: map[string]string{"inventory": "rack", core.LabelTopologyZone: "zone-b", "topology.gpustack.ai/rack": "rack-shared"}}},
	}
	objects := append([]ctrlcli.Object{source}, nodes...)
	cli := ctrlfake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(objects...).
		WithStatusSubresource(&workercore.TopologySource{}).Build()
	r := &TopologySourceReconciler{Client: cli}

	_, err := r.Reconcile(context.Background(), ctrlreconcile.Request{NamespacedName: ctrlcli.ObjectKey{Name: source.Name}})
	require.NoError(t, err)
	got := new(workercore.TopologySource)
	require.NoError(t, cli.Get(context.Background(), ctrlcli.ObjectKeyFromObject(source), got))
	assert.True(t, TopologySourceConditionReady.IsFalse(got))
	assert.Equal(t, "InvalidHierarchy", TopologySourceConditionReady.GetReason(got))
}

func readyNodeLabelSource(name string, selector map[string]string, levels ...string) *workercore.TopologySource {
	source := &workercore.TopologySource{ObjectMeta: meta.ObjectMeta{Name: name}, Spec: workercore.TopologySourceSpec{
		NodeSelector: meta.LabelSelector{MatchLabels: selector}, Levels: levels, NodeLabels: &workercore.TopologySourceNodeLabels{},
	}}
	TopologySourceConditionReady.True(source, "Observed", "ready")
	return source
}

// 同样的层级键可以描述不同的机架。两台节点应共享一份 Topology 和一个引用它的 ResourceFlavor。
func TestTopologySourceProfileReachesResourceFlavor(t *testing.T) {
	const rack = "topology.gpustack.ai/rack"
	ctx := t.Context()
	source := &workercore.TopologySource{
		ObjectMeta: meta.ObjectMeta{Name: "rack-inventory"},
		Spec: workercore.TopologySourceSpec{
			NodeSelector: meta.LabelSelector{MatchLabels: map[string]string{"inventory": "rack"}},
			Levels:       []string{rack}, NodeLabels: &workercore.TopologySourceNodeLabels{},
		},
	}
	nodes := []*core.Node{newUndiscoveredCPUNode("node-a"), newUndiscoveredCPUNode("node-b")}
	for _, node := range nodes {
		maps.Copy(node.Labels, nodefeature.ConstructNodeCapacityLabels(node))
		node.Labels["inventory"] = "rack"
		node.Labels[rack] = "rack-" + node.Name
		node.Labels[core.LabelHostname] = node.Name
	}
	cli := ctrlfake.NewClientBuilder().WithScheme(scheme.Scheme).
		WithObjects(source, nodes[0], nodes[1]).
		WithStatusSubresource(source).
		WithIndex(&core.Node{}, IndexingNodeByScheduleFlavor, indexNodeByScheduleFlavor).Build()

	sources := &TopologySourceReconciler{Client: cli}
	_, err := sources.Reconcile(ctx, ctrlreconcile.Request{NamespacedName: ctrlcli.ObjectKeyFromObject(source)})
	require.NoError(t, err)
	require.NoError(t, cli.Get(ctx, ctrlcli.ObjectKeyFromObject(source), source))
	require.True(t, TopologySourceConditionReady.IsTrue(source))
	require.EqualValues(t, 2, source.Status.SelectedNodes)

	levels := []string{rack, core.LabelHostname}
	profile := topologyProfile(levels)
	profiles := &NodeTopologyReconciler{Client: cli}
	for _, node := range nodes {
		key := ctrlcli.ObjectKeyFromObject(node)
		_, err = profiles.Reconcile(ctx, ctrlreconcile.Request{NamespacedName: key})
		require.NoError(t, err)
		require.NoError(t, cli.Get(ctx, key, node))
		require.Equal(t, profile, node.Labels[TopologyProfileLabel])
	}
	topologies := new(kueue.TopologyList)
	require.NoError(t, cli.List(ctx, topologies))
	require.Len(t, topologies.Items, 1)
	assert.Equal(t, topologyLevelObjects(levels), topologies.Items[0].Spec.Levels)

	keys := indexNodeByScheduleFlavor(nodes[0])
	require.Len(t, keys, 1)
	flavors := &NodeFlavorReconciler{Client: cli, APIReader: cli}
	key := ctrlcli.ObjectKey{Name: keys[0]}
	_, err = flavors.Reconcile(ctx, ctrlreconcile.Request{NamespacedName: key})
	require.NoError(t, err)
	flavor := new(kueue.ResourceFlavor)
	require.NoError(t, cli.Get(ctx, key, flavor))
	require.NotNil(t, flavor.Spec.TopologyName)
	assert.Equal(t, topologies.Items[0].Name, string(*flavor.Spec.TopologyName))
	assert.Equal(t, profile, flavor.Spec.NodeLabels[TopologyProfileLabel])
}
