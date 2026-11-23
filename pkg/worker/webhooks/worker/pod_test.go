package worker

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	kueuectrlconst "sigs.k8s.io/kueue/pkg/controller/constants"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/kubeclients/kubernetes/scheme"
	"gpustack.ai/gpustack/pkg/nodefeature"
)

func TestPodWebhook_DefaultSharedPinsTheCardCount(t *testing.T) {
	const queue = "shared-queue"
	const group = "nvidia-g0"
	shared := nodefeature.GetAcceleratableResourceName(nodefeature.ManufacturerNVIDIA, workercore.DeviceAllocationModeShared)
	pin := core.NodeSelectorRequirement{
		Key: nodefeature.AcceleratableFeatureLabelPrefix + group + ".count", Operator: core.NodeSelectorOpGt, Values: []string{"1"},
	}
	zone := core.NodeSelectorRequirement{Key: core.LabelTopologyZone, Operator: core.NodeSelectorOpIn, Values: []string{"a"}}
	host := core.NodeSelectorRequirement{Key: core.LabelHostname, Operator: core.NodeSelectorOpNotIn, Values: []string{"h"}}
	required := func(terms ...[]core.NodeSelectorRequirement) *core.Affinity {
		sel := &core.NodeSelector{}
		for _, expressions := range terms {
			sel.NodeSelectorTerms = append(sel.NodeSelectorTerms, core.NodeSelectorTerm{MatchExpressions: expressions})
		}
		return &core.Affinity{NodeAffinity: &core.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: sel}}
	}

	cases := []struct {
		name      string
		cards     []int64  // 每个值对应一个容器申请的卡数。
		groups    []string // 每个值对应一个带队列入口标签的 InstanceType。
		affinity  *core.Affinity
		coldCache bool
		want      *core.Affinity
		wantErr   string
	}{
		{name: "a one-card shared request needs no lookup", cards: []int64{1}},
		{name: "two one-card containers can share the same card", cards: []int64{1, 1}},
		{
			name: "a Pod is pinned to the largest request", cards: []int64{1, 2, 2}, groups: []string{group},
			want: required([]core.NodeSelectorRequirement{pin}),
		},
		{
			name: "the pin is added to every existing term", cards: []int64{2}, groups: []string{group},
			affinity: required([]core.NodeSelectorRequirement{zone}, []core.NodeSelectorRequirement{host}),
			want:     required([]core.NodeSelectorRequirement{zone, pin}, []core.NodeSelectorRequirement{host, pin}),
		},
		{
			name: "a cold cache falls back to the API reader", cards: []int64{2}, groups: []string{group}, coldCache: true,
			want: required([]core.NodeSelectorRequirement{pin}),
		},
		{name: "no matching instance type", cards: []int64{2}, wantErr: "no instance type fronts local queue"},
		{name: "the instance type has no group", cards: []int64{2}, groups: []string{""}, wantErr: "names no accelerator group"},
		{name: "ambiguous instance type", cards: []int64{2}, groups: []string{group, group}, wantErr: "more than one instance type"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var objects []ctrlcli.Object
			for i, g := range tc.groups {
				objects = append(objects, &workercore.InstanceType{
					ObjectMeta: meta.ObjectMeta{Name: fmt.Sprintf("it-%d", i), Labels: map[string]string{QueueEntranceLabelKey: queue}},
					Spec:       workercore.InstanceTypeSpec{AcceleratorGroup: g},
				})
			}
			reader := ctrlfake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(objects...).Build()
			w := &PodWebhook{Client: reader, APIReader: reader}
			if tc.coldCache {
				w.Client = ctrlfake.NewClientBuilder().WithScheme(scheme.Scheme).Build()
			}

			pod := &core.Pod{ObjectMeta: meta.ObjectMeta{Labels: map[string]string{kueuectrlconst.QueueLabel: queue}}}
			pod.Spec.Affinity = tc.affinity
			for i, n := range tc.cards {
				rl := core.ResourceList{shared: *resource.NewQuantity(n, resource.DecimalSI)}
				pod.Spec.Containers = append(pod.Spec.Containers, core.Container{
					Name: fmt.Sprintf("app-%d", i), Resources: core.ResourceRequirements{Requests: rl, Limits: rl.DeepCopy()},
				})
			}
			err := w.Default(t.Context(), pod)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, pod.Spec.Affinity)
			// API 服务器可能再次调用默认化 webhook，同一选择项只应保留一份卡数条件。
			require.NoError(t, w.Default(t.Context(), pod))
			assert.Equal(t, tc.want, pod.Spec.Affinity)
		})
	}
}
