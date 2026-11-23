package worker

import (
	"context"
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

const testLocalQueueName = "test-lq"

func newPodWebhook(objs ...ctrlcli.Object) *PodWebhook {
	cli := ctrlfake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithObjects(objs...).
		Build()
	return &PodWebhook{Client: cli, APIReader: cli}
}

// slicedPod构建了一个队列路由的Pod，其单个容器在Requests和Limits两个方面都需要使用给定的（解析过的）
// 资源。
func slicedPod(requests map[core.ResourceName]string) *core.Pod {
	rl := core.ResourceList{}
	for n, v := range requests {
		rl[n] = resource.MustParse(v)
	}
	return &core.Pod{
		ObjectMeta: meta.ObjectMeta{
			Namespace: "default",
			Name:      "p",
			Labels:    map[string]string{kueuectrlconst.QueueLabel: testLocalQueueName},
		},
		Spec: core.PodSpec{
			Containers: []core.Container{
				{Name: "main", Resources: core.ResourceRequirements{Requests: rl, Limits: rl.DeepCopy()}},
			},
		},
	}
}

// instanceTypeWithEntrance 构建由运营商拥有的 InstanceType 实例，该实例负责处理 testLocalQueueName
// 任务，其中包含根据队列入口标签所观察到的每张卡对应的 VRAM 容量（Status.Detail.Memory），以及默认
// 拦截的 webhook 查询功能。空的内存模型表示尚未准备就绪的状态（Detail 未计算）。
func instanceTypeWithEntrance(memory string) *workercore.InstanceType {
	it := &workercore.InstanceType{
		ObjectMeta: meta.ObjectMeta{
			Name:   "it-" + testLocalQueueName,
			Labels: map[string]string{QueueEntranceLabelKey: testLocalQueueName},
		},
	}
	it.Status.Detail.Memory = memory
	return it
}

func TestPodWebhook_Default(t *testing.T) {
	nvidiaBase := nodefeature.GetAcceleratableResourceName(nodefeature.ManufacturerNVIDIA, workercore.DeviceAllocationModeExclusive)
	names := logicalResourceNamesForBase(nvidiaBase)

	cases := []struct {
		name      string
		requests  map[core.ResourceName]string
		itMemory  string // 关联 InstanceType 的 Status.Detail.Memory。为空且 itPresent 为假时不创建规格。
		itPresent bool   // 即使显存为空也创建 InstanceType，表示规格存在但详情尚未就绪。
		wantUnits int64  // 预期的 .sliced.units，0 表示该键应缺失。
		wantCores int64  // 预期的 .sliced.cores-percentage，0 表示本用例不检查。
		wantErr   bool
	}{
		{
			name:      "one card with 25 percent memory and 50 percent compute",
			requests:  map[core.ResourceName]string{names.card: "1", names.memPct: "25", names.coresPct: "50"},
			wantUnits: 400000, wantCores: 50,
		},
		{
			name:      "memory-percentage folds to units, cores defaults to 100",
			requests:  map[core.ResourceName]string{names.card: "1", names.memPct: "20"},
			wantUnits: 320000, // 20 × M/100
			wantCores: 100,
		},
		{
			name:      "memory-mib folds to units using the InstanceType VRAM",
			requests:  map[core.ResourceName]string{names.card: "1", names.memMib: "4096"},
			itMemory:  "40960Mi", // 40 GiB 卡申请 4 GiB，占 10%，折合 160,000 units。
			wantUnits: 160000,
			wantCores: 100,
		},
		{
			name:      "explicit cores-percentage is preserved",
			requests:  map[core.ResourceName]string{names.card: "1", names.memPct: "20", names.coresPct: "50"},
			wantUnits: 320000,
			wantCores: 50,
		},
		{
			name:      "client-supplied units is recomputed from memory (not trusted)",
			requests:  map[core.ResourceName]string{names.card: "1", names.memPct: "20", names.units: "999"},
			wantUnits: 320000, // 用 20 × M/100 覆盖客户端写入的 999。
			wantCores: 100,
		},
		{
			name:      "no memory leaves units unset",
			requests:  map[core.ResourceName]string{names.card: "1"},
			wantUnits: 0,
			wantCores: 100,
		},
		{
			name:     "memory-mib without a resolvable VRAM is rejected",
			requests: map[core.ResourceName]string{names.card: "1", names.memMib: "4096"},
			wantErr:  true,
		},
		{
			name:      "memory-mib rejected as retryable when Detail not ready",
			requests:  map[core.ResourceName]string{names.card: "1", names.memMib: "4096"},
			itPresent: true, // InstanceType 存在，但 Status.Detail.Memory 为空。
			wantErr:   true,
		},
		{
			name:     "memory-percentage over 100 is rejected (no overflow)",
			requests: map[core.ResourceName]string{names.card: "1", names.memPct: "200"},
			wantErr:  true,
		},
		{
			name:     "memory-mib over the per-card VRAM is rejected",
			requests: map[core.ResourceName]string{names.card: "1", names.memMib: "81920"},
			itMemory: "40960Mi", // 40 GiB 的卡无法满足 80 GiB 的单卡请求。
			wantErr:  true,
		},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			var objs []ctrlcli.Object
			if c.itMemory != "" || c.itPresent {
				objs = append(objs, instanceTypeWithEntrance(c.itMemory))
			}
			w := newPodWebhook(objs...)
			pod := slicedPod(c.requests)

			err := w.Default(context.Background(), pod)
			if c.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)

			ctr := &pod.Spec.Containers[0]
			if c.wantUnits == 0 {
				_, ok := ctr.Resources.Requests[names.units]
				assert.False(t, ok, "units should be unset")
			} else {
				reqUnits := ctr.Resources.Requests[names.units]
				limUnits := ctr.Resources.Limits[names.units]
				assert.Equal(t, c.wantUnits, reqUnits.Value(), "units request")
				assert.Equal(t, c.wantUnits, limUnits.Value(), "units limit")
			}
			if c.wantCores != 0 {
				reqCores := ctr.Resources.Requests[names.coresPct]
				assert.Equal(t, c.wantCores, reqCores.Value(), "cores-percentage")
			}
		})
	}
}
