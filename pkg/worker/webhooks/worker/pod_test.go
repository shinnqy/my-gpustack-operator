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

// instanceTypeWithPhysicalProfiles 构建用于前端的 InstanceType，分区请求读取内容：一个计算得到的详细
// 信息（非空的生产商标识表示其已准备就绪），包含每张卡对应的 VRAM 配置以及池的汇总分区规格清单（名称
// → 每个实例的 MemoryMib 内存配置 + 池上限数量）。
func instanceTypeWithPhysicalProfiles(
	memory string, profiles ...workercore.AcceleratorSlicedPhysicalDetailProfile,
) *workercore.InstanceType {
	it := instanceTypeWithEntrance(memory)
	it.Status.Detail.Manufacturer = nodefeature.ManufacturerNVIDIA
	it.Status.Detail.SlicedDetail.Physical.Profiles = profiles
	return it
}

// physicalProfiles 是用于分区测试所遵循的标准 H100-80GB 资源清单。
func physicalProfiles() []workercore.AcceleratorSlicedPhysicalDetailProfile {
	return []workercore.AcceleratorSlicedPhysicalDetailProfile{
		{Name: "1g.10gb", Count: 7, MemoryMib: 10240},
		{Name: "2g.20gb", Count: 3, MemoryMib: 20480},
	}
}

// partitionKey 返回 NVIDIA 每个分析报告的物理分区键（例如“nvidia.com/gpu.partitioned.mig-1g.10gb”）。
func partitionKey(profile string) core.ResourceName {
	return nodefeature.GetAcceleratablePartitionedProfileResourceName(nodefeature.ManufacturerNVIDIA, profile)
}

// twoProfilePod 构建了一个 Pod，该 Pod 中的两个应用容器分别使用不同的分区规格——这种不可归属的多规格
// 结构被 ValidateCreate 拒绝。
func twoProfilePod(card, profileA, profileB core.ResourceName) *core.Pod {
	rlA := core.ResourceList{card: resource.MustParse("1"), profileA: resource.MustParse("1")}
	rlB := core.ResourceList{card: resource.MustParse("1"), profileB: resource.MustParse("1")}
	return &core.Pod{
		ObjectMeta: meta.ObjectMeta{
			Namespace: "default", Name: "p",
			Labels: map[string]string{kueuectrlconst.QueueLabel: testLocalQueueName},
		},
		Spec: core.PodSpec{
			Containers: []core.Container{
				{Name: "main", Resources: core.ResourceRequirements{Requests: rlA, Limits: rlA.DeepCopy()}},
				{Name: "aux", Resources: core.ResourceRequirements{Requests: rlB, Limits: rlB.DeepCopy()}},
			},
		},
	}
}

// initContainerPod 构建了一个队列路由的 Pod，其加速器请求存储在不可重启的初始化容器中，因此 webhook
// 必须对初始化容器进行验证和合并。
func initContainerPod(requests map[core.ResourceName]string) *core.Pod {
	rl := core.ResourceList{}
	for n, v := range requests {
		rl[n] = resource.MustParse(v)
	}
	return &core.Pod{
		ObjectMeta: meta.ObjectMeta{
			Namespace: "default", Name: "p",
			Labels: map[string]string{kueuectrlconst.QueueLabel: testLocalQueueName},
		},
		Spec: core.PodSpec{
			InitContainers: []core.Container{
				{Name: "init", Resources: core.ResourceRequirements{Requests: rl, Limits: rl.DeepCopy()}},
			},
			Containers: []core.Container{{Name: "main"}},
		},
	}
}

// initPartitionPlusAppExclusivePod 在初始化容器上构建带有分区请求的 Pod，在应用容器上构建全卡独占请
// 求的 Pod——分别属于两个容器组中的两个不同类别。
func initPartitionPlusAppExclusivePod(card, profileKey, exclusiveKey core.ResourceName) *core.Pod {
	initRL := core.ResourceList{card: resource.MustParse("1"), profileKey: resource.MustParse("1")}
	appRL := core.ResourceList{exclusiveKey: resource.MustParse("1")}
	return &core.Pod{
		ObjectMeta: meta.ObjectMeta{
			Namespace: "default", Name: "p",
			Labels: map[string]string{kueuectrlconst.QueueLabel: testLocalQueueName},
		},
		Spec: core.PodSpec{
			InitContainers: []core.Container{
				{Name: "init", Resources: core.ResourceRequirements{Requests: initRL, Limits: initRL.DeepCopy()}},
			},
			Containers: []core.Container{
				{Name: "main", Resources: core.ResourceRequirements{Requests: appRL, Limits: appRL.DeepCopy()}},
			},
		},
	}
}

func TestPodWebhook_DefaultPartitioned(t *testing.T) {
	nvidiaBase := nodefeature.GetAcceleratableResourceName(nodefeature.ManufacturerNVIDIA, workercore.DeviceAllocationModeExclusive)
	names := logicalResourceNamesForBase(nvidiaBase)
	pnames := partitionResourceNamesForBase(nvidiaBase)
	profileKey := partitionKey("1g.10gb")
	unknownKey := partitionKey("9g.99gb")

	cases := []struct {
		name      string
		requests  map[core.ResourceName]string
		it        *workercore.InstanceType
		wantUnits int64
		wantErr   bool
	}{
		{
			name: "3g.40gb on an 80 GiB card replaces client-supplied units",
			requests: map[core.ResourceName]string{
				pnames.card: "1", partitionKey("3g.40gb"): "1", pnames.units: "999",
			},
			it: instanceTypeWithPhysicalProfiles("80Gi",
				workercore.AcceleratorSlicedPhysicalDetailProfile{Name: "3g.40gb", Count: 2, MemoryMib: 40960}),
			wantUnits: 800000,
		},
		{
			// 10Gi / 80Gi x D = D/8 = 200000，与逻辑 .sliced.memory-mib 中的 10Gi 请求相同。
			name:      "profile folds units from its VRAM",
			requests:  map[core.ResourceName]string{pnames.card: "1", profileKey: "1"},
			it:        instanceTypeWithPhysicalProfiles("81920Mi", physicalProfiles()...),
			wantUnits: 200000,
		},
		{
			name:     "unknown profile rejected",
			requests: map[core.ResourceName]string{pnames.card: "1", unknownKey: "1"},
			it:       instanceTypeWithPhysicalProfiles("81920Mi", physicalProfiles()...),
			wantErr:  true,
		},
		{
			name:     "detail not ready rejected (retryable)",
			requests: map[core.ResourceName]string{pnames.card: "1", profileKey: "1"},
			it:       instanceTypeWithEntrance("81920Mi"), // 制造商为空 -> 未准备就绪
			wantErr:  true,
		},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			w := newPodWebhook(c.it)
			pod := slicedPod(c.requests)

			err := w.Default(context.Background(), pod)
			if c.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)

			ctr := &pod.Spec.Containers[0]
			reqUnits := ctr.Resources.Requests[pnames.units]
			limUnits := ctr.Resources.Limits[pnames.units]
			assert.Equal(t, c.wantUnits, reqUnits.Value(), "partitioned units request")
			assert.Equal(t, c.wantUnits, limUnits.Value(), "partitioned units limit")
			// 分区请求未使用任何逻辑预算键。
			_, hasCores := ctr.Resources.Requests[names.coresPct]
			assert.False(t, hasCores, "a partition request must not default cores-percentage")
			_, hasLogicalUnits := ctr.Resources.Requests[names.units]
			assert.False(t, hasLogicalUnits, "a partition request must not fold the logical units key")
		})
	}
}

// TestPodWebhook_PartitionedUnitsParity 保护着非冲突属性：一个分区规格请求与该规格的 VRAM 折叠对应的
// 逻辑 .sliced.memory-mib 请求都指向相同的单位值，因此这两个类别会为相同的 VRAM 获得相同的积分。
func TestPodWebhook_PartitionedUnitsParity(t *testing.T) {
	nvidiaBase := nodefeature.GetAcceleratableResourceName(nodefeature.ManufacturerNVIDIA, workercore.DeviceAllocationModeExclusive)
	names := logicalResourceNamesForBase(nvidiaBase)
	pnames := partitionResourceNamesForBase(nvidiaBase)

	partitionPod := slicedPod(map[core.ResourceName]string{pnames.card: "1", partitionKey("1g.10gb"): "1"})
	logicalPod := slicedPod(map[core.ResourceName]string{names.card: "1", names.memMib: "10240"}) // 显存为 10 GiB。
	w := newPodWebhook(instanceTypeWithPhysicalProfiles("81920Mi", physicalProfiles()...))

	assert.NoError(t, w.Default(context.Background(), partitionPod))
	assert.NoError(t, w.Default(context.Background(), logicalPod))

	partitionUnits := partitionPod.Spec.Containers[0].Resources.Requests[pnames.units]
	logicalUnits := logicalPod.Spec.Containers[0].Resources.Requests[names.units]
	assert.Equal(t, logicalUnits.Value(), partitionUnits.Value(),
		"a partition and a same-VRAM logical slice fold identically")
}

func TestPodWebhook_ValidateCreatePartitioned(t *testing.T) {
	nvidiaBase := nodefeature.GetAcceleratableResourceName(nodefeature.ManufacturerNVIDIA, workercore.DeviceAllocationModeExclusive)
	names := logicalResourceNamesForBase(nvidiaBase)
	pnames := partitionResourceNamesForBase(nvidiaBase)
	profileKey := partitionKey("1g.10gb")
	profileKey2 := partitionKey("2g.20gb")

	cases := []struct {
		name    string
		pod     *core.Pod
		wantErr bool
	}{
		{
			name: "one card, one profile is valid",
			pod:  slicedPod(map[core.ResourceName]string{pnames.card: "1", profileKey: "1"}),
		},
		{
			// 规则 3。
			name:    "two partitioned cards rejected",
			pod:     slicedPod(map[core.ResourceName]string{pnames.card: "2", profileKey: "1"}),
			wantErr: true,
		},
		{
			// 规则 5。
			name:    "a per-profile value of 2 rejected",
			pod:     slicedPod(map[core.ResourceName]string{pnames.card: "1", profileKey: "2"}),
			wantErr: true,
		},
		{
			name:    "a profile key without the card key rejected",
			pod:     slicedPod(map[core.ResourceName]string{profileKey: "1"}),
			wantErr: true,
		},
		{
			// 规则 4 在容器范围内：没有配置 profile 的卡片密钥没有硬件形态。
			name:    "the card key alone rejected",
			pod:     slicedPod(map[core.ResourceName]string{pnames.card: "1"}),
			wantErr: true,
		},
		{
			// 规则 4。
			name:    "two profiles on one container rejected",
			pod:     slicedPod(map[core.ResourceName]string{pnames.card: "1", profileKey: "1", profileKey2: "1"}),
			wantErr: true,
		},
		{
			// 规则1：分区和逻辑族在Pod范围内是互斥的，这包含了被替换的每容器分支。
			name:    "a partition plus a logical memory key rejected",
			pod:     slicedPod(map[core.ResourceName]string{pnames.card: "1", profileKey: "1", names.memMib: "4096"}),
			wantErr: true,
		},
		{
			// 规则 4 和 6。
			name:    "two containers naming different profiles rejected",
			pod:     twoProfilePod(pnames.card, profileKey, profileKey2),
			wantErr: true,
		},
		{
			name: "a partition on an init container alone is valid",
			pod:  initContainerPod(map[core.ResourceName]string{pnames.card: "1", profileKey: "1"}),
		},
		{
			// 规则5也适用于初始化路径：初始化容器会被验证，不会被跳过。
			name:    "a per-profile value of 2 on an init container rejected",
			pod:     initContainerPod(map[core.ResourceName]string{pnames.card: "1", profileKey: "2"}),
			wantErr: true,
		},
		{
			// 规则1（两个类别）和再次的规则1（两个组）。
			name:    "an init-container partition plus an app-container exclusive rejected",
			pod:     initPartitionPlusAppExclusivePod(pnames.card, profileKey, nvidiaBase),
			wantErr: true,
		},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			w := newPodWebhook()
			_, err := w.ValidateCreate(context.Background(), c.pod)
			if c.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestPodWebhook_PartitionProfileMissingMemoryIsRetryable 检查一种暂时状态：InstanceType 已列出规格，
// 但探测尚未填入每实例的 MemoryMib。此时 Default 应提示重试。
func TestPodWebhook_PartitionProfileMissingMemoryIsRetryable(t *testing.T) {
	nvidiaBase := nodefeature.GetAcceleratableResourceName(nodefeature.ManufacturerNVIDIA, workercore.DeviceAllocationModeExclusive)
	pnames := partitionResourceNamesForBase(nvidiaBase)

	// 该配置被提供，但每个实例的 MemoryMib值仍为0（细节尚未完全计算）。
	it := instanceTypeWithPhysicalProfiles("81920Mi",
		workercore.AcceleratorSlicedPhysicalDetailProfile{Name: "1g.10gb", Count: 7, MemoryMib: 0})
	w := newPodWebhook(it)

	pod := slicedPod(map[core.ResourceName]string{pnames.card: "1", partitionKey("1g.10gb"): "1"})
	err := w.Default(context.Background(), pod)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not ready",
		"a partition profile with MemoryMib==0 must be a retryable not-ready rejection, not a permanent zero-units error")
}
