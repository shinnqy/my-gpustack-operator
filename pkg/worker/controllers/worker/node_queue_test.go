package worker

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrlcli "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	ctrlreconcile "sigs.k8s.io/controller-runtime/pkg/reconcile"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"gpustack.ai/gpustack/pkg/kubeclients/kubernetes/scheme"
	"gpustack.ai/gpustack/pkg/nodefeature"
	"gpustack.ai/gpustack/pkg/systemmeta"
	"gpustack.ai/gpustack/pkg/systemname"
)

// buildNodeQueueClient 为 NodeQueueReconciler 单测构造客户端。控制器通过标签列出 flavor，
// 无需额外的字段索引。注册 ClusterQueue 的 status 子资源以测试拓扑就绪状态的更新，
// 并保留输入中的预留计数供 hasReserved 读取。
func buildNodeQueueClient(objs ...ctrlcli.Object) ctrlcli.Client {
	profile := topologyProfile([]string{core.LabelHostname})
	topologyNames := make(map[string]struct{})
	var generated []ctrlcli.Object
	for _, obj := range objs {
		rf, ok := obj.(*kueue.ResourceFlavor)
		if !ok {
			continue
		}
		if rf.Labels == nil {
			rf.Labels = make(map[string]string)
		}
		rf.Labels[TopologyProfileLabel] = profile
		if rf.Spec.NodeLabels == nil {
			rf.Spec.NodeLabels = map[string]string{
				systemname.ManagedLabelKey: "true",
				core.LabelOSStable:         rf.Labels[core.LabelOSStable],
				core.LabelArchStable:       rf.Labels[core.LabelArchStable],
				TopologyProfileLabel:       profile,
			}
			for key, value := range rf.Labels {
				if key == nodefeature.NodeAcceleratableLabelKey ||
					(value == "true" && (strings.HasPrefix(key, nodefeature.GeneralFeatureLabelPrefix) ||
						strings.HasPrefix(key, nodefeature.AcceleratableFeatureLabelPrefix))) {
					rf.Spec.NodeLabels[key] = value
					if count := rf.Labels[key+_ResourceFlavorCountLabelSuffix]; count != "" {
						rf.Spec.NodeLabels[key+_ResourceFlavorCountLabelSuffix] = count
					}
				}
			}
		}
		if rf.Spec.TopologyName == nil {
			name := kueue.TopologyReference(topologyName(profile))
			rf.Spec.TopologyName = &name
		}
		topologyNames[string(*rf.Spec.TopologyName)] = struct{}{}
		if rf.DeletionTimestamp != nil {
			continue
		}

		count := parseResourceFlavorCount(rf)
		capacity := parseResourceFlavorCapacity(rf)
		for i := int64(0); count > 0 && i < capacity/count; i++ {
			labels := make(map[string]string, len(rf.Spec.NodeLabels)+1)
			for key, value := range rf.Spec.NodeLabels {
				labels[key] = value
			}
			labels[core.LabelHostname] = fmt.Sprintf("%s-%d", rf.Name, i)
			generated = append(generated, &core.Node{ObjectMeta: meta.ObjectMeta{
				Name:   labels[core.LabelHostname],
				Labels: labels,
			}})
		}
	}
	for name := range topologyNames {
		generated = append(generated, &kueue.Topology{
			ObjectMeta: meta.ObjectMeta{Name: name},
			Spec:       kueue.TopologySpec{Levels: []kueue.TopologyLevel{{NodeLabel: core.LabelHostname}}},
		})
	}
	objs = append(objs, generated...)
	return ctrlfake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithStatusSubresource(&kueue.ClusterQueue{}).
		WithObjects(objs...).
		Build()
}

// reconcileNodeQueueN 按名称协调队列 n 次，返回最后一次结果，方便同时检查收敛状态和重新入队信号。
func reconcileNodeQueueN(t *testing.T, cli ctrlcli.Client, name string, n int) ctrlreconcile.Result {
	t.Helper()
	r := &NodeQueueReconciler{Client: cli}
	var res ctrlreconcile.Result
	for range n {
		var err error
		res, err = r.Reconcile(context.Background(),
			ctrlreconcile.Request{NamespacedName: ctrlcli.ObjectKey{Name: name}})
		require.NoError(t, err)
	}
	return res
}

// newInstanceTypeQueue 构造由 Operator 管理的 ClusterQueue，包含池的特征、系统及架构标签，
// 资源类别设为 instancetypes，StopPolicy 为 None。资源组默认留空，
// 随后由 NodeQueueReconciler 维护配额。
func newInstanceTypeQueue(key string, acceleratable bool, groups ...kueue.ResourceGroup) *kueue.ClusterQueue {
	name := nodeQueueName(key)
	cq := &kueue.ClusterQueue{
		ObjectMeta: meta.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				featureKeyLabel(acceleratable, key):   "true",
				nodefeature.NodeAcceleratableLabelKey: strconv.FormatBool(acceleratable),
				core.LabelOSStable:                    "linux",
				core.LabelArchStable:                  "amd64",
			},
		},
		Spec: kueue.ClusterQueueSpec{
			NamespaceSelector: &meta.LabelSelector{},
			StopPolicy:        ptr.To(kueue.None),
			ResourceGroups:    groups,
		},
	}
	if len(groups) > 0 {
		cq.Annotations = map[string]string{_TASQueueAnnotation: "true"}
	}
	systemmeta.NoteResource(cq, _ClusterQueueResType, nil)
	return cq
}

// creditsValue 将指定整卡数换算为资源池的标称额度。
func creditsValue(cards int64) int64 {
	q := nodefeature.AcceleratorsToCredits(*resource.NewQuantity(cards, resource.DecimalSI))
	return q.Value()
}

// TestNodeQueueReconciler_FillsAndSortsByCount 检查 CPU 队列按核数、加速器队列按厂商 credits 计量，
// 后者等于池内卡数乘每卡额度。flavor 按单节点数量升序排列，
// 未加入 cohort 的队列不设置借入或借出上限。
func TestNodeQueueReconciler_FillsAndSortsByCount(t *testing.T) {
	creditsName := nodefeature.GetAcceleratableCreditsResourceName(nodefeature.ManufacturerNVIDIA)

	cases := []struct {
		name string

		key           string
		acceleratable bool
		// 故意让单节点数量最大的 flavor 排在输入首位，以便验证输出确实按升序排序。
		flavors []*kueue.ResourceFlavor

		wantCovered      core.ResourceName
		wantFirstName    string
		wantFirstNominal int64
	}{
		{
			name: "cpu-only queue covers cpu, smallest per-node count first",
			key:  "generic",
			flavors: []*kueue.ResourceFlavor{
				newNodesFlavor("gpustack-generic-linux-amd64-8c", "generic", 8, 8),
				newNodesFlavor("gpustack-generic-linux-amd64-4c", "generic", 4, 4),
			},
			wantCovered:      core.ResourceCPU,
			wantFirstName:    "gpustack-generic-linux-amd64-4c",
			wantFirstNominal: 4,
		},
		{
			name:          "accelerated queue covers credits, smallest per-node count first",
			key:           "nvidia-a10g",
			acceleratable: true,
			flavors: []*kueue.ResourceFlavor{
				newNodesFlavor("gpustack-nvidia-a10g-linux-amd64-2d", "nvidia-a10g", 2, 4,
					accelerated(nodefeature.ManufacturerNVIDIA)),
				newNodesFlavor("gpustack-nvidia-a10g-linux-amd64-1d", "nvidia-a10g", 1, 3,
					accelerated(nodefeature.ManufacturerNVIDIA)),
			},
			wantCovered:      creditsName,
			wantFirstName:    "gpustack-nvidia-a10g-linux-amd64-1d",
			wantFirstNominal: creditsValue(3),
		},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			objs := []ctrlcli.Object{
				newInstanceTypeQueue(c.key, c.acceleratable),
			}
			for _, rf := range c.flavors {
				objs = append(objs, rf)
			}
			cli := buildNodeQueueClient(objs...)
			name := nodeQueueName(c.key)

			reconcileNodeQueueN(t, cli, name, 2)

			cq, err := getClusterQueue(t, cli, name)
			require.NoError(t, err)
			require.Len(t, cq.Spec.ResourceGroups, 1, "one resource group")
			rg := cq.Spec.ResourceGroups[0]
			require.Len(t, rg.CoveredResources, 1, "one covered resource")
			assert.Equal(t, c.wantCovered, rg.CoveredResources[0], "covered resource")
			require.Len(t, rg.Flavors, len(c.flavors), "one flavor quota per feeding flavor")

			assert.Equal(t, c.wantFirstName, string(rg.Flavors[0].Name),
				"smallest per-node count feeds first")
			rq := rg.Flavors[0].Resources[0]
			assert.Equal(t, c.wantFirstNominal, rq.NominalQuota.Value(), "first flavor nominal quota")
			assert.Nil(t, rq.BorrowingLimit, "no borrowingLimit on a cohort-less queue")
			assert.Nil(t, rq.LendingLimit, "no lendingLimit on a cohort-less queue")
		})
	}
}
