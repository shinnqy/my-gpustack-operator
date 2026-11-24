package nodefeature

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"gpustack.ai/gpustack/pkg/device"
)

// fabricGroups 按给定顺序构造一组加速器。传入 nil 表示该卡没有上报 Fabric 信息。
func fabricGroups(fabrics ...*device.Fabric) device.DevicesGroupList {
	accels := make([]device.Accelerator, 0, len(fabrics))
	for i, fabric := range fabrics {
		accels = append(accels, device.Accelerator{
			ID:       string(rune('a' + i)),
			Topology: device.Topology{Fabric: fabric},
		})
	}
	return device.DevicesGroupList{{ID: "grp", Accelerators: accels}}
}

func TestConstructFabricNodeLabels(t *testing.T) {
	ub7 := &device.Fabric{Kind: "ub", ID: "7", MemberCount: 384}

	cases := []struct {
		name   string
		groups device.DevicesGroupList
		want   map[string]string
	}{
		{
			name:   "one domain across every accelerator",
			groups: fabricGroups(ub7, ub7),
			want: map[string]string{
				NodeFabricDomainLabelKey:  "ub-7",
				NodeFabricMembersLabelKey: "384",
			},
		},
		{
			// 域标识包含互连类型，即使两种互连上报相同的 ID，也不能合并成同一个域。
			name: "two kinds sharing an id do not agree",
			groups: fabricGroups(ub7,
				&device.Fabric{Kind: "xgmi", ID: "7"}),
			want: map[string]string{},
		},
		{
			name: "two domains of one kind do not agree",
			groups: fabricGroups(ub7,
				&device.Fabric{Kind: "ub", ID: "8", MemberCount: 384}),
			want: map[string]string{},
		},
		{
			// 只有部分卡上报 Fabric 信息时，无法为整个节点确认一个共同的域。
			name:   "an accelerator with no record disagrees",
			groups: fabricGroups(ub7, nil),
			want:   map[string]string{},
		},
		{
			// 只观测到互连端点却没有域 ID 时，不能据此发布节点级域标签。
			name:   "a record with no domain id is not a domain",
			groups: fabricGroups(&device.Fabric{Kind: "ub", Endpoints: []string{"fe80"}}),
			want:   map[string]string{},
		},
		{
			// 没有上报成员数的域仍然可以由域标签标识。
			name:   "no member count",
			groups: fabricGroups(&device.Fabric{Kind: "ub", ID: "7"}),
			want:   map[string]string{NodeFabricDomainLabelKey: "ub-7"},
		},
		{
			// 两张卡认同同一个域但上报的成员数不同。这时只省略成员数，避免节点标签取决于遍历顺序。
			name: "cards disagreeing on the size keep the domain and lose the count",
			groups: fabricGroups(ub7,
				&device.Fabric{Kind: "ub", ID: "7", MemberCount: 128}),
			want: map[string]string{NodeFabricDomainLabelKey: "ub-7"},
		},
		{
			// 交换两张卡的顺序后应得到相同结果。
			name: "the disagreement is not resolved by iteration order",
			groups: fabricGroups(&device.Fabric{Kind: "ub", ID: "7", MemberCount: 128},
				ub7),
			want: map[string]string{NodeFabricDomainLabelKey: "ub-7"},
		},
		{
			// 一张卡没有上报成员数时，不能把另一张卡的读数视为整个节点一致的读数。
			name: "a card reporting no size withholds the count too",
			groups: fabricGroups(ub7,
				&device.Fabric{Kind: "ub", ID: "7"}),
			want: map[string]string{NodeFabricDomainLabelKey: "ub-7"},
		},
		{
			// clique 标识属于域值的一部分。这样同一个 Fabric 的不同分区会有不同的标签。
			name: "a partitioned fabric names the clique in the value",
			groups: fabricGroups(
				&device.Fabric{Kind: "nvlink", ID: "c0", CliqueID: "1"},
				&device.Fabric{Kind: "nvlink", ID: "c0", CliqueID: "1"},
			),
			want: map[string]string{NodeFabricDomainLabelKey: "nvlink-c0-1"},
		},
		{
			// 同节点的卡分属两个无法互通的 clique 时，不能为整个节点发布共同域。
			name: "one fabric whose cliques disagree",
			groups: fabricGroups(
				&device.Fabric{Kind: "nvlink", ID: "c0", CliqueID: "0"},
				&device.Fabric{Kind: "nvlink", ID: "c0", CliqueID: "1"},
			),
			want: map[string]string{},
		},
		{
			// 0 是有效的 clique 编号，须和完全没有上报 clique 的情况区分开。
			name:   "the first clique is named like any other",
			groups: fabricGroups(&device.Fabric{Kind: "nvlink", ID: "c0", CliqueID: "0"}),
			want:   map[string]string{NodeFabricDomainLabelKey: "nvlink-c0-0"},
		},
		{
			name:   "no accelerators at all",
			groups: device.DevicesGroupList{},
			want:   map[string]string{},
		},
		{
			// 如果标签清理会改写或截断组合出的域名，就省略这个标签，避免给节点写入另一个域的名称。
			name:   "a domain too long to publish is withheld",
			groups: fabricGroups(&device.Fabric{Kind: "nvlink", ID: strings.Repeat("a", 64)}),
			want:   map[string]string{},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.want, ConstructFabricNodeLabels(c.groups))
		})
	}
}
