package detector

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/util/sets"

	"gpustack.ai/gpustack/pkg/device"
	"gpustack.ai/gpustack/pkg/nodefeature"
)

// TestAlignDeviceGroups 验证已存在组的能力变化能保留在结果中，覆盖早先比较发现变化却从旧切片重建结果的问题。
func TestAlignDeviceGroups(t *testing.T) {
	const (
		manufacturer = "nvidia"
		groupID      = "group-0"
	)
	allowed := sets.New(manufacturer)

	baseGroup := func(status device.AcceleratorStatus) device.DevicesGroup {
		return device.DevicesGroup{
			ID:           groupID,
			Manufacturer: manufacturer,
			Name:         "Tesla-T4",
			Accelerators: []device.Accelerator{
				{ID: "gpu-0", Status: status},
			},
		}
	}

	cases := []struct {
		name    string
		aGroups device.DevicesGroupList
		eGroups device.DevicesGroupList
		want    device.DevicesGroupList
	}{
		{
			name: "existing group's physical slicing profile change is persisted",
			aGroups: device.DevicesGroupList{
				baseGroup(device.AcceleratorStatus{}),
			},
			eGroups: device.DevicesGroupList{
				baseGroup(device.AcceleratorStatus{
					PhysicalSliced: device.AcceleratorPhysicalSliced{
						Profiles: []device.AcceleratorPhysicalSlicedProfile{
							{Name: "1g.5gb", Count: 7},
						},
						Count: 7,
					},
				}),
			},
			want: device.DevicesGroupList{
				baseGroup(device.AcceleratorStatus{
					PhysicalSliced: device.AcceleratorPhysicalSliced{
						Profiles: []device.AcceleratorPhysicalSlicedProfile{
							{Name: "1g.5gb", Count: 7},
						},
						Count: 7,
					},
				}),
			},
		},
		{
			name: "existing group's logical slicing count change is persisted",
			aGroups: device.DevicesGroupList{
				baseGroup(device.AcceleratorStatus{
					LogicalSliced: device.AcceleratorLogicalSliced{Count: 4},
				}),
			},
			eGroups: device.DevicesGroupList{
				baseGroup(device.AcceleratorStatus{
					LogicalSliced: device.AcceleratorLogicalSliced{Count: 8},
				}),
			},
			want: device.DevicesGroupList{
				baseGroup(device.AcceleratorStatus{
					LogicalSliced: device.AcceleratorLogicalSliced{Count: 8},
				}),
			},
		},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, changed := alignDeviceGroups(c.aGroups, c.eGroups, allowed)
			assert.True(t, changed, "a capability change on an existing group must be reported as changed")
			assert.Equal(t, c.want, got)
		})
	}
}

// TestAlignDeviceGroups_RetainsUnhealthyAcceleratorsAcrossAllManufacturers 验证各厂商的故障卡仍留在清单里，
// 并保留自己的健康标记。用八张卡中一张异常的场景检查身份和顺序。
func TestAlignDeviceGroups_RetainsUnhealthyAcceleratorsAcrossAllManufacturers(t *testing.T) {
	const (
		groupID     = "group-0"
		cardCount   = 8
		faultyIndex = 3
	)

	// 构造按枚举索引排列的卡。faulty 为真时仅将指定索引标为故障。
	group := func(manufacturer string, faulty bool) device.DevicesGroup {
		accels := make([]device.Accelerator, 0, cardCount)
		for i := 0; i < cardCount; i++ {
			accels = append(accels, device.Accelerator{
				ID:    fmt.Sprintf("%s-%d", manufacturer, i),
				Index: uint32(i),
				Status: device.AcceleratorStatus{
					Unhealthy: faulty && i == faultyIndex,
				},
			})
		}
		return device.DevicesGroup{
			ID:           groupID,
			Manufacturer: manufacturer,
			Name:         "Test Accelerator",
			Accelerators: accels,
		}
	}

	for _, manufacturer := range nodefeature.GetKnownAcceleratableManufacturers() {
		t.Run(manufacturer, func(t *testing.T) {
			cases := []struct {
				name        string
				stored      device.DevicesGroup
				wantChanged bool
			}{
				{
					// 原有清单为健康，本轮发现故障，需要更新状态并保留全部卡。
					name:        "a card that became faulty is marked in place and retained",
					stored:      group(manufacturer, false),
					wantChanged: true,
				},
				{
					// 旧清单已经标记该卡故障，相同结果应保持幂等。
					name:        "a card that stays faulty is still retained",
					stored:      group(manufacturer, true),
					wantChanged: false,
				},
			}

			for _, c := range cases {
				t.Run(c.name, func(t *testing.T) {
					got, changed := alignDeviceGroups(
						device.DevicesGroupList{c.stored},
						device.DevicesGroupList{group(manufacturer, true)},
						sets.New(manufacturer),
					)
					assert.Equal(t, c.wantChanged, changed)

					require.Len(t, got, 1, "the group is kept")
					require.Len(t, got[0].Accelerators, cardCount,
						"an unhealthy accelerator is still an accelerator, so the group keeps every card")
					for i, accel := range got[0].Accelerators {
						assert.Equal(t, uint32(i), accel.Index, "the cards keep their enumeration order")
						assert.Equal(t, i == faultyIndex, accel.Status.Unhealthy,
							"the health report follows the card it belongs to")
					}
				})
			}
		})
	}
}

func TestAlignDeviceGroupsOrder(t *testing.T) {
	group := func(manufacturer, id string, indexes ...uint32) device.DevicesGroup {
		accels := make([]device.Accelerator, 0, len(indexes))
		for _, idx := range indexes {
			accels = append(accels, device.Accelerator{ID: fmt.Sprintf("%s-%d", id, idx), Index: idx})
		}
		return device.DevicesGroup{ID: id, Manufacturer: manufacturer, Accelerators: accels}
	}
	// 将输出展平成可读的设备 ID 顺序。空组用括号标出自身 ID，便于比较它的排序位置。
	ids := func(groups device.DevicesGroupList) []string {
		out := make([]string, 0, len(groups))
		for i := range groups {
			if len(groups[i].Accelerators) == 0 {
				out = append(out, "("+groups[i].ID+")")
				continue
			}
			for j := range groups[i].Accelerators {
				out = append(out, groups[i].Accelerators[j].ID)
			}
		}
		return out
	}

	cases := []struct {
		name        string
		allowed     sets.Set[string]
		aGroups     device.DevicesGroupList
		eGroups     device.DevicesGroupList
		want        []string
		wantChanged bool
	}{
		{
			name:    "a newly detected group is not stored ahead of the ones already recorded",
			allowed: sets.New("nvidia"),
			aGroups: device.DevicesGroupList{group("nvidia", "l40s", 0, 1)},
			eGroups: device.DevicesGroupList{group("nvidia", "l40s", 0, 1), group("nvidia", "a10", 2)},
			want:    []string{"l40s-0", "l40s-1", "a10-2"},
			// 新增组本身就是内容变化。
			wantChanged: true,
		},
		{
			name:    "a stored order that is not canonical is rewritten",
			allowed: sets.New("nvidia"),
			aGroups: device.DevicesGroupList{group("nvidia", "a10", 2), group("nvidia", "l40s", 0, 1)},
			eGroups: device.DevicesGroupList{group("nvidia", "l40s", 0, 1), group("nvidia", "a10", 2)},
			want:    []string{"l40s-0", "l40s-1", "a10-2"},
			// 硬件保持相同，只有存储顺序待修正，也应报告一次变化。
			wantChanged: true,
		},
		{
			name:        "accelerators are ordered within their group",
			allowed:     sets.New("nvidia"),
			aGroups:     device.DevicesGroupList{group("nvidia", "l40s", 2, 0, 1)},
			eGroups:     device.DevicesGroupList{group("nvidia", "l40s", 2, 0, 1)},
			want:        []string{"l40s-0", "l40s-1", "l40s-2"},
			wantChanged: true,
		},
		{
			name:        "an already canonical list is reported unchanged",
			allowed:     sets.New("nvidia"),
			aGroups:     device.DevicesGroupList{group("nvidia", "l40s", 0, 1), group("nvidia", "a10", 2)},
			eGroups:     device.DevicesGroupList{group("nvidia", "l40s", 0, 1), group("nvidia", "a10", 2)},
			want:        []string{"l40s-0", "l40s-1", "a10-2"},
			wantChanged: false,
		},
		{
			name: "the manufacturer leads, not the index",
			// 本轮只探测 NVIDIA，Ascend 的旧组保留。两个厂商都从索引 0 开始编号，必须先按厂商排序。
			allowed: sets.New("nvidia"),
			aGroups: device.DevicesGroupList{group("nvidia", "l40s", 0), group("ascend", "910b", 1)},
			eGroups: device.DevicesGroupList{group("nvidia", "l40s", 0)},
			want:    []string{"910b-1", "l40s-0"},
			// 原存储顺序将 NVIDIA 放在前，此轮会修正它。
			wantChanged: true,
		},
		{
			name:        "a group holding no accelerator sorts last",
			allowed:     sets.New("nvidia"),
			aGroups:     device.DevicesGroupList{group("nvidia", "empty"), group("nvidia", "l40s", 0)},
			eGroups:     device.DevicesGroupList{group("nvidia", "empty"), group("nvidia", "l40s", 0)},
			want:        []string{"l40s-0", "(empty)"},
			wantChanged: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, changed := alignDeviceGroups(c.aGroups, c.eGroups, c.allowed)
			assert.Equal(t, c.want, ids(got))
			assert.Equal(t, c.wantChanged, changed)

			// 对规范化后的结果再做一次对齐应保持不变，防止重复写入账本。
			stable, stableChanged := alignDeviceGroups(got, c.eGroups, c.allowed)
			assert.Equal(t, ids(got), ids(stable))
			assert.False(t, stableChanged, "a second align must report nothing to change")
		})
	}
}

var errDetectorPass = errors.New("pass panicked")

// scriptedPass 是测试中某厂商一轮枚举的结果，可表示成功有卡、成功为空或读取错误。
type scriptedPass struct {
	groups device.DevicesGroupList
	err    error
}

// scriptedDetector 只用于测试，让连续几轮探测依次返回预设的结果。
type scriptedDetector struct {
	name   string
	detect []scriptedPass
	next   int
}

func (f *scriptedDetector) Name() string {
	return f.name
}

func (f *scriptedDetector) DetectAccelerator(bool) (device.DevicesGroupList, error) {
	if len(f.detect) == 0 {
		return nil, nil
	}
	pass := f.detect[f.next]
	if f.next+1 < len(f.detect) {
		f.next++
	}
	return pass.groups, pass.err
}

// groupsFixture 用给定设备标识构造同一厂商的测试设备组。
func groupsFixture(manufacturer string, deviceIDs ...string) device.DevicesGroupList {
	accelerators := make([]device.Accelerator, 0, len(deviceIDs))
	for i := range deviceIDs {
		accelerators = append(accelerators, device.Accelerator{ID: deviceIDs[i], Index: uint32(i)})
	}
	return device.DevicesGroupList{{
		Manufacturer: manufacturer,
		ID:           manufacturer + "-group-0",
		Accelerators: accelerators,
	}}
}

// detectRound 记录一轮测试期望的合并清单和未测量厂商。
type detectRound struct {
	groups     device.DevicesGroupList
	unmeasured []string
}

// TestDetectAcceleratorFailedPass 验证驱动读取失败与确认无设备的差异。只有后者允许撤销库存。
// 驱动异常时保留成功快照并标明本轮未知。
func TestDetectAcceleratorFailedPass(t *testing.T) {
	testCases := []struct {
		name      string
		detectors []device.Detector
		// expected 依次给出每一轮的预期结果。
		expected []detectRound
	}{
		{
			name: "a pass that fails after a successful one reports what was last detected",
			detectors: []device.Detector{&scriptedDetector{name: "alpha", detect: []scriptedPass{
				{groups: groupsFixture("alpha", "dev-0")},
				{err: errDetectorPass},
			}}},
			expected: []detectRound{
				{groups: groupsFixture("alpha", "dev-0")},
				{groups: groupsFixture("alpha", "dev-0"), unmeasured: []string{"alpha"}},
			},
		},
		{
			name: "a pass that has never succeeded reports nothing",
			detectors: []device.Detector{&scriptedDetector{name: "alpha", detect: []scriptedPass{
				{err: errDetectorPass},
			}}},
			expected: []detectRound{
				{unmeasured: []string{"alpha"}},
				{unmeasured: []string{"alpha"}},
			},
		},
		{
			// 厂商明确返回空清单后清除旧快照，以后再读取失败也不能把已经移除的卡报回来。
			name: "a pass that ran and found nothing drops what was last detected",
			detectors: []device.Detector{&scriptedDetector{name: "alpha", detect: []scriptedPass{
				{groups: groupsFixture("alpha", "dev-0")},
				{},
				{err: errDetectorPass},
			}}},
			expected: []detectRound{
				{groups: groupsFixture("alpha", "dev-0")},
				{},
				{unmeasured: []string{"alpha"}},
			},
		},
		{
			name: "a manufacturer that failed neither shadows nor is shadowed by one that answered",
			detectors: []device.Detector{
				&scriptedDetector{name: "alpha", detect: []scriptedPass{
					{groups: groupsFixture("alpha", "dev-0")},
					{err: errDetectorPass},
				}},
				&scriptedDetector{name: "beta", detect: []scriptedPass{
					{groups: groupsFixture("beta", "dev-0")},
				}},
			},
			expected: []detectRound{
				{groups: append(groupsFixture("alpha", "dev-0"), groupsFixture("beta", "dev-0")...)},
				{
					groups:     append(groupsFixture("alpha", "dev-0"), groupsFixture("beta", "dev-0")...),
					unmeasured: []string{"alpha"},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			d := &Detector{detectors: tc.detectors}

			for i := range tc.expected {
				grpList, unmeasured := d.DetectAccelerator(context.Background())
				assert.Equal(t, tc.expected[i].groups, grpList, "round %d groups", i+1)
				assert.Equal(t, sets.New(tc.expected[i].unmeasured...), unmeasured,
					"round %d unmeasured", i+1)
			}
		})
	}
}
