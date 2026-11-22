package detector

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

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
	name    string
	detect  []scriptedPass
	monitor []scriptedPass

	detectCalls  atomic.Int64
	monitorCalls atomic.Int64
	detected     chan struct{}
	monitored    chan struct{}
}

// newScriptedDetector 构建的版本中，其公告被充分缓冲，因此没有任何消息等待读者，也没有任何消息在到达前丢
// 失。
func newScriptedDetector(name string, detect, monitor []scriptedPass) *scriptedDetector {
	return &scriptedDetector{
		name:      name,
		detect:    detect,
		monitor:   monitor,
		detected:  make(chan struct{}, 64),
		monitored: make(chan struct{}, 64),
	}
}

func (f *scriptedDetector) Name() string {
	return f.name
}

func (f *scriptedDetector) DetectAccelerator(bool) (device.DevicesGroupList, error) {
	p := scriptedAnswer(f.detect, &f.detectCalls)
	announce(f.detected)
	return p.groups, p.err
}

func (f *scriptedDetector) MonitorAccelerator(bool) (device.MetricsGroupList, error) {
	script := f.monitor
	if len(script) == 0 {
		script = f.detect
	}
	p := scriptedAnswer(script, &f.monitorCalls)
	announce(f.monitored)
	if p.err != nil {
		return nil, p.err
	}
	return metricsOf(p.groups), nil
}

// scriptedAnswer 返回本次调用应返回的条目。
func scriptedAnswer(script []scriptedPass, calls *atomic.Int64) scriptedPass {
	if len(script) == 0 {
		return scriptedPass{}
	}
	i := int(calls.Add(1)) - 1
	if i >= len(script) {
		i = len(script) - 1
	}
	return script[i]
}

// announce 记录显示有一次通过，且从未阻止了该通过：导致读取中断的情况在循环结束后即结束，而完整的通道并非
// 其失败原因。
func announce(ch chan struct{}) {
	if ch == nil {
		return
	}
	select {
	case ch <- struct{}{}:
	default:
	}
}

// awaitPasses 会等待某种类型的 n 次迭代开始，因此之后断言的情况取决于循环是否已完成，而非该断言等待了多长
// 时间。
func awaitPasses(t *testing.T, ch chan struct{}, n int, kind string) {
	t.Helper()

	for i := range n {
		select {
		case <-ch:
		case <-time.After(detectorTimeout):
			t.Fatalf("%d of %d %s passes ran", i, n, kind)
		}
	}
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

// metricsOf 是监控通道对同一硬件的视图，因此一个案例会一次性声明其设备，两个通道会就其中哪些设备存在达成
// 一致——这正是重新检测触发器所比较的内容。
func metricsOf(groups device.DevicesGroupList) device.MetricsGroupList {
	out := make(device.MetricsGroupList, 0, len(groups))
	for i := range groups {
		accelerators := make([]device.AcceleratorMetrics, 0, len(groups[i].Accelerators))
		for j := range groups[i].Accelerators {
			accelerators = append(accelerators, device.AcceleratorMetrics{
				ID: groups[i].Accelerators[j].ID,
			})
		}
		out = append(out, device.MetricsGroup{
			Manufacturer: groups[i].Manufacturer,
			Accelerators: accelerators,
		})
	}
	return out
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

// detectorTimeout 限制了所有等待操作，因此如果循环永远无法完成该案例所需的任务，就会导致该案例失败，而非
// 整个测试套件停滞。
const detectorTimeout = 10 * time.Second

// TestStartHoldsBackAFirstPassThatDetectedNothing 记录了 Device Manager 所要求的那项内容，而该厂商在从未
// 回应之前就完成了这一要求。其 DaemonSet 由该厂商的 PCI 厂商标签安排，因此硬件已存在，但软件尚未出现：该
// 轮操作被推迟并重复进行，而不是发布并报告一个没有加速器节点的结果。这也是 --no-fast-failed 所关闭的功能—
// —而代码声称要做的反而是退出操作。
func TestStartHoldsBackAFirstPassThatDetectedNothing(t *testing.T) {
	t.Setenv("KUBERNETES_NODE_NAME", "")
	const manufacturer = "alpha"

	testCases := []struct {
		name         string
		noFastFailed bool
		// `publishes` 表示空结果是否会传递给分配器。
		publishes bool
		// keepsDetecting 表示循环是否会再次进行检测，而不是仅仅监控其未发现的内容。
		keepsDetecting bool
	}{
		{
			name:           "a first pass that detected nothing is held back and repeated",
			noFastFailed:   false,
			publishes:      false,
			keepsDetecting: true,
		},
		{
			name:           "--no-fast-failed publishes it like any other result",
			noFastFailed:   true,
			publishes:      true,
			keepsDetecting: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			detector := newScriptedDetector(manufacturer, []scriptedPass{{}}, nil)
			publishedCh := make(chan sets.Set[string], 8)
			d := &Detector{
				manufacturers:           sets.New(manufacturer),
				noFastFailed:            tc.noFastFailed,
				detectors:               []device.Detector{detector},
				monitorPeriod:           10 * time.Millisecond,
				detectedManufacturersCh: publishedCh,
			}
			// 监控循环还会将宿主的网络接口与上次报告记录中的接口进行比较，而本次报告中并未记
			// 录任何对比结果：没有配置节点，因此在进行读取之前就会失败。在 Linux 系统中，如果
			// 读取操作成功，仅这一比较操作就会使循环重新开始检测。以宿主当前拥有的内容作为记
			// 录的基础，这样无论循环是否再次检测到，都只会显示加速器的结果。
			if interfaces, err := DetectInterfaces(); err == nil {
				d.reportedInterfaces = interfaces
				d.reportedInterfacesKnown = true
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go func() {
				_ = d.Start(ctx)
			}()

			if tc.keepsDetecting {
				// 第二次检测表明该轮次重复了，第一轮也结束了：这个循环是顺序进行的，因此
				// 第一轮将要发布的任何内容，在第二轮开始时就已经发布在频道上。
				awaitPasses(t, detector.detected, 2, "detect")
			} else {
				// 两个监控周期表明循环结束，系统开始关注那些未能被发现的状况，而非重新进
				// 行检测。
				awaitPasses(t, detector.detected, 1, "detect")
				awaitPasses(t, detector.monitored, 2, "monitor")
			}

			var published bool
			select {
			case set := <-publishedCh:
				published = true
				assert.Empty(t, set.UnsortedList(), "nothing was detected, so nothing can be published")
			default:
			}
			assert.Equal(t, tc.publishes, published)

			calls := detector.detectCalls.Load()
			if tc.keepsDetecting {
				assert.Greater(t, calls, int64(1),
					"the round must be repeated while the manufacturer has not answered")
			} else {
				assert.Equal(t, int64(1), calls,
					"a published result is monitored, not detected again")
			}
		})
	}
}

// TestStartKeepsAFailedManufacturerPublished 是相关后果，通过产生它的循环得以体现：在
// detectedManufacturersCh 上发布的集合，就是分配器所识别的对象。而其中缺失的制造商，其分配器将停止工作，
// 其套接字也将被回收。失败的识别过程不应导致这种情况，因此该制造商仍会被发布，而其识别过程持续失败。
func TestStartKeepsAFailedManufacturerPublished(t *testing.T) {
	t.Setenv("KUBERNETES_NODE_NAME", "")
	const manufacturer = "alpha"

	publishedCh := make(chan sets.Set[string], 8)
	d := &Detector{
		manufacturers: sets.New(manufacturer),
		detectors: []device.Detector{newScriptedDetector(manufacturer,
			// 检测过一次后，就永远无法成功了。
			[]scriptedPass{
				{groups: groupsFixture(manufacturer, "dev-0")},
				{err: errDetectorPass},
			},
			// 监控工作仍在继续，这又使得循环重新开始。
			[]scriptedPass{{groups: groupsFixture(manufacturer, "dev-0")}},
		)},
		monitorPeriod:           10 * time.Millisecond,
		detectedManufacturersCh: publishedCh,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		// 注意：如果没有 KUBERNETES_NODE_NAME 和回环客户端，报告设备将无法正常运行，而检测器会记
		// 录并容忍这种情况，因此该回环机制会持续运行于每个周期。
		_ = d.Start(ctx)
	}()

	// 第一轮会被检测。之后的每一轮都会失败。
	for round := 1; round <= 3; round++ {
		select {
		case published := <-publishedCh:
			assert.True(t, published.Has(manufacturer),
				"round %d must publish the manufacturer, got %v", round, published.UnsortedList())
		case <-time.After(detectorTimeout):
			t.Fatalf("round %d published nothing", round)
		}
	}
}
