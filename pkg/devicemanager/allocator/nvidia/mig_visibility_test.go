package nvidia

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/device"
	"gpustack.ai/gpustack/pkg/deviceplugin"
)

const (
	visPodUID  = "pod-vis"
	visOwner   = "main"
	visSidecar = "sshd"
	visProfile = "1g.10gb"
)

// seedOwnedPartition 将加速器置于工作负载 Allocate 遗留状态：即一个活跃的 MIG 实例以及拥有者容器所持
// 有的所有权标记文件。
func seedOwnedPartition(t *testing.T, drv *fakeMigDriver, card string, giID uint32, uuid string) {
	t.Helper()
	drv.seedLive(card, migInstance{
		GiID: giID, CiID: giID, ComputeSlices: 1, Placement: migPlacement{0, 2}, UUID: uuid,
	})
	writeOwnerMarker(t, card, migMarker{
		PodUID: visPodUID, Container: visOwner, Card: card, Profile: visProfile,
		GiID: giID, CiID: giID, MigUUID: uuid, ComputeSlices: 1, Start: 0, Length: 2,
	})
}

// writeOwnerMarker 会原样写入加速器操作对应的所有者容器标记文件，因此可能会出现与实际硬件设置相矛盾
// 的记录。
func writeOwnerMarker(t *testing.T, card string, m migMarker) {
	t.Helper()
	require.NoError(t, writeMarker(markerPath(visPodUID, visOwner, card), m))
}

// TestGetPhysicalSlicedVisibilityResponse 涉及 Sidecar 的分区解析：正常情况下，系统按开发者定义的顺
// 序列出所有分区，而每个记录都表明解析器无法证明该加速器上仍存在属于所有者的活跃分区，此时准入机制会
// 拒绝该请求——若回应使用父加速器，则会让 Sidecar 获得对其上所有分区的设备-cgroup 授权，包括其他租户
// 的分区也是如此。
func TestGetPhysicalSlicedVisibilityResponse(t *testing.T) {
	cases := []struct {
		name    string
		cards   []string
		setup   func(t *testing.T, drv *fakeMigDriver)
		want    string
		wantErr string
	}{
		{
			name:  "one accelerator names the owner's partition",
			cards: []string{testGPUUUID0},
			setup: func(t *testing.T, drv *fakeMigDriver) {
				seedOwnedPartition(t, drv, testGPUUUID0, 1, "MIG-one")
			},
			want: "MIG-one",
		},
		{
			// 以相反顺序进行播种，因此顺序只能来自开发者——即工作负载自身响应的顺序，而断言 sshd == main 的意义就
			// 在于此。
			name:  "two accelerators join in devs order, not marker-write order",
			cards: []string{testGPUUUID0, testGPUUUID1},
			setup: func(t *testing.T, drv *fakeMigDriver) {
				seedOwnedPartition(t, drv, testGPUUUID1, 2, "MIG-second")
				seedOwnedPartition(t, drv, testGPUUUID0, 1, "MIG-first")
			},
			want: "MIG-first,MIG-second",
		},
		{
			name:    "a missing marker fails closed",
			cards:   []string{testGPUUUID0},
			setup:   func(*testing.T, *fakeMigDriver) {},
			wantErr: "ownership marker",
		},
		{
			name:  "a malformed marker fails closed",
			cards: []string{testGPUUUID0},
			setup: func(t *testing.T, _ *fakeMigDriver) {
				path := markerPath(visPodUID, visOwner, testGPUUUID0)
				require.NoError(t, os.MkdirAll(deviceplugin.PodWorkDir(visPodUID, visOwner), 0o777))
				require.NoError(t, os.WriteFile(path, []byte("{not json"), 0o644))
			},
			wantErr: "ownership marker",
		},
		{
			name:  "an incomplete marker fails closed",
			cards: []string{testGPUUUID0},
			setup: func(t *testing.T, drv *fakeMigDriver) {
				drv.seedLive(testGPUUUID0, migInstance{GiID: 1, ComputeSlices: 1, Placement: migPlacement{0, 2}, UUID: "MIG-one"})
				// No MigUUID：这是 sidecar 获得授权的唯一字段。
				writeOwnerMarker(t, testGPUUUID0, migMarker{
					PodUID: visPodUID, Container: visOwner, Card: testGPUUUID0, Profile: visProfile,
					GiID: 1, ComputeSlices: 1, Start: 0, Length: 2,
				})
			},
			wantErr: "ownership marker",
		},
		{
			name:  "a marker recording another accelerator fails closed",
			cards: []string{testGPUUUID0},
			setup: func(t *testing.T, drv *fakeMigDriver) {
				drv.seedLive(testGPUUUID0, migInstance{GiID: 1, ComputeSlices: 1, Placement: migPlacement{0, 2}, UUID: "MIG-one"})
				writeOwnerMarker(t, testGPUUUID0, migMarker{
					PodUID: visPodUID, Container: visOwner, Card: testGPUUUID1, Profile: visProfile,
					GiID: 1, CiID: 1, MigUUID: "MIG-one", ComputeSlices: 1, Start: 0, Length: 2,
				})
			},
			wantErr: "records card",
		},
		{
			name:  "a marker whose gpu instance is gone fails closed",
			cards: []string{testGPUUUID0},
			setup: func(t *testing.T, _ *fakeMigDriver) {
				writeOwnerMarker(t, testGPUUUID0, migMarker{
					PodUID: visPodUID, Container: visOwner, Card: testGPUUUID0, Profile: visProfile,
					GiID: 7, CiID: 7, MigUUID: "MIG-destroyed", ComputeSlices: 1, Start: 0, Length: 2,
				})
			},
			wantErr: "missing gpu instance",
		},
		{
			// GPU实例ID存活于其命名的分区中：NVIDIA 可以将其重新分配给新创建的实例，该实例可能属于其他租户。
			name:  "a reused gpu-instance id fails closed",
			cards: []string{testGPUUUID0},
			setup: func(t *testing.T, drv *fakeMigDriver) {
				drv.seedLive(testGPUUUID0, migInstance{GiID: 1, ComputeSlices: 1, Placement: migPlacement{0, 2}, UUID: "MIG-somebody-else"})
				writeOwnerMarker(t, testGPUUUID0, migMarker{
					PodUID: visPodUID, Container: visOwner, Card: testGPUUUID0, Profile: visProfile,
					GiID: 1, CiID: 1, MigUUID: "MIG-ours", ComputeSlices: 1, Start: 0, Length: 2,
				})
			},
			wantErr: "id reused",
		},
		{
			name:  "a profile the accelerator no longer offers fails closed",
			cards: []string{testGPUUUID0},
			setup: func(t *testing.T, drv *fakeMigDriver) {
				drv.seedLive(testGPUUUID0, migInstance{GiID: 1, ComputeSlices: 1, Placement: migPlacement{0, 2}, UUID: "MIG-one"})
				writeOwnerMarker(t, testGPUUUID0, migMarker{
					PodUID: visPodUID, Container: visOwner, Card: testGPUUUID0, Profile: "7g.80gb",
					GiID: 1, CiID: 1, MigUUID: "MIG-one", ComputeSlices: 7, Start: 0, Length: 8,
				})
			},
			wantErr: "no longer offers",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			redirectLogicalSliceDirs(t)
			drv := newFakeMigDriver()
			for _, card := range c.cards {
				drv.possible[card] = evenSlots()
			}
			c.setup(t, drv)

			s := &server{mig: drv}
			devs := migDevices(visProfile, 1, 2, c.cards...)
			resp, err := s.GetPhysicalSlicedVisibilityResponse(
				context.Background(), visibilityPod(), &core.Container{Name: visSidecar},
				devs, allocatedCardSet(devs, c.cards), visOwner,
			)

			if c.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), c.wantErr)
				assert.Nil(t, resp, "a rejected visibility resolution must return no response at all")
				return
			}
			require.NoError(t, err)
			require.NotNil(t, resp)
			assert.Equal(t, map[string]string{"NVIDIA_VISIBLE_DEVICES": c.want}, resp.Envs,
				"the sidecar is told the partitions, and nothing else")
		})
	}
}

// TestGetPhysicalSlicedVisibilityResponse_NoDriver 指出，在不提供分区服务的节点上构建的可见性服务器
// 会拒绝请求，而不会由父加速器进行响应。
func TestGetPhysicalSlicedVisibilityResponse_NoDriver(t *testing.T) {
	redirectLogicalSliceDirs(t)
	s := &server{}
	devs := migDevices(visProfile, 1, 2, testGPUUUID0)

	resp, err := s.GetPhysicalSlicedVisibilityResponse(
		context.Background(), visibilityPod(), &core.Container{Name: visSidecar},
		devs, allocatedCardSet(devs, []string{testGPUUUID0}), visOwner,
	)
	require.Error(t, err)
	assert.Nil(t, resp)
}

// TestNew_VisibilityHoldsTheMigDriver 记录了哪些服务器被分配给 MIG 驱动程序：可见性服务器需要此机制
// 来证明侧车分区处于运行状态，而不提供分区服务的节点则完全不需要构建此类机制（真正的驱动程序会采用
// NVML 初始化方式）。关于两个服务器所持有的同一实例并未被提及：darwin 存根是一个零尺寸值类型，因此二
// 者无法区分。
func TestNew_VisibilityHoldsTheMigDriver(t *testing.T) {
	assert.Equal(t,
		map[workercore.DeviceAllocationMode]bool{
			workercore.DeviceAllocationModeExclusive:   false,
			workercore.DeviceAllocationModeShared:      false,
			workercore.DeviceAllocationModePartitioned: true,
			workercore.DeviceAllocationModeVisibility:  true,
		},
		migDriverByMode(t, device.AllocatorOptions{NoSliced: true}),
		"the partition-addressing servers hold the driver, the rest hold none")

	assert.Equal(t,
		map[workercore.DeviceAllocationMode]bool{
			workercore.DeviceAllocationModeExclusive:  false,
			workercore.DeviceAllocationModeShared:     false,
			workercore.DeviceAllocationModeSliced:     false,
			workercore.DeviceAllocationModeVisibility: false,
		},
		migDriverByMode(t, device.AllocatorOptions{NoPartitioned: true}),
		"a node that serves no partitioning initializes no MIG driver")
}

// migDriverByMode 报告了每个注册服务器是否持有 MIG 驱动程序。
func migDriverByMode(t *testing.T, opts device.AllocatorOptions) map[workercore.DeviceAllocationMode]bool {
	t.Helper()
	agg, ok := New(opts).(*aggregated)
	require.True(t, ok)
	held := make(map[workercore.DeviceAllocationMode]bool, len(agg.servers))
	for i := range agg.servers {
		srv, ok := agg.servers[i].(*server)
		require.True(t, ok)
		held[srv.AllocationMode] = srv.mig != nil
	}
	return held
}

// visibilityPod 是 sidecar 的 pod：只有其 UID 才重要，因为标记就是由它来控制的。
func visibilityPod() *core.Pod {
	return &core.Pod{ObjectMeta: meta.ObjectMeta{Name: "p", UID: types.UID(visPodUID)}}
}

// allocatedCardSet 是所有者的加速器映射，由服务器负责解析器。
func allocatedCardSet(devs *workercore.Devices, cards []string) map[deviceplugin.Resource]int32 {
	allocated := make(map[deviceplugin.Resource]int32, len(cards))
	for _, card := range cards {
		allocated[resourceForAccelerator(devs, card)] = 1
	}
	return allocated
}
