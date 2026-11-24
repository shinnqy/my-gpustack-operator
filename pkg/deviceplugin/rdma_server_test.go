package deviceplugin

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	klog "k8s.io/klog/v2"
	deviceplugin "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/nodefeature"
)

// rdmaServedModes 列出服务器实际注册的三种分配模式，供测试验证各模式发布的 ID。
var rdmaServedModes = []workercore.DeviceAllocationMode{
	workercore.DeviceAllocationModeExclusive,
	workercore.DeviceAllocationModeShared,
	workercore.DeviceAllocationModePartitioned,
}

// rdmaDevices 用给定接口构造指定节点的 Devices 记录。
func rdmaDevices(nodeName string, ifaces ...workercore.DeviceInterface) *workercore.Devices {
	return &workercore.Devices{
		ObjectMeta: meta.ObjectMeta{Name: nodeName},
		Spec:       workercore.DevicesSpec{Interfaces: ifaces},
	}
}

// wholeFunctionIface 构造完整网卡端点，提供 NUMA、绑定设备名和链路判定。
func wholeFunctionIface(name, numa, rdmaDevice string, link *workercore.DeviceInterfaceLink) workercore.DeviceInterface {
	return workercore.DeviceInterface{
		Name:         name,
		NumaAffinity: numa,
		RDMA:         rdmaDevice != "",
		RDMADevice:   rdmaDevice,
		Link:         link,
	}
}

// sriovPF 构造一个带 NUMA 信息和自身 RDMA 绑定的 SR-IOV PF，并在它下面登记指定的 VF。
func sriovPF(name, numa string, vfs ...workercore.DeviceInterfaceVirtualFunction) workercore.DeviceInterface {
	return workercore.DeviceInterface{
		Name:             name,
		NumaAffinity:     numa,
		SRIOV:            true,
		RDMA:             true,
		RDMADevice:       "mlx5_bond",
		VirtualFunctions: vfs,
	}
}

// rdmaVF 构造带有独立 NUMA 信息、RDMA 设备名和链路判定的 VF。
func rdmaVF(name, numa, rdmaDevice string, link *workercore.DeviceInterfaceLink) workercore.DeviceInterfaceVirtualFunction {
	return workercore.DeviceInterfaceVirtualFunction{
		Name:         name,
		NumaAffinity: numa,
		RDMA:         rdmaDevice != "",
		RDMADevice:   rdmaDevice,
		Link:         link,
	}
}

// rdmaTestServer 使用生产构造函数创建指定模式的 RDMA 服务，以该节点的协调器读取库存。
func rdmaTestServer(t *testing.T, mode workercore.DeviceAllocationMode, rec *DevicesReconciler) *rdmaServer {
	t.Helper()

	s, err := NewRDMAServer(klog.Background(), mode, rec)
	require.NoError(t, err)
	rs, ok := s.(*rdmaServer)
	require.True(t, ok, "NewRDMAServer must return the RDMA server behind the Server interface")

	return rs
}

// numaIDs 从 kubelet 收到的拓扑提示中取出 NUMA 节点编号。
func numaIDs(t *testing.T, topology *deviceplugin.TopologyInfo) []int64 {
	t.Helper()

	require.NotNil(t, topology)
	ids := make([]int64, 0, len(topology.Nodes))
	for _, n := range topology.Nodes {
		ids = append(ids, n.ID)
	}
	return ids
}

// TestRDMAServer_ListAndWatch_LinkGate 验证四种链路观测结果。
// 只有 failed 被标记为 Unhealthy，各种情况下 token 数和 ID 都应保留，
// 避免影响 kubelet 已记录的分配。
func TestRDMAServer_ListAndWatch_LinkGate(t *testing.T) {
	const nodeName = "node-rdma-link"

	cases := []struct {
		name       string
		link       *workercore.DeviceInterfaceLink
		wantHealth string
	}{
		{
			name:       "a verified link leaves tokens healthy",
			link:       &workercore.DeviceInterfaceLink{State: workercore.DeviceInterfaceLinkStateOK},
			wantHealth: deviceplugin.Healthy,
		},
		{
			// unverified 表示节点暂未能核实端口状态，已绑定设备的 token 仍可发布为 Healthy。
			name:       "an unverified link leaves tokens healthy",
			link:       &workercore.DeviceInterfaceLink{State: workercore.DeviceInterfaceLinkStateUnverified},
			wantHealth: deviceplugin.Healthy,
		},
		{
			// 没有链路记录的端点已经绑定了 RDMA 设备。缺少额外的链路判定时，应采用与未核实相同的发布行为。
			name:       "no link record leaves tokens healthy",
			link:       nil,
			wantHealth: deviceplugin.Healthy,
		},
		{
			name:       "a failed link marks tokens unhealthy and keeps them advertised",
			link:       &workercore.DeviceInterfaceLink{State: workercore.DeviceInterfaceLinkStateFailed},
			wantHealth: deviceplugin.Unhealthy,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec := &DevicesReconciler{NodeName: nodeName, Client: nodeFixture(rdmaDevices(nodeName,
				wholeFunctionIface("ib0", "", "mlx5_0", c.link)))}

			wantCounts := map[workercore.DeviceAllocationMode]int{
				workercore.DeviceAllocationModeExclusive: 1,
				workercore.DeviceAllocationModeShared:    nodefeature.RDMAEndpointPoolSize,
			}
			for mode, wantCount := range wantCounts {
				resp, err := rdmaTestServer(t, mode, rec).getListAndWatchResponse(context.Background())
				require.NoError(t, err)
				require.Len(t, resp.Devices, wantCount,
					"mode %s keeps the full token count whatever the verdict", mode)
				for _, d := range resp.Devices {
					assert.Equal(t, c.wantHealth, d.Health)
				}
			}
		})
	}
}

// TestRDMAServer_ListAndWatch_Topology 验证每个 token 的 NUMA 来源。
// VF 优先采用自己的读数，必要时沿用 PF 的值。两者均未知时省略提示，避免让 kubelet 误用 NUMA 0。
func TestRDMAServer_ListAndWatch_Topology(t *testing.T) {
	const nodeName = "node-rdma-numa"

	sharedOver := func(t *testing.T, ifaces ...workercore.DeviceInterface) []*deviceplugin.Device {
		t.Helper()

		rec := &DevicesReconciler{NodeName: nodeName, Client: nodeFixture(rdmaDevices(nodeName, ifaces...))}
		resp, err := rdmaTestServer(t, workercore.DeviceAllocationModeShared, rec).
			getListAndWatchResponse(context.Background())
		require.NoError(t, err)
		return resp.Devices
	}

	t.Run("an affinity names its node on every token", func(t *testing.T) {
		for _, d := range sharedOver(t, wholeFunctionIface("ib0", "1", "mlx5_0", nil)) {
			assert.Equal(t, []int64{1}, numaIDs(t, d.Topology))
		}
	})

	t.Run("an empty affinity attaches no hint rather than node 0", func(t *testing.T) {
		for _, d := range sharedOver(t, wholeFunctionIface("ib0", "", "mlx5_0", nil)) {
			assert.Nil(t, d.Topology)
		}
	})

	t.Run("two interfaces carry their own nodes", func(t *testing.T) {
		devices := sharedOver(t,
			wholeFunctionIface("ib0", "0", "mlx5_0", nil),
			wholeFunctionIface("eth1", "1", "rxe0_eth1", nil))
		require.Len(t, devices, 2*nodefeature.RDMAEndpointPoolSize)
		for _, d := range devices {
			want := int64(1)
			if strings.HasPrefix(d.ID, "ib0:") {
				want = 0
			}
			assert.Equal(t, []int64{want}, numaIDs(t, d.Topology))
		}
	})

	t.Run("a virtual function falls back to its parent's affinity", func(t *testing.T) {
		rec := &DevicesReconciler{NodeName: nodeName, Client: nodeFixture(rdmaDevices(nodeName,
			sriovPF("pf0", "0", rdmaVF("vf0", "", "mlx5_1", nil))))}
		resp, err := rdmaTestServer(t, workercore.DeviceAllocationModePartitioned, rec).
			getListAndWatchResponse(context.Background())
		require.NoError(t, err)
		require.Len(t, resp.Devices, 1)
		assert.Equal(t, []int64{0}, numaIDs(t, resp.Devices[0].Topology))
	})
}
