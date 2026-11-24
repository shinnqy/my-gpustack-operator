package deviceplugin

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
)

// rdmaAllocateFixture 在临时目录中建立分配所需的 sysfs 映射和设备路径。
// NewRWDevice 检查路径是否存在，单测用普通文件验证这项契约，实际打开字符设备由 Linux 容器运行时完成。
type rdmaAllocateFixture struct {
	sysfs  *verbsFixture
	devDir string
}

func newRdmaAllocateFixture(t *testing.T) *rdmaAllocateFixture {
	t.Helper()

	f := &rdmaAllocateFixture{
		sysfs:  newVerbsFixture(t),
		devDir: filepath.Join(t.TempDir(), "dev", "infiniband"),
	}
	require.NoError(t, os.MkdirAll(f.devDir, 0o755))

	origSysfs, origDev := rdmaSysfsRoot, rdmaDevNodesDir
	rdmaSysfsRoot, rdmaDevNodesDir = f.sysfs.root, f.devDir
	t.Cleanup(func() { rdmaSysfsRoot, rdmaDevNodesDir = origSysfs, origDev })

	return f
}

// mapVerbs 在 sysfs 测试目录中建立 RDMA 设备名与一个 uverbs 条目的映射。
func (f *rdmaAllocateFixture) mapVerbs(name, entry string) {
	f.sysfs.addVerbsClassEntry(entry, name)
}

// writeNode 在测试设备目录中建立指定名字的节点路径，缺省的条目保持不存在。
func (f *rdmaAllocateFixture) writeNode(name string) {
	f.sysfs.t.Helper()
	require.NoError(f.sysfs.t, os.WriteFile(filepath.Join(f.devDir, name), nil, 0o600))
}

// nodePath 返回测试设备节点的完整路径。
func (f *rdmaAllocateFixture) nodePath(name string) string {
	return filepath.Join(f.devDir, name)
}

// deviceSpecsOf 按顺序取出每个设备的权限和宿主机路径，避免比较 protobuf 消息内部的编解码状态。
func deviceSpecsOf(ctrResp *ContainerAllocateResponse) []string {
	specs := make([]string, 0, len(ctrResp.Devices))
	for _, d := range ctrResp.Devices {
		specs = append(specs, d.Permissions+":"+d.HostPath)
	}
	return specs
}

// allocateRequest 构造 kubelet 针对单个容器提交的设备 ID 列表。
func allocateRequest(ids ...string) *AllocateRequest {
	return &AllocateRequest{ContainerRequests: []*ContainerAllocateRequest{{DevicesIds: ids}}}
}

// TestRDMAServer_Allocate 验证一个容器只获得获授端点的 verbs 设备和对应的 NCCL_IB_HCA。
// 节点共用的 rdma_cm 至多附带一次。
func TestRDMAServer_Allocate(t *testing.T) {
	const nodeName = "node-rdma-alloc"

	cases := []struct {
		name string
		mode workercore.DeviceAllocationMode
		// ifaces 是节点上报的接口库存。
		ifaces []workercore.DeviceInterface
		// ids 是 kubelet 授予的 token，按本例指定的顺序传入。
		ids []string
		// verbs 把 RDMA 设备名映射到 sysfs 中对应的 uverbs 条目。
		verbs map[string]string
		// nodes 是宿主机上实际存在的设备节点名。
		nodes []string
		// wantPaths 是按响应顺序排列的预期设备路径。
		wantPaths []string
		// wantHCA 是容器应收到的 NCCL_IB_HCA 值。
		wantHCA string
	}{
		{
			// 宿主机同时有另一个端点的设备文件，这样即便错误路径也存在，断言仍能发现把兄弟端点交给了容器。
			name: "one granted endpoint injects its own verbs device and not a sibling's",
			mode: workercore.DeviceAllocationModeShared,
			ifaces: []workercore.DeviceInterface{
				wholeFunctionIface("ib0", "0", "mlx5_0", nil),
				wholeFunctionIface("eth1", "1", "rxe0_eth1", nil),
			},
			ids:       []string{"ib0:ib0:0000"},
			verbs:     map[string]string{"mlx5_0": "uverbs1", "rxe0_eth1": "uverbs0"},
			nodes:     []string{"uverbs0", "uverbs1", "rdma_cm"},
			wantPaths: []string{"uverbs1", "rdma_cm"},
			wantHCA:   "mlx5_0",
		},
		{
			name: "two granted endpoints carry the connection manager once",
			mode: workercore.DeviceAllocationModeShared,
			ifaces: []workercore.DeviceInterface{
				wholeFunctionIface("ib0", "0", "mlx5_0", nil),
				wholeFunctionIface("eth1", "1", "rxe0_eth1", nil),
			},
			ids:       []string{"ib0:ib0:0000", "eth1:eth1:0000"},
			verbs:     map[string]string{"mlx5_0": "uverbs1", "rxe0_eth1": "uverbs0"},
			nodes:     []string{"uverbs0", "uverbs1", "rdma_cm"},
			wantPaths: []string{"uverbs0", "uverbs1", "rdma_cm"},
			wantHCA:   "rxe0_eth1,mlx5_0",
		},
		{
			// 同一端点的第二个共享 token 只增加获授份数，设备文件和 HCA 名称各出现一次即可。
			name: "two tokens of one endpoint inject it once",
			mode: workercore.DeviceAllocationModeShared,
			ifaces: []workercore.DeviceInterface{
				wholeFunctionIface("ib0", "0", "mlx5_0", nil),
			},
			ids:       []string{"ib0:ib0:0001", "ib0:ib0:0000"},
			verbs:     map[string]string{"mlx5_0": "uverbs1"},
			nodes:     []string{"uverbs1", "rdma_cm"},
			wantPaths: []string{"uverbs1", "rdma_cm"},
			wantHCA:   "mlx5_0",
		},
		{
			name: "a host with no connection manager is not an error",
			mode: workercore.DeviceAllocationModeShared,
			ifaces: []workercore.DeviceInterface{
				wholeFunctionIface("ib0", "0", "mlx5_0", nil),
			},
			ids:       []string{"ib0:ib0:0000"},
			verbs:     map[string]string{"mlx5_0": "uverbs1"},
			nodes:     []string{"uverbs1"},
			wantPaths: []string{"uverbs1"},
			wantHCA:   "mlx5_0",
		},
		{
			name: "an exclusive grant injects its endpoint",
			mode: workercore.DeviceAllocationModeExclusive,
			ifaces: []workercore.DeviceInterface{
				wholeFunctionIface("ib0", "0", "mlx5_0", nil),
			},
			ids:       []string{"ib0:ib0:0000"},
			verbs:     map[string]string{"mlx5_0": "uverbs1"},
			nodes:     []string{"uverbs1", "rdma_cm"},
			wantPaths: []string{"uverbs1", "rdma_cm"},
			wantHCA:   "mlx5_0",
		},
		{
			// 宿主机同时提供两个 VF。申请其中一个时，只能交付它自己的设备节点。
			name: "a partitioned grant injects its virtual function and not its sibling's",
			mode: workercore.DeviceAllocationModePartitioned,
			ifaces: []workercore.DeviceInterface{
				sriovPF("pf0", "0",
					rdmaVF("vf0", "", "mlx5_1", nil),
					rdmaVF("vf1", "", "mlx5_2", nil)),
			},
			ids:       []string{"pf0:vf0:0000"},
			verbs:     map[string]string{"mlx5_1": "uverbs2", "mlx5_2": "uverbs3"},
			nodes:     []string{"uverbs2", "uverbs3", "rdma_cm"},
			wantPaths: []string{"uverbs2", "rdma_cm"},
			wantHCA:   "mlx5_1",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newRdmaAllocateFixture(t)
			for name, entry := range c.verbs {
				f.mapVerbs(name, entry)
			}
			for _, node := range c.nodes {
				f.writeNode(node)
			}

			rec := &DevicesReconciler{NodeName: nodeName, Client: nodeFixture(rdmaDevices(nodeName, c.ifaces...))}
			resp, err := rdmaTestServer(t, c.mode, rec).Allocate(context.Background(), allocateRequest(c.ids...))
			require.NoError(t, err)
			require.Len(t, resp.ContainerResponses, 1)

			ctrResp := resp.ContainerResponses[0]
			want := make([]string, 0, len(c.wantPaths))
			for _, p := range c.wantPaths {
				want = append(want, "rw:"+f.nodePath(p))
			}
			assert.Equal(t, want, deviceSpecsOf(ctrResp))
			for _, spec := range ctrResp.Devices {
				assert.Equal(t, spec.HostPath, spec.ContainerPath)
			}
			assert.Equal(t, map[string]string{"NCCL_IB_HCA": c.wantHCA}, ctrResp.Envs)
			assert.Empty(t, ctrResp.Mounts)
			assert.Empty(t, ctrResp.Annotations)
			assert.Empty(t, ctrResp.CdiDevices)
		})
	}
}

// TestRDMAServer_Allocate_Refuses 检查错误 token、与分配模式不符的端点、无法解析的设备名和缺失的设备路径。
// 以上情况均须拒绝整个分配。
func TestRDMAServer_Allocate_Refuses(t *testing.T) {
	const nodeName = "node-rdma-refuse"

	cases := []struct {
		name   string
		mode   workercore.DeviceAllocationMode
		ifaces []workercore.DeviceInterface
		ids    []string
		verbs  map[string]string
		nodes  []string
		// wantErr 记录错误信息必须包含的各段文字。
		wantErr []string
	}{
		{
			name:    "an unparseable device id",
			mode:    workercore.DeviceAllocationModeShared,
			ifaces:  []workercore.DeviceInterface{wholeFunctionIface("ib0", "0", "mlx5_0", nil)},
			ids:     []string{"ib0"},
			verbs:   map[string]string{"mlx5_0": "uverbs1"},
			nodes:   []string{"uverbs1"},
			wantErr: []string{"invalid device id", `"ib0"`},
		},
		{
			name:   "a device id naming no endpoint of the inventory",
			mode:   workercore.DeviceAllocationModeShared,
			ifaces: []workercore.DeviceInterface{wholeFunctionIface("ib0", "0", "mlx5_0", nil)},
			ids:    []string{"ib9:ib9:0000"},
			verbs:  map[string]string{"mlx5_0": "uverbs1"},
			nodes:  []string{"uverbs1"},
			wantErr: []string{
				"names no RDMA endpoint",
				`"ib9:ib9:0000"`,
			},
		},
		{
			// 整端点模式不能交付已切分为 VF 的端点，收到对应 token 时应返回错误。
			name: "a token of a partitioned endpoint under a whole-function family",
			mode: workercore.DeviceAllocationModeShared,
			ifaces: []workercore.DeviceInterface{
				sriovPF("pf0", "0", rdmaVF("vf0", "", "mlx5_1", nil)),
			},
			ids:     []string{"pf0:vf0:0000"},
			verbs:   map[string]string{"mlx5_1": "uverbs2"},
			nodes:   []string{"uverbs2"},
			wantErr: []string{"names no RDMA endpoint", `"pf0:vf0:0000"`},
		},
		{
			// 两种布局都没有匹配项时，错误应说明检查过的位置，便于查找节点上的实际内核布局。
			name:   "a name resolving under neither sysfs layout fails naming both",
			mode:   workercore.DeviceAllocationModeShared,
			ifaces: []workercore.DeviceInterface{wholeFunctionIface("ib0", "0", "mlx5_gone", nil)},
			ids:    []string{"ib0:ib0:0000"},
			verbs:  map[string]string{"mlx5_0": "uverbs1"},
			nodes:  []string{"uverbs1"},
			wantErr: []string{
				"resolve the verbs character device",
				sysfsVerbsClassDir,
				sysfsVerbsClassName + `" directory under`,
				filepath.Join(sysfsInfinibandDir, "mlx5_gone"),
			},
		},
		{
			// 即使 sysfs 能解析出名称，宿主机也可能缺少相应设备路径。此时错误应指明哪个端点无法交付。
			name:   "a resolved endpoint with no device node on the host",
			mode:   workercore.DeviceAllocationModeShared,
			ifaces: []workercore.DeviceInterface{wholeFunctionIface("ib0", "0", "mlx5_0", nil)},
			ids:    []string{"ib0:ib0:0000"},
			verbs:  map[string]string{"mlx5_0": "uverbs1"},
			nodes:  []string{"uverbs0"},
			wantErr: []string{
				"has no verbs device node",
				"uverbs1",
			},
		},
		{
			name:    "an empty grant",
			mode:    workercore.DeviceAllocationModeShared,
			ifaces:  []workercore.DeviceInterface{wholeFunctionIface("ib0", "0", "mlx5_0", nil)},
			ids:     nil,
			verbs:   map[string]string{"mlx5_0": "uverbs1"},
			nodes:   []string{"uverbs1"},
			wantErr: []string{"grants no RDMA device"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newRdmaAllocateFixture(t)
			for name, entry := range c.verbs {
				f.mapVerbs(name, entry)
			}
			for _, node := range c.nodes {
				f.writeNode(node)
			}

			rec := &DevicesReconciler{NodeName: nodeName, Client: nodeFixture(rdmaDevices(nodeName, c.ifaces...))}
			resp, err := rdmaTestServer(t, c.mode, rec).Allocate(context.Background(), allocateRequest(c.ids...))
			require.Error(t, err)
			require.Nil(t, resp)
			for _, want := range c.wantErr {
				assert.Contains(t, err.Error(), want)
			}
		})
	}
}

// TestRDMAServer_Allocate_RefusesARequestWithNoContainer 检查空的 gRPC 请求体，
// 确保返回 InvalidArgument，服务进程继续运行。
func TestRDMAServer_Allocate_RefusesARequestWithNoContainer(t *testing.T) {
	const nodeName = "node-rdma-no-container"

	rec := &DevicesReconciler{
		NodeName: nodeName,
		Client:   nodeFixture(rdmaDevices(nodeName, wholeFunctionIface("ib0", "0", "mlx5_0", nil))),
	}

	resp, err := rdmaTestServer(t, workercore.DeviceAllocationModeExclusive, rec).
		Allocate(context.Background(), &AllocateRequest{})

	require.Error(t, err)
	assert.Nil(t, resp)
	assert.Contains(t, err.Error(), "no container requests")
	assert.Equal(t, grpccodes.InvalidArgument, grpcstatus.Code(err))
}

// TestRDMAServer_Allocate_ReadsCurrentInventory 验证分配总是使用当前的 Devices 记录，
// 网卡的设备绑定发生变化后，下一次响应应采用新的映射。
func TestRDMAServer_Allocate_ReadsCurrentInventory(t *testing.T) {
	const nodeName = "node-rdma-fresh"

	f := newRdmaAllocateFixture(t)
	f.mapVerbs("mlx5_0", "uverbs1")
	f.mapVerbs("mlx5_9", "uverbs4")
	f.writeNode("uverbs1")
	f.writeNode("uverbs4")
	f.writeNode("rdma_cm")

	devs := rdmaDevices(nodeName, wholeFunctionIface("ib0", "0", "mlx5_0", nil))
	rec := &DevicesReconciler{NodeName: nodeName, Client: nodeFixture(devs)}
	s := rdmaTestServer(t, workercore.DeviceAllocationModeShared, rec)

	first, err := s.Allocate(context.Background(), allocateRequest("ib0:ib0:0000"))
	require.NoError(t, err)
	assert.Equal(t, "mlx5_0", first.ContainerResponses[0].Envs["NCCL_IB_HCA"])

	// 探测器将接口重新绑定到另一个 RDMA 设备。随后用同一 token 请求，应看到更新后的设备名和节点路径。
	devs.Spec.Interfaces[0].RDMADevice = "mlx5_9"
	require.NoError(t, rec.Client.Update(context.Background(), devs))

	second, err := s.Allocate(context.Background(), allocateRequest("ib0:ib0:0000"))
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"NCCL_IB_HCA": "mlx5_9"}, second.ContainerResponses[0].Envs)
	assert.Equal(t,
		[]string{"rw:" + f.nodePath("uverbs4"), "rw:" + f.nodePath("rdma_cm")},
		deviceSpecsOf(second.ContainerResponses[0]))

	// PF 启用 VF 后只提供分区端点。此前发布的共享 token 即使还能找到旧设备文件，也不能再交付。
	devs.Spec.Interfaces[0] = sriovPF("ib0", "0", rdmaVF("vf0", "0", "mlx5_9", nil))
	require.NoError(t, rec.Client.Update(t.Context(), devs))
	removed, err := s.Allocate(t.Context(), allocateRequest("ib0:ib0:0000"))
	require.Nil(t, removed)
	require.Equal(t, grpccodes.InvalidArgument, grpcstatus.Code(err))
}

// TestRDMAServer_AdvertisedIDsAllocate 用每种模式实际发布的全部 ID 逐个调用 Allocate，
// 包括共享池的非零编号，检查发布端和分配端的 token 格式是否一致。
func TestRDMAServer_AdvertisedIDsAllocate(t *testing.T) {
	const nodeName = "node-rdma-roundtrip"

	for _, mode := range rdmaServedModes {
		t.Run(mode.String(), func(t *testing.T) {
			f := newRdmaAllocateFixture(t)
			f.mapVerbs("mlx5_0", "uverbs1")
			f.mapVerbs("mlx5_1", "uverbs2")
			f.writeNode("uverbs1")
			f.writeNode("uverbs2")
			f.writeNode("rdma_cm")

			// 清单同时有一个完整接口和一个已启用 VF 的 PF，使三种模式各自都有可分配端点。
			rec := &DevicesReconciler{NodeName: nodeName, Client: nodeFixture(rdmaDevices(nodeName,
				wholeFunctionIface("ib0", "0", "mlx5_0", nil),
				sriovPF("pf0", "0", rdmaVF("vf0", "0", "mlx5_1", nil)),
			))}
			s := rdmaTestServer(t, mode, rec)

			resp, err := s.getListAndWatchResponse(context.Background())
			require.NoError(t, err)
			require.NotEmpty(t, resp.Devices, "mode %s advertises nothing, so nothing is exercised", mode)

			for _, dev := range resp.Devices {
				token, err := ParseResourceToken(dev.ID)
				require.NoError(t, err, "the advertised ID %q does not parse", dev.ID)

				out, err := s.Allocate(context.Background(), allocateRequest(dev.ID))
				require.NoError(t, err, "the advertised ID %q does not allocate", dev.ID)
				require.Len(t, out.ContainerResponses, 1)
				assert.NotEmpty(t, out.ContainerResponses[0].Envs,
					"the grant for %q named no RDMA device", token.Resource)
			}
		})
	}
}
