package ascend

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	workercore "gpustack.ai/gpustack/api/worker/v1alpha1"
	"gpustack.ai/gpustack/pkg/deviceplugin"
	"gpustack.ai/gpustack/pkg/nodefeature"
)

const (
	testAccelID0 = "E0F4EE64 802061B1 6A691492 89528485 104301E3"
	testAccelID1 = "E281A66C 140C979 2CFBED72 A4500485 104301E3"
)

// redirectLogicalSliceDirs 将逻辑切片主机路径指向一个临时目录，用于测试。
func redirectLogicalSliceDirs(t *testing.T) {
	t.Helper()
	root := t.TempDir()
	origLib, origPods := deviceplugin.OperatorLibDir, deviceplugin.OperatorPodsDir
	deviceplugin.OperatorLibDir = filepath.Join(root, "lib")
	deviceplugin.OperatorPodsDir = filepath.Join(root, "pods")
	t.Cleanup(func() {
		deviceplugin.OperatorLibDir = origLib
		deviceplugin.OperatorPodsDir = origPods
	})
}

func ascendDevicesFixture() *workercore.Devices {
	return &workercore.Devices{
		Spec: workercore.DevicesSpec{
			Groups: []workercore.DevicesGroup{{
				ID:             "910b2",
				Manufacturer:   Manufacturer,
				Memory:         65536, // MiB
				RuntimeVersion: "8.5",
				Family:         "910B",
				// PhysicalIndexes是检测器的{物理ID、卡ID、卡中的设备ID}：物理ID用于标识加速器所属的可见性环境或配
				// 额配置，而卡/设备对则对应容器共享标志。这三个值以及逻辑索引被设定为不同的数值，因为检测器是从不
				// 同的计数器中获取这些值的——索引会跳过未能通过检测的加速器，PhysicalIndexes则使用dcmi自己的数值——
				// 因此如果它们出现冲突，会导致响应方读取错误的数值而通过检测。
				Accelerators: []workercore.Accelerator{
					{ID: testAccelID0, Index: 0, PhysicalIndexes: []uint32{3, 0, 0}},
					{ID: testAccelID1, Index: 1, PhysicalIndexes: []uint32{7, 1, 0}},
				},
			}},
		},
	}
}

// slicedPod构建了一个待处理的Pod，其容器请求了分离式计算与分配器所读取的VRAM参数：
// ".sliced.cores-percentage"（核心资源占比）和".sliced.memory-percentage"（每个加速器的VRAM内存占
// 比）。
func slicedPod(uid, ctrName string, coresPercent, memPercent int64) (*core.Pod, *core.Container) {
	coresRes := nodefeature.GetAcceleratableSlicedCoresPercentageResourceName(Manufacturer)
	memPctRes := nodefeature.GetAcceleratableSlicedMemoryPercentageResourceName(Manufacturer)
	pod := &core.Pod{
		ObjectMeta: meta.ObjectMeta{Name: ctrName + "-pod", UID: types.UID(uid)},
		Spec: core.PodSpec{
			Containers: []core.Container{{
				Name: ctrName,
				Resources: core.ResourceRequirements{
					Limits: core.ResourceList{
						coresRes:  *resource.NewQuantity(coresPercent, resource.DecimalSI),
						memPctRes: *resource.NewQuantity(memPercent, resource.DecimalSI),
					},
				},
			}},
		},
	}
	return pod, &pod.Spec.Containers[0]
}

// 注入参数测试只读取共享开关，显式预设为已开启。实际节点使用 DCMI 驱动。
type enabledShareDriver struct{}

func (enabledShareDriver) GetShareEnabled(_, _ int32) (bool, error) { return true, nil }
func (enabledShareDriver) SetShareEnabled(_, _ int32, _ bool) error { return nil }

func newSlicedServer() *server {
	return &server{ResourceServer: deviceplugin.ResourceServer{Manufacturer: Manufacturer, AllocationMode: workercore.DeviceAllocationModeSliced}, share: enabledShareDriver{}}
}

func TestGetSlicedContainerAllocateResponse(t *testing.T) {
	redirectLogicalSliceDirs(t)
	s := newSlicedServer()
	devs := ascendDevicesFixture()

	// 为逻辑序号 0 的卡申请 10% AI Core 和 25% 显存，分别验证两个预算。
	pod, ctr := slicedPod("pod-uid-1", "train", 10, 25)
	allocated := map[deviceplugin.Resource]int32{{Group: "910b2", Device: testAccelID0}: 1}

	resp, err := s.GetContainerAllocateResponse(context.Background(), pod, ctr, devs, allocated)
	require.NoError(t, err)

	// 该测试卡的驱动物理编号为 3，逻辑序号为 0。
	assert.Equal(t, "3", resp.Envs["ASCEND_VISIBLE_DEVICES"])
	assert.Equal(t, "1", resp.Envs["ENPU_LOG_LEVEL"]) // 默认降低 vcann-rt 日志级别。
	assert.Equal(t, "1", resp.Envs["ENPU_DSMI_HOOK"]) // 默认让 npu-smi 显示切片额度。

	// npu_info.config 应包含 10% 的算力和 floor(65,536 × 25%) = 16,384 MiB 的显存。
	podWorkDir := deviceplugin.PodWorkDir("pod-uid-1", "train")
	configPath := filepath.Join(podWorkDir, "etc/enpu/vcann-rt/npu_info.config")
	body, err := os.ReadFile(configPath)
	require.NoError(t, err)
	want := "physical-npu-id=3\n" +
		"virtual-npu-id=0\n" +
		"aicore-quota=10\n" +
		"memory-quota=16384\n" +
		"shm-id=E0F4EE64-802061B1-6A691492-89528485-104301E3\n" +
		"scheduling-policy=2\n"
	assert.Equal(t, want, string(body))

	// 配置文件权限为 0644。
	info, err := os.Stat(configPath)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o644), info.Mode().Perm())

	// 预加载文件、libvruntime、enpu-monitor 和配置均只读挂载，/dev/shm 可写。
	libDir := filepath.Join(deviceplugin.OperatorLibDir, "ascend")
	byContainerPath := make(map[string]*deviceplugin.Mount, len(resp.Mounts))
	for _, m := range resp.Mounts {
		byContainerPath[m.ContainerPath] = m
	}
	cases := []struct {
		ctrPath  string
		hostPath string
		readOnly bool
	}{
		{ctrLdPreloadPath, filepath.Join(libDir, "ld.so.preload"), true},
		{ctrVruntimePath, filepath.Join(libDir, "cann-8-910b/lib/libvruntime.so"), true},
		{ctrMonitorPath, filepath.Join(libDir, "cann-8-910b/tools/enpu-monitor"), true},
		{ctrConfigPath, configPath, true},
		{ctrDevShmPath, ctrDevShmPath, false},
	}
	for _, c := range cases {
		m, ok := byContainerPath[c.ctrPath]
		require.Truef(t, ok, "missing mount for %s", c.ctrPath)
		assert.Equalf(t, c.hostPath, m.HostPath, "host path for %s", c.ctrPath)
		assert.Equalf(t, c.readOnly, m.ReadOnly, "readOnly for %s", c.ctrPath)
	}
}

// 同一物理 NPU 上的两个并发槽位必须获得不同的虚拟-npu-ids；位于不同物理 NPU 上的槽位则从0开始。
func TestSlicedVirtualNPUIDAssignment(t *testing.T) {
	redirectLogicalSliceDirs(t)
	s := newSlicedServer()
	devs := ascendDevicesFixture()

	readVNPU := func(uid, ctr string) int {
		_, vnpu, ok := parseNPUInfoConfig(
			filepath.Join(deviceplugin.PodWorkDir(uid, ctr), "etc/enpu/vcann-rt/npu_info.config"),
		)
		require.True(t, ok)
		return vnpu
	}

	// 物理 NPU 3上的第一个槽位 3 -> vnpu 0。
	p1, c1 := slicedPod("uid-a", "train", 10, 25)
	_, err := s.GetContainerAllocateResponse(context.Background(), p1, c1, devs,
		map[deviceplugin.Resource]int32{{Group: "910b2", Device: testAccelID0}: 1})
	require.NoError(t, err)
	assert.Equal(t, 0, readVNPU("uid-a", "train"))

	// 同一物理 NPU 上的第二个槽位 3 -> vnpu 1（最低空闲的）。
	p2, c2 := slicedPod("uid-b", "train", 10, 25)
	_, err = s.GetContainerAllocateResponse(context.Background(), p2, c2, devs,
		map[deviceplugin.Resource]int32{{Group: "910b2", Device: testAccelID0}: 1})
	require.NoError(t, err)
	assert.Equal(t, 1, readVNPU("uid-b", "train"))

	// 在不同的物理 NPU 上分割（驱动索引 7）-> 再次变为 vnpu 0。
	p3, c3 := slicedPod("uid-c", "train", 10, 25)
	_, err = s.GetContainerAllocateResponse(context.Background(), p3, c3, devs,
		map[deviceplugin.Resource]int32{{Group: "910b2", Device: testAccelID1}: 1})
	require.NoError(t, err)
	assert.Equal(t, 0, readVNPU("uid-c", "train"))

	// 重新分配第一个容器是幂等的 -> 仍然为 vnpu 0。
	_, err = s.GetContainerAllocateResponse(context.Background(), p1, c1, devs,
		map[deviceplugin.Resource]int32{{Group: "910b2", Device: testAccelID0}: 1})
	require.NoError(t, err)
	assert.Equal(t, 0, readVNPU("uid-a", "train"))
}

// 没有内存维度（既无 .sliced.memory-percentage 也无 .sliced.memory-mib）的切片容器必须被拒绝，而不
// 是默默接受整个加速器。
func TestGetSlicedContainerAllocateResponse_NoMemoryRequest(t *testing.T) {
	redirectLogicalSliceDirs(t)
	s := newSlicedServer()
	devs := ascendDevicesFixture()

	pod := &core.Pod{
		ObjectMeta: meta.ObjectMeta{UID: types.UID("uid-x")},
		Spec:       core.PodSpec{Containers: []core.Container{{Name: "train"}}},
	}
	_, err := s.GetContainerAllocateResponse(context.Background(), pod, &pod.Spec.Containers[0], devs,
		map[deviceplugin.Resource]int32{{Group: "910b2", Device: testAccelID0}: 1})
	require.Error(t, err)
}

// vcann-rt 是单NPU：必须拒绝多加速器分片分配，而不是默默地将配额仅限制在第一个加速器上。
func TestGetSlicedContainerAllocateResponse_MultiCardRejected(t *testing.T) {
	redirectLogicalSliceDirs(t)
	s := newSlicedServer()
	devs := ascendDevicesFixture()

	pod, ctr := slicedPod("uid-multi", "train", 10, 25)
	_, err := s.GetContainerAllocateResponse(context.Background(), pod, ctr, devs,
		map[deviceplugin.Resource]int32{
			{Group: "910b2", Device: testAccelID0}: 1,
			{Group: "910b2", Device: testAccelID1}: 1,
		})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "single-NPU")
}
